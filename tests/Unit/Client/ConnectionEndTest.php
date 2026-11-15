<?php

declare(strict_types=1);

use Psr\EventDispatcher\EventDispatcherInterface;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Events\PacketSent;
use ScienceStories\Mqtt\Events\ServerDisconnect;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Util\Bytes;
use ScienceStories\Mqtt\Util\RandomId;

/** A dispatcher that hands each event to $listener. */
function listeningDispatcher(Closure $listener): EventDispatcherInterface
{
    return new readonly class ($listener) implements EventDispatcherInterface {
        public function __construct(private Closure $listener)
        {
        }

        public function dispatch(object $event): object
        {
            ($this->listener)($event);

            return $event;
        }
    };
}

/**
 * Topics of the PUBLISH packets a transport has seen written, in order.
 * @return string[]
 */
function sentTopics(InMemoryTransport $transport): array
{
    $topics = [];
    foreach ($transport->sentPackets() as $packet) {
        if ($packet['type'] === PacketType::PUBLISH->value) {
            $offset   = 0;
            $topics[] = Bytes::decodeString($packet['body'], $offset);
        }
    }

    return $topics;
}

describe('draining the offline queue', function (): void {
    it('drops a message that can never be encoded and delivers the rest', function (): void {
        // Retrying it on every connection blocked every message behind it for good.
        $transport = new InMemoryTransport();
        $client    = new Client(new Options(host: 'fake.broker', clientId: 'drain', version: MqttVersion::V5_0)->withOfflineQueue(10), $transport);

        $client->publish('a', '1', new PublishOptions(properties: ['content_type' => str_repeat('x', 70_000)]));
        $client->publish('b', '2');
        $client->publish('c', '3');

        $transport->feedConnAck(properties: "\x00");
        $client->connect();

        expect(sentTopics($transport))->toBe(['b', 'c'])
            ->and(new ReflectionProperty(Client::class, 'offlineQueue')->getValue($client)->count())->toBe(0);
    });

    it('keeps a message that never reached the wire, at the head, for the next connection', function (): void {
        $inner     = new InMemoryTransport();
        $transport = new class ($inner) implements TransportInterface {
            public int $writesBeforeFailure = PHP_INT_MAX;

            public function __construct(private readonly InMemoryTransport $inner)
            {
            }

            public function open(string $host, int $port, float $timeoutSec = 5.0): void
            {
                $this->inner->open($host, $port, $timeoutSec);
            }

            public function write(string $bytes): int
            {
                if ($this->writesBeforeFailure-- <= 0) {
                    throw new TransportError('Connection reset');
                }

                return $this->inner->write($bytes);
            }

            public function readExact(int $length, ?float $timeoutSec = null): string
            {
                return $this->inner->readExact($length, $timeoutSec);
            }

            public function close(): void
            {
                $this->inner->close();
            }

            public function isOpen(): bool
            {
                return $this->inner->isOpen();
            }

            public function enableTls(?array $tlsOptions = null): void
            {
                $this->inner->enableTls($tlsOptions);
            }
        };
        $client = new Client(new Options(host: 'fake.broker', clientId: 'drain')->withOfflineQueue(10), $transport);
        $client->publish('first', '1');
        $client->publish('second', '2');

        // CONNECT goes out, then the connection fails before the first queued PUBLISH does.
        $inner->feedConnAck();
        $transport->writesBeforeFailure = 1;
        $client->connect();
        expect(sentTopics($inner))->toBe([]);

        $transport->writesBeforeFailure = PHP_INT_MAX;
        $inner->feedConnAck();
        $client->connect();

        expect(sentTopics($inner))->toBe(['first', 'second']);
    });
});

describe('stopping', function (): void {
    it('honours a stop() requested before connect()', function (): void {
        // connect() cleared it, so a supervisor's SIGTERM between connections was lost.
        $transport = new InMemoryTransport();
        $transport->feedConnAck();
        $client = new Client(new Options(host: 'fake.broker', clientId: 'stop'), $transport);

        $client->stop();
        $client->connect();
        $transport->feedPublish('t', 'x');

        $handled = 0;
        $client->run(static function () use (&$handled, $client): void {
            $handled++;
            $client->stop();
        }, 0.0);

        expect($handled)->toBe(0);
    });
});

describe('when the client closes the connection itself', function (): void {
    it('puts nothing on the wire after its DISCONNECT, even if a listener publishes', function (): void {
        // The event was dispatched before the transport closed, so a listener's publish
        // followed the client's own DISCONNECT (MQTT-3.14.4-1).
        $transport = new InMemoryTransport();
        $transport->feedConnAck(properties: "\x00");
        $client   = null;
        $seen     = [];
        $listener = static function (object $event) use (&$client, &$seen): void {
            if ($event instanceof ServerDisconnect) {
                $seen[] = $event;
                try {
                    $client?->publish('late', 'x');
                } catch (TransportError) {
                    // expected: the connection is already closed
                }
            }
        };
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'listener', version: MqttVersion::V5_0)->withTopicAliasMaximum(2),
            $transport,
            events: listeningDispatcher($listener),
        );
        $client->connect();
        $transport->takeWritten();

        $transport->feedPublish('t', 'x', properties: Bytes::encodeVarInt(3)."\x23".pack('n', 9));
        expect(fn (): bool => $client->loopOnce(0.0))->toThrow(ProtocolError::class);

        expect($transport->sentPacketTypes())->toBe([PacketType::DISCONNECT->value])
            ->and($seen)->toHaveCount(1)
            ->and($seen[0]->disconnect->properties['reason_string'] ?? '')->toStartWith('Closed by the client');
    });

    it('says so on the event when the client gives up on a missing PINGRESP', function (): void {
        $transport = new InMemoryTransport();
        $transport->feedConnAck();
        $seen   = [];
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'ping', keepAlive: 2)->withPingResponseTimeout(0.02),
            $transport,
            events: listeningDispatcher(static function (object $e) use (&$seen): void {
                $seen[] = $e;
            }),
        );
        $client->connect();
        new ReflectionProperty(Client::class, 'lastSent')->setValue($client, hrtime(true) / 1e9 - 10.0);
        $client->loopOnce(0.0);
        usleep(40_000);
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        $disconnects = array_values(array_filter($seen, static fn (object $e): bool => $e instanceof ServerDisconnect));
        expect($disconnects)->toHaveCount(1)
            ->and($disconnects[0]->disconnect->properties['reason_string'] ?? '')->toStartWith('Closed by the client');
    });

    it('lets a publish waiting for a flow-control slot complete after the reconnect', function (): void {
        // Resetting connection state on the way out freed the slot under the waiting
        // publish(), which then wrote to the closed transport and lost the message.
        RandomId::resetPacketId();
        $transport = new InMemoryTransport();
        $transport->feedConnAck(properties: "\x03\x21\x00\x01");                 // Receive Maximum 1
        $options = new Options(host: 'fake.broker', clientId: 'flow', version: MqttVersion::V5_0)
            ->withTopicAliasMaximum(2)->withAckTimeout(0.05)->withMaxResendAttempts(0)
            ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0);
        // Acknowledge the second PUBLISH when the client actually sends it, as a broker
        // would; fed in advance, the reconnect's own read would consume it first.
        $publishes = 0;
        $client    = new Client($options, $transport, events: listeningDispatcher(
            static function (object $event) use ($transport, &$publishes): void {
                if ($event instanceof PacketSent && $event->packetType === PacketType::PUBLISH->value && ++$publishes === 2) {
                    $transport->feedPubAck(2);
                }
            },
        ));
        $client->connect();

        // The first message is never acknowledged, so it holds the only slot.
        expect(fn (): int => $client->publish('a', '1', new PublishOptions(qos: QoS::AtLeastOnce)))
            ->toThrow(Timeout::class);

        // The broker then breaks the alias rules; the client reconnects and the waiting
        // publish goes out on the new connection.
        $transport->feedPublish('t', 'x', properties: Bytes::encodeVarInt(3)."\x23".pack('n', 9))
            ->feedConnAck(properties: "\x00");

        expect($client->publish('b', '2', new PublishOptions(qos: QoS::AtLeastOnce)))->toBe(2)
            ->and($transport->isOpen())->toBeTrue();
    });
});
