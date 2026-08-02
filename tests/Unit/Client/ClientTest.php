<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\InboundMessage;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Exception\AuthenticationError;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Exception\ServerError;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Util\RandomId;

/**
 * @return array{0: Client, 1: InMemoryTransport}
 */
function connectedClient(?Options $options = null, int $reasonCode = 0): array
{
    $transport = new InMemoryTransport();
    $options ??= new Options(host: 'fake.broker', port: 1883, clientId: 'unit-test');

    $transport->feedConnAck($reasonCode);
    $client = new Client($options, $transport);
    $client->connect();
    $transport->takeWritten();

    return [$client, $transport];
}

function drain(Client $client): void
{
    $guard = 0;
    while ($client->loopOnce(0.0)) {
        if (++$guard > 10_000) {
            break;
        }
    }
}

describe('inbound QoS 1 de-duplication', function (): void {
    it('does not drop unrelated messages once the cache has evicted entries', function (): void {
        $options = new Options(host: 'fake.broker', clientId: 'dedup')
            ->withQos1DeduplicationSize(16);
        [$client, $transport] = connectedClient($options);

        $received = [];
        $client->onMessage(function (InboundMessage $m) use (&$received): void {
            $received[] = $m->payload;
        });

        // 20 distinct high packet identifiers overflow the 16-entry cache.
        for ($pid = 500; $pid < 520; $pid++) {
            $transport->feedPublish('a/b', "id-$pid", QoS::AtLeastOnce, $pid);
        }
        // A genuinely new message on a low identifier. Brokers recycle identifiers
        // as soon as they are acknowledged, so this is ordinary traffic.
        $transport->feedPublish('a/b', 'FRESH', QoS::AtLeastOnce, 3);

        drain($client);

        expect($received)->toContain('FRESH')
            ->and($received)->toHaveCount(21);
    });

    it('suppresses only re-deliveries flagged DUP', function (): void {
        [$client, $transport] = connectedClient();

        $received = [];
        $client->onMessage(function (InboundMessage $m) use (&$received): void {
            $received[] = $m->payload;
        });

        $transport->feedPublish('a/b', 'first', QoS::AtLeastOnce, 7);
        $transport->feedPublish('a/b', 'redelivery', QoS::AtLeastOnce, 7, dup: true);
        // Same identifier, but a brand new message: DUP is clear.
        $transport->feedPublish('a/b', 'reused-id', QoS::AtLeastOnce, 7);

        drain($client);

        expect($received)->toBe(['first', 'reused-id']);
    });

    it('acknowledges every inbound QoS 1 PUBLISH including suppressed duplicates', function (): void {
        [$client, $transport] = connectedClient();

        $transport->feedPublish('a/b', 'first', QoS::AtLeastOnce, 7);
        $transport->feedPublish('a/b', 'redelivery', QoS::AtLeastOnce, 7, dup: true);
        drain($client);

        expect($transport->countSent(PacketType::PUBACK))->toBe(2);
    });
});

describe('acknowledgement registers', function (): void {
    it('survives a second subscribe when another packet interleaves', function (): void {
        RandomId::resetPacketId();
        [$client, $transport] = connectedClient();

        $transport->feedSubAck(1, [0]);
        $client->subscribeWith([['filter' => 'a/#', 'qos' => 0]]);

        // A retained PUBLISH lands before the second SUBACK.
        $transport->feedPublish('a/x', 'interleaved');
        $transport->feedSubAck(2, [0]);

        $result = $client->subscribeWith([['filter' => 'b/#', 'qos' => 0]]);

        expect($result->packetId)->toBe(2);
    });

    it('survives a second unsubscribe when another packet interleaves', function (): void {
        RandomId::resetPacketId();
        [$client, $transport] = connectedClient();

        $transport->feedUnsubAck(1);
        $client->unsubscribe(['a/#']);

        $transport->feedPublish('a/x', 'interleaved');
        $transport->feedUnsubAck(2);

        $client->unsubscribe(['b/#']);

        expect($transport->countSent(PacketType::UNSUBSCRIBE))->toBe(2);
    });
});

describe('inbound delivery', function (): void {
    it('bounds the queue instead of growing it without limit under a push handler', function (): void {
        $options = new Options(host: 'fake.broker', clientId: 'push')
            ->withInboundQueueSize(10);
        [$client, $transport] = connectedClient($options);

        $handled = 0;
        $client->onMessage(function () use (&$handled): void {
            $handled++;
        });

        for ($i = 0; $i < 500; $i++) {
            $transport->feedPublish('sensors/t', "v$i");
        }
        drain($client);

        $queue = new ReflectionProperty(Client::class, 'inbound')->getValue($client);

        expect($handled)->toBe(500)
            ->and($queue)->toHaveCount(10);
    });

    it('feeds the pull queue even when a push handler is registered', function (): void {
        [$client, $transport] = connectedClient();

        $handled = 0;
        $client->onMessage(function () use (&$handled): void {
            $handled++;
        });

        $transport->feedPublish('sensors/t', 'both');
        drain($client);

        $message = $client->awaitMessage(0.0);

        // The two channels are documented as independent, and Client\RequestResponse
        // drains the pull queue while an application handler is registered.
        expect($handled)->toBe(1)
            ->and($message)->toBeInstanceOf(InboundMessage::class)
            ->and($message?->payload)->toBe('both');
    });

    it('still queues messages for awaitMessage() when no handler is registered', function (): void {
        [$client, $transport] = connectedClient();

        $transport->feedPublish('sensors/t', 'queued');
        drain($client);

        $message = $client->awaitMessage(0.0);

        expect($message)->toBeInstanceOf(InboundMessage::class)
            ->and($message?->payload)->toBe('queued');
    });

    it('drops the oldest message when a bounded inbound queue overflows', function (): void {
        $options = new Options(host: 'fake.broker', clientId: 'bounded')
            ->withInboundQueueSize(3);
        [$client, $transport] = connectedClient($options);

        foreach (['a', 'b', 'c', 'd'] as $payload) {
            $transport->feedPublish('t', $payload);
        }
        drain($client);

        $payloads = [];
        while (($m = $client->awaitMessage(0.0)) instanceof InboundMessage) {
            $payloads[] = $m->payload;
        }

        expect($payloads)->toBe(['b', 'c', 'd']);
    });
});

describe('inbound QoS 2 across a reconnect', function (): void {
    it('keeps the stored message when the broker resumes the session', function (): void {
        $options              = new Options(host: 'fake.broker', clientId: 'qos2', cleanSession: false);
        [$client, $transport] = connectedClient($options);

        $received = [];
        $client->onMessage(function (InboundMessage $m) use (&$received): void {
            $received[] = $m->payload;
        });

        // PUBLISH arrives and we answer PUBREC; the connection then drops before PUBREL.
        $transport->feedPublish('a/b', 'exactly-once', QoS::ExactlyOnce, 42);
        drain($client);
        expect($transport->countSent(PacketType::PUBREC))->toBe(1);

        // On reconnect the broker reports the session is still present and, per §4.3.3,
        // replays only the PUBREL — never the PUBLISH. MQTT-4.1.0-1 makes the stored
        // message Session state, so it must have survived.
        $transport->feedConnAck(0, sessionPresent: true);
        $client->connect();
        $transport->feedPubRel(42);
        drain($client);

        expect($received)->toBe(['exactly-once']);
    });

    it('discards the stored message when the broker reports a fresh session', function (): void {
        $options              = new Options(host: 'fake.broker', clientId: 'qos2-clean', cleanSession: false);
        [$client, $transport] = connectedClient($options);

        $received = [];
        $client->onMessage(function (InboundMessage $m) use (&$received): void {
            $received[] = $m->payload;
        });

        $transport->feedPublish('a/b', 'orphan', QoS::ExactlyOnce, 42);
        drain($client);

        $transport->feedConnAck(0, sessionPresent: false);
        $client->connect();
        $transport->feedPubRel(42);
        drain($client);

        expect($received)->toBe([]);
    });
});

describe('CONNACK handling', function (): void {
    it('throws AuthenticationError when 3.1.1 refuses with "not authorized"', function (): void {
        $transport = new InMemoryTransport();
        $transport->feedConnAck(5);
        $client = new Client(new Options(host: 'fake.broker'), $transport);

        expect(fn (): mixed => $client->connect())->toThrow(AuthenticationError::class);
    });

    it('throws ServerError when 3.1.1 refuses with "server unavailable"', function (): void {
        $transport = new InMemoryTransport();
        $transport->feedConnAck(3);
        $client = new Client(new Options(host: 'fake.broker'), $transport);

        expect(fn (): mixed => $client->connect())->toThrow(ServerError::class);
    });

    it('throws AuthenticationError when MQTT 5 refuses with 0x87', function (): void {
        $transport = new InMemoryTransport();
        $transport->feedConnAck(0x87, properties: "\x00");
        $client = new Client(
            new Options(host: 'fake.broker', version: MqttVersion::V5_0),
            $transport,
        );

        expect(fn (): mixed => $client->connect())->toThrow(AuthenticationError::class);
    });

    it('closes the transport when the broker refuses the connection', function (): void {
        $transport = new InMemoryTransport();
        $transport->feedConnAck(5);
        $client = new Client(new Options(host: 'fake.broker'), $transport);

        try {
            $client->connect();
        } catch (AuthenticationError) {
            // expected
        }

        expect($transport->isOpen())->toBeFalse();
    });

    it('still returns a ConnectResult on success', function (): void {
        [$client] = connectedClient();

        expect($client)->toBeInstanceOf(Client::class);
    });
});

describe('maximum packet size', function (): void {
    it('rejects an oversized CONNACK before allocating', function (): void {
        $transport = new InMemoryTransport();
        // CONNACK claiming a 268,435,455 byte remaining length.
        $transport->feed("\x20\xFF\xFF\xFF\x7F");
        $client = new Client(new Options(host: 'fake.broker'), $transport);

        expect(fn (): mixed => $client->connect())->toThrow(ProtocolError::class);
    });

    it('rejects an oversized PUBLISH before allocating, without breaking loopOnce()', function (): void {
        [$client, $transport] = connectedClient();

        $transport->feed("\x30\xFF\xFF\xFF\x7F");

        // loopOnce() is documented to return bool. A rejected packet reports "nothing
        // processed" and closes the desynchronised transport so reconnect can take over.
        expect($client->loopOnce(0.0))->toBeFalse()
            ->and($transport->isOpen())->toBeFalse();
    });

    it('honours a configured maximum packet size', function (): void {
        $options              = new Options(host: 'fake.broker')->withMaximumPacketSize(64);
        [$client, $transport] = connectedClient($options);

        $transport->feedPublish('t', str_repeat('x', 200));

        expect($client->loopOnce(0.0))->toBeFalse()
            ->and($transport->isOpen())->toBeFalse();
    });

    it('rejects a zero maximum packet size at construction', function (): void {
        expect(fn (): Options => new Options(host: 'fake.broker', maximumPacketSize: 0))
            ->toThrow(InvalidArgumentException::class);
    });

    it('rejects an out-of-range keep alive at construction', function (): void {
        expect(fn (): Options => new Options(host: 'fake.broker', keepAlive: 86400))
            ->toThrow(InvalidArgumentException::class);
    });

    it('accepts packets within the configured limit', function (): void {
        $options              = new Options(host: 'fake.broker')->withMaximumPacketSize(4096);
        [$client, $transport] = connectedClient($options);

        $transport->feedPublish('t', str_repeat('x', 1000));

        expect($client->loopOnce(0.0))->toBeTrue();
    });

    it('advertises Maximum Packet Size in an MQTT 5 CONNECT', function (): void {
        $transport = new InMemoryTransport();
        $transport->feedConnAck(0, properties: "\x00");
        $options = new Options(host: 'fake.broker', version: MqttVersion::V5_0)
            ->withMaximumPacketSize(65536);

        new Client($options, $transport)->connect();

        $connect = $transport->sentPackets()[0];

        expect($connect['type'])->toBe(PacketType::CONNECT->value)
            ->and($connect['body'])->toContain("\x27" . pack('N', 65536));
    });
});
