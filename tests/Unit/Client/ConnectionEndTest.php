<?php

declare(strict_types=1);

use Psr\EventDispatcher\EventDispatcherInterface;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Events\PacketSent;
use ScienceStories\Mqtt\Events\ServerDisconnect;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Tests\Support\InterceptingTransport;
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
        $transport = new InterceptingTransport($inner);
        $client    = new Client(new Options(host: 'fake.broker', clientId: 'drain')->withOfflineQueue(10), $transport);
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
        expect(fn (): bool => $client->loopOnce(0.0))->toThrow(ProtocolError::class)
            ->and($transport->sentPacketTypes())->toBe([PacketType::DISCONNECT->value])
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

/**
 * An MQTT 5 client with auto-reconnect (3 attempts, no backoff), connected over $transport,
 * recording every ServerDisconnect and handing it to $onDisconnect as well.
 *
 * @param  (Closure(ServerDisconnect): void)|null  $onDisconnect  called after the event is recorded
 * @return array{0: Client, 1: ArrayObject<int, ServerDisconnect>}
 */
function reconnectingClient(InMemoryTransport $transport, ?Closure $onDisconnect = null): array
{
    $events = new ArrayObject();
    $transport->feedConnAck(properties: "\x00");
    $client = new Client(
        new Options(host: 'fake.broker', clientId: 'dropped', version: MqttVersion::V5_0)
            ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
        $transport,
        events: listeningDispatcher(static function (object $event) use ($events, $onDisconnect): void {
            if ($event instanceof ServerDisconnect) {
                $events->append($event);
                if ($onDisconnect instanceof Closure) {
                    $onDisconnect($event);
                }
            }
        }),
    );
    $client->connect();

    return [$client, $events];
}

/** The connection drops; the broker answers the next CONNECT at once. */
function dropAndReconnect(Client $client, InMemoryTransport $transport): void
{
    $transport->close();
    $transport->feedConnAck(properties: "\x00");
    $client->loopOnce(0.0);
}

describe('a DISCONNECT from the broker', function (): void {
    it('closes the transport before listeners hear of it', function (): void {
        // Dispatched first, a listener's publish went out on a connection the broker was
        // already tearing down.
        $transport     = new InMemoryTransport();
        $openWhenHeard = [];
        [$client]      = reconnectingClient($transport, static function () use ($transport, &$openWhenHeard): void {
            $openWhenHeard[] = $transport->isOpen();
        });
        $transport->feedDisconnect(0x8B);                                   // Server shutting down
        $client->loopOnce(0.0);

        expect($openWhenHeard)->toBe([false]);
    });

    it('reconnects after an error, as the event says', function (): void {
        $transport         = new InMemoryTransport();
        [$client, $events] = reconnectingClient($transport);
        $transport->feedDisconnect(0x8B);                                   // Server shutting down
        $client->loopOnce(0.0);
        $transport->feedConnAck(properties: "\x00");
        $client->loopOnce(0.0);

        expect($events)->toHaveCount(1)
            ->and($events[0]->willReconnect)->toBeTrue()
            ->and($transport->countSent(PacketType::CONNECT))->toBe(2)
            ->and($transport->isOpen())->toBeTrue();
    });

    it('does not reconnect when the event says it will not', function (int $reasonCode): void {
        // The event said "no reconnect" for a normal disconnection while the next loopOnce()
        // reconnected anyway; and a session taken over by another connection with the same
        // Client Identifier was taken back, throwing that client off in turn, forever.
        $transport         = new InMemoryTransport();
        [$client, $events] = reconnectingClient($transport);
        $transport->feedDisconnect($reasonCode);
        $client->loopOnce(0.0);
        $transport->feedConnAck(properties: "\x00");
        $client->loopOnce(0.0);

        expect($events)->toHaveCount(1)
            ->and($events[0]->willReconnect)->toBeFalse()
            ->and($transport->countSent(PacketType::CONNECT))->toBe(1)
            ->and($transport->isOpen())->toBeFalse();
    })->with(['normal disconnection' => [0x00], 'session taken over' => [0x8E]]);

    it('lets a publish waiting for a flow-control slot complete after the reconnect', function (): void {
        // Resetting connection state on the way out freed the slot under the waiting
        // publish(), which then wrote to the closed transport and lost the message.
        RandomId::resetPacketId();
        $transport = new InMemoryTransport();
        $transport->feedConnAck(properties: "\x03\x21\x00\x01");                 // Receive Maximum 1
        $options = new Options(host: 'fake.broker', clientId: 'flow', version: MqttVersion::V5_0)
            ->withAckTimeout(0.05)->withMaxResendAttempts(0)
            ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0);
        $publishes = 0;
        $client    = new Client($options, $transport, events: listeningDispatcher(
            static function (object $event) use ($transport, &$publishes): void {
                if ($event instanceof PacketSent && $event->packetType === PacketType::PUBLISH->value && ++$publishes === 2) {
                    $transport->feedPubAck(2);
                }
            },
        ));
        $client->connect();
        expect(fn (): int => $client->publish('a', '1', new PublishOptions(qos: QoS::AtLeastOnce)))
            ->toThrow(Timeout::class);

        $transport->feedDisconnect(0x8B)->feedConnAck(properties: "\x00");

        expect($client->publish('b', '2', new PublishOptions(qos: QoS::AtLeastOnce)))->toBe(2)
            ->and($transport->isOpen())->toBeTrue();
    });
});

describe('a missing PINGRESP', function (): void {
    it('lets a publish waiting for a flow-control slot complete after the reconnect', function (): void {
        // As for a DISCONNECT: the slot was freed under the waiting publish(), which then
        // wrote to the transport the expiry had just closed.
        RandomId::resetPacketId();
        $inner     = new InMemoryTransport();
        $transport = new InterceptingTransport($inner);
        $inner->feedConnAck(properties: "\x03\x21\x00\x01");                     // Receive Maximum 1
        $options = new Options(host: 'fake.broker', clientId: 'ping-flow', keepAlive: 2, version: MqttVersion::V5_0)
            ->withPingResponseTimeout(0.02)->withAckTimeout(0.05)->withMaxResendAttempts(0)
            ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0);
        $client = new Client($options, $transport);
        $client->connect();
        expect(fn (): int => $client->publish('a', '1', new PublishOptions(qos: QoS::AtLeastOnce)))
            ->toThrow(Timeout::class);

        // Keep Alive is due, so the wait for a slot sends PINGREQ — which nothing answers. The
        // broker answers only the reconnect, and then the waiting publish.
        new ReflectionProperty(Client::class, 'lastSent')->setValue($client, hrtime(true) / 1e9 - 10.0);
        $transport->onWrite = static function (int $type) use ($inner): void {
            if ($type === PacketType::CONNECT->value) {
                $inner->feedConnAck(properties: "\x00");
            } elseif ($type === PacketType::PUBLISH->value) {
                $inner->feedPubAck(2);
            }
        };

        expect($client->publish('b', '2', new PublishOptions(qos: QoS::AtLeastOnce)))->toBe(2)
            ->and($inner->countSent(PacketType::PINGREQ))->toBe(1)
            ->and($inner->countSent(PacketType::CONNECT))->toBe(2);
    });
});

describe('auto-reconnect against a broker that drops every new connection', function (): void {
    it('gives up after the configured attempts instead of retrying forever', function (): void {
        // The count reset on every CONNACK, so a broker that accepted each connection and
        // dropped it straight away — a ban applied after the handshake — was retried forever
        // at the base delay.
        $transport = new InMemoryTransport();
        [$client]  = reconnectingClient($transport);                          // 3 attempts
        for ($i = 0; $i < 6; $i++) {
            dropAndReconnect($client, $transport);
        }

        expect($transport->countSent(PacketType::CONNECT))->toBe(1 + 3);
    });

    it('starts counting again once a connection has stayed up', function (): void {
        $transport   = new InMemoryTransport();
        [$client]    = reconnectingClient($transport);
        $connectedAt = new ReflectionProperty(Client::class, 'connectedAt');
        for ($i = 0; $i < 6; $i++) {
            // Up for a minute — well past the ten seconds that count as lasting — before it drops.
            $connectedAt->setValue($client, hrtime(true) / 1e9 - 60.0);
            dropAndReconnect($client, $transport);
        }

        expect($transport->countSent(PacketType::CONNECT))->toBe(1 + 6);
    });

    it('starts over when the application connects again', function (): void {
        $transport = new InMemoryTransport();
        [$client]  = reconnectingClient($transport);
        for ($i = 0; $i < 4; $i++) {
            dropAndReconnect($client, $transport);                            // the 4th is refused
        }

        $client->connect();                                                   // reads the CONNACK left over
        dropAndReconnect($client, $transport);

        expect($transport->countSent(PacketType::CONNECT))->toBe(1 + 3 + 1 + 1)
            ->and($transport->isOpen())->toBeTrue();
    });

    it('says on the event that it will not reconnect once its attempts are used up', function (): void {
        // The event promised a reconnect that the next loopOnce() then refused to make.
        $transport         = new InMemoryTransport();
        [$client, $events] = reconnectingClient($transport);                // 3 attempts
        for ($i = 0; $i < 4; $i++) {
            $transport->feedDisconnect(0x89);                                // Server busy, straight away
            $client->loopOnce(0.0);
            $transport->feedConnAck(properties: "\x00");
            $client->loopOnce(0.0);
        }

        expect(array_map(static fn (ServerDisconnect $e): bool => $e->willReconnect, $events->getArrayCopy()))
            ->toBe([true, true, true, false])
            ->and($transport->countSent(PacketType::CONNECT))->toBe(1 + 3);
    });

    it('does not count the wait for a resubscribe the broker cut off as a connection that lasted', function (): void {
        // The broker accepted each reconnect and closed it on the SUBSCRIBE (as AWS IoT does
        // for a filter its policy forbids). The SUBACK wait ran its full five seconds on the
        // closed transport and then counted as connection lifetime, so it never gave up.
        RandomId::resetPacketId();
        $inner     = new InMemoryTransport();
        $transport = new InterceptingTransport($inner);
        $inner->feedConnAck();
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'cut-off')->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
            $transport,
        );
        $client->connect();
        $inner->feedPacket(PacketType::SUBACK, 0, pack('n', 1)."\x00");
        $client->subscribe(['forbidden/#']);

        $inner->close();
        $transport->onWrite = static function (int $type) use ($inner): void {
            if ($type === PacketType::SUBSCRIBE->value) {
                $inner->close();
            }
        };
        $started = hrtime(true);
        for ($i = 0; $i < 6; $i++) {
            $inner->feedConnAck();
            $client->loopOnce(0.0);
        }

        expect($inner->countSent(PacketType::CONNECT))->toBe(1 + 3)
            ->and((hrtime(true) - $started) / 1e9)->toBeLessThan(2.0);
    });
});

describe('once nothing will reconnect', function (): void {
    it('awaitMessage() returns instead of spinning until its timeout', function (): void {
        // Every loopOnce() failed at once on the closed transport, so the wait burned a core
        // for its whole timeout — and forever without one.
        $transport = new InMemoryTransport();
        [$client]  = reconnectingClient($transport);
        $transport->feedDisconnect(0x8E);                                   // session taken over
        $client->loopOnce(0.0);

        $started = hrtime(true);
        expect($client->awaitMessage(2.0))->toBeNull()
            ->and((hrtime(true) - $started) / 1e9)->toBeLessThan(0.5);
    });

    it('messages() still yields what arrived before the connection ended', function (): void {
        // The loop checked the stop flags before the queue, so a message the client had
        // already acknowledged was never handed over.
        $transport = new InMemoryTransport();
        [$client]  = reconnectingClient($transport);
        $transport->feedPublish('t', 'first', QoS::AtLeastOnce, 1, properties: "\x00");
        $seen = [];
        foreach ($client->messages(0.05) as $message) {
            $seen[] = $message->payload;
            if ($message->payload === 'first') {
                // While the application handles it, another arrives, then the session is taken over.
                $transport->feedPublish('t', 'second', QoS::AtLeastOnce, 2, properties: "\x00")->feedDisconnect(0x8E);
                $client->loopOnce(0.0);
                $client->loopOnce(0.0);
            }
        }

        expect($seen)->toBe(['first', 'second']);
    });

    it('run() and messages() stop when a connection without auto-reconnect drops', function (): void {
        // They polled the dead socket for as long as the process lived.
        $transport = new InMemoryTransport();
        $transport->feedConnAck();
        $client = new Client(new Options(host: 'fake.broker', clientId: 'no-reconnect'), $transport);
        $client->connect();
        $transport->closeByPeer();
        $client->loopOnce(0.0);

        expect(new ReflectionProperty(Client::class, 'shouldStop')->getValue($client))->toBeTrue()
            ->and($client->awaitMessage(2.0))->toBeNull();
    });

    it('disconnect() stops auto-reconnect until connect()', function (): void {
        // The next loopOnce() — run()'s own, or a handler's — reconnected straight away.
        $transport = new InMemoryTransport();
        [$client]  = reconnectingClient($transport);
        $client->disconnect();
        $transport->feedConnAck(properties: "\x00");
        $client->loopOnce(0.0);

        expect($transport->countSent(PacketType::CONNECT))->toBe(1);

        $client->connect();
        $transport->close();
        $transport->feedConnAck(properties: "\x00");
        $client->loopOnce(0.0);

        expect($transport->countSent(PacketType::CONNECT))->toBe(3);
    });
});

describe('a ServerDisconnect listener that reconnects itself', function (): void {
    it('is not undone when the client gave up on a missing PINGRESP', function (): void {
        $transport = new InMemoryTransport();
        $transport->feedConnAck();
        $client = null;
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'own-reconnect', keepAlive: 2)->withPingResponseTimeout(0.02),
            $transport,
            events: listeningDispatcher(static function (object $event) use (&$client, $transport): void {
                if ($event instanceof ServerDisconnect) {
                    $transport->feedConnAck();
                    $client?->connect();
                }
            }),
        );
        $client->connect();
        new ReflectionProperty(Client::class, 'lastSent')->setValue($client, hrtime(true) / 1e9 - 10.0);
        $client->loopOnce(0.0);                                              // PINGREQ
        usleep(40_000);
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);                                              // expired; the listener reconnects

        expect($transport->countSent(PacketType::CONNECT))->toBe(2)
            ->and(new ReflectionProperty(Client::class, 'shouldStop')->getValue($client))->toBeFalse();
    });

    it('is not undone when the broker broke the protocol', function (): void {
        $transport = new InMemoryTransport();
        $transport->feedConnAck(properties: "\x00");
        $client = null;
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'own-reconnect', version: MqttVersion::V5_0)->withTopicAliasMaximum(2),
            $transport,
            events: listeningDispatcher(static function (object $event) use (&$client, $transport): void {
                if ($event instanceof ServerDisconnect) {
                    $transport->feedConnAck(properties: "\x00");
                    $client?->connect();
                }
            }),
        );
        $client->connect();
        $transport->feedPublish('t', 'x', properties: Bytes::encodeVarInt(3)."\x23".pack('n', 9));   // alias above the maximum

        // The violation is still reported; the connection the listener made is kept.
        expect(fn (): bool => $client->loopOnce(0.0))->toThrow(ProtocolError::class)
            ->and($transport->isOpen())->toBeTrue()
            ->and(new ReflectionProperty(Client::class, 'shouldStop')->getValue($client))->toBeFalse();
    });
});

describe('a publish() waiting when the connection ends for good', function (): void {
    it('fails at once with TransportError, not a Timeout at the end of every resend period', function (string $how): void {
        // It waited out the whole budget — 20 s with the defaults — before reporting a
        // Timeout for a connection nothing was ever going to reopen.
        $inner     = new InMemoryTransport();
        $transport = new InterceptingTransport($inner);
        $inner->feedConnAck(properties: "\x00");
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'ended', version: MqttVersion::V5_0)
                ->withAckTimeout(0.25)->withMaxResendAttempts(3)
                ->withAutoReconnect($how === 'normal DISCONNECT', baseDelay: 0.0, jitter: 0.0),
            $transport,
        );
        $client->connect();
        $transport->onWrite = static function (int $type) use ($inner, $how): void {
            if ($type === PacketType::PUBLISH->value) {
                $how === 'normal DISCONNECT' ? $inner->feedDisconnect(0x00) : $inner->closeByPeer();
            }
        };

        $started = hrtime(true);
        expect(fn (): int => $client->publish('t', 'x', new PublishOptions(qos: QoS::AtLeastOnce)))->toThrow(TransportError::class)
            ->and((hrtime(true) - $started) / 1e9)->toBeLessThan(0.2);
    })->with(['dropped, no auto-reconnect' => ['dropped'], 'normal DISCONNECT' => ['normal DISCONNECT']]);
});
