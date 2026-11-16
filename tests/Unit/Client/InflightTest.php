<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Session\FileSessionStore;
use ScienceStories\Mqtt\Session\SessionState;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Tests\Support\InterceptingTransport;

/**
 * A connected client with auto-reconnect (no backoff) over an intercepting transport, with
 * nothing written yet. Acknowledgements time out after 100 ms, with no extra waits, unless
 * $configure says otherwise.
 *
 * @param  MqttVersion  $version  the protocol version the client speaks
 * @param  (Closure(Options): Options)|null  $configure  adjusts the options before connecting
 * @return array{0: Client, 1: InterceptingTransport, 2: InMemoryTransport}
 */
function inflightClient(MqttVersion $version = MqttVersion::V3_1_1, ?Closure $configure = null): array
{
    $inner     = new InMemoryTransport();
    $transport = new InterceptingTransport($inner);
    $inner->feedConnAck(properties: $version === MqttVersion::V5_0 ? "\x00" : '');
    $options = new Options(host: 'fake.broker', clientId: 'inflight', version: $version)
        ->withAckTimeout(0.1)->withMaxResendAttempts(0)
        ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0);
    $client = new Client($configure instanceof Closure ? $configure($options) : $options, $transport);
    $client->connect();
    $inner->takeWritten();

    return [$client, $transport, $inner];
}

/**
 * Packets written after the last CONNECT, as [type, flags] pairs.
 *
 * @return list<array{0: int, 1: int}>
 */
function writtenAfterReconnect(InMemoryTransport $transport): array
{
    $packets = $transport->sentPackets();
    $last    = -1;
    foreach ($packets as $i => $packet) {
        if ($packet['type'] === PacketType::CONNECT->value) {
            $last = $i;
        }
    }

    return array_map(static fn (array $p): array => [$p['type'], $p['flags']], array_slice($packets, $last + 1));
}

function atLeastOnce(): PublishOptions
{
    return new PublishOptions(qos: QoS::AtLeastOnce);
}

function exactlyOnceDelivery(): PublishOptions
{
    return new PublishOptions(qos: QoS::ExactlyOnce);
}

describe('a reconnect', function (): void {
    it('resends what is still in flight first, in order: the PUBLISH with DUP, then the PUBREL', function (): void {
        // A waiting publish() resent only when its own timer expired — a full ackTimeout
        // after the reconnect — and a message nobody waited for any more was never resent.
        [$client, $transport, $inner] = inflightClient();
        expect(fn (): int => $client->publish('a', '1', atLeastOnce()))->toThrow(Timeout::class);   // packet 1, abandoned

        $pubRels            = 0;
        $transport->onWrite = static function (int $type) use ($inner, &$pubRels): void {
            if ($type === PacketType::PUBLISH->value && $inner->countSent(PacketType::PUBLISH) === 2) {
                $inner->feedPubRec(2);                                                         // 'b' received
            } elseif ($type === PacketType::PUBREL->value && ++$pubRels === 1) {
                $inner->close();                                                               // dropped right after
                $inner->feedConnAck(sessionPresent: true);
            } elseif ($type === PacketType::PUBREL->value) {
                $inner->feedPubComp(2);
            }
        };

        expect($client->publish('b', '2', exactlyOnceDelivery()))->toBe(2)
            ->and(writtenAfterReconnect($inner))->toBe([
                [PacketType::PUBLISH->value, 0x08 | 0x02],                                     // 'a' again: DUP, QoS 1
                [PacketType::PUBREL->value, 0x02],                                             // 'b': only the PUBREL
            ]);
    });

    it('resends PUBRELs in the order their PUBRECs arrived (MQTT-4.6.0-4)', function (): void {
        [$client, , $inner] = inflightClient();
        expect(fn (): int => $client->publish('a', '1', exactlyOnceDelivery()))->toThrow(Timeout::class);   // 1: no PUBREC yet
        $inner->feedPubRec(2);
        expect(fn (): int => $client->publish('b', '2', exactlyOnceDelivery()))->toThrow(Timeout::class);   // 2: received first
        $inner->feedPubRec(1);                                                                              // 1: received second
        $client->loopOnce(0.0);

        $inner->close();
        $inner->feedConnAck(sessionPresent: true);
        $client->loopOnce(0.0);

        $pubRels = array_values(array_filter(
            array_slice($inner->sentPackets(), -2),
            static fn (array $p): bool => $p['type'] === PacketType::PUBREL->value,
        ));
        expect(array_map(static fn (array $p): string => $p['body'], $pubRels))->toBe([pack('n', 2), pack('n', 1)]);
    });

    it('resends at once, not an ackTimeout later', function (): void {
        [$client, $transport, $inner] = inflightClient(configure: static fn (Options $o): Options => $o->withAckTimeout(2.0)->withMaxResendAttempts(1));
        $published                    = 0;
        $transport->onWrite           = static function (int $type) use ($inner, &$published): void {
            if ($type !== PacketType::PUBLISH->value) {
                return;
            }
            if (++$published === 1) {
                $inner->close();                                                               // lost with the connection
                $inner->feedConnAck(sessionPresent: true);
            } else {
                $inner->feedPubAck(1);
            }
        };

        $started = hrtime(true);
        expect($client->publish('a', '1', atLeastOnce()))->toBe(1)
            ->and((hrtime(true) - $started) / 1e9)->toBeLessThan(1.0);
    });

    it('publishes anew what a publish() still waits for when the session is gone, and drops the rest', function (): void {
        [$client, $transport, $inner] = inflightClient();
        expect(fn (): int => $client->publish('a', '1', atLeastOnce()))->toThrow(Timeout::class);   // abandoned

        $published          = 0;
        $transport->onWrite = static function (int $type) use ($inner, &$published): void {
            if ($type !== PacketType::PUBLISH->value) {
                return;
            }
            if (++$published === 1) {                                                          // 'b', first time
                $inner->close();
                $inner->feedConnAck();                                              // no session: the broker kept nothing
            } else {
                $inner->feedPubAck(2);
            }
        };

        expect($client->publish('b', '2', atLeastOnce()))->toBe(2)
            ->and(writtenAfterReconnect($inner))->toBe([[PacketType::PUBLISH->value, 0x02]]);  // 'b' only, no DUP
    });

    it('is not followed by a second resend of the same message when the wait expires', function (): void {
        // The reconnect has just resent it; the timer resending it again only doubles it.
        [$client, $transport, $inner] = inflightClient(configure: static fn (Options $o): Options => $o->withMaxResendAttempts(1));
        $published                    = 0;
        $transport->onWrite           = static function (int $type) use ($inner, &$published): void {
            if ($type === PacketType::PUBLISH->value && ++$published === 1) {
                $inner->close();
                $inner->feedConnAck(sessionPresent: true);
            }
        };

        expect(fn (): int => $client->publish('a', '1', atLeastOnce()))->toThrow(Timeout::class, 'after 1 resend(s)')
            ->and(writtenAfterReconnect($inner))->toBe([[PacketType::PUBLISH->value, 0x08 | 0x02]]);
    });

    it('does not resend a message the broker acknowledged after publish() gave up', function (): void {
        [$client, , $inner] = inflightClient();
        expect(fn (): int => $client->publish('a', '1', atLeastOnce()))->toThrow(Timeout::class);
        $inner->feedPubAck(1);
        $client->loopOnce(0.0);

        $inner->close();
        $inner->feedConnAck(sessionPresent: true);
        $client->loopOnce(0.0);

        expect(writtenAfterReconnect($inner))->toBe([]);
    });
});

describe('on a live connection', function (): void {
    it('resends nothing on MQTT 5, which forbids it (MQTT-4.4.0-1)', function (): void {
        [$client, , $inner] = inflightClient(MqttVersion::V5_0, static fn (Options $o): Options => $o->withMaxResendAttempts(2));

        expect(fn (): int => $client->publish('a', '1', atLeastOnce()))->toThrow(Timeout::class, 'PUBACK after 0 resend(s)')
            ->and($inner->sentPacketTypes())->toBe([PacketType::PUBLISH->value]);
    });

    it('still resends on MQTT 3.1.1, which allows it', function (): void {
        [$client, , $inner] = inflightClient(configure: static fn (Options $o): Options => $o->withMaxResendAttempts(2));

        expect(fn (): int => $client->publish('a', '1', atLeastOnce()))->toThrow(Timeout::class, 'PUBACK after 2 resend(s)')
            ->and($inner->sentPacketTypes())->toBe(array_fill(0, 3, PacketType::PUBLISH->value));
    });
});

describe('packet identifiers', function (): void {
    it('are counted per client, from 1', function (): void {
        // From a process-wide counter, a second client started wherever the first had got to.
        [$first, , $firstWire]   = inflightClient();
        [$second, , $secondWire] = inflightClient();
        $firstWire->feedPubAck(1);
        $secondWire->feedPubAck(1);

        expect($first->publish('a', '1', atLeastOnce()))->toBe(1)
            ->and($second->publish('a', '1', atLeastOnce()))->toBe(1);
    });

    it('skip one still in flight', function (): void {
        // The broker may still hold packet 1; a QoS 2 message reusing it would be taken for
        // a duplicate and dropped.
        [$client, , $inner] = inflightClient();
        expect(fn (): int => $client->publish('a', '1', exactlyOnceDelivery()))->toThrow(Timeout::class);   // 1, abandoned
        new ReflectionProperty(Client::class, 'nextPacketId')->setValue($client, 1);                   // as after a wrap
        $inner->feedPubAck(2);

        expect($client->publish('b', '2', atLeastOnce()))->toBe(2);
    });

    it('wrap from 65535 to 1', function (): void {
        [$client, , $inner] = inflightClient();
        new ReflectionProperty(Client::class, 'nextPacketId')->setValue($client, 0xFFFF);
        $inner->feedPubAck(0xFFFF)->feedPubAck(1);

        expect($client->publish('a', '1', atLeastOnce()))->toBe(0xFFFF)
            ->and($client->publish('b', '2', atLeastOnce()))->toBe(1);
    });
});

describe('Receive Maximum on a new connection', function (): void {
    it('holds back what it has no room for, then sends it in order as slots free up', function (): void {
        // The rest were never sent on that connection: an MQTT 5 waiter does not resend on a
        // live connection, so the message publish() was still waiting for simply timed out.
        $receiveMaximumOne            = "\x03\x21\x00\x01";
        [$client, $transport, $inner] = inflightClient(MqttVersion::V5_0, static fn (Options $o): Options => $o);
        $inner->takeWritten();
        $client->disconnect();
        $inner->feedConnAck(properties: $receiveMaximumOne);
        $client->connect();
        expect(fn (): int => $client->publish('a', 'A', atLeastOnce()))->toThrow(Timeout::class);   // 1, holds the only slot
        $flowControl = new ReflectionProperty(Client::class, 'flowControl')->getValue($client);
        new ReflectionProperty($flowControl, 'pending')->setValue($flowControl, [1 => hrtime(true) / 1e9 - 100.0]);

        $published          = 0;
        $transport->onWrite = static function (int $type) use ($inner, $receiveMaximumOne, &$published): void {
            if ($type !== PacketType::PUBLISH->value) {
                return;
            }
            $published++;
            if ($published === 1) {
                $inner->close();                                        // 'b' out, link drops
                $inner->feedConnAck(sessionPresent: true, properties: $receiveMaximumOne);
            } elseif ($published === 2) {
                $inner->feedPubAck(1);                                  // 'a' resent first, and acknowledged
            } elseif ($published === 3) {
                $inner->feedPubAck(2);                                  // 'b' follows once the slot is free
            }
        };

        expect($client->publish('b', 'B', atLeastOnce()))->toBe(2)
            ->and(writtenAfterReconnect($inner))->toBe([
                [PacketType::PUBLISH->value, 0x08 | 0x02],
                [PacketType::PUBLISH->value, 0x08 | 0x02],
            ]);
    });
});

describe('a message the broker refuses by ending the connection', function (): void {
    it('is given up on once it was refused while sent alone, so auto-reconnect is not used up', function (): void {
        // Resent first on every reconnect, it ended each new connection in turn until
        // auto-reconnect stopped for good — and the next message failed too. The broker says
        // why (DISCONNECT 0x87); the next connection resends one message at a time, and a
        // second refusal while it waits alone pins it on that message.
        [$client, $transport, $inner] = inflightClient(MqttVersion::V5_0, static fn (Options $o): Options => $o->withMaxResendAttempts(3)->withAutoReconnect(maxAttempts: 5, baseDelay: 0.0, jitter: 0.0));
        $transport->onWrite           = static function (int $type, string $bytes) use ($inner): void {
            if ($type !== PacketType::PUBLISH->value) {
                return;
            }
            if (str_contains($bytes, 'forbidden')) {
                $inner->feedDisconnect(0x87);                           // Not authorized
                $inner->feedConnAck(sessionPresent: true, properties: "\x00");
            } else {
                $inner->feedPubAck(2);
            }
        };

        expect(fn (): int => $client->publish('forbidden', 'x', atLeastOnce()))->toThrow(Timeout::class, 'given up on')
            ->and($client->publish('allowed', 'y', atLeastOnce()))->toBe(2)
            ->and($inner->countSent(PacketType::CONNECT))->toBe(2);      // the probe, then one that lasts
    });

    it('is kept through connections that merely dropped (MQTT 5 §4.1)', function (): void {
        // Three connections that ended right after resending it used to be taken for a
        // refusal, and a valid message the broker still held was thrown away. A dropped
        // connection proves nothing about any message.
        [$client, $transport, $inner] = inflightClient(MqttVersion::V5_0, static fn (Options $o): Options => $o->withAutoReconnect(maxAttempts: 9, baseDelay: 0.0, jitter: 0.0));
        expect(fn (): int => $client->publish('x', 'payload', atLeastOnce()))->toThrow(Timeout::class);   // abandoned

        $resends            = 0;
        $transport->onWrite = static function (int $type) use ($inner, &$resends): void {
            if ($type !== PacketType::PUBLISH->value) {
                return;
            }
            if (++$resends <= 4) {
                $inner->close();                                        // the network, not the broker
                $inner->feedConnAck(sessionPresent: true, properties: "\x00");
            } else {
                $inner->feedPubAck(1);
            }
        };
        $inner->close();
        $inner->feedConnAck(sessionPresent: true, properties: "\x00");
        for ($i = 0; $i < 12 && $resends < 5; $i++) {
            $client->loopOnce(0.0);
        }
        $client->loopOnce(0.0);                                         // reads the PUBACK

        expect($resends)->toBe(5)
            ->and(new ReflectionProperty(Client::class, 'outbound')->getValue($client)->count())->toBe(0);
    });
});

describe('a restored backlog that takes longer to drain than any timeout', function (): void {
    it('is resent in full: elapsed time does not end a session', function (): void {
        // Every restored message got the same grace period; with Receive Maximum 1 the ones
        // still waiting their turn expired before they were ever resent.
        $directory = sys_get_temp_dir().'/mqtt-slow-backlog-'.bin2hex(random_bytes(4));
        $store     = new FileSessionStore($directory);
        $outbound  = [];
        for ($id = 1; $id <= 25; $id++) {
            $outbound[] = ['packetId' => $id, 'topic' => 't', 'payload' => "m$id", 'qos' => 1, 'retain' => false, 'properties' => null, 'released' => false];
        }
        $store->save('slow', new SessionState(outbound: $outbound));

        $inner     = new InMemoryTransport();
        $transport = new InterceptingTransport($inner);
        $inner->feedConnAck(sessionPresent: true, properties: "\x03\x21\x00\x01");     // Receive Maximum 1
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'slow', cleanSession: false, version: MqttVersion::V5_0)
                ->withSessionStore($store)->withAckTimeout(0.1)->withMaxResendAttempts(0),   // a one-second grace period
            $transport,
        );
        $resent             = [];
        $transport->onWrite = static function (int $type, string $bytes) use ($inner, &$resent): void {
            if ($type === PacketType::PUBLISH->value) {
                $resent[] = $bytes;                                     // resent in order: identifiers 1, 2, 3…
                usleep(60_000);                                         // a slow broker: 25 × 60 ms > 1 s
                $inner->feedPubAck(count($resent));
            }
        };
        $client->connect();
        for ($i = 0; $i < 100 && count($resent) < 25; $i++) {
            $client->loopOnce(0.0);
        }
        $client->loopOnce(0.0);

        array_map(unlink(...), glob($directory.'/*') ?: []);
        @rmdir($directory);
        expect(count($resent))->toBe(25)
            ->and(new ReflectionProperty(Client::class, 'outbound')->getValue($client)->count())->toBe(0);
    });
});
