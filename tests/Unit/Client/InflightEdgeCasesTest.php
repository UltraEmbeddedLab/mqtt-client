<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Tests\Support\InterceptingTransport;
use ScienceStories\Mqtt\Util\Bytes;

/** The topic of a PUBLISH the client wrote, from the bytes it wrote. */
function writtenTopic(string $bytes): string
{
    $consumed = 0;
    Bytes::decodeVarInt($bytes, $consumed, 1);
    $offset = 1 + $consumed;

    return Bytes::decodeString($bytes, $offset);
}

describe('an acknowledgement followed by a resend that fails', function (): void {
    it('still reaches the publish() waiting for it', function (): void {
        // The freed slot was handed to the resend backlog before the acknowledgement was
        // recorded; the resend's write failed, and publish() reported a message the broker
        // had acknowledged as given up on.
        $inner     = new InMemoryTransport();
        $transport = new InterceptingTransport($inner);
        $inner->feedConnAck(properties: "\x00");
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'drain-fails', version: MqttVersion::V5_0)
                ->withAckTimeout(0.5)->withMaxResendAttempts(0)
                ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
            $transport,
        );
        $client->connect();

        $receiveMaximumOne  = "\x03\x21\x00\x01";
        $publishes          = 0;
        $transport->onWrite = static function (int $type) use ($inner, $transport, $receiveMaximumOne, &$publishes): void {
            if ($type !== PacketType::PUBLISH->value) {
                return;
            }
            $publishes++;
            if ($publishes === 1) {
                $inner->feedPublish('trigger', 'go', properties: "\x00");      // a handler publishes 'b'
            } elseif ($publishes === 2) {
                $inner->close();                                              // dropped after 'b'
                $inner->feedConnAck(sessionPresent: true, properties: $receiveMaximumOne);
            } elseif ($publishes === 3) {
                $inner->feedPubAck(1);                                        // 'a' acknowledged…
                $transport->writesBeforeFailure = 0;                          // …and the socket dies
            }
        };
        $client->onMessage(static function () use ($client): void {
            try {
                $client->publish('b', 'B', new PublishOptions(qos: QoS::AtLeastOnce));
            } catch (Throwable) {
                // 'b' never got out again; that is not what this test is about
            }
        });

        expect($client->publish('a', 'A', new PublishOptions(qos: QoS::AtLeastOnce)))->toBe(1);
    });
});

describe('an offline message whose wait for a slot is cut short', function (): void {
    it('is kept for the next connection, even though resends went out meanwhile', function (): void {
        // The drain decided "written or not" from the byte counter, which the resends moved;
        // the message was dropped although it never reached the wire. Inside the reconnect the
        // wait also sat out its full five seconds on a closed transport.
        $inner     = new InMemoryTransport();
        $transport = new InterceptingTransport($inner);
        $inner->feedConnAck(properties: "\x00");
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'offline', cleanSession: false, version: MqttVersion::V5_0)
                ->withSessionExpiry(300)->withAckTimeout(0.1)->withMaxResendAttempts(0)->withOfflineQueue(10)
                ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
            $transport,
        );
        $client->connect();
        foreach (['A', 'B'] as $topic) {
            try {
                $client->publish($topic, $topic, new PublishOptions(qos: QoS::AtLeastOnce));
            } catch (Throwable) {
                // left in flight
            }
        }
        $inner->close();
        $client->publish('M', 'M', new PublishOptions(qos: QoS::AtLeastOnce));    // queued offline

        $inner->feedConnAck(sessionPresent: true, properties: "\x03\x21\x00\x01");   // Receive Maximum 1
        $transport->onWrite = static function (int $type, string $bytes) use ($inner): void {
            if ($type !== PacketType::PUBLISH->value) {
                return;
            }
            match (writtenTopic($bytes)) {
                'A'     => $inner->feedPubAck(1),
                'B'     => $inner->close(),                        // the link drops again
                default => null,
            };
        };

        $started = hrtime(true);
        $client->loopOnce(0.0);                                    // reconnect, resends, offline drain

        $queue = new ReflectionProperty(Client::class, 'offlineQueue')->getValue($client);
        expect($queue->count())->toBe(1)
            ->and((hrtime(true) - $started) / 1e9)->toBeLessThan(1.0)
            ->and(str_contains($inner->written(), Bytes::encodeString('M')))->toBeFalse();
    });
});

describe('a message the broker refuses by ending the connection, without auto-reconnect', function (): void {
    /**
     * The application's own reconnect loop: connect(), let the connection run, repeat.
     *
     * @return list<int> the rounds on which the message was sent again
     */
    function roundsResent(MqttVersion $version, Closure $refuse): array
    {
        $inner              = new InMemoryTransport();
        $transport          = new InterceptingTransport($inner);
        $client             = new Client(new Options(host: 'fake.broker', clientId: 'poison', cleanSession: false, version: $version)->withAckTimeout(0.1)->withMaxResendAttempts(0), $transport);
        $transport->onWrite = static function (int $type, string $bytes) use ($inner, $refuse): void {
            if ($type === PacketType::PUBLISH->value && str_contains($bytes, 'forbidden')) {
                $refuse($inner);
            }
        };
        $properties = $version === MqttVersion::V5_0 ? "\x00" : '';
        $inner->feedConnAck(properties: $properties);
        $client->connect();
        try {
            $client->publish('forbidden', 'x', new PublishOptions(qos: QoS::AtLeastOnce));
        } catch (Throwable) {
            // the connection ended under it
        }

        $resentOn = [];
        for ($round = 1; $round <= 4; $round++) {
            $inner->feedConnAck(sessionPresent: true, properties: $properties);
            $before = $inner->countSent(PacketType::PUBLISH);
            $client->connect();
            $client->loopOnce(0.0);
            if ($inner->countSent(PacketType::PUBLISH) > $before) {
                $resentOn[] = $round;
            }
        }

        return $resentOn;
    }

    it('is given up on after the application\'s connect() sent it alone and it was refused again (MQTT 5)', function (): void {
        // Only auto-reconnect used to look: an application reconnecting itself resent the
        // message first on every connect(), forever.
        expect(roundsResent(MqttVersion::V5_0, static fn (InMemoryTransport $inner): InMemoryTransport => $inner->feedDisconnect(0x87)))->toBe([1]);
    });

    it('is never given up on when the broker gives no reason, as an MQTT 3.1.1 one cannot', function (): void {
        // A closed socket may be the network: guessing would throw away messages the broker
        // still holds. The application sees every connection end and can decide.
        expect(roundsResent(MqttVersion::V3_1_1, static fn (InMemoryTransport $inner): InMemoryTransport => $inner->closeByPeer()))->toBe([1, 2, 3, 4]);
    });
});

describe('a topic filter made only of digits', function (): void {
    it('is resubscribed after a reconnect', function (): void {
        // PHP keeps the filter "42" as an int array key. The encoders cast it back today; the
        // client now hands them a string anyway, so no stricter encoder can trip over it.
        $transport = new InMemoryTransport();
        $transport->feedConnAck();
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'digits')->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
            $transport,
        );
        $client->connect();
        $transport->feedPacket(PacketType::SUBACK, 0, pack('n', 1)."\x00");
        $client->subscribe(['42']);

        $transport->close();
        $transport->feedConnAck()->feedPacket(PacketType::SUBACK, 0, pack('n', 2)."\x00");
        $client->loopOnce(0.0);

        expect($transport->countSent(PacketType::SUBSCRIBE))->toBe(2)
            ->and($transport->isOpen())->toBeTrue();
    });
});
