<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Contract\MetricsInterface;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Tests\Support\InterceptingTransport;
use ScienceStories\Mqtt\Util\RandomId;

/**
 * A connected client over an intercepting transport, with nothing written yet.
 *
 * Acknowledgements time out after 50 ms and are resent at most twice, unless $configure
 * says otherwise.
 *
 * @param  MqttVersion  $version  the protocol version the client speaks
 * @param  (Closure(Options): Options)|null  $configure
 * @param  string  $connAckProperties  MQTT 5 CONNACK property block, length-prefixed
 * @param  MetricsInterface|null  $metrics  receives the client's metrics, if given
 * @return array{0: Client, 1: InterceptingTransport, 2: InMemoryTransport}
 */
function qos2Sender(MqttVersion $version, ?Closure $configure = null, string $connAckProperties = "\x00", ?MetricsInterface $metrics = null): array
{
    RandomId::resetPacketId();
    $inner     = new InMemoryTransport();
    $transport = new InterceptingTransport($inner);
    $inner->feedConnAck(properties: $version === MqttVersion::V5_0 ? $connAckProperties : '');

    $options = new Options(host: 'fake.broker', clientId: 'qos2', version: $version)
        ->withAckTimeout(0.05)->withMaxResendAttempts(2);
    $client = new Client($configure instanceof Closure ? $configure($options) : $options, $transport, metrics: $metrics);
    $client->connect();
    $inner->takeWritten();

    return [$client, $transport, $inner];
}

/** PUBREL exactly as it must go on the wire: type 6, flags 0b0010, Remaining Length 2. */
function pubRelBytes(int $packetId): string
{
    return "\x62\x02".pack('n', $packetId);
}

function exactlyOnce(): PublishOptions
{
    return new PublishOptions(qos: QoS::ExactlyOnce);
}

describe('the QoS 2 sender', function (): void {
    it('releases the message with PUBREL once PUBREC arrives, and completes on PUBCOMP', function (MqttVersion $version): void {
        [$client, , $inner] = qos2Sender($version);
        $inner->feedPubRec(1)->feedPubComp(1);

        expect($client->publish('t', 'x', exactlyOnce()))->toBe(1)
            ->and($inner->sentPacketTypes())->toBe([PacketType::PUBLISH->value, PacketType::PUBREL->value])
            ->and(substr($inner->written(), -4))->toBe(pubRelBytes(1));
    })->with(['3.1.1' => [MqttVersion::V3_1_1], '5.0' => [MqttVersion::V5_0]]);

    it('resends the PUBLISH, with DUP, while no PUBREC has arrived', function (): void {
        [$client, , $inner] = qos2Sender(MqttVersion::V3_1_1, static fn (Options $o): Options => $o->withMaxResendAttempts(1));

        expect(fn (): int => $client->publish('t', 'x', exactlyOnce()))->toThrow(Timeout::class, 'PUBREC after 1 resend(s)')
            ->and($inner->sentPacketTypes())->toBe([PacketType::PUBLISH->value, PacketType::PUBLISH->value])
            ->and(array_column($inner->sentPackets(), 'flags'))->toBe([0x04, 0x0C]);   // QoS 2, then QoS 2 + DUP
    });

    it('resends only the PUBREL once PUBREC has arrived, on MQTT 3.1.1', function (): void {
        // Resending the PUBLISH after PUBREL (MQTT-4.3.3-6) is a new message to the
        // receiver, which then delivers it twice — the one thing QoS 2 exists to prevent.
        [$client, , $inner] = qos2Sender(MqttVersion::V3_1_1);
        $inner->feedPubRec(1);

        expect(fn (): int => $client->publish('t', 'x', exactlyOnce()))->toThrow(Timeout::class, 'PUBCOMP after 2 resend(s)')
            ->and($inner->sentPacketTypes())->toBe([PacketType::PUBLISH->value, PacketType::PUBREL->value, PacketType::PUBREL->value, PacketType::PUBREL->value])
            ->and(substr($inner->written(), -12))->toBe(str_repeat(pubRelBytes(1), 3));
    });

    it('resends nothing on a live MQTT 5 connection, PUBREL included (MQTT-4.4.0-1)', function (): void {
        [$client, , $inner] = qos2Sender(MqttVersion::V5_0);
        $inner->feedPubRec(1);

        expect(fn (): int => $client->publish('t', 'x', exactlyOnce()))->toThrow(Timeout::class, 'PUBCOMP after 0 resend(s)')
            ->and($inner->sentPacketTypes())->toBe([PacketType::PUBLISH->value, PacketType::PUBREL->value]);
    });

    it('completes when a resent PUBREL is answered', function (): void {
        [$client, $transport, $inner] = qos2Sender(MqttVersion::V3_1_1);
        $inner->feedPubRec(1);
        $pubRels            = 0;
        $transport->onWrite = static function (int $type) use ($inner, &$pubRels): void {
            if ($type === PacketType::PUBREL->value && ++$pubRels === 2) {
                $inner->feedPubComp(1);
            }
        };

        expect($client->publish('t', 'x', exactlyOnce()))->toBe(1)
            ->and($inner->sentPacketTypes())->toBe([PacketType::PUBLISH->value, PacketType::PUBREL->value, PacketType::PUBREL->value]);
    });

    it('resends the PUBREL, not the PUBLISH, on the connection that replaces a dropped one', function (): void {
        [$client, $transport, $inner] = qos2Sender(
            MqttVersion::V3_1_1,
            static fn (Options $o): Options => $o->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
        );
        $inner->feedPubRec(1);
        $pubRels            = 0;
        $transport->onWrite = static function (int $type) use ($inner, &$pubRels): void {
            if ($type !== PacketType::PUBREL->value) {
                return;
            }
            if (++$pubRels === 1) {
                // The link drops right after the first PUBREL; the broker is back at once.
                $inner->close();
                $inner->feedConnAck();
            } else {
                $inner->feedPubComp(1);
            }
        };

        expect($client->publish('t', 'x', exactlyOnce()))->toBe(1)
            ->and($inner->sentPacketTypes())->toBe([
                PacketType::PUBLISH->value,
                PacketType::PUBREL->value,
                PacketType::CONNECT->value,
                PacketType::PUBREL->value,
            ]);
    });

    it('sends no PUBREL for a PUBREC with an error code, and frees its flow-control slot', function (): void {
        // MQTT-4.3.3-4: a PUBREC of 0x80 or above ends the exchange. The client released the
        // message anyway and then waited for a PUBCOMP that never came, holding the slot.
        [$client, , $inner] = qos2Sender(MqttVersion::V5_0, connAckProperties: "\x03\x21\x00\x01");   // Receive Maximum 1
        $inner->feedPacket(PacketType::PUBREC, 0, pack('n', 1)."\x87")                                 // Not authorized
            ->feedPubAck(2);

        $refused = $client->publish('t', 'x', exactlyOnce());
        $next    = $client->publish('t', 'y', new PublishOptions(qos: QoS::AtLeastOnce));

        expect([$refused, $next])->toBe([1, 2])
            ->and($inner->sentPacketTypes())->toBe([PacketType::PUBLISH->value, PacketType::PUBLISH->value]);
    });

    it('counts a refused message on publish_failed, but not a PUBCOMP that no longer knows the identifier', function (): void {
        // PUBCOMP 0x92 is what a resent PUBREL gets once the first PUBCOMP was lost — "not an
        // error during recovery" — and the message was accepted at PUBREC anyway. Counting it
        // would raise a false alarm after every such reconnect.
        $failed  = new ArrayObject();
        $metrics = new readonly class ($failed) implements MetricsInterface {
            /** @param ArrayObject<int, array<string, mixed>> $failed */
            public function __construct(private ArrayObject $failed)
            {
            }

            public function increment(string $name, float $by = 1.0, array $tags = []): void
            {
                if ($name === 'publish_failed') {
                    $this->failed->append($tags);
                }
            }

            public function observe(string $name, float $seconds, array $tags = []): void
            {
            }

            public function size(string $name, int $bytes, array $tags = []): void
            {
            }
        };
        [$client, , $inner] = qos2Sender(MqttVersion::V5_0, metrics: $metrics);
        $inner->feedPacket(PacketType::PUBREC, 0, pack('n', 1)."\x87")       // refused
            ->feedPubRec(2)
            ->feedPacket(PacketType::PUBCOMP, 0, pack('n', 2)."\x92");       // accepted, then identifier unknown

        $client->publish('t', 'x', exactlyOnce());
        $client->publish('t', 'y', exactlyOnce());

        expect($failed->getArrayCopy())->toBe([['packet' => 'PUBREC', 'reason' => 0x87]]);
    });

    it('answers a PUBREC that arrives after publish() stopped waiting', function (): void {
        // The broker holds the message; without PUBREL its side of the exchange never ends.
        [$client, , $inner] = qos2Sender(MqttVersion::V3_1_1, static fn (Options $o): Options => $o->withMaxResendAttempts(0));
        expect(fn (): int => $client->publish('t', 'x', exactlyOnce()))->toThrow(Timeout::class);

        $inner->feedPubRec(1);
        $client->loopOnce(0.0);

        expect(substr($inner->written(), -4))->toBe(pubRelBytes(1));
    });

    it('keeps the PUBCOMP that a publish() inside a message handler read first', function (): void {
        // The nested publish pumps loopOnce() and reads this exchange's PUBREC and PUBCOMP
        // before the outer wait gets to look. The PUBCOMP must be waiting for it, not
        // dropped as unsolicited — which cost a full ackTimeout and a PUBREL resend.
        [$client, , $inner] = qos2Sender(MqttVersion::V3_1_1, static fn (Options $o): Options => $o->withMaxResendAttempts(0));
        $client->onMessage(static function () use ($client): void {
            $client->publish('inner', 'y', new PublishOptions(qos: QoS::AtLeastOnce));
        });
        $inner->feedPublish('trigger', 'x')
            ->feedPubRec(1)
            ->feedPubComp(1)
            ->feedPubAck(2);

        expect($client->publish('outer', 'x', exactlyOnce()))->toBe(1)
            ->and($inner->sentPacketTypes())->toBe([PacketType::PUBLISH->value, PacketType::PUBLISH->value, PacketType::PUBREL->value]);
    });
});
