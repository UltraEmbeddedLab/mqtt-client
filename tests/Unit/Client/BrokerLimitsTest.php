<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Exception\MqttException;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Tests\Support\InterceptingTransport;
use ScienceStories\Mqtt\Util\Bytes;

/**
 * An MQTT 5 client connected to a broker whose CONNACK carries $properties (without the
 * length prefix), with nothing written yet.
 *
 * @return array{0: Client, 1: InMemoryTransport}
 */
function limitedClient(string $properties, MqttVersion $version = MqttVersion::V5_0): array
{
    $transport = new InMemoryTransport();
    $transport->feedConnAck(properties: $version === MqttVersion::V5_0 ? Bytes::encodeVarInt(strlen($properties)).$properties : '');
    $client = new Client(new Options(host: 'fake.broker', clientId: 'limited', version: $version), $transport);
    $client->connect();
    $transport->takeWritten();

    return [$client, $transport];
}

describe('a publish the broker said it will not accept', function (): void {
    it('is not sent above the Maximum QoS (MQTT-3.2.2-11)', function (): void {
        // Sent anyway, the broker closed the connection — and a reconnect resent it.
        [$client, $transport] = limitedClient("\x24\x01");                  // Maximum QoS 1

        expect(fn (): int => $client->publish('t', 'x', new PublishOptions(qos: QoS::ExactlyOnce)))
            ->toThrow(MqttException::class, 'Maximum QoS of 1')
            ->and($transport->written())->toBe('');
    });

    it('is not sent retained when the broker has no retained messages (MQTT-3.2.2-14)', function (): void {
        [$client, $transport] = limitedClient("\x25\x00");                  // Retain Available 0

        expect(fn (): int => $client->publish('t', 'x', new PublishOptions(retain: true)))
            ->toThrow(MqttException::class, 'retained')
            ->and($transport->written())->toBe('');
    });

    it('is not sent above the Maximum Packet Size (MQTT-3.2.2-15)', function (): void {
        [$client, $transport] = limitedClient("\x27\x00\x00\x00\x40");      // Maximum Packet Size 64

        expect(fn (): int => $client->publish('t', str_repeat('x', 100)))->toThrow(ProtocolError::class, 'Maximum Packet Size of 64')
            ->and($transport->written())->toBe('');

        $client->publish('t', 'small');
        expect($transport->sentPacketTypes())->toBe([PacketType::PUBLISH->value]);
    });

    it('is sent when within the limits, and MQTT 3.1.1 has none', function (): void {
        [$client, $transport] = limitedClient("\x24\x01\x25\x00");
        $client->publish('t', 'x', new PublishOptions(qos: QoS::AtMostOnce));

        [$legacy, $legacyWire] = limitedClient('', MqttVersion::V3_1_1);
        $legacyWire->feedPubRec(1)->feedPubComp(1);
        $legacy->publish('t', 'x', new PublishOptions(qos: QoS::ExactlyOnce, retain: true));

        expect($transport->sentPacketTypes())->toBe([PacketType::PUBLISH->value])
            ->and($legacyWire->sentPacketTypes())->toBe([PacketType::PUBLISH->value, PacketType::PUBREL->value]);
    });
});

it('leaves no Topic Alias behind when a publish is refused for its size', function (): void {
    // The alias was set up before the size check threw; the next publish to the topic went out
    // by alias alone, for a mapping the broker never received, and it closed the connection.
    [$client, $transport] = limitedClient("\x22\x00\x05\x27\x00\x00\x00\x40");   // Topic Alias Maximum 5, Maximum Packet Size 64

    expect(fn (): int => $client->publish('sensors/big', str_repeat('x', 100)))->toThrow(ProtocolError::class);
    $client->publish('sensors/big', 'small');

    $body   = $transport->sentPackets()[0]['body'];
    $offset = 0;
    expect(Bytes::decodeString($body, $offset))->toBe('sensors/big')
        ->and(substr($body, $offset))->toBe("\x03\x23\x00\x01small");              // establishes alias 1, with the topic
});

it('gives up on a message in flight that the reconnected broker no longer allows', function (): void {
    // Resent regardless, it got each new connection closed in turn.
    $inner     = new InMemoryTransport();
    $transport = new InterceptingTransport($inner);
    $inner->feedConnAck(properties: "\x00");
    $client = new Client(
        new Options(host: 'fake.broker', clientId: 'stricter', version: MqttVersion::V5_0)
            ->withAckTimeout(0.1)->withMaxResendAttempts(0)
            ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
        $transport,
    );
    $client->connect();
    $transport->onWrite = static function (int $type) use ($inner): void {
        if ($type === PacketType::PUBLISH->value) {
            $inner->close();
            $inner->feedConnAck(sessionPresent: true, properties: "\x02\x24\x00");   // now Maximum QoS 0
        }
    };

    expect(fn (): int => $client->publish('t', 'x', new PublishOptions(qos: QoS::AtLeastOnce)))->toThrow(Timeout::class, 'given up on')
        ->and($inner->countSent(PacketType::PUBLISH))->toBe(1)
        ->and($inner->isOpen())->toBeTrue();
});
