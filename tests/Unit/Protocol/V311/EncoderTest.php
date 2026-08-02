<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\WillOptions;
use ScienceStories\Mqtt\Protocol\Packet\Connect;
use ScienceStories\Mqtt\Protocol\Packet\Publish;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Protocol\V311\Encoder;

test('encodeConnect minimal contains MQTT protocol name and level 4', function (): void {
    $encoder = new Encoder();
    $pkt     = new Connect(clientId: 'test-client');
    $result  = $encoder->encodeConnect($pkt);

    // Protocol name "MQTT" should appear in the packet
    expect(str_contains($result, 'MQTT'))->toBeTrue();

    // Find protocol level byte (immediately after "MQTT")
    $mqttPos = strpos($result, 'MQTT');
    // Protocol name is encoded as 2-byte length + "MQTT", so level byte is at mqttPos + 4
    $protocolLevel = ord($result[$mqttPos + 4]);
    expect($protocolLevel)->toBe(4);
});

test('encodeConnect with cleanSession flag set', function (): void {
    $encoder = new Encoder();
    $pkt     = new Connect(clientId: 'test-client', cleanSession: true);
    $result  = $encoder->encodeConnect($pkt);

    // Connect flags byte is after: fixed header (2 bytes min) + 2-byte len + "MQTT" (4) + protocol level (1)
    // Fixed header: 1 byte type + varint remaining length
    // Variable header starts after fixed header: 2-byte len prefix + "MQTT" + protocol level + flags
    $mqttPos   = strpos($result, 'MQTT');
    $flagsByte = ord($result[$mqttPos + 5]); // flags byte is after protocol level

    // Clean Session is bit 1 (0x02)
    expect($flagsByte & 0x02)->toBe(0x02);
});

test('encodeConnect without cleanSession flag', function (): void {
    $encoder = new Encoder();
    $pkt     = new Connect(clientId: 'test-client', cleanSession: false);
    $result  = $encoder->encodeConnect($pkt);

    $mqttPos   = strpos($result, 'MQTT');
    $flagsByte = ord($result[$mqttPos + 5]);

    // Clean Session bit should NOT be set
    expect($flagsByte & 0x02)->toBe(0);
});

test('encodeConnect with username and password', function (): void {
    $encoder = new Encoder();
    $pkt     = new Connect(
        clientId: 'test-client',
        username: 'myuser',
        password: 'mypass',
    );
    $result = $encoder->encodeConnect($pkt);

    $mqttPos   = strpos($result, 'MQTT');
    $flagsByte = ord($result[$mqttPos + 5]);

    // Username flag is bit 7 (0x80)
    expect($flagsByte & 0x80)->toBe(0x80);
    // Password flag is bit 6 (0x40)
    expect($flagsByte & 0x40)->toBe(0x40);

    // Username and password should appear in the payload
    expect(str_contains($result, 'myuser'))->toBeTrue()
        ->and(str_contains($result, 'mypass'))->toBeTrue();
});

test('encodeConnect with will message', function (): void {
    $encoder = new Encoder();
    $will    = new WillOptions(
        topic: 'will/topic',
        payload: 'will-payload',
        qos: QoS::AtMostOnce,
        retain: false,
    );
    $pkt    = new Connect(clientId: 'test-client', will: $will);
    $result = $encoder->encodeConnect($pkt);

    $mqttPos   = strpos($result, 'MQTT');
    $flagsByte = ord($result[$mqttPos + 5]);

    // Will flag is bit 2 (0x04)
    expect($flagsByte & 0x04)->toBe(0x04);

    // Will topic and payload should appear in the packet
    expect(str_contains($result, 'will/topic'))->toBeTrue()
        ->and(str_contains($result, 'will-payload'))->toBeTrue();
});

test('encodeConnect with will QoS 2 and retain', function (): void {
    $encoder = new Encoder();
    $will    = new WillOptions(
        topic: 'will/topic',
        payload: 'will-payload',
        qos: QoS::ExactlyOnce,
        retain: true,
    );
    $pkt    = new Connect(clientId: 'test-client', will: $will);
    $result = $encoder->encodeConnect($pkt);

    $mqttPos   = strpos($result, 'MQTT');
    $flagsByte = ord($result[$mqttPos + 5]);

    // Will flag (bit 2)
    expect($flagsByte & 0x04)->toBe(0x04);
    // Will QoS 2 in bits 3-4 (0x10)
    expect(($flagsByte >> 3) & 0x03)->toBe(2);
    // Will Retain (bit 5, 0x20)
    expect($flagsByte & 0x20)->toBe(0x20);
});

test('encodePublish QoS 0', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(
        topic: 'test/topic',
        payload: 'hello',
        qos: QoS::AtMostOnce,
    );
    $result = $encoder->encodePublish($pkt);

    // First byte: packet type 3 in upper nibble, QoS 0 means no QoS bits
    $firstByte = ord($result[0]);
    expect($firstByte >> 4)->toBe(3) // PUBLISH type
        ->and(($firstByte >> 1) & 0x03)->toBe(0); // QoS 0

    // Topic and payload should be in the packet
    expect(str_contains($result, 'test/topic'))->toBeTrue()
        ->and(str_contains($result, 'hello'))->toBeTrue();
});

test('encodePublish QoS 1 with packetId', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(
        topic: 'test/topic',
        payload: 'hello',
        qos: QoS::AtLeastOnce,
        packetId: 42,
    );
    $result = $encoder->encodePublish($pkt);

    $firstByte = ord($result[0]);
    expect(($firstByte >> 1) & 0x03)->toBe(1); // QoS 1

    // Packet ID 42 should be encoded as 2-byte big-endian after the topic
    // Topic is 2-byte length + "test/topic" (10 bytes) = 12 bytes in variable header
    // Then 2-byte packet ID
    expect(str_contains($result, 'test/topic'))->toBeTrue()
        ->and(str_contains($result, pack('n', 42)))->toBeTrue();
});

test('encodePublish QoS 2 with packetId', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(
        topic: 'test/topic',
        payload: 'hello',
        qos: QoS::ExactlyOnce,
        packetId: 1000,
    );
    $result = $encoder->encodePublish($pkt);

    $firstByte = ord($result[0]);
    expect(($firstByte >> 1) & 0x03)->toBe(2); // QoS 2

    expect(str_contains($result, pack('n', 1000)))->toBeTrue();
});

test('encodePublish throws LogicException for QoS greater than 0 without packetId', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(
        topic: 'test/topic',
        payload: 'hello',
        qos: QoS::AtLeastOnce,
    );
    $encoder->encodePublish($pkt);
})->throws(LogicException::class);

test('encodePublish with retain flag', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(
        topic: 'test/topic',
        payload: 'hello',
        qos: QoS::AtMostOnce,
        retain: true,
    );
    $result = $encoder->encodePublish($pkt);

    $firstByte = ord($result[0]);
    // Retain is bit 0
    expect($firstByte & 0x01)->toBe(0x01);
});

test('encodePublish with dup flag', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(
        topic: 'test/topic',
        payload: 'hello',
        qos: QoS::AtLeastOnce,
        dup: true,
        packetId: 1,
    );
    $result = $encoder->encodePublish($pkt);

    $firstByte = ord($result[0]);
    // DUP is bit 3 (0x08)
    expect($firstByte & 0x08)->toBe(0x08);
});

test('encodeSubscribe single filter', function (): void {
    $encoder = new Encoder();
    $filters = [['filter' => 'test/topic', 'qos' => 1]];
    $result  = $encoder->encodeSubscribe($filters, 1);

    // First byte: SUBSCRIBE type (8) with reserved flags 0x02
    $firstByte = ord($result[0]);
    expect($firstByte)->toBe((8 << 4) | 0x02);

    // Packet ID 1 in variable header
    expect(str_contains($result, pack('n', 1)))->toBeTrue();

    // Topic filter should appear in the packet
    expect(str_contains($result, 'test/topic'))->toBeTrue();
});

test('encodeSubscribe multiple filters', function (): void {
    $encoder = new Encoder();
    $filters = [
        ['filter' => 'topic/a', 'qos' => 0],
        ['filter' => 'topic/b', 'qos' => 1],
        ['filter' => 'topic/c', 'qos' => 2],
    ];
    $result = $encoder->encodeSubscribe($filters, 10);

    expect(str_contains($result, 'topic/a'))->toBeTrue()
        ->and(str_contains($result, 'topic/b'))->toBeTrue()
        ->and(str_contains($result, 'topic/c'))->toBeTrue();

    // Find each topic and verify the QoS byte after it
    $posA = strpos($result, 'topic/a');
    expect(ord($result[$posA + strlen('topic/a')]))->toBe(0);

    $posB = strpos($result, 'topic/b');
    expect(ord($result[$posB + strlen('topic/b')]))->toBe(1);

    $posC = strpos($result, 'topic/c');
    expect(ord($result[$posC + strlen('topic/c')]))->toBe(2);
});

test('encodeSubscribe skips empty filters', function (): void {
    $encoder = new Encoder();
    $filters = [
        ['filter' => '', 'qos' => 0],
        ['filter' => 'valid/topic', 'qos' => 1],
    ];
    $result = $encoder->encodeSubscribe($filters, 1);

    // Only the valid topic should be present
    expect(str_contains($result, 'valid/topic'))->toBeTrue();

    // The remaining length should reflect only one filter entry
    // Variable header: 2 bytes (packet ID)
    // Payload: 2 (len) + 11 (valid/topic) + 1 (qos) = 14
    // Total remaining: 16
    // Verify by decoding remaining length from byte 1
    $remainingLength = ord($result[1]);
    expect($remainingLength)->toBe(16);
});

test('encodeSubscribe clamps QoS to 0-2 range', function (): void {
    $encoder = new Encoder();

    // QoS > 2 should be clamped to 2
    $filters = [['filter' => 'test/topic', 'qos' => 5]];
    $result  = $encoder->encodeSubscribe($filters, 1);

    $topicPos = strpos($result, 'test/topic');
    $qosByte  = ord($result[$topicPos + strlen('test/topic')]);
    expect($qosByte)->toBe(2);

    // QoS < 0 should be clamped to 0
    $filters2 = [['filter' => 'test/topic', 'qos' => -1]];
    $result2  = $encoder->encodeSubscribe($filters2, 1);

    $topicPos2 = strpos($result2, 'test/topic');
    $qosByte2  = ord($result2[$topicPos2 + strlen('test/topic')]);
    expect($qosByte2)->toBe(0);
});

test('encodeUnsubscribe single filter', function (): void {
    $encoder = new Encoder();
    $result  = $encoder->encodeUnsubscribe(['test/topic'], 5);

    // First byte: UNSUBSCRIBE type (10) with reserved flags 0x02
    $firstByte = ord($result[0]);
    expect($firstByte)->toBe((10 << 4) | 0x02);

    // Packet ID
    expect(str_contains($result, pack('n', 5)))->toBeTrue();

    // Topic filter
    expect(str_contains($result, 'test/topic'))->toBeTrue();
});

test('encodeUnsubscribe multiple filters', function (): void {
    $encoder = new Encoder();
    $result  = $encoder->encodeUnsubscribe(['topic/a', 'topic/b'], 7);

    expect(str_contains($result, 'topic/a'))->toBeTrue()
        ->and(str_contains($result, 'topic/b'))->toBeTrue();
});

test('encodeUnsubscribe skips empty filters', function (): void {
    $encoder = new Encoder();
    $result  = $encoder->encodeUnsubscribe(['', 'valid/topic'], 1);

    expect(str_contains($result, 'valid/topic'))->toBeTrue();

    // Remaining length: 2 (packet ID) + 2 (len) + 11 (valid/topic) = 15
    $remainingLength = ord($result[1]);
    expect($remainingLength)->toBe(15);
});
