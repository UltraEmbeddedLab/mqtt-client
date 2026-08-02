<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\SubscribeOptions;
use ScienceStories\Mqtt\Client\WillOptions;
use ScienceStories\Mqtt\Protocol\Packet\Connect;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\Packet\Publish;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Protocol\V5\Encoder;
use ScienceStories\Mqtt\Util\Bytes;

test('encodeConnect creates valid packet with minimal options', function (): void {
    $encoder = new Encoder();
    $pkt     = new Connect(clientId: 'test-client');
    $data    = $encoder->encodeConnect($pkt);

    // First byte: CONNECT type (1) << 4 = 0x10
    expect(ord($data[0]))->toBe(0x10);

    // Variable header starts after fixed header (byte 0 + remaining length varint)
    // Find 'MQTT' protocol name in the packet
    expect(str_contains($data, 'MQTT'))->toBeTrue();

    // Protocol level should be 5
    $mqttPos       = strpos($data, 'MQTT');
    $protocolLevel = ord($data[$mqttPos + 4]);
    expect($protocolLevel)->toBe(5);
});

test('encodeConnect includes session_expiry_interval property', function (): void {
    $encoder = new Encoder();
    $pkt     = new Connect(clientId: 'test', properties: [
        'session_expiry_interval' => 3600,
    ]);
    $data = $encoder->encodeConnect($pkt);

    // Property identifier 0x11 should appear followed by 4-byte big-endian value
    $propId = chr(0x11);
    expect(str_contains($data, $propId . pack('N', 3600)))->toBeTrue();
});

test('encodeConnect includes receive_maximum property', function (): void {
    $encoder = new Encoder();
    $pkt     = new Connect(clientId: 'test', properties: [
        'receive_maximum' => 100,
    ]);
    $data = $encoder->encodeConnect($pkt);

    // Property identifier 0x21 followed by 2-byte big-endian value
    expect(str_contains($data, chr(0x21) . pack('n', 100)))->toBeTrue();
});

test('encodeConnect includes topic_alias_maximum property', function (): void {
    $encoder = new Encoder();
    $pkt     = new Connect(clientId: 'test', properties: [
        'topic_alias_maximum' => 50,
    ]);
    $data = $encoder->encodeConnect($pkt);

    // Property identifier 0x22 followed by 2-byte big-endian value
    expect(str_contains($data, chr(0x22) . pack('n', 50)))->toBeTrue();
});

test('encodeConnect with username and password sets correct flags', function (): void {
    $encoder = new Encoder();
    $pkt     = new Connect(clientId: 'test', username: 'user', password: 'pass');
    $data    = $encoder->encodeConnect($pkt);

    // Find connect flags byte (after MQTT + protocol level byte)
    $mqttPos   = strpos($data, 'MQTT');
    $flagsByte = ord($data[$mqttPos + 5]);

    // Username flag = 0x80, Password flag = 0x40, Clean Session = 0x02
    expect($flagsByte & 0x80)->toBe(0x80) // username flag
        ->and($flagsByte & 0x40)->toBe(0x40); // password flag

    // Payload should contain the username and password strings
    expect(str_contains($data, 'user'))->toBeTrue()
        ->and(str_contains($data, 'pass'))->toBeTrue();
});

test('encodeConnect with will sets will flag and encodes will topic and payload', function (): void {
    $encoder = new Encoder();
    $will    = new WillOptions(topic: 'will/topic', payload: 'will-message');
    $pkt     = new Connect(clientId: 'test', will: $will);
    $data    = $encoder->encodeConnect($pkt);

    // Find connect flags byte
    $mqttPos   = strpos($data, 'MQTT');
    $flagsByte = ord($data[$mqttPos + 5]);

    // Will flag = 0x04
    expect($flagsByte & 0x04)->toBe(0x04);

    // Will topic and payload should be in the packet
    expect(str_contains($data, 'will/topic'))->toBeTrue()
        ->and(str_contains($data, 'will-message'))->toBeTrue();
});

test('encodeConnect with will QoS 2 and retain sets correct flag bits', function (): void {
    $encoder = new Encoder();
    $will    = new WillOptions(topic: 'will/topic', payload: 'msg', qos: QoS::ExactlyOnce, retain: true);
    $pkt     = new Connect(clientId: 'test', will: $will);
    $data    = $encoder->encodeConnect($pkt);

    $mqttPos   = strpos($data, 'MQTT');
    $flagsByte = ord($data[$mqttPos + 5]);

    // Will flag = 0x04
    expect($flagsByte & 0x04)->toBe(0x04);
    // Will QoS 2 = bits 3-4 = 0x10 (value 2 << 3 = 0x10)
    expect(($flagsByte >> 3) & 0x03)->toBe(2);
    // Will retain = 0x20
    expect($flagsByte & 0x20)->toBe(0x20);
});

test('encodePublish QoS 0 creates valid packet without packetId', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(topic: 'test/topic', payload: 'hello', qos: QoS::AtMostOnce);
    $data    = $encoder->encodePublish($pkt);

    // First byte: PUBLISH type (3) << 4 = 0x30, QoS 0 = no extra bits
    expect(ord($data[0]) & 0xF0)->toBe(PacketType::PUBLISH->value << 4)
        ->and((ord($data[0]) >> 1) & 0x03)->toBe(0); // QoS 0

    // Packet should contain the topic and payload
    expect(str_contains($data, 'test/topic'))->toBeTrue()
        ->and(str_contains($data, 'hello'))->toBeTrue();
});

test('encodePublish QoS 1 includes packetId', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(topic: 'test/topic', payload: 'hello', qos: QoS::AtLeastOnce, packetId: 42);
    $data    = $encoder->encodePublish($pkt);

    // QoS 1 flag bits
    expect((ord($data[0]) >> 1) & 0x03)->toBe(1);

    // PacketId 42 should be encoded as big-endian 2-byte integer after the topic
    expect(str_contains($data, pack('n', 42)))->toBeTrue();
});

test('encodePublish QoS 2 includes packetId', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(topic: 'test/topic', payload: 'hello', qos: QoS::ExactlyOnce, packetId: 1000);
    $data    = $encoder->encodePublish($pkt);

    // QoS 2 flag bits
    expect((ord($data[0]) >> 1) & 0x03)->toBe(2);

    // PacketId should be present
    expect(str_contains($data, pack('n', 1000)))->toBeTrue();
});

test('encodePublish throws LogicException when QoS > 0 without packetId', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(topic: 'test/topic', payload: 'hello', qos: QoS::AtLeastOnce);
    $encoder->encodePublish($pkt);
})->throws(LogicException::class);

test('encodePublish with retain flag sets bit 0', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(topic: 'test/topic', payload: 'hello', qos: QoS::AtMostOnce, retain: true);
    $data    = $encoder->encodePublish($pkt);

    expect(ord($data[0]) & 0x01)->toBe(1);
});

test('encodePublish with dup flag sets bit 3', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(topic: 'test/topic', payload: 'hello', qos: QoS::AtLeastOnce, dup: true, packetId: 1);
    $data    = $encoder->encodePublish($pkt);

    expect(ord($data[0]) & 0x08)->toBe(0x08);
});

test('encodePublish with properties includes topic_alias, content_type, and user_properties', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(
        topic: 'test/topic',
        payload: 'hello',
        qos: QoS::AtMostOnce,
        properties: [
            'topic_alias'     => 5,
            'content_type'    => 'application/json',
            'user_properties' => ['key1' => 'val1'],
        ],
    );
    $data = $encoder->encodePublish($pkt);

    // topic_alias property (0x23) + 2-byte value
    expect(str_contains($data, chr(0x23) . pack('n', 5)))->toBeTrue();
    // content_type property (0x03) + encoded string
    expect(str_contains($data, chr(0x03) . Bytes::encodeString('application/json')))->toBeTrue();
    // user_property (0x26)
    expect(str_contains($data, chr(0x26) . Bytes::encodeString('key1') . Bytes::encodeString('val1')))->toBeTrue();
});

test('encodeSubscribe creates valid packet with single filter', function (): void {
    $encoder = new Encoder();
    $data    = $encoder->encodeSubscribe(
        [['filter' => 'test/topic', 'qos' => 1]],
        packetId: 1,
    );

    // First byte: SUBSCRIBE type (8) << 4 | 0x02 = 0x82
    expect(ord($data[0]))->toBe(0x82);

    // Should contain the topic filter
    expect(str_contains($data, 'test/topic'))->toBeTrue();
});

test('encodeSubscribe with multiple filters', function (): void {
    $encoder = new Encoder();
    $data    = $encoder->encodeSubscribe(
        [
            ['filter' => 'topic/a', 'qos' => 0],
            ['filter' => 'topic/b', 'qos' => 1],
            ['filter' => 'topic/c', 'qos' => 2],
        ],
        packetId: 10,
    );

    expect(str_contains($data, 'topic/a'))->toBeTrue()
        ->and(str_contains($data, 'topic/b'))->toBeTrue()
        ->and(str_contains($data, 'topic/c'))->toBeTrue();
});

test('encodeSubscribe with SubscribeOptions sets noLocal, retainAsPublished, retainHandling bits', function (): void {
    $encoder = new Encoder();
    $options = new SubscribeOptions(noLocal: true, retainAsPublished: true, retainHandling: 2);
    $data    = $encoder->encodeSubscribe(
        [['filter' => 'test/topic', 'qos' => 1]],
        packetId: 1,
        options: $options,
    );

    // Find the subscription options byte (last byte after the topic filter string)
    // The topic is encoded as 2-byte length + "test/topic" (10 bytes), then 1-byte options
    // Options byte should have: QoS=1 (bits 0-1), noLocal (bit 2), retainAsPublished (bit 3), retainHandling=2 (bits 4-5)
    // Expected: 0b00101101 = 0x2D = 1 | 0x04 | 0x08 | (2 << 4) = 1 + 4 + 8 + 32 = 45
    $expectedOptsByte = 1 | 0x04 | 0x08 | (2 << 4);
    $lastByte         = ord($data[strlen($data) - 1]);
    expect($lastByte)->toBe($expectedOptsByte);
});

test('encodeSubscribe with user_properties in options', function (): void {
    $encoder = new Encoder();
    $options = new SubscribeOptions(properties: [
        'user_properties' => ['myKey' => 'myVal'],
    ]);
    $data = $encoder->encodeSubscribe(
        [['filter' => 'test/topic', 'qos' => 0]],
        packetId: 1,
        options: $options,
    );

    // User property identifier 0x26 followed by key and value strings
    expect(str_contains($data, chr(0x26) . Bytes::encodeString('myKey') . Bytes::encodeString('myVal')))->toBeTrue();
});

test('encodeSubscribe skips empty filters', function (): void {
    $encoder = new Encoder();
    $data    = $encoder->encodeSubscribe(
        [
            ['filter' => '', 'qos' => 0],
            ['filter' => 'valid/topic', 'qos' => 1],
        ],
        packetId: 1,
    );

    // Only the valid topic should be present
    expect(str_contains($data, 'valid/topic'))->toBeTrue();
});

test('encodeUnsubscribe creates valid packet', function (): void {
    $encoder = new Encoder();
    $data    = $encoder->encodeUnsubscribe(['test/topic'], packetId: 5);

    // First byte: UNSUBSCRIBE type (10) << 4 | 0x02 = 0xA2
    expect(ord($data[0]))->toBe(0xA2);

    // Should contain packetId and topic
    expect(str_contains($data, pack('n', 5)))->toBeTrue()
        ->and(str_contains($data, 'test/topic'))->toBeTrue();
});

test('encodeUnsubscribe with multiple filters', function (): void {
    $encoder = new Encoder();
    $data    = $encoder->encodeUnsubscribe(['topic/a', 'topic/b', 'topic/c'], packetId: 7);

    expect(str_contains($data, 'topic/a'))->toBeTrue()
        ->and(str_contains($data, 'topic/b'))->toBeTrue()
        ->and(str_contains($data, 'topic/c'))->toBeTrue();
});

test('encodeUnsubscribe skips empty filters', function (): void {
    $encoder = new Encoder();
    $data    = $encoder->encodeUnsubscribe(['', 'valid/topic'], packetId: 1);

    expect(str_contains($data, 'valid/topic'))->toBeTrue();
});

test('type coercion rejects array input with InvalidArgumentException', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(
        topic: 'test',
        payload: 'data',
        qos: QoS::AtMostOnce,
        properties: [
            'topic_alias' => [1, 2, 3],
        ],
    );
    $encoder->encodePublish($pkt);
})->throws(InvalidArgumentException::class);

test('type coercion converts valid numeric string correctly', function (): void {
    $encoder = new Encoder();
    $pkt     = new Publish(
        topic: 'test',
        payload: 'data',
        qos: QoS::AtMostOnce,
        properties: [
            'topic_alias' => '42',
        ],
    );
    $data = $encoder->encodePublish($pkt);

    // topic_alias 42 encoded as 2-byte big-endian after property identifier 0x23
    expect(str_contains($data, chr(0x23) . pack('n', 42)))->toBeTrue();
});
