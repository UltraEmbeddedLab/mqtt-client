<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Protocol\V5\Decoder;
use ScienceStories\Mqtt\Util\Bytes;

test('decodeConnAck with success and no session present', function (): void {
    $decoder = new Decoder();
    // ackFlags=0x00, reasonCode=0x00, properties length=0
    $body   = chr(0x00) . chr(0x00) . Bytes::encodeVarInt(0);
    $result = $decoder->decodeConnAck($body);

    expect($result->sessionPresent)->toBeFalse()
        ->and($result->returnCode)->toBe(0)
        ->and($result->isSuccess())->toBeTrue();
});

test('decodeConnAck with session present flag', function (): void {
    $decoder = new Decoder();
    // ackFlags=0x01 (session present), reasonCode=0x00, properties length=0
    $body   = chr(0x01) . chr(0x00) . Bytes::encodeVarInt(0);
    $result = $decoder->decodeConnAck($body);

    expect($result->sessionPresent)->toBeTrue()
        ->and($result->returnCode)->toBe(0);
});

test('decodeConnAck with reason code 0x86 not authorized', function (): void {
    $decoder = new Decoder();
    // ackFlags=0x00, reasonCode=0x86 (134 = Bad User Name or Password), properties length=0
    $body   = chr(0x00) . chr(0x86) . Bytes::encodeVarInt(0);
    $result = $decoder->decodeConnAck($body);

    expect($result->sessionPresent)->toBeFalse()
        ->and($result->returnCode)->toBe(0x86)
        ->and($result->isSuccess())->toBeFalse();
});

test('decodeConnAck too short throws ProtocolError', function (): void {
    $decoder = new Decoder();
    $decoder->decodeConnAck(chr(0x00));
})->throws(ProtocolError::class);

test('decodeConnAck with properties', function (): void {
    $decoder = new Decoder();

    // Build properties: topic_alias_maximum (0x22) = 100, receive_maximum (0x21) = 50,
    // assigned_client_identifier (0x12) = "server-id"
    $props = chr(0x22) . pack('n', 100);            // topic_alias_maximum
    $props .= chr(0x21) . pack('n', 50);             // receive_maximum
    $props .= chr(0x12) . Bytes::encodeString('server-id'); // assigned_client_identifier

    $body   = chr(0x00) . chr(0x00) . Bytes::encodeVarInt(strlen($props)) . $props;
    $result = $decoder->decodeConnAck($body);

    expect($result->returnCode)->toBe(0)
        ->and($result->properties)->toBeArray()
        ->and($result->properties['topic_alias_maximum'])->toBe(100)
        ->and($result->properties['receive_maximum'])->toBe(50)
        ->and($result->properties['assigned_client_identifier'])->toBe('server-id');
});

test('decodeSubAck with success reason codes', function (): void {
    $decoder = new Decoder();
    // packetId=1, properties length=0, reason codes: 0x00, 0x01, 0x02
    $body   = pack('n', 1) . Bytes::encodeVarInt(0) . chr(0x00) . chr(0x01) . chr(0x02);
    $result = $decoder->decodeSubAck($body);

    expect($result->packetId)->toBe(1)
        ->and($result->returnCodes)->toBe([0x00, 0x01, 0x02])
        ->and($result->isSuccess())->toBeTrue();
});

test('decodeSubAck with failure reason code 0x80', function (): void {
    $decoder = new Decoder();
    // packetId=5, properties length=0, reason code: 0x80 (failure)
    $body   = pack('n', 5) . Bytes::encodeVarInt(0) . chr(0x80);
    $result = $decoder->decodeSubAck($body);

    expect($result->packetId)->toBe(5)
        ->and($result->returnCodes)->toBe([0x80])
        ->and($result->isSuccess())->toBeFalse();
});

test('decodeSubAck too short throws ProtocolError', function (): void {
    $decoder = new Decoder();
    // Only 2 bytes (packetId), missing properties length
    $decoder->decodeSubAck(pack('n', 1));
})->throws(ProtocolError::class);

test('decodePublish QoS 0 decodes topic and payload', function (): void {
    $decoder = new Decoder();
    // flags: QoS 0 = 0x00
    // body: topic length + topic + properties length (0) + payload
    $body   = Bytes::encodeString('sensor/temp') . Bytes::encodeVarInt(0) . '25.3';
    $result = $decoder->decodePublish(0x00, $body);

    expect($result->topic)->toBe('sensor/temp')
        ->and($result->payload)->toBe('25.3')
        ->and($result->qos)->toBe(QoS::AtMostOnce)
        ->and($result->packetId)->toBeNull()
        ->and($result->retain)->toBeFalse()
        ->and($result->dup)->toBeFalse();
});

test('decodePublish QoS 1 includes packetId', function (): void {
    $decoder = new Decoder();
    // flags: QoS 1 = 0x02 (bits 1-2)
    // body: topic + packetId + properties length (0) + payload
    $body   = Bytes::encodeString('sensor/temp') . pack('n', 42) . Bytes::encodeVarInt(0) . 'data';
    $result = $decoder->decodePublish(0x02, $body);

    expect($result->topic)->toBe('sensor/temp')
        ->and($result->payload)->toBe('data')
        ->and($result->qos)->toBe(QoS::AtLeastOnce)
        ->and($result->packetId)->toBe(42);
});

test('decodePublish QoS 2 includes packetId', function (): void {
    $decoder = new Decoder();
    // flags: QoS 2 = 0x04 (bits 1-2)
    $body   = Bytes::encodeString('sensor/temp') . pack('n', 999) . Bytes::encodeVarInt(0) . 'payload';
    $result = $decoder->decodePublish(0x04, $body);

    expect($result->qos)->toBe(QoS::ExactlyOnce)
        ->and($result->packetId)->toBe(999);
});

test('decodePublish with retain flag', function (): void {
    $decoder = new Decoder();
    // flags: retain = bit 0 = 0x01
    $body   = Bytes::encodeString('topic') . Bytes::encodeVarInt(0) . 'data';
    $result = $decoder->decodePublish(0x01, $body);

    expect($result->retain)->toBeTrue();
});

test('decodePublish with dup flag', function (): void {
    $decoder = new Decoder();
    // flags: dup = bit 3 = 0x08, also QoS 1 (0x02) since dup only applies to QoS>0
    $body   = Bytes::encodeString('topic') . pack('n', 1) . Bytes::encodeVarInt(0) . 'data';
    $result = $decoder->decodePublish(0x08 | 0x02, $body);

    expect($result->dup)->toBeTrue();
});

test('decodePublish with properties', function (): void {
    $decoder = new Decoder();

    // Build properties: content_type (0x03), user_property (0x26), topic_alias (0x23)
    $props = chr(0x03) . Bytes::encodeString('application/json');   // content_type
    $props .= chr(0x26) . Bytes::encodeString('key1') . Bytes::encodeString('val1'); // user_property
    $props .= chr(0x23) . pack('n', 7);  // topic_alias

    $body   = Bytes::encodeString('test/topic') . Bytes::encodeVarInt(strlen($props)) . $props . 'payload';
    $result = $decoder->decodePublish(0x00, $body);

    expect($result->topic)->toBe('test/topic')
        ->and($result->payload)->toBe('payload')
        ->and($result->properties)->toBeArray()
        ->and($result->properties['content_type'])->toBe('application/json')
        ->and($result->properties['user_properties'])->toBe(['key1' => 'val1'])
        ->and($result->properties['topic_alias'])->toBe(7);
});

test('decodeUnsubAck with reason codes', function (): void {
    $decoder = new Decoder();
    // packetId=10, properties length=0, reason codes: 0x00 (success), 0x11 (no subscription existed)
    $body   = pack('n', 10) . Bytes::encodeVarInt(0) . chr(0x00) . chr(0x11);
    $result = $decoder->decodeUnsubAck($body);

    expect($result->packetId)->toBe(10)
        ->and($result->reasonCodes)->toBe([0x00, 0x11])
        ->and($result->isSuccess())->toBeTrue();
});

test('decodePubAck minimal with only packetId defaults reasonCode to 0', function (): void {
    $decoder = new Decoder();
    // Only 2 bytes: packetId
    $body   = pack('n', 100);
    $result = $decoder->decodePubAck($body);

    expect($result->packetId)->toBe(100)
        ->and($result->reasonCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});

test('decodePubAck with reason code', function (): void {
    $decoder = new Decoder();
    // packetId + reasonCode
    $body   = pack('n', 100) . chr(0x10); // 0x10 = No matching subscribers
    $result = $decoder->decodePubAck($body);

    expect($result->packetId)->toBe(100)
        ->and($result->reasonCode)->toBe(0x10);
});

test('decodePubRec minimal with only packetId defaults reasonCode to 0', function (): void {
    $decoder = new Decoder();
    $body    = pack('n', 200);
    $result  = $decoder->decodePubRec($body);

    expect($result->packetId)->toBe(200)
        ->and($result->reasonCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});

test('decodePubRel minimal with only packetId defaults reasonCode to 0', function (): void {
    $decoder = new Decoder();
    $body    = pack('n', 300);
    $result  = $decoder->decodePubRel($body);

    expect($result->packetId)->toBe(300)
        ->and($result->reasonCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});

test('decodePubComp minimal with only packetId defaults reasonCode to 0', function (): void {
    $decoder = new Decoder();
    $body    = pack('n', 400);
    $result  = $decoder->decodePubComp($body);

    expect($result->packetId)->toBe(400)
        ->and($result->reasonCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});

test('decodeDisconnect empty body returns normal disconnect with reasonCode 0', function (): void {
    $decoder = new Decoder();
    $result  = $decoder->decodeDisconnect('');

    expect($result->reasonCode)->toBe(0x00)
        ->and($result->properties)->toBeNull()
        ->and($result->isNormal())->toBeTrue();
});

test('decodeDisconnect with reason code', function (): void {
    $decoder = new Decoder();
    // reasonCode = 0x8D (Keep Alive timeout), no properties
    $body   = chr(0x8D);
    $result = $decoder->decodeDisconnect($body);

    expect($result->reasonCode)->toBe(0x8D)
        ->and($result->isError())->toBeTrue();
});
