<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Protocol\V311\Decoder;
use ScienceStories\Mqtt\Util\Bytes;

test('decodeConnAck success with return code 0', function (): void {
    $decoder = new Decoder();
    // byte 0: ack flags (0 = no session present), byte 1: return code 0 (accepted)
    $body   = "\x00\x00";
    $result = $decoder->decodeConnAck($body);

    expect($result->sessionPresent)->toBeFalse()
        ->and($result->returnCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});

test('decodeConnAck with session present', function (): void {
    $decoder = new Decoder();
    // byte 0: ack flags (bit 0 set = session present), byte 1: return code 0
    $body   = "\x01\x00";
    $result = $decoder->decodeConnAck($body);

    expect($result->sessionPresent)->toBeTrue()
        ->and($result->returnCode)->toBe(0);
});

test('decodeConnAck with failure code not authorized', function (): void {
    $decoder = new Decoder();
    // byte 0: ack flags 0, byte 1: return code 5 (not authorized)
    $body   = "\x00\x05";
    $result = $decoder->decodeConnAck($body);

    expect($result->sessionPresent)->toBeFalse()
        ->and($result->returnCode)->toBe(5);
});

test('decodeConnAck too short throws ProtocolError', function (): void {
    $decoder = new Decoder();
    $decoder->decodeConnAck("\x00");
})->throws(ProtocolError::class);

test('decodeSubAck with return codes', function (): void {
    $decoder = new Decoder();
    // 2-byte packet ID (42) + return codes: QoS 0, QoS 1, QoS 2, failure (0x80)
    $body   = pack('n', 42) . "\x00\x01\x02\x80";
    $result = $decoder->decodeSubAck($body);

    expect($result->packetId)->toBe(42)
        ->and($result->returnCodes)->toBe([0, 1, 2, 0x80]);
});

test('decodeSubAck too short throws ProtocolError', function (): void {
    $decoder = new Decoder();
    $decoder->decodeSubAck("\x00");
})->throws(ProtocolError::class);

test('decodePublish QoS 0', function (): void {
    $decoder = new Decoder();
    // flags: 0x00 (QoS 0, no retain, no dup)
    // body: 2-byte topic length + topic + payload
    $body   = Bytes::encodeString('sensor/temp') . 'hello';
    $result = $decoder->decodePublish(0x00, $body);

    expect($result->topic)->toBe('sensor/temp')
        ->and($result->payload)->toBe('hello')
        ->and($result->qos)->toBe(QoS::AtMostOnce)
        ->and($result->retain)->toBeFalse()
        ->and($result->dup)->toBeFalse()
        ->and($result->packetId)->toBeNull()
        ->and($result->properties)->toBeNull();
});

test('decodePublish QoS 1', function (): void {
    $decoder = new Decoder();
    // flags: 0x02 (QoS 1)
    // body: topic + 2-byte packet ID + payload
    $body   = Bytes::encodeString('sensor/temp') . pack('n', 100) . 'data';
    $result = $decoder->decodePublish(0x02, $body);

    expect($result->topic)->toBe('sensor/temp')
        ->and($result->payload)->toBe('data')
        ->and($result->qos)->toBe(QoS::AtLeastOnce)
        ->and($result->packetId)->toBe(100);
});

test('decodePublish QoS 2', function (): void {
    $decoder = new Decoder();
    // flags: 0x04 (QoS 2)
    $body   = Bytes::encodeString('sensor/temp') . pack('n', 200) . 'payload';
    $result = $decoder->decodePublish(0x04, $body);

    expect($result->topic)->toBe('sensor/temp')
        ->and($result->payload)->toBe('payload')
        ->and($result->qos)->toBe(QoS::ExactlyOnce)
        ->and($result->packetId)->toBe(200);
});

test('decodePublish with retain flag', function (): void {
    $decoder = new Decoder();
    // flags: 0x01 (retain bit set, QoS 0)
    $body   = Bytes::encodeString('sensor/temp') . 'retained';
    $result = $decoder->decodePublish(0x01, $body);

    expect($result->retain)->toBeTrue()
        ->and($result->qos)->toBe(QoS::AtMostOnce);
});

test('decodePublish with dup flag', function (): void {
    $decoder = new Decoder();
    // flags: 0x0A (dup bit 3 set + QoS 1 in bits 1-2)
    $body   = Bytes::encodeString('sensor/temp') . pack('n', 50) . 'dup-msg';
    $result = $decoder->decodePublish(0x0A, $body);

    expect($result->dup)->toBeTrue()
        ->and($result->qos)->toBe(QoS::AtLeastOnce)
        ->and($result->packetId)->toBe(50);
});

test('decodeUnsubAck with packetId', function (): void {
    $decoder = new Decoder();
    $body    = pack('n', 123);
    $result  = $decoder->decodeUnsubAck($body);

    expect($result->packetId)->toBe(123);
});

test('decodePubAck with packetId', function (): void {
    $decoder = new Decoder();
    $body    = pack('n', 300);
    $result  = $decoder->decodePubAck($body);

    expect($result->packetId)->toBe(300)
        ->and($result->reasonCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});

test('decodePubRec with packetId', function (): void {
    $decoder = new Decoder();
    $body    = pack('n', 400);
    $result  = $decoder->decodePubRec($body);

    expect($result->packetId)->toBe(400)
        ->and($result->reasonCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});

test('decodePubRel with packetId', function (): void {
    $decoder = new Decoder();
    $body    = pack('n', 500);
    $result  = $decoder->decodePubRel($body);

    expect($result->packetId)->toBe(500)
        ->and($result->reasonCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});

test('decodePubComp with packetId', function (): void {
    $decoder = new Decoder();
    $body    = pack('n', 600);
    $result  = $decoder->decodePubComp($body);

    expect($result->packetId)->toBe(600)
        ->and($result->reasonCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});

test('decodeDisconnect returns reason code 0', function (): void {
    $decoder = new Decoder();
    $result  = $decoder->decodeDisconnect('');

    expect($result->reasonCode)->toBe(0)
        ->and($result->properties)->toBeNull();
});
