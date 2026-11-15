<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Util\Bytes;

test('encodeVarInt encodes small values correctly', function (): void {
    expect(Bytes::encodeVarInt(0))->toBe("\x00")
        ->and(Bytes::encodeVarInt(127))->toBe("\x7F")
        ->and(Bytes::encodeVarInt(128))->toBe("\x80\x01");
});

test('encodeVarInt encodes larger values correctly', function (): void {
    $encoded = Bytes::encodeVarInt(16383);
    expect($encoded)->toBe("\xFF\x7F");
});

test('encodeVarInt throws exception for negative values', function (): void {
    Bytes::encodeVarInt(-1);
})->throws(ProtocolError::class);

test('encodeVarInt throws exception for values too large', function (): void {
    Bytes::encodeVarInt(268_435_456);
})->throws(ProtocolError::class);

test('decodeVarInt decodes small values correctly', function (): void {
    $consumed = 0;
    $value    = Bytes::decodeVarInt("\x00", $consumed);
    expect($value)->toBe(0)
        ->and($consumed)->toBe(1);
});

test('decodeVarInt decodes multi-byte values correctly', function (): void {
    $consumed = 0;
    $value    = Bytes::decodeVarInt("\x80\x01", $consumed);
    expect($value)->toBe(128)
        ->and($consumed)->toBe(2);
});

test('decodeVarInt throws exception for malformed data', function (): void {
    $consumed = 0;
    Bytes::decodeVarInt("\x80", $consumed);
})->throws(ProtocolError::class);

test('encodeString encodes empty string', function (): void {
    $encoded = Bytes::encodeString('');
    expect($encoded)->toBe("\x00\x00");
});

test('encodeString encodes simple string', function (): void {
    $encoded = Bytes::encodeString('test');
    expect($encoded)->toBe("\x00\x04test");
});

test('encodeString throws exception for too long string', function (): void {
    $longString = str_repeat('a', 65536);
    Bytes::encodeString($longString);
})->throws(ProtocolError::class);

test('decodeString decodes simple string', function (): void {
    $offset  = 0;
    $decoded = Bytes::decodeString("\x00\x04test", $offset);
    expect($decoded)->toBe('test')
        ->and($offset)->toBe(6);
});

test('decodeString decodes empty string', function (): void {
    $offset  = 0;
    $decoded = Bytes::decodeString("\x00\x00", $offset);
    expect($decoded)->toBe('')
        ->and($offset)->toBe(2);
});

test('decodeString throws exception for malformed data', function (): void {
    $offset = 0;
    Bytes::decodeString("\x00", $offset);
})->throws(ProtocolError::class);

test('encode and decode string roundtrip', function (): void {
    $original = 'Hello MQTT!';
    $encoded  = Bytes::encodeString($original);
    $offset   = 0;
    $decoded  = Bytes::decodeString($encoded, $offset);
    expect($decoded)->toBe($original);
});

test('encode and decode varInt roundtrip', function (): void {
    $values = [0, 1, 127, 128, 16383, 16384, 2097151, 268435455];
    foreach ($values as $original) {
        $encoded  = Bytes::encodeVarInt($original);
        $consumed = 0;
        $decoded  = Bytes::decodeVarInt($encoded, $consumed);
        expect($decoded)->toBe($original);
    }
});

test('decodeVarInt reads in place at an offset without copying the remainder', function (): void {
    // The MQTT 5 decoder used to substr() the rest of the packet — payload included — just
    // to decode a one-to-four byte property length.
    $consumed = 0;
    $value    = Bytes::decodeVarInt("\xFF\xFF\x80\x01payload", $consumed, 2);

    expect($value)->toBe(128)
        ->and($consumed)->toBe(2);
});

test('decodeVarInt rejects a negative offset instead of reading from the end', function (): void {
    $consumed = 0;
    Bytes::decodeVarInt("\x05", $consumed, -1);
})->throws(ProtocolError::class);
