<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Protocol\Packet\Connect;
use ScienceStories\Mqtt\Protocol\V311\Decoder as V311Decoder;
use ScienceStories\Mqtt\Protocol\V311\Encoder as V311Encoder;
use ScienceStories\Mqtt\Protocol\V5\Decoder as V5Decoder;
use ScienceStories\Mqtt\Protocol\V5\Encoder as V5Encoder;

/** Offset of the Connect Flags byte in a 3.1.1 CONNECT: fixed header (2) + "MQTT" (6) + level (1). */
const V311_CONNECT_FLAGS_OFFSET = 9;

describe('MQTT 3.1.1 CONNECT flags', function (): void {
    it('rejects a password without a username (MQTT-3.1.2-22)', function (): void {
        $encoder = new V311Encoder();

        expect(fn (): string => $encoder->encodeConnect(
            new Connect('cid', 60, true, null, 'secret'),
        ))->toThrow(ProtocolError::class);
    });

    it('sets both flags when username and password are supplied', function (): void {
        $bytes = new V311Encoder()->encodeConnect(
            new Connect('cid', 60, true, 'user', 'secret'),
        );
        $flags = ord($bytes[V311_CONNECT_FLAGS_OFFSET]);

        expect($flags & 0x80)->toBe(0x80)
            ->and($flags & 0x40)->toBe(0x40);
    });

    it('allows a username without a password', function (): void {
        $bytes = new V311Encoder()->encodeConnect(
            new Connect('cid', 60, true, 'user'),
        );
        $flags = ord($bytes[V311_CONNECT_FLAGS_OFFSET]);

        expect($flags & 0x80)->toBe(0x80)
            ->and($flags & 0x40)->toBe(0);
    });

    it('allows a password without a username on MQTT 5', function (): void {
        $bytes = new V5Encoder()->encodeConnect(
            new Connect('cid', 60, true, null, 'secret'),
        );

        expect($bytes)->toBeString()->not->toBeEmpty();
    });
});

describe('keep alive encoding', function (): void {
    it('rejects a keep alive above 65535 instead of truncating it', function (): void {
        $encoder = new V311Encoder();

        expect(fn (): string => $encoder->encodeConnect(new Connect('cid', 86400, true)))
            ->toThrow(ProtocolError::class);
    });

    it('rejects a keep alive above 65535 on MQTT 5 as well', function (): void {
        $encoder = new V5Encoder();

        expect(fn (): string => $encoder->encodeConnect(new Connect('cid', 86400, true)))
            ->toThrow(ProtocolError::class);
    });

    it('rejects a negative keep alive', function (): void {
        $encoder = new V311Encoder();

        expect(fn (): string => $encoder->encodeConnect(new Connect('cid', -1, true)))
            ->toThrow(ProtocolError::class);
    });

    it('encodes the boundary value 65535 unchanged', function (): void {
        $bytes     = new V311Encoder()->encodeConnect(new Connect('cid', 65535, true));
        $keepAlive = unpack('n', substr($bytes, V311_CONNECT_FLAGS_OFFSET + 1, 2))[1];

        expect($keepAlive)->toBe(65535);
    });
});

describe('malformed inbound packets', function (): void {
    it('raises ProtocolError, not ValueError, for QoS bits 0b11 on 3.1.1', function (): void {
        $decoder = new V311Decoder();
        $body    = pack('n', 3) . 'a/b' . 'x';

        expect(fn (): mixed => $decoder->decodePublish(0x06, $body))
            ->toThrow(ProtocolError::class);
    });

    it('raises ProtocolError, not ValueError, for QoS bits 0b11 on MQTT 5', function (): void {
        $decoder = new V5Decoder();
        $body    = pack('n', 3) . 'a/b' . "\x00" . 'x';

        expect(fn (): mixed => $decoder->decodePublish(0x06, $body))
            ->toThrow(ProtocolError::class);
    });

    it('keeps every malformed-packet failure inside the MqttException hierarchy', function (): void {
        $decoder = new V311Decoder();

        try {
            $decoder->decodePublish(0x06, pack('n', 3) . 'a/b');
            $thrown = null;
        } catch (Throwable $e) {
            $thrown = $e;
        }

        expect($thrown)->toBeInstanceOf(ScienceStories\Mqtt\Exception\MqttException::class);
    });
});
