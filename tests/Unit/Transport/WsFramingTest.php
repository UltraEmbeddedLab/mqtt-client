<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Tests\Support\Loopback;
use ScienceStories\Mqtt\Transport\WsTransport;

/**
 * Framing is tested below the HTTP upgrade: the handshake needs a server that answers
 * with a key derived from the client's random one, which a single-process test cannot
 * provide before open() returns. So the transport is handed an already-connected stream.
 *
 * @return array{0: WsTransport, 1: resource}
 */
function wsPair(): array
{
    $loopback = Loopback::listener();
    $client   = stream_socket_client("tcp://{$loopback->host}:{$loopback->port}", $errno, $errstr, 5);
    if ($client === false) {
        throw new RuntimeException($errstr);
    }
    $transport = new WsTransport();
    new ReflectionProperty(WsTransport::class, 'stream')->setValue($transport, $client);

    return [$transport, $loopback->accept()];
}

/** An unmasked server-to-client binary frame (RFC 6455 §5.2). */
function serverFrame(string $payload): string
{
    $length = strlen($payload);
    $header = "\x82";
    if ($length < 126) {
        $header .= chr($length);
    } elseif ($length < 65536) {
        $header .= chr(126).pack('n', $length);
    } else {
        $header .= chr(127).pack('J', $length);
    }

    return $header.$payload;
}

/**
 * Decode a client frame the slow, obvious way — an independent reference for the
 * vectorised mask in the transport.
 *
 * @return array{opcode: int, payload: string}
 */
function decodeClientFrame(string $frame): array
{
    $length = ord($frame[1]) & 0x7F;
    $offset = 2;
    if ($length === 126) {
        $length = unpack('n', $frame, 2)[1];
        $offset = 4;
    } elseif ($length === 127) {
        $length = unpack('J', $frame, 2)[1];
        $offset = 10;
    }
    $mask    = substr($frame, $offset, 4);
    $payload = '';
    for ($i = 0; $i < $length; $i++) {
        $payload .= chr(ord($frame[$offset + 4 + $i]) ^ ord($mask[$i % 4]));
    }

    return ['opcode' => ord($frame[0]) & 0x0F, 'payload' => $payload];
}

describe('WebSocket client frames', function (): void {
    it('masks payloads of every length class correctly', function (int $length): void {
        $transport = new WsTransport();
        $encode    = new ReflectionMethod(WsTransport::class, 'encodeFrame');
        $payload   = $length === 0 ? '' : random_bytes($length);

        $frame = $encode->invoke($transport, $payload, 0x02);

        expect(ord($frame[0]))->toBe(0x82)                 // FIN + binary
            ->and(ord($frame[1]) & 0x80)->toBe(0x80)        // client frames are always masked
            ->and(decodeClientFrame($frame))->toBe(['opcode' => 0x02, 'payload' => $payload]);
    })->with([0, 1, 3, 4, 5, 125, 126, 127, 65_535, 65_536, 100_003]);
});

describe('WebSocket frames are read atomically', function (): void {
    it('keeps a frame split across a timeout and returns it whole', function (): void {
        // Header, length, mask and payload used to be separate blocking reads, so a
        // timeout during the payload discarded the header and desynchronised the stream.
        [$transport, $peer] = wsPair();
        $frame              = serverFrame(str_repeat('M', 300));

        fwrite($peer, substr($frame, 0, 3));
        usleep(20_000);
        expect(fn (): string => $transport->readExact(300, 0.05))->toThrow(Timeout::class);

        fwrite($peer, substr($frame, 3).serverFrame('next'));

        expect($transport->readExact(300, 1.0))->toBe(str_repeat('M', 300))
            ->and($transport->readExact(4, 1.0))->toBe('next');
    });

    it('treats a zero timeout as a poll', function (): void {
        [$transport, $peer] = wsPair();
        fwrite($peer, serverFrame("\x30\x00"));
        usleep(20_000);

        expect($transport->readExact(2, 0.0))->toBe("\x30\x00");
    });

    it('reassembles an MQTT packet spread over several WebSocket frames', function (): void {
        [$transport, $peer] = wsPair();
        fwrite($peer, serverFrame('ab').serverFrame('cd').serverFrame('e'));
        usleep(20_000);

        expect($transport->readExact(5, 1.0))->toBe('abcde');
    });

    it('answers a ping with a pong carrying the same payload', function (): void {
        [$transport, $peer] = wsPair();
        fwrite($peer, "\x89\x04ping".serverFrame('x'));
        usleep(20_000);

        expect($transport->readExact(1, 1.0))->toBe('x');
        usleep(20_000);

        expect(decodeClientFrame((string) fread($peer, 64)))->toBe(['opcode' => 0x0A, 'payload' => 'ping']);
    });
});

describe('fragmented WebSocket messages', function (): void {
    it('appends a binary message split across continuation frames, with a ping in between', function (): void {
        // The first fragment used to enter the MQTT stream and the continuations were
        // dropped, so the stream lost bytes mid-packet.
        [$transport, $peer] = wsPair();
        fwrite($peer, "\x02\x03abc"     // binary, FIN=0
            ."\x89\x00"                  // ping between fragments
            ."\x00\x02de"                // continuation, FIN=0
            ."\x80\x01f");               // continuation, FIN=1
        usleep(20_000);

        expect($transport->readExact(6, 1.0))->toBe('abcdef');
    });

    it('keeps a fragmented text message out of the MQTT byte stream', function (): void {
        [$transport, $peer] = wsPair();
        fwrite($peer, "\x01\x02no"       // text, FIN=0
            ."\x80\x02pe"                // continuation of the text, FIN=1
            .serverFrame('mqtt'));
        usleep(20_000);

        expect($transport->readExact(4, 1.0))->toBe('mqtt');
    });

    it('serves many small packets out of one large frame in order', function (): void {
        [$transport, $peer] = wsPair();
        $packets            = '';
        for ($i = 0; $i < 2_000; $i++) {
            $packets .= sprintf('%05d', $i);
        }
        fwrite($peer, serverFrame($packets));
        usleep(20_000);

        $out = '';
        for ($i = 0; $i < 2_000; $i++) {
            $out .= $transport->readExact(5, 1.0);
        }

        expect($out)->toBe($packets);
    });
});
