<?php

declare(strict_types=1);

/**
 * Benchmark: per-operation cost of two hot paths whose cost grows with payload size.
 *
 *  - MQTT 5 PUBLISH decode. Every property-length read used to substr() the remainder of
 *    the packet, payload included, so decoding copied the payload once more than needed.
 *  - WebSocket client-frame masking. Every byte went through chr()/ord() in a PHP loop.
 *
 * Both are pure CPU — no sockets — so the numbers are stable across runs. The masking
 * case calls the transport's private encodeFrame() through reflection: benchmarks are not
 * public API, and going through a socket would measure the kernel instead.
 *
 * Usage: php benchmarks/hot-paths.php
 */

require_once __DIR__ . '/../vendor/autoload.php';

use ScienceStories\Mqtt\Protocol\V5\Decoder as V5Decoder;
use ScienceStories\Mqtt\Transport\WsTransport;
use ScienceStories\Mqtt\Util\Bytes;

const REPEATS = 5;

/** @return float best seconds per call */
function bestOf(callable $fn, int $iterations): float
{
    $fn(); // warm-up
    $best = INF;
    for ($r = 0; $r < REPEATS; $r++) {
        $start = hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $fn();
        }
        $best = min($best, (hrtime(true) - $start) / 1e9 / $iterations);
    }

    return $best;
}

function report(string $label, float $seconds, int $bytes): void
{
    printf("  %-34s %12.2f µs   %9.1f MiB/s\n", $label, $seconds * 1e6, $bytes / $seconds / 1_048_576);
}

echo "=== Hot paths (pure CPU) ===\n";
printf("PHP %s, best of %d\n\n", PHP_VERSION, REPEATS);

echo "--- MQTT 5.0 PUBLISH decode (QoS 1, 2 properties) ---\n";
$decoder = new V5Decoder();
foreach ([64, 16_384, 262_144, 1_048_576] as $size) {
    $properties = "\x01\x01" . "\x03" . Bytes::encodeString('application/json');
    $body       = Bytes::encodeString('bench/topic') . pack('n', 7)
        . Bytes::encodeVarInt(strlen($properties)) . $properties
        . str_repeat('p', $size);
    $iterations = max(20, intdiv(20_000_000, $size + 1_000));
    report(number_format($size) . ' B payload', bestOf(static fn () => $decoder->decodePublish(0x02, $body), $iterations), $size);
}

echo "\n--- WebSocket client frame (mask + header) ---\n";
$ws     = new WsTransport();
$encode = new ReflectionMethod(WsTransport::class, 'encodeFrame');
foreach ([64, 16_384, 262_144, 1_048_576] as $size) {
    $payload    = str_repeat('w', $size);
    $iterations = max(5, intdiv(2_000_000, $size + 1_000));
    report(number_format($size) . ' B payload', bestOf(static fn () => $encode->invoke($ws, $payload, 0x02), $iterations), $size);
}
