<?php

declare(strict_types=1);

/**
 * Benchmark: MQTT packet ENCODING throughput.
 *
 * This measures the codec and nothing else. No socket is opened, no broker is involved,
 * and no acknowledgement is awaited — so the numbers are an upper bound on the protocol
 * layer, not a publish rate. Real throughput against a broker is bounded by the network,
 * TLS record processing and the QoS 1/2 round trip, all of which are orders of magnitude
 * slower than what is measured here. Do not quote these figures as "messages per second".
 *
 * Two things the previous version of this script got wrong, worth stating so they are not
 * reintroduced:
 *   - It re-encoded one shared payload string in a tight loop. PHP's copy-on-write means
 *     the payload is never actually copied, which flatters the result. Payloads are now
 *     distinct per iteration.
 *   - It printed the byte rate of that loop as "MB/sec", which read as wire throughput and
 *     produced a physically impossible figure.
 *
 * Usage: php benchmarks/encode-throughput.php
 */

require_once __DIR__ . '/../vendor/autoload.php';

use ScienceStories\Mqtt\Protocol\Packet\Publish;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Protocol\V311\Encoder as V311Encoder;
use ScienceStories\Mqtt\Protocol\V5\Encoder as V5Encoder;

const WARMUP_ITERATIONS = 2_000;
const REPEATS           = 5;

/**
 * Run $fn $iterations times, $repeats times over, and return the best wall-clock seconds.
 *
 * The minimum is the honest statistic for a microbenchmark: it is the run least disturbed
 * by GC, scheduling and turbo-clock variation. The spread is reported so a noisy machine
 * is visible rather than silently averaged away.
 *
 * @param  callable(int): void  $fn
 * @return array{best: float, worst: float}
 */
function measure(callable $fn, int $iterations, int $repeats = REPEATS): array
{
    // Warm up the VM, the opcode cache and any lazily-built encoder state.
    for ($i = 0; $i < WARMUP_ITERATIONS; $i++) {
        $fn($i);
    }

    $timings = [];
    for ($r = 0; $r < $repeats; $r++) {
        $start = hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $fn($i);
        }
        $timings[] = (hrtime(true) - $start) / 1e9;
    }

    return ['best' => min($timings), 'worst' => max($timings)];
}

/** @param array{best: float, worst: float} $t */
function report(string $label, int $iterations, array $t): void
{
    $perSecond = $iterations / $t['best'];
    $spread    = $t['worst'] > 0.0 ? (($t['worst'] - $t['best']) / $t['worst']) * 100 : 0.0;

    printf(
        "  %-34s %9s packets/sec   (%.3f s best of %d, spread %.1f%%)\n",
        $label,
        number_format((int) $perSecond),
        $t['best'],
        REPEATS,
        $spread,
    );
}

echo "=== MQTT encoder throughput ===\n";
printf("PHP %s, %s iterations per measurement, best of %d\n", PHP_VERSION, number_format(50_000), REPEATS);
echo "Encoder only — no I/O, no broker, no acknowledgements.\n\n";

$v311 = new V311Encoder();
$v5   = new V5Encoder();

// Distinct payloads so copy-on-write does not hide the cost of handling real data.
$payloads = [];
for ($i = 0; $i < 256; $i++) {
    $payloads[] = 'Hello World payload for benchmarking #' . str_pad((string) $i, 4, '0', STR_PAD_LEFT);
}

$iterations = 50_000;

echo "--- Small payload (~40 bytes) ---\n";

report('MQTT 3.1.1 PUBLISH, QoS 0', $iterations, measure(
    function (int $i) use ($v311, $payloads): void {
        $v311->encodePublish(new Publish('bench/test/topic', $payloads[$i & 255], QoS::AtMostOnce, false, false));
    },
    $iterations,
));

report('MQTT 3.1.1 PUBLISH, QoS 1', $iterations, measure(
    function (int $i) use ($v311, $payloads): void {
        $v311->encodePublish(new Publish('bench/test/topic', $payloads[$i & 255], QoS::AtLeastOnce, false, false, packetId: ($i % 65535) + 1));
    },
    $iterations,
));

report('MQTT 5.0 PUBLISH, QoS 0', $iterations, measure(
    function (int $i) use ($v5, $payloads): void {
        $v5->encodePublish(new Publish('bench/test/topic', $payloads[$i & 255], QoS::AtMostOnce, false, false));
    },
    $iterations,
));

report('MQTT 5.0 PUBLISH, QoS 1 + 5 properties', $iterations, measure(
    function (int $i) use ($v5, $payloads): void {
        $v5->encodePublish(new Publish(
            'bench/test/topic',
            $payloads[$i & 255],
            QoS::AtLeastOnce,
            false,
            false,
            packetId: ($i % 65535) + 1,
            properties: [
                'payload_format_indicator' => 1,
                'message_expiry_interval'  => 3600,
                'content_type'             => 'application/json',
                'response_topic'           => 'bench/response',
                'correlation_data'         => 'req-12345',
            ],
        ));
    },
    $iterations,
));

echo "\n--- Payload size impact (MQTT 3.1.1, QoS 0) ---\n";

foreach ([32, 128, 512, 1024, 4096, 16384, 65536] as $size) {
    // A distinct buffer per iteration slot, so each encode handles a string the engine has
    // not already got a shared reference to.
    $buffers = [];
    for ($i = 0; $i < 16; $i++) {
        $buffers[] = str_repeat(chr(65 + $i), $size);
    }

    $iters = $size > 4096 ? 5_000 : 25_000;
    $t     = measure(
        function (int $i) use ($v311, $buffers): void {
            $v311->encodePublish(new Publish('bench/test', $buffers[$i & 15], QoS::AtMostOnce, false, false));
        },
        $iters,
    );

    $bytesPerSecond = ($iters * $size) / $t['best'];

    printf(
        "  %6s byte payload   %9s packets/sec   %8.1f MiB/sec of encoder output\n",
        number_format($size),
        number_format((int) ($iters / $t['best'])),
        $bytesPerSecond / 1024 / 1024,
    );
}

echo "\nMiB/sec above is the rate at which this process can build packet bytes in memory.\n";
echo "It is not network throughput and must not be published as such.\n";
