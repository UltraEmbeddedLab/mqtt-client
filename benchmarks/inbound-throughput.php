<?php

declare(strict_types=1);

/**
 * Benchmark: inbound PUBLISH processing cost through the real client and a real socket.
 *
 * Unlike encode-throughput.php this exercises the whole receive path — TcpTransport,
 * frame reading, decoding, de-duplication and delivery — through public API only, so the
 * same script measures any version of the library.
 *
 * Method. One process, one loopback TCP connection. The "broker" end is non-blocking: each
 * round it writes as many pre-built PUBLISH packets as the kernel accepts, and the client
 * then consumes exactly the packets that arrived complete. Only the client's calls are
 * timed, and every byte they need is already in the kernel buffer, so the figure is the
 * client's own cost — syscalls included — and never time spent waiting for a producer.
 * A two-process version of this benchmark was abandoned because, on a desktop OS, how the
 * scheduler interleaves producer and consumer swamped the thing being measured.
 *
 * The client is never connect()ed: loopOnce() needs only an open transport, and skipping
 * the handshake keeps a broker out of the picture. That is fine for a receive benchmark and
 * would be wrong for anything else.
 *
 * What the numbers are: per-message client cost on this machine. What they are not:
 * broker throughput, network throughput, or anything to quote for a deployment.
 *
 * Usage: php benchmarks/inbound-throughput.php
 */

require_once __DIR__ . '/../vendor/autoload.php';

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\InboundMessage;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Transport\TcpTransport;
use ScienceStories\Mqtt\Util\Bytes;

const REPEATS = 5;

function publishPacket(bool $v5, int $qos, int $packetId, string $payload): string
{
    $body = Bytes::encodeString('bench/inbound/topic');
    if ($qos > 0) {
        $body .= pack('n', $packetId);
    }
    if ($v5) {
        $body .= "\x00"; // empty property block
    }
    $body .= $payload;

    return chr(0x30 | ($qos << 1)) . Bytes::encodeVarInt(strlen($body)) . $body;
}

/**
 * @param  resource  $server  one listener for every run: closing a listening socket costs
 *                            500 ms on Windows, which PHP spends waiting for it to drain
 * @return float seconds the client spent receiving $count messages
 */
function runOnce($server, bool $v5, int $qos, int $payloadSize, int $count): float
{
    [$host, $port] = explode(':', (string) stream_socket_get_name($server, false));

    $transport = new TcpTransport();
    $transport->open($host, (int) $port);
    $peer = stream_socket_accept($server, 5);
    if ($peer === false) {
        throw new RuntimeException('accept failed');
    }
    stream_set_blocking($peer, false);
    // A packet only counts once the kernel holds all of it, so the send buffer must fit the
    // largest one. ext-sockets is optional here, as it is for the library.
    if (function_exists('socket_import_stream')) {
        $sock = socket_import_stream($peer);
        if ($sock !== false) {
            /** @noinspection PhpComposerExtensionStubsInspection — optional, guarded above; requiring it would bring back the phantom ext-sockets dependency 2.0 removed */
            socket_set_option($sock, SOL_SOCKET, SO_SNDBUF, 8 * 1_048_576);
        }
    }

    $options = new Options(
        host: $host,
        port: (int) $port,
        clientId: 'bench',
        keepAlive: 0,
        version: $v5 ? MqttVersion::V5_0 : MqttVersion::V3_1_1,
    );
    $client = new Client($options, $transport);

    // A few distinct packets, so copy-on-write cannot share one payload across messages.
    $packets = [];
    for ($i = 0; $i < 8; $i++) {
        $payload   = str_repeat(chr(65 + $i), $payloadSize);
        $packets[] = publishPacket($v5, $qos, $i + 1, $payload);
    }

    $sent     = 0; // complete packets handed to the kernel
    $pending  = ''; // tail of a packet the kernel only partly accepted
    $received = 0;
    $elapsed  = 0;

    while ($received < $count) {
        // Producer: fill the socket until the kernel pushes back. Not timed.
        while ($sent < $count) {
            if ($pending === '') {
                $pending = $packets[$sent & 7];
            }
            $written = @fwrite($peer, $pending);
            if ($written === false || $written === 0) {
                break;
            }
            $pending = substr($pending, $written);
            if ($pending === '') {
                $sent++;
            }
        }
        // Drain whatever the client wrote back (PUBACKs) so its writes never block.
        do {
            $ack = @fread($peer, 65536);
        } while ($ack !== false && $ack !== '');

        if ($sent === $received) {
            throw new RuntimeException('the socket buffer cannot hold one packet; enable ext-sockets or shrink the payload');
        }

        // Consumer: packets that arrived complete. Timed. Capped per round because QoS 1
        // answers every message with a PUBACK, and an unbounded round would fill the
        // socket in the other direction and block the client's write — in one process,
        // forever.
        $until = min($sent, $received + 2_048);
        $start = hrtime(true);
        for (; $received < $until; $received++) {
            $msg = $client->awaitMessage(5.0);
            if (! $msg instanceof InboundMessage || strlen($msg->payload) !== $payloadSize) {
                throw new RuntimeException("bad or missing message at $received of $count");
            }
        }
        $elapsed += hrtime(true) - $start;
    }

    $transport->close();
    fclose($peer);

    return $elapsed / 1e9;
}

$scenarios = [
    // label,                          v5,    qos, payload,   count
    ['MQTT 3.1.1  QoS 0     64 B',     false, 0,   64,        50_000],
    ['MQTT 3.1.1  QoS 0      1 KiB',   false, 0,   1_024,     50_000],
    ['MQTT 3.1.1  QoS 0     64 KiB',   false, 0,   65_536,    2_000],
    ['MQTT 5.0    QoS 0     64 B',     true,  0,   64,        50_000],
    ['MQTT 5.0    QoS 1     64 B',     true,  1,   64,        50_000],
    ['MQTT 5.0    QoS 0     64 KiB',   true,  0,   65_536,    2_000],
    ['MQTT 5.0    QoS 0      1 MiB',   true,  0,   1_048_576, 200],
];

$server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if ($server === false) {
    throw new RuntimeException("listen failed: $errstr");
}

echo "=== Inbound PUBLISH processing cost (client over TCP loopback) ===\n";
printf("PHP %s, best of %d. Client time only; the producer is never on the clock.\n\n", PHP_VERSION, REPEATS);
printf("  %-28s %12s %14s\n", '', 'µs/msg', 'msg/s');

foreach ($scenarios as [$label, $v5, $qos, $size, $count]) {
    $best = INF;
    for ($r = 0; $r < REPEATS; $r++) {
        $best = min($best, runOnce($server, $v5, $qos, $size, $count));
    }
    printf("  %-28s %12.2f %14s\n", $label, $best / $count * 1e6, number_format((int) ($count / $best)));
}
