<?php

declare(strict_types=1);

use Random\RandomException;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Transport\TcpTransport;
use ScienceStories\Mqtt\Util\RandomId;

require __DIR__.'/../vendor/autoload.php';

// Load shared broker config
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

// Setup client ID
$clientId = 'php-iot-ping-fallback';
try {
    $clientId = 'php-iot-ping-'.RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// ============================================================================
// MQTT 3.1.1 Ping Example
// ============================================================================
echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                    MQTT 3.1.1 PING Example                            \n";
echo "═══════════════════════════════════════════════════════════════════════\n\n";

// Configure MQTT 3.1.1 connection with 20-second keepalive
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V3_1_1,
)
    ->withClientId($clientId.'-v3')
    ->withKeepAlive(20)  // 20-second keepalive for demonstration
    ->withCleanSession(true);

if (($config['username'] ?? null) !== null) {
    $options = $options->withUser($config['username'], $config['password'] ?? null);
}

if (($config['scheme'] ?? 'tcp') === 'tls') {
    $options = $options->withTls($config['tls'] ?? [
        'ssl' => [
            'verify_peer'      => true,
            'verify_peer_name' => true,
        ],
    ]);
}

// Init transport + client
$transport = new TcpTransport();
$client    = new Client($options, $transport);

echo "🔌 Connecting to MQTT 3.1.1 broker...\n";
echo "   Host: {$config['host']}\n";
echo "   Port: {$options->port}\n";
echo "   Client ID: {$clientId}-v3\n";
echo "   Keepalive: {$options->keepAlive} seconds\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: {$result->reasonCode})");
    }

    echo "✅ Successfully connected to MQTT 3.1.1 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    // ============================================================================
    // Example 1: Manual PING
    // ============================================================================
    echo "📨 Example 1: Manual PING (MQTT 3.1.1)\n";
    echo "   Sending PINGREQ and waiting for PINGRESP...\n";

    $startTime = microtime(true);
    $success   = $client->ping(5.0);
    $duration  = (microtime(true) - $startTime) * 1000;

    if ($success) {
        echo '   ✅ PINGRESP received in '.number_format($duration, 2)." ms\n";
        echo "   Broker is alive and responsive\n\n";
    } else {
        echo "   ❌ PINGRESP not received (timeout)\n\n";
    }

    // ============================================================================
    // Example 2: Multiple PINGs
    // ============================================================================
    echo "📨 Example 2: Multiple PINGs to test latency\n";
    echo "   Sending 5 PINGREQ packets...\n\n";

    $latencies = [];
    for ($i = 1; $i <= 5; $i++) {
        $startTime = microtime(true);
        $success   = $client->ping(5.0);
        $duration  = (microtime(true) - $startTime) * 1000;

        if ($success) {
            $latencies[] = $duration;
            echo "   Ping {$i}/5: ".number_format($duration, 2)." ms\n";
        } else {
            echo "   Ping {$i}/5: timeout\n";
        }

        // Small delay between pings
        usleep(100000); // 100ms
    }

    if (count($latencies) > 0) {
        $avgLatency = array_sum($latencies) / count($latencies);
        $minLatency = min($latencies);
        $maxLatency = max($latencies);

        echo "\n   Statistics:\n";
        echo '      Average latency: '.number_format($avgLatency, 2)." ms\n";
        echo '      Min latency: '.number_format($minLatency, 2)." ms\n";
        echo '      Max latency: '.number_format($maxLatency, 2)." ms\n\n";
    }

    // ============================================================================
    // Example 3: Observe auto-ping behavior
    // ============================================================================
    echo "📨 Example 3: Observing auto-ping behavior\n";
    echo "   Keepalive is set to {$options->keepAlive} seconds\n";
    echo "   Waiting 25 seconds to observe automatic PINGREQ...\n";
    echo '   (Auto-ping triggers at ~90% of keepalive interval: ~'.($options->keepAlive * 0.9)." seconds)\n\n";

    $startTime    = time();
    $duration     = 25;
    $lastActivity = time();

    while (time() - $startTime < $duration) {
        $client->loopOnce(0.5);
        $elapsed = time() - $startTime;

        // Show progress every 5 seconds
        if ($elapsed > 0 && $elapsed % 5 === 0 && time() !== $lastActivity) {
            echo "   ... {$elapsed} seconds elapsed (waiting for auto-ping)\n";
            $lastActivity = time();
        }
    }

    echo "   ✅ Observation complete\n";
    echo "   Note: Check the logs above for automatic PINGREQ messages\n\n";

    echo "👋 Disconnecting from MQTT 3.1.1...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ MQTT 3.1.1 Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n\n");
}

// ============================================================================
// MQTT 5.0 Ping Example
// ============================================================================
echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                     MQTT 5.0 PING Example                             \n";
echo "═══════════════════════════════════════════════════════════════════════\n\n";

// Configure MQTT 5.0 connection with 20-second keepalive
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0,
)
    ->withClientId($clientId.'-v5')
    ->withKeepAlive(20)  // 20-second keepalive for demonstration
    ->withCleanSession(true);

if (($config['username'] ?? null) !== null) {
    $options = $options->withUser($config['username'], $config['password'] ?? null);
}

if (($config['scheme'] ?? 'tcp') === 'tls') {
    $options = $options->withTls($config['tls'] ?? [
        'ssl' => [
            'verify_peer'      => true,
            'verify_peer_name' => true,
        ],
    ]);
}

// Init transport + client
$transport = new TcpTransport();
$client    = new Client($options, $transport);

echo "🔌 Connecting to MQTT 5.0 broker...\n";
echo "   Host: {$config['host']}\n";
echo "   Port: {$options->port}\n";
echo "   Client ID: {$clientId}-v5\n";
echo "   Keepalive: {$options->keepAlive} seconds\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: {$result->reasonCode})");
    }

    echo "✅ Successfully connected to MQTT 5.0 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    // ============================================================================
    // Example 1: Manual PING (identical to MQTT 3.1.1)
    // ============================================================================
    echo "📨 Example 1: Manual PING (MQTT 5.0)\n";
    echo "   Note: PINGREQ/PINGRESP packets are identical in MQTT 3.1.1 and 5.0\n";
    echo "   Sending PINGREQ and waiting for PINGRESP...\n";

    $startTime = microtime(true);
    $success   = $client->ping(5.0);
    $duration  = (microtime(true) - $startTime) * 1000;

    if ($success) {
        echo '   ✅ PINGRESP received in '.number_format($duration, 2)." ms\n";
        echo "   Broker is alive and responsive\n\n";
    } else {
        echo "   ❌ PINGRESP not received (timeout)\n\n";
    }

    // ============================================================================
    // Example 2: Connection health check
    // ============================================================================
    echo "📨 Example 2: Connection health check\n";
    echo "   Performing health check with PING...\n";

    try {
        $success = $client->ping(3.0);
        if ($success) {
            echo "   ✅ Connection is healthy\n";
            echo "   ✅ Broker is responsive\n";
            echo "   ✅ Network is functional\n\n";
        }
    } catch (Exception $e) {
        echo '   ❌ Connection health check failed: '.$e->getMessage()."\n\n";
    }

    echo "📊 Summary (MQTT 5.0):\n";
    echo "   - PINGREQ/PINGRESP packets are identical to MQTT 3.1.1\n";
    echo "   - Both versions use 2-byte packets with no body\n";
    echo "   - No reason codes or properties in ping packets\n";
    echo "   - Keepalive mechanism works identically in both versions\n";
    echo "   - Auto-ping triggers at ~90% of keepalive interval\n\n";

    echo "👋 Disconnecting from MQTT 5.0...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ MQTT 5.0 Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n\n");
}

echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                       All Examples Completed                           \n";
echo "═══════════════════════════════════════════════════════════════════════\n";
echo "\nKey Takeaways:\n";
echo "• PINGREQ/PINGRESP are identical in MQTT 3.1.1 and 5.0\n";
echo "• Both are 2-byte packets with no body, properties, or reason codes\n";
echo "• Keepalive mechanism prevents idle connection timeouts\n";
echo "• Auto-ping triggers at ~90% of the keepalive interval\n";
echo "• Manual ping() can be used to test broker responsiveness\n";
echo "• Missing PINGRESP indicates a broken connection\n";
