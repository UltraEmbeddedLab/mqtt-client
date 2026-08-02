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
$clientId = 'php-iot-unsub-fallback';
try {
    $clientId = 'php-iot-unsub-'.RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// ============================================================================
// MQTT 5.0 Example
// ============================================================================
echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                    MQTT 5.0 UNSUBSCRIBE Example                       \n";
echo "═══════════════════════════════════════════════════════════════════════\n\n";

// Configure MQTT 5.0 connection
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0,
)
    ->withClientId($clientId.'-v5')
    ->withKeepAlive(60)
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
echo "   Port: $options->port\n";
echo "   Client ID: $clientId-v5\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: $result->reasonCode)");
    }

    echo "✅ Successfully connected to MQTT 5.0 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    // ============================================================================
    // Step 1: Subscribe to topics
    // ============================================================================
    echo "📥 Step 1: Subscribing to topics...\n\n";

    $filters = [
        ['filter' => 'test/unsubscribe/topic1', 'qos' => 1],
        ['filter' => 'test/unsubscribe/topic2', 'qos' => 1],
        ['filter' => 'test/unsubscribe/+/wildcard', 'qos' => 0],
    ];

    echo "   Subscribing to:\n";
    foreach ($filters as $f) {
        echo "      - {$f['filter']} (QoS {$f['qos']})\n";
    }

    $subResult = $client->subscribeWith($filters);

    echo "\n   ✅ Subscription successful\n";
    echo "   Packet ID: $subResult->packetId\n";
    echo "   Granted QoS codes:\n";
    foreach ($subResult->results as $idx => $code) {
        $qosGranted  = $code <= 2 ? "QoS $code" : 'Failure (0x'.dechex($code).')';
        $topicFilter = $filters[$idx]['filter'] ?? 'unknown';
        echo "      - $topicFilter: $qosGranted\n";
    }

    echo "\n";

    // ============================================================================
    // Step 2: Unsubscribe from topics
    // ============================================================================
    echo "📤 Step 2: Unsubscribing from topics...\n\n";

    $unsubTopics = [
        'test/unsubscribe/topic1',
        'test/unsubscribe/topic2',
        'test/unsubscribe/+/wildcard',
    ];

    echo "   Unsubscribing from:\n";
    foreach ($unsubTopics as $topic) {
        echo "      - $topic\n";
    }

    // Note: Current unsubscribe() returns void, but we can inspect via logging
    $client->unsubscribe($unsubTopics);

    echo "\n   ✅ Unsubscribe successful\n";
    echo "   All topics have been unsubscribed\n\n";

    echo "📊 Summary (MQTT 5.0):\n";
    echo "   - Subscribed to 3 topics\n";
    echo "   - Unsubscribed from 3 topics\n";
    echo "   - UNSUBACK includes reason codes per topic (0x00=Success, 0x11=No subscription existed)\n\n";

    echo "👋 Disconnecting from MQTT 5.0...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ MQTT 5.0 Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n\n");
}

// ============================================================================
// MQTT 3.1.1 Example
// ============================================================================
echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                   MQTT 3.1.1 UNSUBSCRIBE Example                      \n";
echo "═══════════════════════════════════════════════════════════════════════\n\n";

// Configure MQTT 3.1.1 connection
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V3_1_1,
)
    ->withClientId($clientId.'-v3')
    ->withKeepAlive(60)
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
echo "   Port: $options->port\n";
echo "   Client ID: $clientId-v3\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: $result->reasonCode)");
    }

    echo "✅ Successfully connected to MQTT 3.1.1 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    // ============================================================================
    // Step 1: Subscribe to topics
    // ============================================================================
    echo "📥 Step 1: Subscribing to topics...\n\n";

    $filters = [
        ['filter' => 'test/unsubscribe/v3/topic1', 'qos' => 1],
        ['filter' => 'test/unsubscribe/v3/topic2', 'qos' => 0],
    ];

    echo "   Subscribing to:\n";
    foreach ($filters as $f) {
        echo "      - {$f['filter']} (QoS {$f['qos']})\n";
    }

    $subResult = $client->subscribeWith($filters);

    echo "\n   ✅ Subscription successful\n";
    echo "   Packet ID: $subResult->packetId\n";
    echo "   Granted QoS codes:\n";
    foreach ($subResult->results as $idx => $code) {
        $qosGranted  = $code <= 2 ? "QoS $code" : 'Failure (0x80)';
        $topicFilter = $filters[$idx]['filter'] ?? 'unknown';
        echo "      - $topicFilter: $qosGranted\n";
    }

    echo "\n";

    // ============================================================================
    // Step 2: Unsubscribe from topics
    // ============================================================================
    echo "📤 Step 2: Unsubscribing from topics...\n\n";

    $unsubTopics = [
        'test/unsubscribe/v3/topic1',
        'test/unsubscribe/v3/topic2',
    ];

    echo "   Unsubscribing from:\n";
    foreach ($unsubTopics as $topic) {
        echo "      - $topic\n";
    }

    $client->unsubscribe($unsubTopics);

    echo "\n   ✅ Unsubscribe successful\n";
    echo "   All topics have been unsubscribed\n\n";

    echo "📊 Summary (MQTT 3.1.1):\n";
    echo "   - Subscribed to 2 topics\n";
    echo "   - Unsubscribed from 2 topics\n";
    echo "   - UNSUBACK only contains packet ID (no reason codes in v3.1.1)\n\n";

    echo "👋 Disconnecting from MQTT 3.1.1...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ MQTT 3.1.1 Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n\n");
}

echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                       All Examples Completed                           \n";
echo "═══════════════════════════════════════════════════════════════════════\n";
