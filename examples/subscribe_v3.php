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
$clientId = 'php-iot-sub-v3-fallback';
try {
    $clientId = 'php-iot-sub-v3-'.RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// Configure MQTT 3.1.1 connection
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V3_1_1,
)
    ->withClientId($clientId)
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
echo "   Port: {$options->port}\n";
echo "   Client ID: {$clientId}\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: {$result->reasonCode})");
    }

    echo "✅ Successfully connected to MQTT 3.1.1 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    // ============================================================================
    // CONFIGURABLE TOPIC - Change this to subscribe to different topics
    // ============================================================================
    $topic = 'php-iot/test/#';  // You can change this to any topic you want
    // Examples:
    // - 'sensors/temperature'       (exact topic)
    // - 'home/+/temperature'        (single-level wildcard)
    // - 'devices/#'                 (multi-level wildcard)
    // - 'php-iot/test/v3'          (specific test topic)
    // ============================================================================

    echo "📥 Subscribing to topics...\n\n";

    // Subscribe to multiple topics with different QoS levels
    $filters = [
        ['filter' => $topic, 'qos' => 1],                    // Main topic (configurable)
        ['filter' => 'php-iot/test/v3', 'qos' => 1],        // Specific v3 topic
        ['filter' => 'sensors/+/data', 'qos' => 0],         // Wildcard subscription
    ];

    echo "📨 Subscribing with MQTT 3.1.1:\n";
    foreach ($filters as $f) {
        echo "   - Topic: {$f['filter']}, QoS: {$f['qos']}\n";
    }

    $subResult = $client->subscribeWith($filters);

    echo "\n✅ Subscription successful\n";
    echo "   Packet ID: {$subResult->packetId}\n";
    echo "   Granted QoS codes:\n";
    foreach ($subResult->results as $idx => $code) {
        $qosGranted  = $code <= 2 ? "QoS {$code}" : 'Failure (0x80)';
        $topicFilter = $filters[$idx]['filter'] ?? 'unknown';
        echo "      - {$topicFilter}: {$qosGranted}\n";
    }

    echo "\n🎧 Listening for messages (press Ctrl+C to stop)...\n";
    echo "   Waiting for messages on subscribed topics...\n";
    echo "   You can test by publishing to: {$topic}\n";
    echo "   Example: php examples/publish_v3.php\n\n";

    // Set up a message handler
    $messageCount = 0;
    $client->onMessage(function ($message) use (&$messageCount) {
        $messageCount++;
        echo "\n📬 Message #{$messageCount} received:\n";
        echo "   Topic: {$message->topic}\n";
        echo "   Payload: {$message->payload}\n";
        echo "   QoS: {$message->qos->value}\n";
        echo '   Retain: '.($message->retain ? 'yes' : 'no')."\n";
        if ($message->dup) {
            echo "   Duplicate: yes\n";
        }
        echo "\n";
    });

    // Listen for messages (loop for 30 seconds)
    $startTime = time();
    $duration  = 30;
    while (time() - $startTime < $duration) {
        $client->loopOnce(1.0);
    }

    echo "\n⏱️  Listening timeout ({$duration} seconds)\n";
    echo "   Total messages received: {$messageCount}\n\n";

    echo "👋 Disconnecting...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n");
    exit(1);
}
