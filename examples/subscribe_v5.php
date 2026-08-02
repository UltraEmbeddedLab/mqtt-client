<?php

declare(strict_types=1);

use Random\RandomException;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\SubscribeOptions;
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
$clientId = 'php-iot-sub-v5-fallback';
try {
    $clientId = 'php-iot-sub-v5-'.RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// Configure MQTT 5.0 connection
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0,
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

echo "🔌 Connecting to MQTT 5.0 broker...\n";
echo "   Host: {$config['host']}\n";
echo "   Port: {$options->port}\n";
echo "   Client ID: {$clientId}\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: {$result->reasonCode})");
    }

    echo "✅ Successfully connected to MQTT 5.0 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    // ============================================================================
    // CONFIGURABLE TOPIC - Change this to subscribe to different topics
    // ============================================================================
    $topic = 'php-iot/test/#';  // You can change this to any topic you want
    // Examples:
    // - 'sensors/temperature'       (exact topic)
    // - 'home/+/temperature'        (single-level wildcard)
    // - 'devices/#'                 (multi-level wildcard)
    // - 'php-iot/test/v5'          (specific test topic)
    // ============================================================================

    echo "📥 Subscribing to topics with MQTT 5.0 options...\n\n";

    // ============================================================================
    // Example 1: Basic subscription with No Local option
    // ============================================================================
    echo "📨 Subscription 1: Basic with No Local option\n";
    echo "   Topics:\n";
    echo "      - {$topic} (QoS 1)\n";
    echo "      - php-iot/test/v5 (QoS 1)\n";
    echo "   Options:\n";
    echo "      - No Local: true (won't receive own publications)\n";

    $filters1 = [
        ['filter' => $topic, 'qos' => 1],
        ['filter' => 'php-iot/test/v5', 'qos' => 1],
    ];

    $options1  = new SubscribeOptions(noLocal: true);
    $subResult = $client->subscribeWith($filters1, $options1);

    echo "\n✅ Subscription 1 successful\n";
    echo "   Packet ID: {$subResult->packetId}\n";
    echo "   Granted QoS codes:\n";
    foreach ($subResult->results as $idx => $code) {
        $qosGranted  = $code <= 2 ? "QoS {$code}" : "Failure (0x{$code})";
        $topicFilter = $filters1[$idx]['filter'] ?? 'unknown';
        echo "      - {$topicFilter}: {$qosGranted}\n";
    }
    echo "\n";

    // ============================================================================
    // Example 2: Subscription with Retain Handling
    // ============================================================================
    echo "📨 Subscription 2: With Retain Handling\n";
    echo "   Topics:\n";
    echo "      - config/# (QoS 1)\n";
    echo "   Options:\n";
    echo "      - Retain Handling: 1 (send retained only if new subscription)\n";

    $filters2 = [
        ['filter' => 'config/#', 'qos' => 1],
    ];

    // retainHandling: 0=send retained, 1=send if new subscription, 2=don't send retained
    $options2  = new SubscribeOptions(retainHandling: 1);
    $subResult = $client->subscribeWith($filters2, $options2);

    echo "\n✅ Subscription 2 successful\n";
    echo "   Packet ID: {$subResult->packetId}\n";
    echo "   Granted QoS codes:\n";
    foreach ($subResult->results as $idx => $code) {
        $qosGranted  = $code <= 2 ? "QoS {$code}" : "Failure (0x{$code})";
        $topicFilter = $filters2[$idx]['filter'] ?? 'unknown';
        echo "      - {$topicFilter}: {$qosGranted}\n";
    }
    echo "\n";

    // ============================================================================
    // Example 3: Advanced subscription with all options and user properties
    // ============================================================================
    echo "📨 Subscription 3: Advanced with all MQTT 5.0 options\n";
    echo "   Topics:\n";
    echo "      - sensors/+/data (QoS 2)\n";
    echo "   Options:\n";
    echo "      - No Local: true\n";
    echo "      - Retain As Published: true\n";
    echo "      - Retain Handling: 2 (don't send retained messages)\n";
    echo "      - User Properties:\n";
    echo "         * subscription_type: monitoring\n";
    echo "         * priority: high\n";

    $filters3 = [
        ['filter' => 'sensors/+/data', 'qos' => 2],
    ];

    $options3 = new SubscribeOptions(
        noLocal: true,
        retainAsPublished: true,
        retainHandling: 2,
        properties: [
            'user_properties' => [
                'subscription_type' => 'monitoring',
                'priority'          => 'high',
            ],
        ],
    );

    $subResult = $client->subscribeWith($filters3, $options3);

    echo "\n✅ Subscription 3 successful\n";
    echo "   Packet ID: {$subResult->packetId}\n";
    echo "   Granted QoS codes:\n";
    foreach ($subResult->results as $idx => $code) {
        $qosGranted  = $code <= 2 ? "QoS {$code}" : "Failure (0x{$code})";
        $topicFilter = $filters3[$idx]['filter'] ?? 'unknown';
        echo "      - {$topicFilter}: {$qosGranted}\n";
    }
    echo "\n";

    // ============================================================================
    // Listen for messages
    // ============================================================================
    echo "🎧 Listening for messages (press Ctrl+C to stop)...\n";
    echo "   Waiting for messages on all subscribed topics...\n";
    echo "   Main topic: {$topic}\n";
    echo "   You can test by publishing to: {$topic}\n";
    echo "   Example: php examples/publish_v5.php\n\n";

    // Set up message handler
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

        // Display MQTT 5.0 properties if available
        if ($message->properties !== null && count($message->properties) > 0) {
            echo "   MQTT 5.0 Properties:\n";

            if (isset($message->properties['content_type'])) {
                echo "      - Content Type: {$message->properties['content_type']}\n";
            }
            if (isset($message->properties['response_topic'])) {
                echo "      - Response Topic: {$message->properties['response_topic']}\n";
            }
            if (isset($message->properties['correlation_data'])) {
                echo "      - Correlation Data: {$message->properties['correlation_data']}\n";
            }
            if (isset($message->properties['message_expiry_interval'])) {
                echo "      - Message Expiry: {$message->properties['message_expiry_interval']} seconds\n";
            }
            if (isset($message->properties['user_properties']) && is_array($message->properties['user_properties'])) {
                echo "      - User Properties:\n";
                foreach ($message->properties['user_properties'] as $key => $value) {
                    echo "         * {$key}: {$value}\n";
                }
            }
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

    echo "📊 Summary:\n";
    echo "   Total subscriptions: 3\n";
    echo "   - Subscription 1: No Local option\n";
    echo "   - Subscription 2: Retain Handling\n";
    echo "   - Subscription 3: All MQTT 5.0 options + user properties\n";
    echo "   Protocol: MQTT 5.0\n\n";

    echo "👋 Disconnecting...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n");
    exit(1);
}
