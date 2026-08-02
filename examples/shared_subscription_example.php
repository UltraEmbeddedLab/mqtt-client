<?php

declare(strict_types=1);

use Random\RandomException;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Transport\TcpTransport;
use ScienceStories\Mqtt\Util\RandomId;

require __DIR__.'/../vendor/autoload.php';

/**
 * MQTT 5.0 Shared Subscriptions Example
 *
 * Shared subscriptions allow multiple clients to share message delivery,
 * enabling load balancing across subscriber instances.
 *
 * Topic Filter Format: $share/{ShareName}/{TopicFilter}
 *
 * How it works:
 * 1. Multiple clients subscribe to: $share/mygroup/sensors/#
 * 2. Broker delivers each message to only ONE client in the group
 * 3. Load is distributed across all clients in the share group
 *
 * Benefits:
 * - Load balancing for message processing
 * - Horizontal scaling of subscribers
 * - Fault tolerance (if one client fails, others continue)
 *
 * Requirements:
 * - MQTT 5.0 (not supported in MQTT 3.1.1)
 * - Broker must support shared subscriptions (most do)
 *
 * To test this example:
 * 1. Run multiple instances of this script (different terminals)
 * 2. Publish messages to 'sensors/temperature' from another client
 * 3. Observe that each message is delivered to only one instance
 */

// Load shared broker config
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

// Setup client ID (unique per instance)
$clientId = 'php-iot-shared-sub-fallback';
try {
    $clientId = 'php-iot-shared-sub-'.RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// Configure MQTT 5.0 connection
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0, // Shared subscriptions require MQTT 5.0
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

echo "Shared Subscriptions Example (MQTT 5.0)\n";
echo "   Host: {$config['host']}\n";
echo "   Port: {$options->port}\n";
echo "   Client ID: {$clientId}\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused (reason code: {$result->reasonCode})");
    }

    echo "Connected to MQTT 5.0 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n";

    // Check if broker supports shared subscriptions
    $sharedSupported = $result->connack->properties['shared_subscription_available'] ?? 1;
    echo '   Shared Subscriptions Available: '.($sharedSupported ? 'yes' : 'no')."\n\n";

    if (! $sharedSupported) {
        echo "This broker does not support shared subscriptions.\n";
        $client->disconnect();
        exit(1);
    }

    // Subscribe to a shared subscription
    // Format: $share/{ShareName}/{TopicFilter}
    $shareGroup  = 'workers';
    $topicFilter = 'sensors/#';
    $sharedTopic = '$share/'.$shareGroup.'/'.$topicFilter;

    echo "Subscribing to shared topic:\n";
    echo "   Share Group: {$shareGroup}\n";
    echo "   Topic Filter: {$topicFilter}\n";
    echo "   Full Topic: {$sharedTopic}\n\n";

    $client->subscribe([$sharedTopic], qos: 1);

    echo "Subscribed successfully. Waiting for messages...\n";
    echo "(Run multiple instances of this script to see load balancing)\n";
    echo "(Publish to 'sensors/temperature' from another client)\n";
    echo "(Press Ctrl+C to stop)\n\n";

    // Set up message handler
    $messageCount = 0;
    $client->onMessage(function ($msg) use (&$messageCount, $clientId) {
        $messageCount++;
        echo "[{$clientId}] Received message #{$messageCount}:\n";
        echo "   Topic: {$msg->topic}\n";
        echo "   Payload: {$msg->payload}\n";
        echo "   QoS: {$msg->qos->value}\n\n";
    });

    // Run message loop (Ctrl+C to stop)
    $client->run(fn ($msg) => null, idleSleep: 0.1);
} catch (Throwable $e) {
    fwrite(STDERR, "\nError: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Type: '.get_class($e)."\n");
    fwrite(STDERR, '   Trace: '.$e->getTraceAsString()."\n");
    exit(1);
}
