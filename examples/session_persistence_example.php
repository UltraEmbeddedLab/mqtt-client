<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Session\FileSessionStore;
use ScienceStories\Mqtt\Transport\TcpTransport;

require __DIR__.'/../vendor/autoload.php';

/**
 * MQTT Session Persistence Example
 *
 * Session persistence allows the client to save and restore its session state
 * (subscriptions, pending QoS 2 messages) across restarts.
 *
 * How it works:
 * 1. Configure Options with a SessionStore (e.g., FileSessionStore)
 * 2. Set cleanSession=false to use persistent sessions
 * 3. On connect, if broker indicates session present, state is restored
 * 4. On disconnect, state is saved to the session store
 *
 * Benefits:
 * - Subscriptions are automatically restored after client restart
 * - Pending QoS 2 messages are tracked across restarts
 * - Seamless recovery from network interruptions
 *
 * MQTT 5.0 Note:
 * - Use sessionExpiry to control how long the broker keeps the session
 * - sessionExpiry=0 means session expires immediately on disconnect
 * - sessionExpiry=0xFFFFFFFF means session never expires
 */

// Load shared broker config
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

// Use a fixed client ID for session persistence (required!)
// In production, use a stable identifier for each device/client
$clientId = 'php-iot-persistent-client';

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// Create session store (stores sessions in /tmp by default)
$sessionDir   = sys_get_temp_dir().'/mqtt-sessions';
$sessionStore = new FileSessionStore($sessionDir, defaultExpirySeconds: 3600); // 1 hour expiry

echo "Session Persistence Example\n";
echo "   Session Directory: $sessionDir\n\n";

// Check if we have a saved session
if ($sessionStore->exists($clientId)) {
    $state = $sessionStore->load($clientId);
    if ($state !== null) {
        echo "Found existing session for '$clientId':\n";
        echo "   Subscriptions: {$state->getSubscriptionCount()}\n";
        echo "   Age: {$state->getAge()} seconds\n";
        echo '   Saved at: '.date('Y-m-d H:i:s', $state->savedAt)."\n\n";
    }
} else {
    echo "No existing session found for '$clientId'.\n";
    echo "A new session will be created.\n\n";
}

// Configure MQTT connection with session persistence
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0,
)
    ->withClientId($clientId)
    ->withKeepAlive(60)
    ->withCleanSession(false) // Required for persistent sessions!
    ->withSessionExpiry(3600) // Keep session for 1 hour (MQTT 5.0)
    ->withSessionStore($sessionStore);

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

echo "Connecting with cleanSession=false...\n";
echo "   Host: {$config['host']}\n";
echo "   Port: $options->port\n";
echo "   Client ID: $clientId\n";
echo "   Session Expiry: $options->sessionExpiry seconds\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused (reason code: $result->reasonCode)");
    }

    echo "Connected to MQTT broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    if ($result->sessionPresent) {
        echo "Broker has an existing session for this client.\n";
        echo "Subscriptions and pending messages may be restored.\n\n";
    } else {
        echo "No existing session on broker (new session created).\n\n";
    }

    // Subscribe to some topics (will be saved in session)
    echo "Subscribing to topics (will be persisted)...\n";
    $topics = ['sensors/+/temperature', 'alerts/#'];
    foreach ($topics as $topic) {
        echo "   Subscribing to: $topic\n";
        $client->subscribe([$topic], qos: 1);
    }
    echo "\n";

    // Demonstrate session persistence
    echo "Session state will be saved on disconnect.\n";
    echo "Run this example again to see session restoration.\n\n";

    // Wait for a few messages (optional)
    echo "Waiting for messages (5 seconds)...\n";
    $deadline = microtime(true) + 5.0;
    while (microtime(true) < $deadline) {
        $msg = $client->awaitMessage(0.5);
        if ($msg !== null) {
            echo "   Received: $msg->topic -> $msg->payload\n";
        }
    }
    echo "\n";

    // Disconnect (session will be saved)
    echo "Disconnecting (session will be saved)...\n";
    $client->disconnect();

    // Verify session was saved
    if ($sessionStore->exists($clientId)) {
        $state = $sessionStore->load($clientId);
        if ($state !== null) {
            echo "\nSession saved successfully:\n";
            echo "   Subscriptions: {$state->getSubscriptionCount()}\n";
            foreach ($state->subscriptions as $filter => $settings) {
                echo "      - $filter (QoS {$settings['qos']})\n";
            }
        }
    }

    echo "\nDone. Run this example again to see the session restored.\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\nError: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Type: '.get_class($e)."\n");
    fwrite(STDERR, '   Trace: '.$e->getTraceAsString()."\n");
    exit(1);
}
