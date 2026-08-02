<?php

declare(strict_types=1);

use Random\RandomException;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Transport\TcpTransport;

require __DIR__.'/../vendor/autoload.php';

// Load shared broker config
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

// Setup client ID
$clientId = 'php-iot-v3-fallback';
try {
    $clientId = 'php-iot-v3-khN7VA';
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
    ->withCleanSession(false);

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
echo "   Client ID: $clientId\n\n";

try {
    $result = $client->connect();

    echo "✅ Successfully connected to MQTT 3.1.1 broker\n\n";
    echo "📥 CONNACK Response from broker:\n";
    echo "   Protocol: $result->protocol $result->version\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n";
    echo "   Return Code: $result->reasonCode";

    // Use ConnAck's enhanced functionality to decode return code
    if ($result->connAck !== null) {
        $description = $result->connAck->getReasonDescription($result->version);
        echo " ($description)\n";
        echo '   Connection Status: '.($result->connAck->isSuccess() ? '✅ Success' : '❌ Failed')."\n";
    } else {
        echo "\n";
    }

    if ($result->assignedClientId !== null) {
        echo "   Assigned Client ID: $result->assignedClientId\n";
    }

    echo "\n👋 Disconnecting...";
    $client->disconnect();
    echo "✅ Disconnected successfully\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ Connection failed: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n");
    exit(1);
}
