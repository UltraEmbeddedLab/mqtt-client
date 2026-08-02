<?php

declare(strict_types=1);

use Random\RandomException;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Transport\TcpTransport;
use ScienceStories\Mqtt\Util\RandomId;

require __DIR__.'/../vendor/autoload.php';

// Load shared broker config
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

// Setup client ID
$clientId = 'php-iot-pub-v3-fallback';
try {
    $clientId = 'php-iot-pub-v3-'.RandomId::clientId(6);
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

    // Publish test messages with different QoS levels
    $topic   = 'php-iot/test/v3';
    $payload = 'Hello from MQTT 3.1.1 at '.date('Y-m-d H:i:s');

    echo "📤 Publishing messages...\n\n";

    // QoS 0 publish
    echo "📨 Publishing with QoS 0 (At most once)\n";
    echo "   Topic: {$topic}/qos0\n";
    echo "   Payload: {$payload}\n";
    echo "   QoS: 0 (Fire and forget)\n";

    $client->publish(
        "{$topic}/qos0",
        $payload,
        new PublishOptions(qos: QoS::AtMostOnce)
    );

    echo "   ✅ Published (no acknowledgment expected)\n\n";

    // QoS 1 publish
    echo "📨 Publishing with QoS 1 (At least once)\n";
    echo "   Topic: {$topic}/qos1\n";
    echo "   Payload: {$payload}\n";
    echo "   QoS: 1 (Acknowledged delivery)\n";

    $packetId1 = $client->publish(
        "{$topic}/qos1",
        $payload,
        new PublishOptions(qos: QoS::AtLeastOnce)
    );

    echo "   ✅ Published and acknowledged (Packet ID: {$packetId1})\n\n";

    // QoS 2 publish
    echo "📨 Publishing with QoS 2 (Exactly once)\n";
    echo "   Topic: {$topic}/qos2\n";
    echo "   Payload: {$payload}\n";
    echo "   QoS: 2 (Exactly once delivery)\n";

    $packetId2 = $client->publish(
        "{$topic}/qos2",
        $payload,
        new PublishOptions(qos: QoS::ExactlyOnce)
    );

    echo "   ✅ Published with QoS 2 handshake complete (Packet ID: {$packetId2})\n\n";

    // Publish with retain flag
    echo "📨 Publishing retained message\n";
    echo "   Topic: {$topic}/retained\n";
    echo "   Payload: Last known status\n";
    echo "   QoS: 1\n";
    echo "   Retain: true (broker will store as last known good value)\n";

    $packetId3 = $client->publish(
        "{$topic}/retained",
        'Last known status',
        new PublishOptions(qos: QoS::AtLeastOnce, retain: true)
    );

    echo "   ✅ Published and retained (Packet ID: {$packetId3})\n\n";

    echo "📊 Summary:\n";
    echo "   Total messages published: 4\n";
    echo "   - 1 message with QoS 0\n";
    echo "   - 2 messages with QoS 1 (1 retained)\n";
    echo "   - 1 message with QoS 2\n";
    echo "   Protocol: MQTT 3.1.1 (no properties support)\n\n";

    echo "👋 Disconnecting...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n");
    exit(1);
}
