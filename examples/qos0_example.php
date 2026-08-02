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

/**
 * QoS 0 (At Most Once) Example
 *
 * This example demonstrates QoS 0 publishing, which provides:
 * - Fire-and-forget delivery (no acknowledgment)
 * - Message may be lost if network fails
 * - Fastest delivery with lowest overhead
 * - No packet identifier required
 *
 * Use Case: Sensor data where occasional loss is acceptable (e.g., temperature readings)
 */

// Load shared broker config
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

// Choose the MQTT version without editing this file:
//   MQTT_EXAMPLE_VERSION=v3 php examples/qos0_example.php
$testVersion = getenv('MQTT_EXAMPLE_VERSION') ?: 'v5';

$version    = $testVersion === 'v5' ? MqttVersion::V5_0 : MqttVersion::V3_1_1;
$versionStr = $testVersion === 'v5' ? '5.0' : '3.1.1';

// Setup client ID
$clientId = "php-iot-qos0-$testVersion-fallback";
try {
    $clientId = "php-iot-qos0-$testVersion-".RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// Configure MQTT connection
$options = new Options(
    host: $config['host'],
    port: $port,
    version: $version,
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

echo "🔌 QoS 0 (At Most Once) Publishing Example\n";
echo "   MQTT Version: $versionStr\n";
echo "   Host: {$config['host']}\n";
echo "   Port: $options->port\n";
echo "   Client ID: $clientId\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: $result->reasonCode)");
    }

    echo "✅ Successfully connected to MQTT $versionStr broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    echo "📤 Publishing messages with QoS 0 (Fire and Forget)...\n\n";

    // Example 1: Simple QoS 0 publish
    echo "📨 Message 1: Simple sensor reading\n";
    echo "   Topic: php-iot/sensors/temperature\n";
    echo "   Payload: 22.5\n";
    echo "   QoS: 0 (no acknowledgment)\n";

    $packetId1 = $client->publish(
        'php-iot/sensors/temperature',
        '22.5',
        new PublishOptions(qos: QoS::AtMostOnce)
    );

    echo "   ✅ Published (Packet ID: $packetId1, no PUBACK expected)\n";
    echo "   ⚡ Fast: No acknowledgment wait time\n\n";

    // Example 2: Multiple rapid QoS 0 publishes
    echo "📨 Message 2-5: Rapid fire publishing\n";
    echo "   Demonstrating high-throughput capability of QoS 0\n\n";

    for ($i = 1; $i <= 4; $i++) {
        $topic   = "php-iot/stream/data/$i";
        $payload = "data-$i-".time();
        echo "   Publishing to $topic: $payload\n";

        $client->publish(
            $topic,
            $payload,
            new PublishOptions(qos: QoS::AtMostOnce)
        );
    }

    echo "   ✅ All 4 messages published instantly (no waiting)\n\n";

    // Example 3: QoS 0 with MQTT 5.0 properties (if v5)
    if ($version === MqttVersion::V5_0) {
        echo "📨 Message 6: QoS 0 with MQTT 5.0 properties\n";
        echo "   Topic: php-iot/telemetry/device1\n";
        echo "   Properties: content_type, user_properties\n";

        $client->publish(
            'php-iot/telemetry/device1',
            '{"status":"online","uptime":3600}',
            new PublishOptions(
                qos: QoS::AtMostOnce,
                properties: [
                    'content_type'    => 'application/json',
                    'user_properties' => [
                        'device_id' => 'sensor-01',
                        'location'  => 'warehouse',
                    ],
                ]
            )
        );

        echo "   ✅ Published with properties (still no acknowledgment)\n\n";
    }

    echo "📊 Summary:\n";
    echo "   QoS Level: 0 (At Most Once)\n";
    echo "   Delivery Guarantee: None (fire and forget)\n";
    echo "   Acknowledgment: No PUBACK required\n";
    echo "   Performance: Fastest (no wait time)\n";
    echo "   Use Case: High-frequency data where occasional loss is acceptable\n";
    echo "   Network Overhead: Minimal (no acknowledgment packets)\n\n";

    echo "⚠️  Important Notes:\n";
    echo "   • Messages are NOT guaranteed to arrive\n";
    echo "   • No retransmission if network fails\n";
    echo "   • No duplicate detection\n";
    echo "   • Best for: sensor data, metrics, logs where loss is tolerable\n\n";

    echo "👋 Disconnecting...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n");
    fwrite(STDERR, '   Stack trace: '.$e->getTraceAsString()."\n");
    exit(1);
}
