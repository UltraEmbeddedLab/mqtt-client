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
$clientId = 'php-iot-pub-v5-fallback';
try {
    $clientId = 'php-iot-pub-v5-'.RandomId::clientId(6);
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

    // Publish test messages with different QoS levels and MQTT 5 properties
    $topic = 'php-iot/test/v5';

    echo "📤 Publishing messages with MQTT 5.0 properties...\n\n";

    // 1. Simple QoS 0 publish with content type
    echo "📨 Message 1: Simple JSON with content type\n";
    echo "   Topic: {$topic}/json\n";
    echo "   Payload: {\"temperature\":22.5,\"humidity\":65}\n";
    echo "   QoS: 0\n";
    echo "   Properties:\n";
    echo "      - content_type: application/json\n";
    echo "      - payload_format_indicator: 1 (UTF-8 text)\n";

    $client->publish(
        "{$topic}/json",
        '{"temperature":22.5,"humidity":65}',
        new PublishOptions(
            qos: QoS::AtMostOnce,
            properties: [
                'content_type'             => 'application/json',
                'payload_format_indicator' => 1,
            ]
        )
    );

    echo "   ✅ Published\n\n";

    // 2. QoS 1 with message expiry and user properties
    echo "📨 Message 2: Sensor data with expiry and metadata\n";
    echo "   Topic: {$topic}/sensor\n";
    echo "   Payload: Sensor reading from warehouse\n";
    echo "   QoS: 1\n";
    echo "   Properties:\n";
    echo "      - message_expiry_interval: 3600 seconds (1 hour)\n";
    echo "      - user_properties:\n";
    echo "         * sensor_id: temp-sensor-01\n";
    echo "         * location: warehouse-a\n";
    echo "         * unit: celsius\n";

    $packetId1 = $client->publish(
        "{$topic}/sensor",
        'Sensor reading from warehouse',
        new PublishOptions(
            qos: QoS::AtLeastOnce,
            properties: [
                'message_expiry_interval' => 3600,
                'user_properties'         => [
                    'sensor_id' => 'temp-sensor-01',
                    'location'  => 'warehouse-a',
                    'unit'      => 'celsius',
                ],
            ]
        )
    );

    echo "   ✅ Published and acknowledged (Packet ID: {$packetId1})\n\n";

    // 3. QoS 2 with response topic and correlation data (request/response pattern)
    echo "📨 Message 3: Request with response topic (request/response pattern)\n";
    echo "   Topic: {$topic}/request\n";
    echo "   Payload: get-device-status\n";
    echo "   QoS: 2\n";
    echo "   Properties:\n";
    echo "      - response_topic: {$topic}/response\n";
    echo '      - correlation_data: req-'.time()."\n";
    echo "      - content_type: text/plain\n";

    $correlationId = 'req-'.time();
    $packetId2     = $client->publish(
        "{$topic}/request",
        'get-device-status',
        new PublishOptions(
            qos: QoS::ExactlyOnce,
            properties: [
                'response_topic'   => "{$topic}/response",
                'correlation_data' => $correlationId,
                'content_type'     => 'text/plain',
            ]
        )
    );

    echo "   ✅ Published with QoS 2 handshake complete (Packet ID: {$packetId2})\n\n";

    // 4. Retained message with multiple properties
    echo "📨 Message 4: Status update with retain and comprehensive properties\n";
    echo "   Topic: {$topic}/status\n";
    echo "   Payload: {\"status\":\"online\",\"version\":\"1.0.0\"}\n";
    echo "   QoS: 1\n";
    echo "   Retain: true\n";
    echo "   Properties:\n";
    echo "      - content_type: application/json\n";
    echo "      - payload_format_indicator: 1 (UTF-8 text)\n";
    echo "      - message_expiry_interval: 86400 seconds (24 hours)\n";
    echo "      - user_properties:\n";
    echo "         * device: gateway-01\n";
    echo "         * firmware: 2.1.5\n";

    $packetId3 = $client->publish(
        "{$topic}/status",
        '{"status":"online","version":"1.0.0"}',
        new PublishOptions(
            qos: QoS::AtLeastOnce,
            retain: true,
            properties: [
                'content_type'             => 'application/json',
                'payload_format_indicator' => 1,
                'message_expiry_interval'  => 86400,
                'user_properties'          => [
                    'device'   => 'gateway-01',
                    'firmware' => '2.1.5',
                ],
            ]
        )
    );

    echo "   ✅ Published, acknowledged, and retained (Packet ID: {$packetId3})\n\n";

    // 5. Binary payload with correlation data
    echo "📨 Message 5: Binary data with metadata\n";
    echo "   Topic: {$topic}/binary\n";
    echo "   Payload: [binary data - 32 bytes]\n";
    echo "   QoS: 0\n";
    echo "   Properties:\n";
    echo "      - payload_format_indicator: 0 (binary)\n";
    echo "      - content_type: application/octet-stream\n";
    echo "      - user_properties:\n";
    echo "         * encoding: base64\n";

    $binaryData = random_bytes(32);
    $client->publish(
        "{$topic}/binary",
        $binaryData,
        new PublishOptions(
            qos: QoS::AtMostOnce,
            properties: [
                'payload_format_indicator' => 0,
                'content_type'             => 'application/octet-stream',
                'user_properties'          => [
                    'encoding' => 'base64',
                ],
            ]
        )
    );

    echo "   ✅ Published\n\n";

    echo "📊 Summary:\n";
    echo "   Total messages published: 5\n";
    echo "   - 2 messages with QoS 0\n";
    echo "   - 2 messages with QoS 1 (1 retained)\n";
    echo "   - 1 message with QoS 2\n";
    echo "   Protocol: MQTT 5.0\n";
    echo "   Properties used:\n";
    echo "      ✓ content_type (5 messages)\n";
    echo "      ✓ payload_format_indicator (4 messages)\n";
    echo "      ✓ message_expiry_interval (2 messages)\n";
    echo "      ✓ user_properties (4 messages)\n";
    echo "      ✓ response_topic (1 message)\n";
    echo "      ✓ correlation_data (1 message)\n\n";

    echo "👋 Disconnecting...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n");
    exit(1);
}
