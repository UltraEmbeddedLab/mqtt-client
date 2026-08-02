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

// Choose MQTT version: 'v3' or 'v5' (change this to test different versions)
// Choose the MQTT version without editing this file:
//   MQTT_EXAMPLE_VERSION=v3 php examples/suback_example.php
$testVersion = getenv('MQTT_EXAMPLE_VERSION') ?: 'v5';

$version    = $testVersion === 'v5' ? MqttVersion::V5_0 : MqttVersion::V3_1_1;
$versionStr = $testVersion === 'v5' ? '5.0' : '3.1.1';

// Setup client ID
$clientId = "php-iot-suback-$testVersion-fallback";
try {
    $clientId = "php-iot-suback-$testVersion-".RandomId::clientId(6);
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

echo "🔌 Connecting to MQTT $versionStr broker...\n";
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

    // Example 1: Subscribe to multiple topics with different QoS levels
    echo "📨 Example 1: Basic subscription with multiple topics\n";
    echo "   Subscribing to:\n";
    echo "      - php-iot/test/suback/qos0 (QoS 0)\n";
    echo "      - php-iot/test/suback/qos1 (QoS 1)\n";
    echo "      - php-iot/test/suback/qos2 (QoS 2)\n\n";

    $filters = [
        ['filter' => 'php-iot/test/suback/qos0', 'qos' => 0],
        ['filter' => 'php-iot/test/suback/qos1', 'qos' => 1],
        ['filter' => 'php-iot/test/suback/qos2', 'qos' => 2],
    ];

    $subResult = $client->subscribeWith($filters);

    echo "📥 SUBACK Response:\n";
    echo "   Packet ID: $subResult->packetId\n";
    echo '   Return Codes: ['.implode(', ', $subResult->results)."]\n";

    if ($subResult->subAck !== null) {
        $subAck = $subResult->subAck;
        echo '   Overall Status: '.($subAck->isSuccess() ? '✅ All subscriptions successful' : '❌ Some subscriptions failed')."\n";
        echo '   Granted QoS Levels: ['.implode(', ', $subAck->getGrantedQoS())."]\n\n";

        echo "   Detailed Results:\n";
        $descriptions = $subAck->getAllReasonDescriptions($versionStr);
        foreach ($filters as $index => $filter) {
            $code   = $subResult->results[$index] ?? null;
            $desc   = $descriptions[$index]       ?? 'Unknown';
            $status = ($code !== null && $code >= 0x00 && $code <= 0x02) ? '✅' : '❌';
            echo "      $status {$filter['filter']}: Code 0x".dechex($code ?? 0)." ($desc)\n";
        }

        // Display MQTT 5.0 properties if available
        if ($version === MqttVersion::V5_0 && $subAck->properties !== null) {
            echo "\n   MQTT 5.0 Properties:\n";
            if ($subAck->hasProperty('reason_string')) {
                echo '      Reason String: '.$subAck->getReasonString()."\n";
            }
            $userProps = $subAck->getUserProperties();
            if (count($userProps) > 0) {
                echo "      User Properties:\n";
                foreach ($userProps as $key => $value) {
                    echo "         $key: $value\n";
                }
            }
            if (!$subAck->hasProperty('reason_string') && count($userProps) === 0) {
                echo "      (none)\n";
            }
        }
    }

    echo "\n";

    // Example 2: Subscribe with MQTT 5.0 options (only for v5)
    if ($version === MqttVersion::V5_0) {
        echo "📨 Example 2: MQTT 5.0 subscription with options\n";
        echo "   Subscribing to:\n";
        echo "      - php-iot/test/suback/advanced (QoS 1, No Local, Retain As Published)\n\n";

        $advancedFilters = [
            ['filter' => 'php-iot/test/suback/advanced', 'qos' => 1],
        ];

        $advancedOptions = new SubscribeOptions(
            noLocal: true,
            retainAsPublished: true,
            retainHandling: 0
        );

        $subResult2 = $client->subscribeWith($advancedFilters, $advancedOptions);

        echo "📥 SUBACK Response:\n";
        echo "   Packet ID: $subResult2->packetId\n";
        echo '   Return Code: 0x'.dechex($subResult2->results[0] ?? 0)."\n";

        if ($subResult2->subAck !== null) {
            $subAck2 = $subResult2->subAck;
            $desc    = $subAck2->getReasonDescription($subResult2->results[0] ?? 0, $versionStr);
            echo "   Description: $desc\n";
            echo '   Status: '.($subAck2->isSuccess() ? '✅ Success' : '❌ Failed')."\n\n";
        }
    }

    echo "👋 Disconnecting...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n");
    exit(1);
}
