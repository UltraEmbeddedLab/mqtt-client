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
$clientId = 'php-iot-v5-fallback';
try {
    $clientId = 'php-iot-v5-'.RandomId::clientId(6);
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

    echo "✅ Successfully connected to MQTT 5.0 broker\n\n";
    echo "📥 CONNACK Response from broker:\n";
    echo "   Protocol: {$result->protocol} {$result->version}\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n";
    echo "   Reason Code: {$result->reasonCode}";

    // Use ConnAck's enhanced functionality to decode reason code
    if ($result->connAck !== null) {
        $description = $result->connAck->getReasonDescription($result->version);
        echo " ({$description})\n";
        echo '   Connection Status: '.($result->connAck->isSuccess() ? '✅ Success' : '❌ Failed')."\n";

        // Display MQTT 5.0 specific properties if available
        if ($result->connAck->properties !== null && count($result->connAck->properties) > 0) {
            echo "\n📋 Broker Properties (MQTT 5.0):\n";

            if ($result->connAck->hasProperty('server_keep_alive')) {
                echo '   Server Keep Alive: '.$result->connAck->getServerKeepAlive()." seconds\n";
            }

            if ($result->connAck->hasProperty('receive_maximum')) {
                echo '   Receive Maximum: '.$result->connAck->getReceiveMaximum()."\n";
            }

            if ($result->connAck->hasProperty('maximum_qos')) {
                echo '   Maximum QoS: '.$result->connAck->getMaximumQoS()."\n";
            }

            if ($result->connAck->hasProperty('retain_available')) {
                $available = $result->connAck->isRetainAvailable();
                echo '   Retain Available: '.($available ? 'yes' : 'no')."\n";
            }

            if ($result->connAck->hasProperty('maximum_packet_size')) {
                echo '   Maximum Packet Size: '.$result->connAck->getMaximumPacketSize()." bytes\n";
            }

            if ($result->connAck->hasProperty('topic_alias_maximum')) {
                echo '   Topic Alias Maximum: '.$result->connAck->getTopicAliasMaximum()."\n";
            }

            if ($result->connAck->hasProperty('wildcard_subscription_available')) {
                $available = $result->connAck->isWildcardSubscriptionAvailable();
                echo '   Wildcard Subscriptions: '.($available ? 'yes' : 'no')."\n";
            }

            if ($result->connAck->hasProperty('subscription_identifier_available')) {
                $available = $result->connAck->isSubscriptionIdentifierAvailable();
                echo '   Subscription Identifiers: '.($available ? 'yes' : 'no')."\n";
            }

            if ($result->connAck->hasProperty('shared_subscription_available')) {
                $available = $result->connAck->isSharedSubscriptionAvailable();
                echo '   Shared Subscriptions: '.($available ? 'yes' : 'no')."\n";
            }

            if ($result->connAck->hasProperty('reason_string')) {
                echo '   Reason String: '.$result->connAck->getReasonString()."\n";
            }

            if ($result->connAck->hasProperty('response_information')) {
                echo '   Response Information: '.$result->connAck->getResponseInformation()."\n";
            }

            if ($result->connAck->hasProperty('server_reference')) {
                echo '   Server Reference: '.$result->connAck->getServerReference()."\n";
            }

            $userProps = $result->connAck->getUserProperties();
            if (count($userProps) > 0) {
                echo "   User Properties:\n";
                foreach ($userProps as $key => $value) {
                    echo "      {$key}: {$value}\n";
                }
            }
        }
    } else {
        echo "\n";
    }

    if ($result->assignedClientId !== null) {
        echo "   Assigned Client ID: {$result->assignedClientId}\n";
    }

    echo "\n👋 Disconnecting...\n";
    $client->disconnect();
    echo "✅ Disconnected successfully\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ Connection failed: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n");
    exit(1);
}
