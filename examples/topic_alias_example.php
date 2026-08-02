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
 * MQTT 5.0 Topic Alias Example
 *
 * Topic aliases allow replacing a topic string with a 2-byte integer,
 * significantly reducing bandwidth for repeated publishes to the same topic.
 *
 * How it works:
 * 1. Client requests topic_alias_maximum in CONNECT
 * 2. Broker advertises its topic_alias_maximum in CONNACK
 * 3. First publish with topic + alias establishes the mapping
 * 4. Subsequent publishes use alias only (topic can be empty)
 *
 * Benefits:
 * - Reduced bandwidth for repeated publishes to same topic
 * - Especially useful for IoT devices with frequent sensor readings
 * - Aliases are connection-scoped (reset on reconnect)
 */

// Load shared broker config
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

// Setup client ID
$clientId = 'php-iot-topic-alias-fallback';
try {
    $clientId = 'php-iot-topic-alias-'.RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// Configure MQTT 5.0 connection with topic alias support
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0, // Topic aliases require MQTT 5.0
)
    ->withClientId($clientId)
    ->withKeepAlive(60)
    ->withCleanSession(true)
    ->withTopicAliasMaximum(10); // Request up to 10 topic aliases

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

echo "Topic Alias Example (MQTT 5.0)\n";
echo "   Host: {$config['host']}\n";
echo "   Port: {$options->port}\n";
echo "   Client ID: {$clientId}\n";
echo "   Requested Topic Alias Max: {$options->topicAliasMaximum}\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused (reason code: {$result->reasonCode})");
    }

    echo "Connected to MQTT 5.0 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n";

    // Check broker's topic alias maximum
    $brokerMax = $result->connack->properties['topic_alias_maximum'] ?? 0;
    echo "   Broker Topic Alias Maximum: {$brokerMax}\n\n";

    if ($brokerMax === 0) {
        echo "Broker does not support topic aliases.\n";
        echo "Topic aliases are disabled for this connection.\n\n";
    }

    $aliasManager = $client->topicAliasManager;
    if ($aliasManager === null) {
        echo "Topic Alias Manager not initialized (broker may not support topic aliases).\n";
    }

    // Example: Publish multiple messages to the same topic
    echo "Publishing messages with topic aliases...\n\n";

    $topic = 'sensors/device1/temperature';

    // First publish - establishes alias
    echo "Message 1: Publishing to '{$topic}' (establishes alias)\n";
    $client->publish($topic, '22.5', new PublishOptions(qos: QoS::AtMostOnce));

    if ($aliasManager !== null) {
        $alias = $aliasManager->getAlias($topic);
        echo '   Alias assigned: '.($alias ?? 'none')."\n";
        echo "   Total aliases used: {$aliasManager->getAliasCount()}/{$aliasManager->maxAliases}\n\n";
    }

    // Subsequent publishes - reuse alias (topic can be omitted internally)
    for ($i = 2; $i <= 5; $i++) {
        $payload = (20 + $i * 0.5).'';
        echo "Message {$i}: Publishing to '{$topic}' (reuses alias)\n";
        echo "   Payload: {$payload}\n";
        $client->publish($topic, $payload, new PublishOptions(qos: QoS::AtMostOnce));
    }

    echo "\n";

    // Publish to different topics to show alias assignment
    $topics = [
        'sensors/device1/humidity',
        'sensors/device1/pressure',
        'sensors/device2/temperature',
    ];

    echo "Publishing to different topics...\n\n";
    foreach ($topics as $t) {
        echo "Publishing to '{$t}'\n";
        $client->publish($t, '42.0', new PublishOptions(qos: QoS::AtMostOnce));

        if ($aliasManager !== null) {
            echo '   Alias: '.($aliasManager->getAlias($t) ?? 'none')."\n";
        }
    }

    if ($aliasManager !== null) {
        echo "\nFinal alias status:\n";
        echo "   Total aliases used: {$aliasManager->getAliasCount()}/{$aliasManager->maxAliases}\n";
        echo '   Available slots: '.($aliasManager->hasAvailableSlots() ? 'yes' : 'no')."\n";
    }

    echo "\nSummary:\n";
    echo "   Topic aliases reduce bandwidth by replacing topic strings with 2-byte integers\n";
    echo "   First publish establishes alias; subsequent publishes reuse it\n";
    echo "   Aliases are connection-scoped and reset on disconnect\n\n";

    echo "Disconnecting...\n";
    $client->disconnect();
    echo "Done.\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\nError: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Type: '.get_class($e)."\n");
    fwrite(STDERR, '   Trace: '.$e->getTraceAsString()."\n");
    exit(1);
}
