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
 * MQTT 5.0 Flow Control Example
 *
 * Flow control limits the number of unacknowledged QoS 1/2 messages in flight
 * to prevent overwhelming slower receivers.
 *
 * How it works:
 * 1. Client sends receive_maximum in CONNECT properties
 * 2. Broker sends its receive_maximum in CONNACK properties
 * 3. Neither side may have more than receive_maximum unacknowledged QoS 1/2 packets
 *
 * Benefits:
 * - Prevents buffer overflow on slower clients
 * - Ensures fair resource allocation
 * - Required for proper QoS 1/2 handling in high-throughput scenarios
 *
 * Default value is 65,535 if not specified.
 */

// Load shared broker config
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

// Setup client ID
$clientId = 'php-iot-flow-control-fallback';
try {
    $clientId = 'php-iot-flow-control-'.RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// Configure MQTT 5.0 connection with flow control
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0, // Flow control is MQTT 5.0 feature
)
    ->withClientId($clientId)
    ->withKeepAlive(60)
    ->withCleanSession(true)
    ->withReceiveMaximum(10); // Limit to 10 concurrent in-flight messages

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

echo "Flow Control Example (MQTT 5.0)\n";
echo "   Host: {$config['host']}\n";
echo "   Port: {$options->port}\n";
echo "   Client ID: {$clientId}\n";
echo "   Client Receive Maximum: {$options->receiveMaximum}\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused (reason code: {$result->reasonCode})");
    }

    echo "Connected to MQTT 5.0 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n";

    // Check broker's receive maximum
    $brokerMax = $result->connack->properties['receive_maximum'] ?? 65535;
    echo "   Broker Receive Maximum: {$brokerMax}\n\n";

    $flowControl = $client->flowControl;
    if ($flowControl === null) {
        echo "Flow Control not initialized.\n";
    } else {
        echo "Flow Control initialized with max: {$flowControl->maxInFlight}\n\n";
    }

    // Example: Publish QoS 1 messages with flow control
    echo "Publishing QoS 1 messages with flow control...\n\n";

    $topic   = 'test/flow-control/data';
    $count   = 5;
    $success = 0;

    for ($i = 1; $i <= $count; $i++) {
        $payload = json_encode([
            'message_number' => $i,
            'timestamp'      => time(),
        ]);

        echo "Message {$i}: Publishing QoS 1 message\n";

        if ($flowControl !== null) {
            echo "   In-flight before: {$flowControl->currentInFlight}/{$flowControl->maxInFlight}\n";
            echo '   Can send: '.($flowControl->canSend() ? 'yes' : 'no')."\n";
        }

        $packetId = $client->publish($topic, $payload, new PublishOptions(qos: QoS::AtLeastOnce));

        echo "   Published with Packet ID: {$packetId}\n";

        if ($flowControl !== null) {
            echo "   In-flight after: {$flowControl->currentInFlight}/{$flowControl->maxInFlight}\n";
        }

        echo "\n";
        $success++;
    }

    echo "Summary:\n";
    echo "   Messages published: {$success}/{$count}\n";

    if ($flowControl !== null) {
        echo "   Final in-flight count: {$flowControl->currentInFlight}\n";
        echo "   Max in-flight: {$flowControl->maxInFlight}\n";
    }

    echo "\nFlow Control Behavior:\n";
    echo "   - Client waits for slot before sending QoS 1/2 if at max capacity\n";
    echo "   - Tracks sent messages until PUBACK (QoS 1) or PUBCOMP (QoS 2) received\n";
    echo "   - Prevents overwhelming broker or slower subscribers\n";
    echo "   - Value is negotiated: client sends its limit, broker sends its limit\n\n";

    echo "Disconnecting...\n";
    $client->disconnect();
    echo "Done.\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\nError: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Type: '.get_class($e)."\n");
    fwrite(STDERR, '   Trace: '.$e->getTraceAsString()."\n");
    exit(1);
}
