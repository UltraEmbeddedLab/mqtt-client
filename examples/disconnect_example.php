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
$clientId = 'php-iot-disconnect-fallback';
try {
    $clientId = 'php-iot-disconnect-'.RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// ============================================================================
// MQTT 3.1.1 Disconnect Example
// ============================================================================
echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                  MQTT 3.1.1 DISCONNECT Example                        \n";
echo "═══════════════════════════════════════════════════════════════════════\n\n";

// Configure MQTT 3.1.1 connection
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V3_1_1,
)
    ->withClientId($clientId.'-v3')
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
echo "   Client ID: {$clientId}-v3\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: {$result->reasonCode})");
    }

    echo "✅ Successfully connected to MQTT 3.1.1 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    // ============================================================================
    // Example 1: Simple graceful disconnect
    // ============================================================================
    echo "📤 Example 1: Simple graceful disconnect (MQTT 3.1.1)\n";
    echo "   MQTT 3.1.1 DISCONNECT packet structure:\n";
    echo "      - Fixed header only (2 bytes)\n";
    echo "      - Type: 14, Flags: 0, Remaining Length: 0\n";
    echo "      - No reason code or properties\n";
    echo "      - Always indicates clean disconnect\n\n";

    echo "   Sending DISCONNECT packet...\n";
    $client->disconnect();
    echo "   ✅ Disconnected successfully\n\n";

    echo "📊 Summary (MQTT 3.1.1):\n";
    echo "   - DISCONNECT is a simple 2-byte packet\n";
    echo "   - Always client-initiated (broker cannot disconnect in v3.1.1)\n";
    echo "   - No reason codes or error information\n";
    echo "   - Clean disconnect preserves session if cleanSession=false\n\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\n❌ MQTT 3.1.1 Error: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Error type: '.get_class($e)."\n\n");
}

// ============================================================================
// MQTT 5.0 Disconnect Examples
// ============================================================================
echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                   MQTT 5.0 DISCONNECT Examples                        \n";
echo "═══════════════════════════════════════════════════════════════════════\n\n";

// ============================================================================
// Example 1: Normal disconnect
// ============================================================================
echo "📤 Example 1: Normal disconnect (MQTT 5.0)\n\n";

$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0,
)
    ->withClientId($clientId.'-v5-1')
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

$transport = new TcpTransport();
$client    = new Client($options, $transport);

echo "   Connecting to broker...\n";
try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: {$result->reasonCode})");
    }

    echo "   ✅ Connected successfully\n\n";

    echo "   MQTT 5.0 DISCONNECT packet structure:\n";
    echo "      - Fixed header: Type (14), Flags (0)\n";
    echo "      - Variable header: Reason Code (1 byte, optional)\n";
    echo "      - Properties (varint length + data, optional)\n\n";

    echo "   Sending normal DISCONNECT (reason code: 0x00)...\n";
    $client->disconnect('Normal disconnection');
    echo "   ✅ Disconnected successfully\n\n";
} catch (Throwable $e) {
    fwrite(STDERR, '   ❌ Error: '.$e->getMessage()."\n\n");
}

// ============================================================================
// Example 2: Disconnect with Will Message
// ============================================================================
echo "📤 Example 2: Disconnect with Will Message (MQTT 5.0)\n\n";

$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0,
)
    ->withClientId($clientId.'-v5-2')
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

$transport = new TcpTransport();
$client    = new Client($options, $transport);

echo "   Connecting to broker...\n";
try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: {$result->reasonCode})");
    }

    echo "   ✅ Connected successfully\n\n";

    echo "   MQTT 5.0 Reason Code: 0x04 (Disconnect with Will Message)\n";
    echo "   This reason code tells the broker to publish the Will Message\n";
    echo "   even though this is a normal disconnect.\n\n";

    echo "   Note: Current implementation sends basic DISCONNECT (0x00)\n";
    echo "   Enhanced disconnect with custom reason codes can be added later.\n\n";

    $client->disconnect('Disconnect with Will Message');
    echo "   ✅ Disconnected successfully\n\n";
} catch (Throwable $e) {
    fwrite(STDERR, '   ❌ Error: '.$e->getMessage()."\n\n");
}

// ============================================================================
// Example 3: Connection duration and clean disconnect
// ============================================================================
echo "📤 Example 3: Connection duration tracking (MQTT 5.0)\n\n";

$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0,
)
    ->withClientId($clientId.'-v5-3')
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

$transport = new TcpTransport();
$client    = new Client($options, $transport);

echo "   Connecting to broker...\n";
try {
    $connectTime = microtime(true);
    $result      = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused by broker (reason code: {$result->reasonCode})");
    }

    echo "   ✅ Connected successfully\n\n";

    echo "   Simulating active session...\n";
    echo "   Waiting 3 seconds...\n";
    sleep(3);

    echo "   Sending ping to verify connection...\n";
    if ($client->ping(5.0)) {
        echo "   ✅ Connection is still active\n\n";
    }

    $disconnectTime = microtime(true);
    $duration       = $disconnectTime - $connectTime;

    echo '   Connection duration: '.number_format($duration, 2)." seconds\n";
    echo "   Sending clean DISCONNECT...\n";
    $client->disconnect('Session completed');
    echo "   ✅ Disconnected successfully\n\n";
} catch (Throwable $e) {
    fwrite(STDERR, '   ❌ Error: '.$e->getMessage()."\n\n");
}

// ============================================================================
// Summary of MQTT 5.0 DISCONNECT Features
// ============================================================================
echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                 MQTT 5.0 DISCONNECT Features Summary                  \n";
echo "═══════════════════════════════════════════════════════════════════════\n\n";

echo "📋 MQTT 5.0 Reason Codes (examples):\n";
echo "   • 0x00 (0): Normal disconnection\n";
echo "   • 0x04 (4): Disconnect with Will Message\n";
echo "   • 0x80 (128): Unspecified error\n";
echo "   • 0x81 (129): Malformed Packet\n";
echo "   • 0x87 (135): Not authorized\n";
echo "   • 0x89 (137): Server busy\n";
echo "   • 0x8B (139): Server shutting down\n";
echo "   • 0x8D (141): Keep Alive timeout\n";
echo "   • 0x8E (142): Session taken over\n";
echo "   • 0x97 (151): Quota exceeded\n";
echo "   • 0x9C (156): Use another server\n";
echo "   • 0x9D (157): Server moved\n\n";

echo "📋 MQTT 5.0 DISCONNECT Properties:\n";
echo "   • session_expiry_interval: Session lifetime after disconnect\n";
echo "   • reason_string: Human-readable disconnect reason\n";
echo "   • user_properties: Custom metadata (key-value pairs)\n";
echo "   • server_reference: Alternative server hostname\n\n";

echo "📋 Key Differences from MQTT 3.1.1:\n";
echo "   ✓ MQTT 5.0 supports reason codes for detailed disconnect reasons\n";
echo "   ✓ MQTT 5.0 allows broker-initiated disconnects\n";
echo "   ✓ MQTT 5.0 includes properties for enhanced error information\n";
echo "   ✓ MQTT 5.0 can suggest alternative servers (server_reference)\n";
echo "   ✓ MQTT 3.1.1 is always a simple 2-byte packet (no reason/properties)\n\n";

echo "📋 Use Cases:\n";
echo "   • Normal disconnect: Graceful client shutdown\n";
echo "   • Disconnect with Will: Publish Will Message on clean disconnect\n";
echo "   • Server shutting down: Broker notifies clients of maintenance\n";
echo "   • Session taken over: Another client connected with same Client ID\n";
echo "   • Keep Alive timeout: Client stopped responding to PINGs\n";
echo "   • Quota exceeded: Client exceeded message rate or storage limits\n";
echo "   • Server moved: Broker redirects client to new server\n\n";

echo "═══════════════════════════════════════════════════════════════════════\n";
echo "                       All Examples Completed                           \n";
echo "═══════════════════════════════════════════════════════════════════════\n";
echo "\nKey Takeaways:\n";
echo "• MQTT 3.1.1: Simple 2-byte DISCONNECT packet, client-initiated only\n";
echo "• MQTT 5.0: Enhanced with reason codes and properties\n";
echo "• MQTT 5.0 allows broker-initiated disconnects\n";
echo "• Graceful disconnect preserves clean session state\n";
echo "• Reason codes provide detailed disconnect information\n";
echo "• Properties enable custom metadata and server references\n";
