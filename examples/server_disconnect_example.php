<?php

declare(strict_types=1);

use Psr\EventDispatcher\EventDispatcherInterface;
use Psr\EventDispatcher\StoppableEventInterface;
use Random\RandomException;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Events\ServerDisconnect;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Transport\TcpTransport;
use ScienceStories\Mqtt\Util\RandomId;

require __DIR__.'/../vendor/autoload.php';

/**
 * MQTT 5.0 Server-Initiated Disconnect Handling Example
 *
 * In MQTT 5.0, the broker can send a DISCONNECT packet to the client
 * with a reason code explaining why the connection is being closed.
 *
 * Common server-initiated disconnect reasons:
 * - 0x8B (139): Server shutting down
 * - 0x8D (141): Keep Alive timeout
 * - 0x8E (142): Session taken over (another client connected with same ID)
 * - 0x93 (147): Receive Maximum exceeded
 * - 0x95 (149): Packet too large
 * - 0x96 (150): Message rate too high
 * - 0x97 (151): Quota exceeded
 *
 * MQTT 5.0 DISCONNECT may also include:
 * - reason_string: Human-readable explanation
 * - server_reference: Alternative server to connect to
 * - session_expiry_interval: Updated session expiry
 *
 * This example demonstrates:
 * - Handling ServerDisconnect events via PSR-14 event dispatcher
 * - Inspecting disconnect reason codes and properties
 * - Implementing reconnection logic based on disconnect reason
 */

// Simple PSR-14 implementation for this example
class SimpleEventDispatcher implements EventDispatcherInterface
{
    /** @var array<string, list<callable>> */
    private array $listeners = [];

    public function addListener(string $eventClass, callable $listener): void
    {
        $this->listeners[$eventClass][] = $listener;
    }

    public function dispatch(object $event): object
    {
        $eventClass = get_class($event);
        foreach ($this->listeners[$eventClass] ?? [] as $listener) {
            $listener($event);
            if ($event instanceof StoppableEventInterface && $event->isPropagationStopped()) {
                break;
            }
        }

        return $event;
    }
}

// Load shared broker config
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

// Setup client ID
$clientId = 'php-iot-disconnect-demo-fallback';
try {
    $clientId = 'php-iot-disconnect-demo-'.RandomId::clientId(6);
} catch (RandomException $e) {
    // Keep fallback client ID
}

$port = $config['port'] ?? (($config['scheme'] ?? 'tcp') === 'tls' ? 8883 : 1883);

// Create event dispatcher and register ServerDisconnect listener
$eventDispatcher = new SimpleEventDispatcher();

$eventDispatcher->addListener(ServerDisconnect::class, function (ServerDisconnect $event) {
    $disconnect = $event->disconnect;

    echo "\n========================================\n";
    echo "SERVER DISCONNECT EVENT RECEIVED\n";
    echo "========================================\n";
    echo '   Reason Code: 0x'.dechex($disconnect->reasonCode)." ($disconnect->reasonCode)\n";
    echo "   Description: {$disconnect->getReasonDescription()}\n";
    echo '   Is Error: '.($disconnect->isError() ? 'yes' : 'no')."\n";
    echo '   Is Normal: '.($disconnect->isNormal() ? 'yes' : 'no')."\n";
    echo '   Will Reconnect: '.($event->willReconnect ? 'yes' : 'no')."\n";

    // Check for reason string
    $reasonString = $disconnect->getReasonString();
    if ($reasonString !== null) {
        echo "   Reason String: $reasonString\n";
    }

    // Check for server reference (alternative server)
    $serverRef = $disconnect->getServerReference();
    if ($serverRef !== null) {
        echo "   Server Reference: $serverRef\n";
        echo "   (Consider connecting to this alternative server)\n";
    }

    // Check for session expiry update
    $sessionExpiry = $disconnect->getSessionExpiryInterval();
    if ($sessionExpiry !== null) {
        echo "   Session Expiry: $sessionExpiry seconds\n";
    }

    // Log user properties
    $userProps = $disconnect->getUserProperties();
    if (count($userProps) > 0) {
        echo "   User Properties:\n";
        foreach ($userProps as $key => $value) {
            echo "      $key: $value\n";
        }
    }

    echo "========================================\n\n";

    // Handle specific disconnect reasons
    switch ($disconnect->reasonCode) {
        case 0x8E: // Session taken over
            echo "Another client connected with the same Client ID.\n";
            echo "Consider using a unique Client ID per device.\n";
            break;
        case 0x8B: // Server shutting down
            echo "Server is shutting down for maintenance.\n";
            echo "Try reconnecting after a delay.\n";
            break;
        case 0x8D: // Keep Alive timeout
            echo "Connection timed out due to keep-alive.\n";
            echo "Consider increasing keep-alive interval or sending pings more frequently.\n";
            break;
        case 0x97: // Quota exceeded
            echo "Resource quota exceeded.\n";
            echo "Reduce message rate or contact broker administrator.\n";
            break;
    }
});

// Configure MQTT 5.0 connection
$options = new Options(
    host: $config['host'],
    port: $port,
    version: MqttVersion::V5_0, // Server DISCONNECT is MQTT 5.0 feature
)
    ->withClientId($clientId)
    ->withKeepAlive(60)
    ->withCleanSession(true)
    ->withAutoReconnect(true, maxAttempts: 3); // Auto-reconnect on disconnect

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

// Init transport + client with event dispatcher
$transport = new TcpTransport();
$client    = new Client($options, $transport, events: $eventDispatcher);

echo "Server Disconnect Handling Example (MQTT 5.0)\n";
echo "   Host: {$config['host']}\n";
echo "   Port: $options->port\n";
echo "   Client ID: $clientId\n";
echo "   Auto-Reconnect: enabled\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Connection refused (reason code: $result->reasonCode)");
    }

    echo "Connected to MQTT 5.0 broker\n";
    echo '   Session Present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    echo "Subscribing to test topic...\n";
    $client->subscribe(['test/server-disconnect/#'], qos: 1);

    echo "Waiting for messages or server disconnect...\n";
    echo "(The broker may send DISCONNECT for various reasons)\n";
    echo "(To trigger disconnect, try connecting another client with same ID)\n";
    echo "(Press Ctrl+C to stop)\n\n";

    // Set up message handler
    $client->onMessage(function ($msg) {
        echo "Received: $msg->topic -> $msg->payload\n";
    });

    // Run message loop
    $client->run(fn ($msg) => null, idleSleep: 0.1);
} catch (Throwable $e) {
    fwrite(STDERR, "\nError: ".$e->getMessage()."\n");
    fwrite(STDERR, '   Type: '.get_class($e)."\n");
    fwrite(STDERR, '   Trace: '.$e->getTraceAsString()."\n");
    exit(1);
}
