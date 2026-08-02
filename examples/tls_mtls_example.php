<?php

declare(strict_types=1);

/**
 * Mutual TLS (mTLS) example — client authenticates with certificate.
 *
 * Prerequisites:
 *   1. Generate test certs:  bash examples/certs/generate.sh
 *   2. Start Mosquitto with mTLS:
 *        mosquitto -c examples/certs/mosquitto.conf
 *   3. Run this example:
 *        php examples/tls_mtls_example.php
 */

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Client\TlsOptions;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Transport\TcpTransport;
use ScienceStories\Mqtt\Util\RandomId;

require __DIR__.'/../vendor/autoload.php';

// --- Certificate paths ---
$certsDir = __DIR__.'/certs';
$caFile   = $certsDir.'/ca.pem';
$certFile = $certsDir.'/client.pem';
$keyFile  = $certsDir.'/client.key';

// Verify certs exist
foreach (['ca.pem' => $caFile, 'client.pem' => $certFile, 'client.key' => $keyFile] as $name => $path) {
    if (! is_file($path)) {
        fwrite(STDERR, "Missing $name at $path\n");
        fwrite(STDERR, "Run: bash examples/certs/generate.sh\n");
        exit(1);
    }
}

echo "=== mTLS Example ===\n\n";

// --- Build TlsOptions with client certificate ---
//
// Full verification stays ON. The test CA in $caFile is what makes that work: it signs
// the server certificate, and generate.sh puts localhost/127.0.0.1 in the SAN so the
// hostname check passes. Turning off verifyPeerName or turning on allowSelfSigned here
// would authenticate *us* to the broker while accepting any certificate in return —
// which is the opposite of what mTLS is for. Do not copy those flags into production.
$tls = new TlsOptions(
    verifyPeer: true,
    verifyPeerName: true,
    allowSelfSigned: false,
    caFile: $caFile,
    clientCertificateFile: $certFile,
    clientCertificateKeyFile: $keyFile,
);

echo "TLS config:\n";
echo "  CA file:     $caFile\n";
echo "  Client cert: $certFile\n";
echo "  Client key:  $keyFile\n";
echo "  Verification: peer + hostname, anchored on the test CA\n\n";

// --- Show what gets passed to PHP stream context ---
$ctx = $tls->toStreamContext();
echo "Stream context (ssl):\n";
foreach ($ctx['ssl'] as $k => $v) {
    $display = is_bool($v) ? ($v ? 'true' : 'false') : (string) $v;
    echo "  $k: $display\n";
}
echo "\n";

// --- Connect to local Mosquitto with mTLS ---
$clientId = 'php-iot-mtls-'.RandomId::clientId(6);

$options = new Options(
    host: '127.0.0.1',
    port: 8883,
    version: MqttVersion::V3_1_1,
)
    ->withClientId($clientId)
    ->withKeepAlive(30)
    ->withCleanSession(true)
    ->withTls($tls);

$transport = new TcpTransport();
$client    = new Client($options, $transport);

echo "Connecting to 127.0.0.1:8883 with mTLS...\n";
echo "  Client ID: $clientId\n\n";

try {
    $result = $client->connect();

    if ($result->reasonCode !== 0) {
        throw new RuntimeException("Broker refused connection (code: $result->reasonCode)");
    }

    echo 'Connected! Session present: '.($result->sessionPresent ? 'yes' : 'no')."\n\n";

    // --- Publish ---
    $topic   = 'php-iot/mtls-test';
    $payload = 'mTLS message at '.date('H:i:s');

    echo "Publishing to $topic ...\n";
    $pid = $client->publish($topic, $payload, new PublishOptions(qos: QoS::AtLeastOnce));
    echo "  PUBACK received (packet ID: $pid)\n";

    // --- Byte counters ---
    echo "\nTraffic stats:\n";
    echo '  Bytes sent:     '.$client->bytesSent()."\n";
    echo '  Bytes received: '.$client->bytesReceived()."\n\n";

    // --- Disconnect ---
    $client->disconnect();
    echo "Disconnected.\n";
} catch (Throwable $e) {
    fwrite(STDERR, "\nError: ".$e->getMessage()."\n");
    fwrite(STDERR, 'Type:  '.get_class($e)."\n");
    exit(1);
}
