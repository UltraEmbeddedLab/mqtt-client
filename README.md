# PHP IoT MQTT Client

[![CI](https://github.com/UltraEmbeddedLab/php-iot/actions/workflows/ci.yml/badge.svg)](https://github.com/UltraEmbeddedLab/php-iot/actions/workflows/ci.yml)
[![Latest Stable Version](https://poser.pugx.org/ultraembeddedlab/php-iot/v)](https://packagist.org/packages/ultraembeddedlab/php-iot)
[![License](https://poser.pugx.org/ultraembeddedlab/php-iot/license)](https://packagist.org/packages/ultraembeddedlab/php-iot)
[![PHP Version](https://img.shields.io/packagist/php-v/ultraembeddedlab/php-iot)](https://packagist.org/packages/ultraembeddedlab/php-iot)

Modern, production-grade MQTT 3.1.1 & 5.0 client for PHP 8.4+

## Features

- **Modern PHP 8.4+** with strict types and modern syntax
- **MQTT 3.1.1 & 5.0** protocol support
- **TLS/SSL & mutual TLS (mTLS)** with typed `TlsOptions` — client certificates, CA verification, ALPN
- **WebSocket transport** (`ws://`, `wss://`) with RFC 6455 framing
- **Auto-reconnect** with exponential backoff and jitter
- **QoS 0, 1, 2** with automatic resend on ACK timeout
- **Session persistence** for reliable message delivery
- **Rate limiter** (token bucket) to prevent broker flooding
- **Offline message queue** with automatic drain on reconnect
- **Topic aliases** (MQTT 5.0)
- **Flow control** (MQTT 5.0)
- **Shared subscriptions** (MQTT 5.0)
- **Byte counters** for traffic monitoring (`bytesSent()` / `bytesReceived()`)
- **PSR-3** logging support
- **PSR-14** event dispatcher support

## Requirements

- PHP 8.4 or higher
- `ext-json` (bundled with PHP and not removable since 8.0)
- `ext-openssl` — only for TLS (`mqtts://`, `wss://`). Plain TCP works without it.

No other extensions are needed: all I/O goes through PHP's stream functions.

## Installation

Install via Composer:

```bash
composer require ultraembeddedlab/php-iot
```

## Quick Start

### Simple Publish (Fire and Forget)

The easiest way to publish a message:

```php
use ScienceStories\Mqtt\Easy\Mqtt;

Mqtt::publish(
    host: 'broker.example.com',
    topic: 'sensors/temperature',
    payload: '23.5',
);
```

### Publish with TLS and Authentication

```php
use ScienceStories\Mqtt\Easy\Mqtt;

Mqtt::publish(
    host: 'broker.example.com',
    topic: 'sensors/temperature',
    payload: '23.5',
    tls: true,
    username: 'user',
    password: 'secret',
);
```

### Using MQTT 5.0

```php
use ScienceStories\Mqtt\Easy\Mqtt;
use ScienceStories\Mqtt\Protocol\QoS;

Mqtt::publish(
    host: 'broker.example.com',
    topic: 'sensors/temperature',
    payload: '23.5',
    version: 'v5',
    qos: QoS::AtLeastOnce,
    properties: [
        'message_expiry_interval' => 3600,
        'content_type' => 'text/plain',
    ],
);
```

### Subscribe to Topics

For more complex use cases, use the full client:

```php
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Transport\TcpTransport;

$options = new Options(
    host: 'broker.example.com',
    port: 1883,
    version: MqttVersion::V5_0,
);

$options = $options
    ->withClientId('my-client')
    ->withKeepAlive(60)
    ->withCleanSession(true);

$client = new Client($options, new TcpTransport());
$client->connect();

// Subscribe to topics
$client->subscribe(['sensors/#'], qos: 1);

// ...or subscribeWith() when each filter needs its own QoS
$client->subscribeWith([
    ['filter' => 'sensors/#', 'qos' => 1],
    ['filter' => 'commands/+', 'qos' => 2],
]);

// Handle incoming messages
$client->onMessage(function ($message) {
    echo "Received: {$message->payload} on {$message->topic}\n";
});

// Listen for messages
while (true) {
    $client->loopOnce(1.0);
}
```

### Long-Running Connection

Use the `Mqtt::connect()` method for sessions that need to publish multiple messages:

```php
use ScienceStories\Mqtt\Easy\Mqtt;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Protocol\QoS;

$client = Mqtt::connect(
    host: 'broker.example.com',
    port: 1883,
    version: 'v5',
);

// Publish multiple messages
$client->publish('sensors/temp', '23.5', new PublishOptions(qos: QoS::AtLeastOnce));
$client->publish('sensors/humidity', '65', new PublishOptions(qos: QoS::AtLeastOnce));

$client->disconnect();
```

## Configuration Options

### Client Options

| Option              | Type        | Default   | Description                                             |
|---------------------|-------------|-----------|---------------------------------------------------------|
| `host`              | string      | required  | MQTT broker hostname                                    |
| `port`              | int         | 1883/8883 | Broker port (auto-detected based on TLS)                |
| `version`           | MqttVersion | V3_1_1    | MQTT protocol version                                   |
| `clientId`          | string      | auto      | Client identifier                                       |
| `keepAlive`         | int         | 60        | Keep alive interval in seconds                          |
| `cleanSession`      | bool        | true      | Start with clean session                                |
| `username`          | string      | null      | Authentication username                                 |
| `password`          | string      | null      | Authentication password                                 |
| `ackTimeout`         | float       | 5.0       | Timeout (seconds) waiting for QoS 1/2 ACK before resend                         |
| `maxResendAttempts`  | int         | 3         | Max resend attempts for unacknowledged QoS 1/2 messages                        |
| `maximumPacketSize`  | int         | 16 MiB    | Largest accepted inbound packet; also sent as MQTT 5 property `0x27`           |
| `inboundQueueSize`   | int         | 1000      | Bound on the `awaitMessage()`/`messages()` queue (0 = unlimited)               |

### Publish Options

| Option       | Type  | Default    | Description              |
|--------------|-------|------------|--------------------------|
| `qos`        | QoS   | AtMostOnce | Quality of Service level |
| `retain`     | bool  | false      | Retain message on broker |
| `properties` | array | null       | MQTT 5.0 properties      |

### TLS Configuration

Simple TLS (server verification only):

```php
use ScienceStories\Mqtt\Client\TlsOptions;

$options = $options->withTls(new TlsOptions());
```

Mutual TLS with client certificate (AWS IoT, Azure IoT Hub):

```php
use ScienceStories\Mqtt\Client\TlsOptions;

$tls = (new TlsOptions())
    ->withCaFile('/etc/mqtt/certs/ca.pem')
    ->withClientCertificate(
        certFile: '/etc/mqtt/certs/client.pem',
        keyFile: '/etc/mqtt/certs/client.key',
        passphrase: 'optional-passphrase',
    );

$options = $options->withTls($tls);
```

MQTT over port 443 with ALPN (when 8883 is blocked):

```php
$tls = (new TlsOptions())
    ->withCaFile('/etc/mqtt/certs/ca.pem')
    ->withClientCertificate('/etc/mqtt/certs/client.pem', '/etc/mqtt/certs/client.key')
    ->withAlpn('mqtt');

$options = (new Options('broker.example.com', 443))->withTls($tls);
```

Self-signed certificates (development):

```php
$tls = (new TlsOptions())
    ->withCaFile('/path/to/my-ca.pem')
    ->withAllowSelfSigned(true);

$options = $options->withTls($tls);
```

| TlsOptions Method                                  | Description                                 |
|----------------------------------------------------|---------------------------------------------|
| `withCaFile(?string)`                              | CA certificate file for server verification |
| `withCaPath(?string)`                              | Directory of CA certificates                |
| `withClientCertificate(?string, ?string, ?string)` | Client cert, key, and optional passphrase   |
| `withAlpn(?string)`                                | ALPN protocol (e.g., `'mqtt'` for port 443) |
| `withVerifyPeer(bool)`                             | Verify server certificate (default: `true`) |
| `withVerifyPeerName(bool)`                         | Verify server hostname (default: `true`)    |
| `withAllowSelfSigned(bool)`                        | Allow self-signed certs (default: `false`)  |
| `withPeerName(?string)`                            | Override peer name for SNI                  |
| `withSni(bool)`                                    | Enable/disable SNI (default: `true`)        |

> Legacy `array` syntax is still supported for backward compatibility:
> `$options->withTls(['ssl' => ['verify_peer' => true]])`

## MQTT 5.0 Features

### Topic Aliases

Reduce bandwidth by using numeric aliases for frequently used topics:

```php
$client->publish('long/topic/name', 'data', new PublishOptions(
    properties: ['topic_alias' => 1],
));
```

### Message Expiry

Set expiration time for messages:

```php
$client->publish('alerts/warning', 'Alert!', new PublishOptions(
    properties: ['message_expiry_interval' => 300], // 5 minutes
));
```

### User Properties

Attach custom metadata to messages:

```php
$client->publish('events/user', $payload, new PublishOptions(
    properties: [
        'user_properties' => [
            'source' => 'web-app',
            'version' => '1.0',
        ],
    ],
));
```

## Documentation

Detailed documentation is available in the `docs/` directory:

- [Flow Control](docs/flow-control.md)
- [Session Persistence](docs/session-persistence.md)
- [Shared Subscriptions](docs/shared-subscriptions.md)
- [Topic Aliases](docs/topic-aliases.md)
- [Server Disconnect](docs/server-disconnect.md)

## Examples

Check the `examples/` directory for complete working examples:

- Basic connect/publish/subscribe (MQTT 3.1.1 and 5.0)
- QoS 0, 1, 2 demonstrations
- **mTLS with client certificates** (`tls_mtls_example.php` + cert generation script)
- Session persistence, shared subscriptions, topic aliases
- Flow control, server disconnect handling

## Testing

```bash
# Run tests
composer test

# Run tests with coverage
composer test:coverage

# Static analysis
composer stan

# Code style
composer pint
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

PHP IoT MQTT Client is open-sourced software licensed under the [MIT license](LICENSE.md).

## Credits

Developed by [Bogdan Gewald](mailto:gewaldb@gmail.com)
