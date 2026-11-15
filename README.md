# PHP IoT MQTT Client

[![CI](https://github.com/UltraEmbeddedLab/mqtt-client/actions/workflows/ci.yml/badge.svg)](https://github.com/UltraEmbeddedLab/mqtt-client/actions/workflows/ci.yml)
[![Latest Stable Version](https://poser.pugx.org/ultraembeddedlab/mqtt-client/v)](https://packagist.org/packages/ultraembeddedlab/mqtt-client)
[![License](https://poser.pugx.org/ultraembeddedlab/mqtt-client/license)](https://packagist.org/packages/ultraembeddedlab/mqtt-client)
[![PHP Version](https://img.shields.io/packagist/php-v/ultraembeddedlab/mqtt-client)](https://packagist.org/packages/ultraembeddedlab/mqtt-client)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%20max-brightgreen)](https://phpstan.org/)

Modern, production-grade MQTT 3.1.1 & 5.0 client for PHP 8.5+

## Features

- **MQTT 3.1.1 & 5.0** — QoS 0, 1 and 2, retained messages, Last Will, MQTT 5 properties
- **Built for connections that stay open for days** — Keep Alive driven by what the client
  sends, a silent or black-holed link detected and closed, every deadline on a monotonic
  clock, and a receive path that survives a timeout at any byte of a packet
- **TLS 1.2+ & mutual TLS (mTLS)** — TLS 1.0/1.1 refused by default (RFC 8996); typed `TlsOptions` for client certificates, CA verification and ALPN
- **WebSocket transport** (`ws://`) with RFC 6455 framing — `wss://` is not functional yet, see [Known Limitations](#known-limitations)
- **Auto-reconnect** with exponential backoff and jitter, re-subscribing on its own
- **QoS 1 and 2 resend** with the DUP flag on acknowledgement timeout
- **Session persistence** — subscriptions survive a restart (`FileSessionStore`, or your own `SessionStoreInterface`)
- **Offline message queue** with automatic drain on reconnect
- **Rate limiter** (token bucket) to keep from flooding a broker
- **MQTT 5 flow control, shared subscriptions and topic aliases** in both directions
- **Unit-testable without a broker** — `Testing\InMemoryTransport` scripts the broker's side, see [Testing](docs/testing.md)
- **PSR-3** logging, **PSR-14** events, byte counters (`bytesSent()` / `bytesReceived()`)
- **Modern PHP 8.5** with strict types and PHPStan at level max; the immutable options carry
  `#[\NoDiscard]`, so a `with*()` call whose result is thrown away warns instead of silently
  doing nothing

## Requirements

- PHP 8.5 or higher
- `ext-json` (bundled with PHP and not removable since 8.0)
- `ext-openssl` — only for TLS (`mqtts://`, `wss://`). Plain TCP works without it.

No other extensions are needed: all I/O goes through PHP's stream functions.

## Installation

Install via Composer:

```bash
composer require ultraembeddedlab/mqtt-client
```

> Upgrading from `ultraembeddedlab/php-iot` 1.x? The package was renamed in 2.0 — the PHP
> namespace is unchanged, so no `use` statement moves. See [UPGRADE.md](UPGRADE.md).

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
| `port`              | int         | `1883`    | Broker port. **Not** derived from TLS — pass `8883` yourself. (`Easy\Mqtt::publish()` auto-detects; `Options` does not.) |
| `version`           | MqttVersion | V3_1_1    | MQTT protocol version                                   |
| `clientId`          | string      | `''`      | Client identifier. Empty means the broker assigns one, which MQTT 3.1.1 allows only with `cleanSession: true`. `Easy\Mqtt` generates one for you. |
| `keepAlive`         | int         | 60        | Keep alive interval in seconds (0–65535). A broker that sends Server Keep Alive in CONNACK overrides this. |
| `pingResponseTimeout` | float     | 10.0      | How long to wait for a PINGRESP before treating the connection as dead and closing it |
| `cleanSession`      | bool        | true      | Start with clean session                                |
| `username`          | string      | null      | Authentication username                                 |
| `password`          | string      | null      | Authentication password                                 |
| `will`              | ?WillOptions | null     | Last Will and Testament                                 |
| `autoReconnect`     | bool        | false     | Reconnect with exponential backoff — see `withAutoReconnect()` |
| `offlineQueueSize`  | int         | 0         | Publishes buffered while disconnected, drained on reconnect (0 = off) |
| `rateLimiter`       | ?RateLimiter | null     | Token-bucket throttle for outbound publishes            |
| `sessionStore`      | ?SessionStoreInterface | null | Persist subscriptions across restarts (use with `cleanSession: false`) |
| `topicAliasMaximum` | int         | 0         | MQTT 5 topic aliases the broker may use when sending to this client (0 = none). Outbound aliases follow the broker's limit. |
| `receiveMaximum`    | int         | 65535     | MQTT 5 flow-control window                              |
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

// Note the explicit port: withTls() does not change it, and the default is 1883.
$options = (new Options('broker.example.com', 8883))->withTls(new TlsOptions());
```

> `withHost('other-broker')` resets the port to 1883. When changing host on an existing
> `Options`, pass both: `withHost('other-broker', 8883)`.

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
| `withCryptoMethod(int)`                            | Override negotiated TLS versions (bitmask of `STREAM_CRYPTO_METHOD_*_CLIENT`) |

> **TLS 1.2 and 1.3 only, by default.** PHP's own `STREAM_CRYPTO_METHOD_TLS_CLIENT` still
> enables TLS 1.0 and 1.1, which RFC 8996 marks MUST NOT and PCI-DSS prohibits. This client
> refuses them unless you widen the policy explicitly:
>
> ```php
> // Only for a broker that genuinely cannot do TLS 1.2.
> $tls = (new TlsOptions())->withCryptoMethod(
>     STREAM_CRYPTO_METHOD_TLSv1_1_CLIENT | TlsOptions::DEFAULT_CRYPTO_METHOD,
> );
> ```

> Legacy `array` syntax is still supported for backward compatibility:
> `$options->withTls(['ssl' => ['verify_peer' => true]])`

## MQTT 5.0 Features

### Topic Aliases

Topic aliases replace a repeated topic string with a two-byte integer. Each direction has
its own aliases and its own limit, and both are handled for you:

```php
$options = (new Options('broker.example.com', 1883, version: MqttVersion::V5_0))
    // Let the broker use up to 10 aliases when it sends to us. Aliased messages still
    // reach your handler with their full topic.
    ->withTopicAliasMaximum(10);

$client = new Client($options, new TcpTransport());
$client->connect();

// Outbound aliasing needs no setting: it follows the limit the broker sends in CONNACK.
// The first publish sends the topic and establishes the alias...
$client->publish('factory/line-3/press/temperature', '218.4');
// ...later ones to the same topic send two bytes instead of thirty-four.
$client->publish('factory/line-3/press/temperature', '218.9');
```

If the broker advertises a Topic Alias Maximum of 0, publishes use full topic strings. A
broker that sends an alias above the limit you set, or one it never established, has the
connection closed, as MQTT 5 requires, and a `ServerDisconnect` event dispatched. With
auto-reconnect on the client then reconnects; with it off, `loopOnce()` throws
`ProtocolError`.

> Do not set the `topic_alias` property on `PublishOptions`. Aliases belong to the client
> and to the connection, so the client drops a hand-set one — including when you forward a
> received message's properties, which never contain the broker's alias.

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

## Error Handling

Every error this library raises extends `ScienceStories\Mqtt\Exception\MqttException`,
which extends `RuntimeException` — so a single `catch` covers the whole surface.

| Exception            | Raised when                                                                 |
|----------------------|-----------------------------------------------------------------------------|
| `AuthenticationError` | The broker refused the credentials (CONNACK 4/5 on 3.1.1, 0x86/0x87 on MQTT 5) |
| `ServerError`         | The broker is unavailable, busy, or shutting down                          |
| `QuotaExceeded`       | A broker rate or quota limit was hit                                       |
| `ProtocolError`       | A malformed packet, an oversized packet, or an invalid local configuration  |
| `Timeout`             | No ACK or data arrived within the deadline                                  |
| `TransportError`      | Socket or TLS failure — connection refused, closed by peer, handshake failed |

`connect()` throws rather than returning a failed result, so a refused connection cannot
be mistaken for a live one:

```php
use ScienceStories\Mqtt\Exception\AuthenticationError;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;

try {
    $client->connect();
} catch (AuthenticationError $e) {
    // Bad credentials — retrying will not help.
    throw $e;
} catch (TransportError|Timeout $e) {
    // Network-level; safe to retry with backoff.
    $logger->warning('Broker unreachable', ['error' => $e->getMessage()]);
}
```

A long-running loop should survive transient failures rather than dying on the first one:

```php
use ScienceStories\Mqtt\Exception\MqttException;

$client->onMessage(fn ($message) => handle($message));

while (true) {
    try {
        $client->loopOnce(1.0);
    } catch (MqttException $e) {
        $logger->error('MQTT loop error', ['error' => $e->getMessage()]);
        usleep(500_000);
    }
}
```

With `withAutoReconnect()` enabled, `loopOnce()` re-establishes the connection and
re-subscribes on its own; it returns `false` while disconnected rather than throwing.

## Known Limitations

Stated up front rather than discovered in production:

- **`wss://` does not work.** `WsTransport` performs the HTTP upgrade before TLS can be
  enabled, so only `ws://` completes a handshake. `ws://` itself — masking, fragmented
  messages, ping/pong — is unit-tested and verified against Mosquitto 2.0.
- **Acknowledgement reason codes are not acted on.** A PUBACK or SUBACK carrying a failure
  code is logged, not raised — a rejected subscription looks successful. `subscribeWith()`
  returns the codes in its `SubscribeResult`; `subscribe()` does not.
- **QoS 2 retransmission is not yet conforming.** Once PUBREC has arrived only PUBREL may
  be sent again, but if PUBCOMP is late the client resends the PUBLISH (with DUP) instead.
- **Broker-advertised limits are not enforced.** Maximum QoS, Retain Available and Maximum
  Packet Size from CONNACK are decoded but not checked before publishing.
- **Session persistence covers subscriptions only.** In-flight QoS 1/2 messages are not
  saved, so they do not survive a process restart.
- **The client is blocking.** There is no ReactPHP/Amp adapter yet; run it in a dedicated
  process or worker.
- **No Laravel or Symfony bridge yet.**

Each of these is scheduled in the [Roadmap](ROADMAP.md) — most of them for 2.2.

## Documentation

- [Upgrade guide](UPGRADE.md) — 2.0 → 2.1, and 1.x → 2.0
- [Changelog](CHANGELOG.md) — every change, with the reasoning behind it
- [Backward Compatibility Promise](docs/backward-compatibility.md) — what semver covers here
- [Roadmap](ROADMAP.md) — what is planned, and where help is wanted

Feature guides in `docs/`:

- [Testing](docs/testing.md) — unit-test your MQTT code with `InMemoryTransport`, no broker needed
- [Flow Control](docs/flow-control.md) — MQTT 5 receive-maximum and in-flight limits
- [Session Persistence](docs/session-persistence.md) — surviving restarts with `cleanSession: false`
- [Shared Subscriptions](docs/shared-subscriptions.md) — `$share/` load balancing across consumers
- [Topic Aliases](docs/topic-aliases.md) — MQTT 5 bandwidth optimisation
- [Server Disconnect](docs/server-disconnect.md) — reacting to a broker-initiated DISCONNECT

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

Developed by [Bogdan Gewald](mailto:gewaldb@gmail.com).

UltraEmbeddedLab is the publishing organisation for this package; copyright is held by
Bogdan Gewald, as stated in [LICENSE.md](LICENSE.md). Contributions are accepted under the
[Developer Certificate of Origin](DCO) — inbound licence equals outbound licence, MIT. There
is no copyright assignment and no CLA.

The PHP namespace is `ScienceStories\Mqtt\` for historical reasons: the package was
originally published as `science-stories/php-iot`. It is unchanged for backwards
compatibility and is scheduled to be renamed in 3.0 with `class_alias()` shims — see
[ROADMAP.md](ROADMAP.md).
