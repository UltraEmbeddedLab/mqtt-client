# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Inbound QoS 1 messages were silently dropped after the de-duplication cache filled.** The cache is keyed by Packet Identifier, and `array_shift()` renumbers integer keys from 0 — so past `qos1DeduplicationSize` entries every identifier in that range was treated as already-seen, PUBACKed, and discarded. Eviction is now by key. De-duplication also only suppresses re-deliveries flagged `DUP` (MQTT-3.3.1-3); brokers reuse identifiers as soon as they are acknowledged, so matching on the identifier alone discarded unrelated messages.
- **A second `subscribe()`/`unsubscribe()` could kill the process.** `unset()` on a typed property leaves it uninitialized rather than restoring its default, so if any packet interleaved before the SUBACK the next read raised `Error: Typed property must not be accessed before initialization` — outside the `MqttException` hierarchy and uncatchable by documented handlers.
- **Unbounded memory growth under `run()`/`onMessage()`.** Messages were pushed onto the `awaitMessage()` queue *and* passed to the registered handler, but that queue is only drained by `awaitMessage()`/`messages()`. A registered handler now consumes the message without queueing it.
- **A refused CONNECT was reported as a successful connection.** `Client::connect()` never inspected the CONNACK return code, so bad credentials surfaced later as an unrelated read timeout while buffered publishes were replayed into a closed socket. It now closes the transport and throws `AuthenticationError`, `ServerError` or `ProtocolError` via `ReasonCode::toException()`, with a separate mapping for MQTT 3.1.1 return codes 1-5.
- **No bound on inbound packet size.** A five-byte header declaring a 268,435,455-byte Remaining Length forced an allocation of that size, pre-authentication. See `Options::withMaximumPacketSize()` below.
- **The WebSocket handshake could never succeed.** `WS_GUID` was 35 characters instead of the 36 mandated by RFC 6455 §1.3, so `Sec-WebSocket-Accept` never matched and every `ws://`/`wss://` connection failed after a valid HTTP 101. Pinned with the RFC's own test vector.
- **MQTT 3.1.1 CONNECT could be built with a password but no username**, violating MQTT-3.1.2-22 — brokers close the connection without a CONNACK, which is undiagnosable client-side. Now throws `ProtocolError`. (MQTT 5 still permits it.)
- **Keep Alive above 65535 was silently truncated** modulo 65536 by `pack('n', ...)`, so `withKeepAlive(86400)` advertised 20864. Both encoders and `Options::withKeepAlive()` now reject out-of-range values.
- **A PUBLISH with both QoS bits set raised `\ValueError`**, which is outside `MqttException` and escapes every documented catch block. Both decoders now raise `ProtocolError` (MQTT-3.3.1-4).
- `resetConnectionState()` now clears every connection-scoped field — de-duplication cache, pending inbound QoS 2 messages, the outstanding-ping flag and the acknowledgement registers — and runs at the start of `connect()`, so nothing leaks across a reconnect. The inbound message queue is deliberately preserved.
- `Easy\Mqtt::connect()` no longer duplicates the CONNACK check with an untyped `RuntimeException`; the typed exception from `Client::connect()` propagates (it extends `RuntimeException`, so existing catch blocks are unaffected).

### Added
- **`Testing\InMemoryTransport`** — a scriptable `TransportInterface` shipped in `src/` so this library *and its consumers* can unit-test client behaviour without a broker, a socket, or Docker. Provides `feedConnAck()`, `feedPublish()`, `feedSubAck()`, `feedPubAck()` and friends for inbound scripting, plus `sentPackets()`, `countSent()` and `written()` for assertions.
- `Options::withMaximumPacketSize()` — largest accepted inbound packet, checked against the declared Remaining Length *before* the body is read. Defaults to 16 MiB. On MQTT 5 the value is also sent as the Maximum Packet Size property (0x27), making the limit negotiated rather than unilateral.
- `Options::withInboundQueueSize()` — optional bound on the queue backing `awaitMessage()`/`messages()` (0 = unlimited, the default). On overflow the oldest message is dropped and counted on the `inbound_queue_dropped` metric.
- `Bytes::encodeUint16()` — two-byte field encoder that rejects values it cannot represent instead of wrapping modulo 65536.
- MQTT 5 CONNECT now encodes the Maximum Packet Size property (0x27).

### Changed (BREAKING — target 2.0.0, or 1.4.0 only if the two new defaults are made opt-in)

These are behavioural, not source-level, breaks: code that compiles today keeps compiling, but observable behaviour changes.

- **`Client::connect()` throws instead of returning a refused `ConnectResult`.** `ConnectResult::$reasonCode` is consequently always `0`; code branching on it (including the shipped `examples/*.php`) is now dead. Catch `AuthenticationError`, `ServerError` or `ProtocolError` — all extend `MqttException extends RuntimeException`, so an existing `catch (RuntimeException)` still works. `attemptReconnect()` now counts a refused CONNECT against `reconnectMaxAttempts`, where it previously treated the refusal as a successful connect.
- **Inbound packets larger than `maximumPacketSize` (new default 16 MiB) are rejected.** `loopOnce()` logs a warning, closes the transport and returns `false` — it does not throw, so the documented `bool` contract holds and auto-reconnect can recover. `connect()` still throws, having no loop contract to honour. Previously such packets were read in full.
- **MQTT 5 CONNECT now carries the Maximum Packet Size property (0x27)** by default, derived from `maximumPacketSize`. Per §3.1.2.11.4 a compliant broker will then discard larger packets rather than sending them, with no reason code — so a topic carrying payloads above the limit goes quiet. Raise `withMaximumPacketSize()` if that applies to you.
- **The inbound queue is bounded by default** at `Options::DEFAULT_INBOUND_QUEUE_SIZE` (1000 messages). A consumer that keeps up never sees it; one that falls 1000 messages behind now loses the oldest, logged at warning and counted on `inbound_queue_dropped`. Pass `withInboundQueueSize(0)` for the previous unlimited behaviour. `run()` additionally drains the queue each iteration, since the handler it registers has already consumed every message.
- `Options::withKeepAlive()` and the `Options` constructor throw `InvalidArgumentException` for a Keep Alive outside 0..65535, which was previously accepted and truncated by the encoder. `withMaximumPacketSize()` and `withInboundQueueSize()` validate the same way. Note this is an SPL `LogicException`, not an `MqttException` — unifying the configuration-error type is tracked for the next major.
- MQTT 3.1.1 `Connect` packets with a password but no username now throw `ProtocolError` at encode time instead of producing a packet brokers silently reject.

### Changed

- `WsTransport` takes an optional `$maxFrameSize` (default 16 MiB) and rejects a declared WebSocket payload length above it, or one that `unpack('J', ...)` renders negative, before allocating. Necessary because fixing `WS_GUID` made this code path reachable for the first time.
- README: the Quick Start subscribe snippet passed an array of filter arrays to `subscribe()`, which takes `list<string>` — the array was coerced to the literal topic `"Array"` and the SUBACK looked successful. Corrected, with a `subscribeWith()` example alongside.

### Known gaps in this release

- `SessionState::$pendingQos2` is written by `saveSession()` and ignored by `restoreSession()`, so inbound QoS 2 recovery works across a reconnect but not across a process restart.
- `WsTransport` still cannot do `wss://` (TLS is enabled after the HTTP upgrade), drops WebSocket continuation frames, and has no test coverage beyond the handshake constant.
- The shipped `examples/*.php` still branch on `ConnectResult::$reasonCode`; they fail correctly via the exception, but the branch is dead.

## [1.3.0] - 2026-04-04

### Added
- **TlsOptions value object**: Typed, immutable configuration for TLS/SSL connections replacing raw arrays. Supports mutual TLS (client certificates), CA verification, ALPN protocol negotiation, self-signed certificate allowance, and SNI control. Fully backward compatible — `Options::withTls()` accepts both `TlsOptions` and legacy arrays.
- **Mutual TLS (mTLS)**: Client certificate authentication via `TlsOptions::withClientCertificate()` with support for separate cert/key files and optional passphrase. Enables secure device authentication for AWS IoT Core, Azure IoT Hub, and enterprise brokers.
- **ALPN support**: `TlsOptions::withAlpn('mqtt')` for MQTT over port 443 when standard MQTT ports are blocked by firewalls.
- **Byte counters**: `ClientInterface::bytesSent()` and `ClientInterface::bytesReceived()` for monitoring total traffic across all connections and reconnects.
- **QoS 1/2 resend mechanism**: Automatic retransmission of unacknowledged PUBLISH messages with DUP flag. Configurable via `Options::withAckTimeout()` (default 5s) and `Options::withMaxResendAttempts()` (default 3).
- **mTLS example**: `examples/tls_mtls_example.php` with self-signed certificate generation script and Mosquitto Docker configuration for local testing.
- **TlsOptions test suite**: 14 unit tests covering immutability, builder methods, and `toStreamContext()` conversion.

### Changed
- `Options::withTls()` now accepts `TlsOptions|array|null` (union type) for typed TLS configuration while preserving backward compatibility with raw arrays.
- `Easy\Mqtt::publish()`, `send()`, and `connect()` now accept `TlsOptions` in addition to arrays for the `$tlsOptions` parameter.
- `Easy\Mqtt::connect()` defaults to `new TlsOptions()` (verify_peer + verify_peer_name enabled) instead of a raw array when no TLS options are provided.
- `examples/config.php` now builds a `TlsOptions` object from environment variables, supporting `MQTT_TLS_CA_FILE`, `MQTT_TLS_CLIENT_CERT`, `MQTT_TLS_CLIENT_KEY`, `MQTT_TLS_CLIENT_KEY_PASSPHRASE`, `MQTT_TLS_VERIFY_PEER`, `MQTT_TLS_ALLOW_SELF_SIGNED`, and `MQTT_TLS_ALPN`.
- All transport I/O in `Client` now routes through `trackWrite()`/`trackRead()` for byte counting.

## [1.2.0] - 2026-03-29

### Added
- **WebSocket Transport** (`WsTransport`): Full RFC 6455 WebSocket support for `ws://` and `wss://` connections with MQTT subprotocol
- **Rate Limiter** (`RateLimiter`): Token bucket client-side rate limiting to prevent broker flooding, configurable via `Options::withRateLimiter()`
- **Offline Message Queue** (`OfflineQueue`): Buffer publishes during disconnects with automatic drain on reconnect, configurable via `Options::withOfflineQueue()`
- **Request/Response Helper** (`RequestResponse`): MQTT 5.0 request/response pattern with automatic correlation data and response topic management
- **MQTT 5.0 Reason Code Enum** (`ReasonCode`): Complete mapping of 40+ MQTT 5.0 reason codes with descriptions and exception conversion
- **Specific Exception Types**: `AuthenticationError`, `ServerError`, `QuotaExceeded` for granular error handling
- **Performance Benchmarks**: `benchmarks/publish-throughput.php` and `benchmarks/encode-decode.php` for measuring encoding performance
- **Integration Tests**: 10 integration tests with Docker Compose + Mosquitto for real broker testing (connect, publish QoS 0/1/2, subscribe, MQTT 5.0 properties)
- **Code Coverage CI**: Codecov integration in GitHub Actions pipeline
- **SECURITY.md**: Vulnerability reporting policy and security best practices
- **Configurable QoS 1 De-duplication**: `Options::withQos1DeduplicationSize()` to tune the duplicate suppression cache (default 256)
- **Full Metrics Instrumentation**: `MetricsInterface` calls throughout Client for connect, disconnect, subscribe, unsubscribe, ping, reconnect, offline queue, and rate limiting

### Changed
- `TransportScheme` enum now includes `WS` and `WSS` variants
- `Options` constructor accepts `qos1DeduplicationSize`, `rateLimiter`, and `offlineQueueSize` parameters
- CI pipeline now uploads code coverage to Codecov

## [1.1.0] - 2026-03-29

### Added
- Topic validation per MQTT spec (`TopicValidator`) with integration in `Client::publish()` and `Client::subscribe()`
- Shared subscriptions helper (`SharedSubscription`) for MQTT 5.0 `$share/{name}/{filter}` support
- Comprehensive test suite: 217 tests (up from 29), covering V5/V311 Encoder+Decoder, FlowControl, TopicAliasManager, Session, Events, and all DTOs
- CI: Rector compliance check and security audit jobs in GitHub Actions
- Composer scripts: `rector:check`, `test:unit`, `test:integration`, `security:audit`, `validate:strict`, `ci`

### Changed
- DTOs converted to `readonly class` (PHP 8.4): `ConnectResult`, `InboundMessage`, `PublishOptions`, `SubscribeResult`, `UnsubscribeResult`, `MessageReceived`, `PacketReceived`, `PacketSent`, `ServerDisconnect`
- V5 Decoder: consolidated duplicate property parsing into `parseAckProperties()` and `decodeQoSAck()`, reducing ~120 lines of duplicated code
- Rector config updated to `RectorConfig::configure()` fluent API
- Rector modernization applied across 33 files (void return types, instanceof checks, early returns)

### Fixed
- **Security**: TLS `verify_peer` and `verify_peer_name` now default to `true` in `TcpTransport::enableTls()` when not explicitly set
- **Security**: `TopicAliasManager::registerAlias()` now cleans up stale bidirectional mappings when reassigning aliases
- **Security**: V5 Encoder type coercion methods (`toUInt16`, `toUInt32`, `toByte`) now throw `InvalidArgumentException` for invalid types (array, resource) instead of silently returning 0
- Message handler exceptions no longer crash the event loop; errors are logged via PSR-3

## [1.0.0] - 2026-01-18

### Added
- Initial release
- MQTT 3.1.1 protocol support
- MQTT 5.0 protocol support with all properties
- TLS/SSL encryption support
- Auto-reconnect with exponential backoff
- QoS 0, 1, 2 support
- Session persistence
- Topic aliases (MQTT 5.0)
- Flow control (MQTT 5.0)
- Shared subscriptions (MQTT 5.0)
- Event-driven architecture with PSR-14 event dispatcher
- PSR-3 logging support
- Easy facade for simple usage
- Comprehensive examples and documentation
