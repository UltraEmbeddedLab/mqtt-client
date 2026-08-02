# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed — the client can now hold a connection open

Four defects that only show up on a long-lived connection, which is what this library is for.

- **Keep Alive was driven by inbound traffic.** `touchActivity()` ran after every read and the ping timer measured from it, but MQTT-3.1.2-23 obliges the *client* to send within the window — receiving does not count. A subscriber on a busy topic therefore never sent PINGREQ and was dropped by the broker every 1.5×keepAlive, then flapped forever. The clock is now split: `$lastSent` (updated in `trackWrite()`, so every outbound packet counts) drives the ping; `$lastReceived` is kept for liveness only.
- **A single lost PINGRESP disabled keepalive for the life of the process.** The outstanding-ping flag was set on send and cleared only on receipt, with no deadline — so after one unanswered PINGREQ the client sat on a dead socket that still reported `isOpen()`, receiving nothing, with auto-reconnect never firing because the transport looked healthy. This is the classic NAT-eviction / black-holed-broker failure. There is now a deadline (`Options::withPingResponseTimeout()`, default 10s). It is evaluated only after a poll that found the socket **silent**, never on elapsed wall time alone — judging it on the clock would tear down a healthy connection whenever the caller's loop interval exceeds the timeout, which is ordinary for a worker polling every 30s or a handler that blocks on a database write. Two silent polls are required, so one slow iteration cannot condemn a live link, and any inbound packet clears the ping since it proves the link is alive even if the PINGRESP itself was lost. On expiry the client dispatches `Events\ServerDisconnect`, closes the transport, and — when `autoReconnect` is off, which is the default — stops `run()`/`messages()` rather than spinning on a closed socket.
- **`server_keep_alive` from CONNACK was decoded and ignored**, violating MQTT-3.2.2-22. Requesting 300s against a broker that grants 60s produced an endless 90-second connect/drop loop. The granted value is now what the client pings on, and the override is logged.
- **Every deadline used wall-clock `microtime()`.** An NTP step of +45s expired all of them at once: an in-flight QoS 1 publish "timed out", resent with DUP, exhausted its retries and threw for a message the broker had already acknowledged. A backward step stalled keepalive past the broker's window. All 36 sites across the client, flow control, rate limiter and both transports now read `Util\Clock::now()`, which is monotonic `hrtime()`. `time()` is deliberately kept for `SessionState::$savedAt`, where wall-clock is the correct meaning.
- **Flow control leaked a slot on every failed publish.** The QoS 1/2 wait released its slot only via PUBACK/PUBCOMP, so a timeout kept it forever. With `receive_maximum = 10`, ten timeouts during a GC pause permanently exhausted the window and every later QoS 1/2 publish threw "timed out waiting for slot" until the process restarted. The slot is now reclaimed by a sweep in `loopOnce()` — deliberately not the instant `publish()` gives up, because MQTT-4.9.0-1 counts an unacknowledged message against Receive Maximum until the broker acknowledges it, and freeing it early would let the client exceed the window the broker granted. The sweep waits out the full resend budget, doubled.

### Added

- `Options::withPingResponseTimeout()` and `Options::$pingResponseTimeout` (default 10.0s).
- `Util\Clock` — the monotonic time source the whole library now reads.

## [2.0.0] - 2026-08-02

Two things happened at once: the package was renamed, and a set of defects were fixed in
ways that change observable behaviour. See **[UPGRADE.md](UPGRADE.md)** for the migration —
it is about fifteen minutes for a typical application, and if you already wrap `connect()`
in a broad `catch (RuntimeException)` you may need to do nothing at all.

What is and is not covered by semver from here on is written down in
[docs/backward-compatibility.md](docs/backward-compatibility.md).

### Changed — BREAKING

- **The package is now `ultraembeddedlab/mqtt-client`** (was `ultraembeddedlab/php-iot`).
  The PHP namespace is unchanged — still `ScienceStories\Mqtt\` — so no `use` statement in
  your code moves. The old package name is declared in `replace`, so Composer will not
  install both; they share a namespace. Renaming the namespace is deferred to 3.0 to keep
  this upgrade mechanical.
- **`Client::connect()` throws instead of returning a refused `ConnectResult`.** It never
  inspected the CONNACK return code, so a wrong password looked like a live connection and
  surfaced later as an unrelated read timeout. It now closes the transport and throws
  `AuthenticationError`, `ServerError` or `ProtocolError` via `ReasonCode::toException()`,
  with a separate mapping for MQTT 3.1.1 return codes 1–5. All extend `RuntimeException`.
  `ConnectResult::$reasonCode` is consequently always `0` and is deprecated.
- **Inbound packets are capped at 16 MiB** (`Options::$maximumPacketSize`), checked against
  the declared Remaining Length before the body is read. There was previously no bound, so
  five header bytes could force a 256 MiB allocation pre-authentication. On MQTT 5 the value
  is advertised as property `0x27` so the limit is negotiated rather than unilateral.
- **The inbound queue is bounded at 1000 messages** (`Options::$inboundQueueSize`). It backs
  `awaitMessage()`/`messages()` and was unbounded; in the push-only pattern nothing drained
  it. Pass `withInboundQueueSize(0)` for the old behaviour.
- **TLS 1.0 and 1.1 are refused.** Both transports hard-coded
  `STREAM_CRYPTO_METHOD_TLS_CLIENT`, which enables versions RFC 8996 marks MUST NOT and
  PCI-DSS prohibits. Default is TLS 1.2 + 1.3, overridable via
  `TlsOptions::withCryptoMethod()`.
- **Out-of-range configuration throws** rather than being silently accepted.
  `Options::withKeepAlive()` and the `Options` constructor reject a Keep Alive outside
  0–65535 — previously `withKeepAlive(86400)` was truncated to 20864 by `pack('n', ...)`,
  dropping the connection every few hours for no visible reason.
- **MQTT 3.1.1 CONNECT with a password but no username throws `ProtocolError`**
  (MQTT-3.1.2-22). Brokers closed the connection without a CONNACK, which was
  undiagnosable client-side. MQTT 5 still permits it.
- **A malformed PUBLISH raises `ProtocolError`, not `\ValueError`.** Both QoS bits set
  previously escaped the `MqttException` hierarchy and killed long-running loops.

### Deprecated

- `ConnectResult::$reasonCode` — always `0` now that `connect()` throws on refusal.
  Scheduled for removal in 3.0.

### Fixed — distribution and secure defaults

- **`composer require` failed on the official `php:8.4-cli` and `php:8.4-fpm` images.** `ext-sockets` sat in `require` while nothing in `src/` ever called a `socket_*` function — all I/O goes through `stream_socket_client()`. Removed. `ext-json` (used by `FileSessionStore` and `ConsoleLogger`) is now declared; `ext-openssl` stays a suggestion, since plain TCP does not need it, but both transports now fail with `Cannot enable TLS: ext-openssl is not loaded` instead of a cryptic crypto error. A new CI job installs the package on a stock `php:8.4-cli` container so a phantom extension requirement cannot come back.
- **`.gitignore` was untracked because it ignored itself**, so a fresh clone had no ignore rules at all and `git add -A` could commit `vendor/`, `.env` and the private keys `examples/certs/generate.sh` produces. The file and `examples/certs/.gitignore` are now tracked. (`CLAUDE.md` remains ignored by intent.)
- **17 of 20 examples fatally failed on a fresh clone**, requiring a gitignored `examples/config.php` with no committed template. `examples/config.php.dist` is now committed and targets the Mosquitto in `docker-compose.yml`; every example falls back to it. `.env.example` documents all ten variables instead of three.
- **`examples/config.php.dist` parses booleans with `filter_var()`.** The previous `(bool) $env(...)` made `MQTT_TLS_ALLOW_SELF_SIGNED=false` evaluate to `true` — writing the flag out explicitly disabled certificate checking. An unparseable value is now an error, and enabling self-signed certificates with no CA file prints a warning.
- **The mTLS example no longer ships insecure flags.** It set `verifyPeerName: false, allowSelfSigned: true` alongside a CA file, which made the CA irrelevant: it authenticated the client to the broker while accepting any server certificate. `examples/certs/generate.sh` now writes a `subjectAltName` (localhost, mosquitto, 127.0.0.1) so full verification works, sets `extendedKeyUsage` on both leaf certificates, and `chmod 600`s the private keys.
- **TLS 1.0 and 1.1 are no longer negotiable.** Both transports hard-coded `STREAM_CRYPTO_METHOD_TLS_CLIENT`, which enables 1.0/1.1 — RFC 8996 says MUST NOT and PCI-DSS prohibits them. The default is now TLS 1.2 + 1.3, overridable via `TlsOptions::withCryptoMethod()` for a broker that genuinely cannot do 1.2.
- **A failed TLS handshake reported only "TLS negotiation failed"** and left a half-open socket, which kept `isOpen()` true and suppressed auto-reconnect. Both transports now drain `openssl_error_string()` into the message — so "certificate verify failed" is distinguishable from "wrong version number" (i.e. connected to the plaintext port) — and close the socket before throwing.
- `docs/flow-control.md`, `docs/topic-aliases.md` and `docs/shared-subscriptions.md` referenced private properties (`$client->flowControl`, `$client->topicAliasManager`), a method that does not exist (`FlowControl::getSendTime()`), and `$result->connack` where the property is `$connAck` — the last of which failed *silently*, making the broker-capability guard always report "supported". Rewritten against the real public API.

### Changed — CI and packaging

- CI now starts the repo's Mosquitto (`docker compose up -d --wait`) and **fails if the Integration suite skips**. Previously there was no broker in CI, so all ten real-broker tests self-skipped on every run and the build reported green having exercised nothing that touches a socket.
- Coverage gates are actually enforced: `--min=85` on type coverage (the documented floor could never fail without it) and a line-coverage floor to be ratcheted to the first measured value.
- Every GitHub Action is pinned to a commit SHA with the tag in a trailing comment.
- New `examples` job lints every example, benchmark and the config template, and asserts the config loads on a fresh clone.
- `.gitattributes` gained the full `export-ignore` set: the dist tarball drops from 169 files to 86 (`src/` plus `composer.json`, `README.md`, `CHANGELOG.md`, `LICENSE.md`, `SECURITY.md`).

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

### Changed — BREAKING, in detail

The summary is under "Changed — BREAKING" above; these are the same changes with the
reasoning. All are behavioural, not source-level: code that compiles against 1.3 still
compiles against 2.0.

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

[Unreleased]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v1.3.0...v2.0.0
[1.3.0]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/UltraEmbeddedLab/mqtt-client/releases/tag/v1.0.0
