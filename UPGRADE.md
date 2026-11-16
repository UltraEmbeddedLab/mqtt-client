# Upgrade Guide

## 2.1 → 2.2

A minor release: no signature changes. Each item below is a defect fix whose effect you can
observe; if your code relied on the old behaviour, this is what to look at.

### MQTT 5 no longer resends on a live connection

MQTT 5 allows resending a PUBLISH or PUBREL only after a reconnect with the session resumed
(MQTT-4.4.0-1). On MQTT 5, `maxResendAttempts` is now the number of extra `ackTimeout`
periods `publish()` waits — nothing is sent again while the connection lasts — and the
`Timeout` message reports `0 resend(s)`. On MQTT 3.1.1 nothing changes. On both, every
unacknowledged message is resent at once, in order, as soon as a connection is
re-established — where before each waiting `publish()` resent only when its own timer ran
out (about 3 s instead of 40 ms against Mosquitto with `ackTimeout` 3).

### Packet identifiers are per client and start at 1

Each `Client` now allocates its own identifiers, skipping any still in use. Tests that
expected identifiers to continue from another client in the same process — or called
`RandomId::resetPacketId()` to get 1 — now get 1 from every new client.
`Util\RandomId::packetId()` still exists for code that calls it directly.

### `publish()` refuses what the broker said it will not accept

On MQTT 5, a PUBLISH above the CONNACK's Maximum QoS or Maximum Packet Size, or retained when
Retain Available is 0, now throws before anything is sent — the exception
`ReasonCode::QoSNotSupported`, `PacketTooLarge` or `RetainNotSupported` maps to — where the
broker used to close the connection. A `publish()` waiting when the connection ends and
nothing will reopen it throws `TransportError` at once instead of waiting out its resend
periods.

### Session files now hold message payloads

With a session store and a persistent session, `disconnect()` also saves the QoS 1/2
messages still in flight, payloads included. Protect the session directory like the data
itself. Files written by 2.1 load unchanged.

### When the connection ends, more things stop

- `disconnect()` turns auto-reconnect off until the next `connect()`; the next
  `loopOnce()` used to reconnect straight away.
- After a broker's DISCONNECT with `0x00` (normal) or `0x8E` (session taken over), the
  client no longer reconnects, as `ServerDisconnect::$willReconnect` already said. `run()`
  and `messages()` return; call `connect()` to start again.
- Without auto-reconnect, `run()` and `messages()` return when the connection drops, and
  `awaitMessage()` returns `null` instead of waiting on a dead socket.
- Auto-reconnect gives up after `reconnectMaxAttempts` on a broker that drops every new
  connection straight away; only a connection that lasts ten seconds, or your own
  `connect()`, starts the count over.

### `ServerDisconnect` listeners run with the transport already closed

For a broker's DISCONNECT too, not only when the client ends the connection. A listener's
`publish()` goes to the offline queue, or throws `TransportError`.

### A signal no longer drops the connection

A pcntl signal arriving while the client waits for data makes `loopOnce()` return `false`
instead of closing the connection. A handler that calls `stop()` ends `run()` at once.

## 2.0 → 2.1

A minor release. No source change is required; everything below is either the new runtime
requirement or a defect fix whose effect you can observe.

### PHP 8.5 is required

Raising the minimum PHP version is a minor change under the
[compatibility promise](docs/backward-compatibility.md#php-version-support): on PHP 8.4,
Composer simply keeps installing 2.0.x.

### Discarding a `with*()` result now warns

Every `with*()` on `Options`, `TlsOptions`, `SubscribeOptions` and `WillOptions` carries
`#[\NoDiscard]`. They return a new instance and never modify the receiver, so this line has
always done nothing:

```php
$options->withKeepAlive(30);            // Warning in 2.1 — it never had any effect
$options = $options->withKeepAlive(30); // what was meant
```

If a test runner turns warnings into failures, such a line now fails a test. That is the
intent: it was a bug before.

### `ping()` processes whatever arrives before the PINGRESP

It used to read two raw bytes and throw `ProtocolError` unless they were a PINGRESP — on a
subscribed connection that ate the head of the next PUBLISH and desynchronised the stream.
It now pumps the normal receive loop, so earlier packets are delivered as usual. It still
returns `true` on PINGRESP and throws `Timeout` when none arrives in time; if the
connection closes while it waits it throws `TransportError`.

### `loopOnce()` may return `false` early to keep the connection alive

When the requested timeout runs past the moment the next PINGREQ is due, the read now
returns `false` at that moment so the next call can send it. Code that calls `loopOnce()`,
`awaitMessage()`, `messages()` or `run()` in a loop is unaffected. The visible effect is a
fix: `awaitMessage(null)` on a quiet connection used to block in the read without ever
pinging — and on most systems the transport reported the socket's 5-second stream timeout
as a fatal error and closed the connection.

### Custom `TransportInterface` implementations

`Client` now survives a `Timeout` in the middle of a packet and resumes on the next call,
and it now calls `readExact()` with a timeout of `0.0` when Keep Alive housekeeping is due.
Two properties of `readExact()` matter, both now written on `TransportInterface` and both
kept by `TcpTransport`, `WsTransport` and `InMemoryTransport`:

- **Consume nothing on `Timeout`.** Keep bytes already received for the next call. A
  transport that discards them is no worse off than before, but it does not get the fix.
- **Treat `0.0` as a poll.** Return data that is already waiting; do not check the deadline
  before looking. A transport that fails a zero timeout outright cannot see a PINGRESP
  sitting in its socket on those calls.

### A failed `connect()` closes the transport

If anything fails after the socket opens — TLS, no CONNACK in time, a malformed one — the
transport is now closed before the exception propagates. `isOpen()` used to stay `true` in
those cases, so auto-reconnect never retried and the next `loopOnce()` treated whatever
arrived as traffic on a live session. The exception types are unchanged.

---

## 1.x → 2.0

Two things changed at once in 2.0: the package was renamed, and a set of defects were
fixed in ways that change observable behaviour. Nothing here is a source-level break — code
that compiles against 1.3 still compiles against 2.0 — but several things now *throw* where
they previously returned, and two new limits apply by default.

Budget about fifteen minutes for a typical application.

---

### 1. The package was renamed

`ultraembeddedlab/php-iot` → `ultraembeddedlab/mqtt-client`

```diff
 "require": {
-    "ultraembeddedlab/php-iot": "^1.3"
+    "ultraembeddedlab/mqtt-client": "^2.0"
 }
```

Then `composer update`.

**The PHP namespace is unchanged.** It is still `ScienceStories\Mqtt\` — no `use`
statement in your code needs to move. (The namespace is a historical artefact of the
package's original name; renaming it is deferred to 3.0 so that this upgrade stays
mechanical.)

The old package declares `replace`, so Composer will not let both be installed at once —
which is what you want, since they share a namespace.

---

### 2. `connect()` throws instead of returning a refused result

This is the change most likely to affect you.

Previously `Client::connect()` returned a `ConnectResult` whatever the broker answered, so
a wrong password looked like a successful connection and surfaced later as an unrelated
read timeout.

```diff
-$result = $client->connect();
-if ($result->reasonCode !== 0) {
-    throw new RuntimeException("Connection refused: {$result->reasonCode}");
-}
+use ScienceStories\Mqtt\Exception\AuthenticationError;
+use ScienceStories\Mqtt\Exception\ServerError;
+
+try {
+    $client->connect();
+} catch (AuthenticationError $e) {
+    // Bad credentials — retrying will not help.
+} catch (ServerError $e) {
+    // Broker busy/unavailable — retry with backoff.
+}
```

`ConnectResult::$reasonCode` is now always `0`, since a non-zero code is thrown. The
property remains for compatibility and will be removed in 3.0.

All thrown types extend `MqttException`, which extends `RuntimeException` — so an existing
`catch (RuntimeException $e)` keeps working unchanged. **If your code already wrapped
`connect()` in a broad catch, you may need to do nothing at all.**

`Easy\Mqtt::connect()` previously threw a plain `RuntimeException` with its own message; it
now lets the typed exception propagate. Same base class, better type.

---

### 3. Inbound packets are capped at 16 MiB

New default: `Options::$maximumPacketSize = 16 * 1024 * 1024`.

A packet whose declared Remaining Length exceeds this is rejected before the body is read —
previously there was no bound at all, so a broker (or anything on the wire) could force an
allocation of up to 256 MiB with a five-byte header.

On MQTT 5 the value is also advertised as the Maximum Packet Size property (`0x27`), so a
compliant broker will not send larger packets rather than having them rejected locally.

Raise it if you genuinely move payloads above 16 MiB over MQTT:

```php
$options = $options->withMaximumPacketSize(64 * 1024 * 1024);
```

---

### 4. The inbound queue is bounded at 1000 messages

New default: `Options::$inboundQueueSize = 1000`.

This queue backs `awaitMessage()` and `messages()`. Previously it was unbounded, and in the
push-only pattern (an `onMessage()` handler plus a hand-rolled `loopOnce()` loop) nothing
ever drained it — a slow memory leak.

A consumer that keeps up never reaches the bound. One that falls 1000 messages behind now
loses the oldest, logged at warning level and counted on the `inbound_queue_dropped` metric.

To restore the old unbounded behaviour:

```php
$options = $options->withInboundQueueSize(0);
```

`run()` additionally drains the queue on every iteration, since the handler it registers has
already consumed each message.

---

### 5. Configuration errors throw instead of being silently accepted

`Options::withKeepAlive()`, the `Options` constructor, `withMaximumPacketSize()` and
`withInboundQueueSize()` now throw `InvalidArgumentException` for out-of-range values.

The one likely to bite: **Keep Alive above 65535**. The CONNECT field is two bytes, so
`withKeepAlive(86400)` used to be silently truncated to 20864 — the connection would then
drop every few hours for no visible reason.

```diff
-$options = $options->withKeepAlive(86400); // silently became 20864
+$options = $options->withKeepAlive(3600);  // or any value 0..65535
```

Similarly, `Protocol\V311\Encoder` now throws `ProtocolError` when a CONNECT carries a
password with no username (MQTT-3.1.2-22). Brokers closed the connection without a CONNACK
in that case, which was undiagnosable from the client. MQTT 5 still permits it.

---

### 6. TLS 1.0 and 1.1 are refused

The transports previously used `STREAM_CRYPTO_METHOD_TLS_CLIENT`, which enables TLS 1.0 and
1.1 — deprecated as MUST NOT by RFC 8996 and prohibited under PCI-DSS. The default is now
TLS 1.2 + 1.3.

If you must talk to a broker that cannot do TLS 1.2:

```php
use ScienceStories\Mqtt\Client\TlsOptions;

$tls = (new TlsOptions())->withCryptoMethod(
    STREAM_CRYPTO_METHOD_TLSv1_1_CLIENT | TlsOptions::DEFAULT_CRYPTO_METHOD,
);
```

---

### 7. Malformed packets raise `ProtocolError`

A PUBLISH with both QoS bits set previously raised `\ValueError` from `QoS::from()`, which
is outside the `MqttException` hierarchy and escaped every documented catch block, killing
long-running loops. It is now `ProtocolError`.

If you were catching `\ValueError` around the message loop for this reason, you can drop it.

---

### 8. `loopOnce()` behaviour on oversized packets

`loopOnce()` still returns `bool`. When it rejects an oversized packet it logs a warning,
closes the (now desynchronised) transport and returns `false`, so auto-reconnect can take
over. It does not throw — that would have broken the documented contract.

---

## New in 2.0 that you may want

### `Testing\InMemoryTransport`

A scriptable `TransportInterface` shipped in `src/`, so you can unit-test your own MQTT code
without a broker or Docker:

```php
use ScienceStories\Mqtt\Testing\InMemoryTransport;

$transport = new InMemoryTransport();
$transport->feedConnAck();

$client = new Client(new Options('fake.broker'), $transport);
$client->connect();

$transport->feedPublish('sensors/temp', '21.5');
$client->loopOnce(0.0);

expect($transport->countSent(PacketType::PUBACK))->toBe(0);
```

See [Testing](docs/testing.md).

---

## Checklist

- [ ] `composer.json` requires `ultraembeddedlab/mqtt-client: ^2.0`
- [ ] Every `connect()` call site handles the thrown exception (or is inside a broad catch)
- [ ] No `withKeepAlive()` value above 65535
- [ ] Payloads above 16 MiB — raise `withMaximumPacketSize()`
- [ ] Pull consumers that intentionally buffer more than 1000 messages — set `withInboundQueueSize(0)`
- [ ] Brokers stuck on TLS 1.0/1.1 — set `withCryptoMethod()`

Anything unclear or broken by this upgrade is a bug in this guide as much as in the code —
please [open an issue](https://github.com/UltraEmbeddedLab/mqtt-client/issues).
