# Roadmap

What is planned, in priority order, with the version it is targeted at. Items without a
version are wanted but unscheduled. This is a statement of intent, not a promise of dates.

Everything here is open to contribution — see [CONTRIBUTING.md](CONTRIBUTING.md).

The order is deliberate: first nothing may be silently lost (2.2), then the remaining MQTT 5
surface (2.3), then every API decision that needs a major release, taken in one go while it
is still cheap (3.0) — and only then the framework integrations built on that API.

## 2.1 — PHP 8.5, long-lived connections, the receive path ✅

See the [2.1.0 changelog](CHANGELOG.md).

- ~~PHP 8.5 minimum~~ — `clone()` with properties and `#[\NoDiscard]` on every wither
- ~~Keep Alive driven by outbound traffic~~, ~~missing PINGRESP detected~~,
  ~~`server_keep_alive` honoured~~, ~~a monotonic clock for every deadline~~
- ~~A read buffer~~ — a timeout at any byte of a packet loses nothing
- ~~`tick()` cannot read a byte~~, ~~`awaitMessage(null)` closes quiet connections~~
- ~~`ping()` eats the next packet~~ — it pumps the receive loop and owns its deadline
- ~~Single-slot acknowledgement registers~~ — matched by packet identifier, reentrancy-safe
- ~~WebSocket framing~~ — atomic frames, continuation frames, masking 74–133x faster

## 2.2 — nothing silently lost ✅ (unreleased)

Defects a first user would hit. All fixable without changing a signature — and all done; see
`[Unreleased]` in the changelog and the 2.1 → 2.2 section of the upgrade guide.

- ~~**Resolve inbound topic aliases.**~~ Done — in their own map, apart from outbound
  aliases, with the receiver's protocol checks (see `[Unreleased]` in the changelog).
- ~~**Stop re-subscribing to filters the broker refused.**~~ Done, with an
  `Events\SubscriptionRefused`. The resubscribe deliberately asks again for the *requested*
  QoS, not the granted one: re-requesting what one broker allowed would lose the
  application's intent across a failover to a broker that supports more.
- ~~**A conforming QoS 2 sender.**~~ Done — after PUBREC only the PUBREL is retransmitted,
  and a PUBREC with an error code ends the exchange without one (MQTT-4.3.3).
- ~~**Enforce broker-advertised limits**~~ Done — a PUBLISH above Maximum QoS or Maximum
  Packet Size, or retained without Retain Available, is refused before it is sent.
- ~~**Parse the complete property set on decode.**~~ Done — one decoder for every property
  MQTT 5 defines, whatever packet carries it.
- ~~**A packet-identifier allocator per client instance**~~ Done, with the in-flight record
  it needs: unacknowledged messages are resent at once and in order on reconnect, and MQTT 5
  no longer resends on a live connection (MQTT-4.4.0-1).
- ~~**Connections that end cleanly**~~ Done — no reconnect after a normal DISCONNECT or a
  session takeover, `disconnect()` stops auto-reconnect, a broker that drops every new
  connection is given up on, and a pcntl signal no longer tears the connection down.
- ~~**Session persistence that keeps its promise.**~~ Done — both halves of what is in flight
  are saved on `disconnect()` and finished by the next process; `SubscribeOptions` survive.
- ~~**Full CONNECT and Will property sets.**~~ Done — every documented CONNECT property is
  encoded, Will Properties are sent (`WillOptions::withDelayInterval()`), and Options carry
  User Properties and the two request flags.

## 2.3 — the rest of MQTT 5, and `wss://`

- **AUTH packet and enhanced authentication** (SCRAM, Kerberos, OAuth token refresh). Packet
  type 15 does not exist yet; brokers that require it cannot be used.
- **`wss://`.** TLS has to be negotiated before the HTTP upgrade, not after. Many cloud
  brokers only offer MQTT over secure WebSocket.
- **Documented request/response.** `Client\RequestResponse` exists and is undocumented.

## 3.0 — every API decision that needs a major, at once

Taken together, before anyone depends on the 2.x shapes, rather than spread over several
majors.

- **Rename the PHP namespace** `ScienceStories\Mqtt` → `UltraEmbeddedLab\Mqtt`, matching the
  package name, with `class_alias()` shims for one minor cycle.
- **Per-subscription callbacks** — `subscribe('sensors/+', fn (InboundMessage $m) => …)`,
  routed by topic filter, alongside the global `onMessage()`.
- **Refused operations raise.** A SUBACK refusal, or a PUBACK/PUBREC refusal, becomes an
  exception instead of an event, a warning and a metric.
- **`PublishResult`** replacing `publish(): int`, which returns `0` for "queued offline",
  "dropped, queue full" and "QoS 0 sent" alike.
- **Typed MQTT 5 properties**, with ordered, repeatable User Properties, replacing
  `array<string, mixed>` on `PublishOptions`, `SubscribeOptions`, `WillOptions` and
  `InboundMessage`. A misspelled key is silently ignored today.
- **`TopicFilter` value object and a `RetainHandling` enum** for `subscribeWith()`.
- **Connect from a URI** — `mqtts://user:pass@broker:8883`, `ws://…` — parsed with PHP 8.5's
  `Uri\Rfc3986\Uri`, choosing the transport and TLS from the scheme.
- **Enforced immutability** — `readonly` on `Options` and the `*Options` family.
- **Split `ClientInterface`** from the message pump, and add `isConnected()` / `state()`.
- **Remove what 2.x deprecated** — `ConnectResult::$reasonCode`, `Transport\TransportScheme`.

## After 3.0 — integrations and proof

- **Laravel package** — service provider, config, facade, an `mqtt:listen` Artisan command,
  and queue/event integration. Then a **Symfony bundle**.
- **Interoperability matrix in CI** — Mosquitto 2.0 and 2.1, EMQX, HiveMQ CE, NanoMQ. (The
  `eclipse-mosquitto:2` image now resolves to 2.1, whose WebSocket listener rejects every
  upgrade with the repository's config; that needs sorting out first.)
- **Coverage of 85 % or more**, and a published mutation score (`pest --mutate`).
- **A documentation site**, and benchmarks against real brokers alongside other PHP clients.
- **Async adapter** (ReactPHP / Amp / Revolt). The `Protocol\` namespace is pure and reads
  survive a timeout at any byte, so this is unblocked — but it is the largest single piece of
  work here.

## Not planned

- Supporting PHP versions older than the current minimum.
- An MQTT **broker**. This is a client library.

---

Something you need that is not here? [Open an issue](https://github.com/UltraEmbeddedLab/mqtt-client/issues)
or start a [discussion](https://github.com/UltraEmbeddedLab/mqtt-client/discussions).
