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

## 2.2 — nothing silently lost

Defects a first user would hit. All fixable without changing a signature.

- ~~**Resolve inbound topic aliases.**~~ Done — in their own map, apart from outbound
  aliases, with the receiver's protocol checks (see `[Unreleased]` in the changelog).
- ~~**Stop re-subscribing to filters the broker refused.**~~ Done, with an
  `Events\SubscriptionRefused`. The resubscribe deliberately asks again for the *requested*
  QoS, not the granted one: re-requesting what one broker allowed would lose the
  application's intent across a failover to a broker that supports more.
- **A conforming QoS 2 sender.** After PUBREC the client must retransmit PUBREL, not the
  PUBLISH, and must not send PUBREL at all when PUBREC carries an error code (MQTT-4.3.3).
- **Enforce broker-advertised limits** — Maximum QoS, Retain Available, Maximum Packet Size.
  All are decoded and none are enforced, so the client writes packets the broker rejects.
- **Parse the complete property set on decode.** All four property parsers stop at the first
  identifier they do not know, discarding every property after it.
- **A packet-identifier allocator per client instance** that skips identifiers still in
  flight, replacing the process-global counter. `Util\RandomId::packetId()` stays for
  anyone calling it directly.
- **Session persistence that keeps its promise.** QoS 2 state is saved, loaded and then
  discarded, and in-flight QoS 1/2 publishes are not saved at all.
- **Full CONNECT and Will property sets.** Six of nine documented CONNECT properties and all
  Will Properties are dropped by the encoder — including `will_delay_interval`, the main
  reason to adopt MQTT 5 for device presence.

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
- **Refused operations raise.** A SUBACK refusal, or a PUBACK/PUBCOMP failure code, becomes
  an exception instead of a log line.
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
