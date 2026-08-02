# Roadmap

What is planned, in priority order, with the version it is targeted at. Items without a
version are wanted but unscheduled. This is a statement of intent, not a promise of dates.

Everything here is open to contribution — see [CONTRIBUTING.md](CONTRIBUTING.md).

## 2.1 — correctness on long-lived connections

The client's weakest area is holding a connection open for days rather than minutes.

- **Keepalive driven by outbound traffic.** Today the keepalive timer is reset by *received*
  packets, so a busy subscriber never sends PINGREQ and is dropped by the broker at
  1.5×keepAlive. MQTT-3.1.2-23 requires the client to *send* within the window.
- **Detect a missing PINGRESP.** The outstanding-ping flag is set on send and cleared only
  on receipt, with no deadline — so one lost PINGRESP disables keepalive for the life of the
  process and the client sits on a dead socket indefinitely. This is the most common IoT
  field failure.
- **Honour `server_keep_alive`.** The value is decoded from CONNACK and ignored. A broker
  that overrides your keepalive currently disconnects you on a schedule you cannot see.
- **A monotonic clock.** Every deadline uses wall-clock `microtime()`, so an NTP step
  expires all of them at once.
- **Release the flow-control slot on a failed publish.** The quota leaks on every timeout
  and never recovers; after `receiveMaximum` failures every QoS 1/2 publish throws.

## 2.2 — acknowledgement semantics

- **Act on PUBACK / PUBCOMP / SUBACK reason codes.** They are logged, not raised. Publishing
  to an ACL-denied topic returns a packet ID as if delivered; subscribing to a denied topic
  returns `void` and the application waits forever for messages that will never arrive.
- **Record granted QoS, not requested QoS,** and stop re-subscribing to filters the broker
  rejected.
- **Suppress PUBREL when PUBREC carries an error code** (MQTT-4.3.3-4).

## 2.3 — MQTT 5 completeness

- **Resolve inbound topic aliases.** The client invites the broker to use aliases but never
  resolves them, so every PUBLISH after the first arrives with an empty topic. Until this
  lands, leave `withTopicAliasMaximum()` at 0.
- **Enforce broker-advertised limits** — Maximum QoS, Retain Available, Maximum Packet Size.
  All are decoded and none are enforced, so the client writes packets the broker will reject.
- **Full CONNECT and Will property sets.** Six of nine documented CONNECT properties and all
  Will Properties are silently dropped by the encoder — including `will_delay_interval`,
  which is the main reason to adopt MQTT 5 for device presence.
- **Parse the complete property set on decode.** All four property parsers stop at the first
  unrecognised identifier, so a legal property the client does not know discards every
  property after it.
- **Ordered, repeatable User Properties.** Currently collapsed into an associative array, so
  duplicates are lost and the library cannot read back what its own encoder can write.
- **AUTH packet and enhanced authentication** (SCRAM, Kerberos, OAuth token refresh). Packet
  type 15 does not exist; enterprise brokers requiring it are unusable.

## 3.0 — the API decisions that need a major

- **Typed MQTT 5 properties.** `array<string, mixed>` across `PublishOptions`,
  `SubscribeOptions`, `WillOptions` and `InboundMessage`. A misspelled key is silently
  ignored today and static analysis cannot see it.
- **`TopicFilter` value object and a `RetainHandling` enum** for `subscribeWith()`, replacing
  the `array{filter: string, qos: int}` shape.
- **`PublishResult`** replacing `publish(): int`, which currently returns `0` for
  "queued offline", "dropped, queue full" and "QoS 0 sent" alike.
- **Enforced immutability** — `public private(set)` on `Options` and the `*Options` family,
  which are documented immutable but have plain public properties.
- **Split `ClientInterface`** from the message pump, and add `isConnected()` / `state()`.
- **Rename the PHP namespace** `ScienceStories\Mqtt` → `UltraEmbeddedLab\Mqtt`, with
  `class_alias()` shims for one minor cycle.
- **A packet-identifier allocator per client instance** with an in-use set, replacing the
  process-global static counter.

## Unscheduled

- **Async adapter** (ReactPHP / Amp / Revolt). Requires splitting the pure protocol engine
  from the I/O driver. The `Protocol\` namespace is already pure, so this is available
  rather than blocked — but it is the largest single piece of work here.
- **Laravel service provider and Symfony bundle.** Blocked on the async work only insofar as
  a blocking client is awkward in Octane and queue workers; a plain listener command is
  possible today.
- **WebSocket transport completion** — continuation frames, `wss://`, and test coverage. The
  handshake was fixed in 2.0; the rest of RFC 6455 is not done.
- **A read buffer** (`Protocol\PacketReader`). Today a frame is read in three or more
  separate socket calls with no buffer behind them, so a mid-frame timeout desynchronises
  the stream permanently. This is also a prerequisite for the async work.
- **A documentation site** and a benchmark page with figures measured against a real broker.

## Not planned

- Dropping PHP 8.4 support for a lower minimum.
- An MQTT **broker**. This is a client library.

---

Something you need that is not here? [Open an issue](https://github.com/UltraEmbeddedLab/mqtt-client/issues)
or start a [discussion](https://github.com/UltraEmbeddedLab/mqtt-client/discussions).
