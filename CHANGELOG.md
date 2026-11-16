# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **A broker that sends a topic alias the client never allowed now has the connection closed.** At the default `topicAliasMaximum` of 0 the client tells the broker it accepts no aliases, so any alias it receives breaks MQTT-3.3.2-11 — previously such a PUBLISH was still delivered when it also carried its topic. A conforming broker never does this; the change is listed because the observable result differs. See the topic-alias entries under Fixed for what happens on closing.
- **A message the broker refuses is a warning, not an info line.** A PUBACK or PUBREC with a reason code of 0x80 or above is logged at warning level with the reason and Reason String, and counted on the new `publish_failed` metric (labels `packet`, `reason`). `publish()` still returns the packet identifier; raising is a 3.0 change. A PUBCOMP's only error code, 0x92 Packet Identifier not found, is not a refusal — the message was accepted at PUBREC, and a resent PUBREL gets 0x92 whenever the first PUBCOMP was lost — so it is logged as a notice and not counted.
- **`FileSessionStore` keeps its files private**: a new directory is created `0700` and each file written `0600`, since session files can now hold message payloads.
- **A QoS 2 timeout names the step it was waiting for** — "Timed out waiting for PUBREC after N resend(s)" or "… PUBCOMP …" instead of "… QoS2 handshake …".
- **`Events\ServerDisconnect` is also dispatched when the client ends the connection** — a missing PINGRESP (`0x8D`) or a broker protocol violation (`0x94`, `0x82`). The packet is then one the client built, and its `reason_string` property begins "Closed by the client", so listeners can tell which side gave up. In those cases the transport is already closed when listeners run, so a listener's publish can no longer follow the client's own DISCONNECT (MQTT-3.14.4-1).
- **The transport is closed before `Events\ServerDisconnect` is dispatched for the broker's own DISCONNECT too**, as for the cases above. Listeners ran while it was still open, so a listener's publish went out on a connection the broker was tearing down; it now goes to the offline queue, or throws `TransportError`.

### Fixed — topic aliases

- **Inbound MQTT 5 topic aliases are resolved.** The client could invite the broker to use aliases (`withTopicAliasMaximum()`) but never resolved them, so every aliased PUBLISH after the first reached the application with an empty topic. The broker's aliases now live in their own map, apart from the client's outbound ones — the two directions are independent alias namespaces with independent limits (MQTT-3.3.2.3.4), and sharing a map would let an alias the broker assigns overwrite one the client assigned to a different topic. Mappings are cleared on every reconnect.
- **The broker's alias no longer reaches the application.** `topic_alias` is removed from a delivered message's properties: it means something only on this connection, in the broker-to-client direction. Left in place, forwarding a message with its properties — the usual way to keep user properties and content type — sent the number back the other way, re-pointed one of the client's own aliases, and the client's next publish to that topic was routed to the forwarded one.
- **A `topic_alias` set by the caller is dropped.** Aliases belong to the client; a hand-set value was passed through whenever the client's alias map was full or the broker allowed none.
- **A QoS 1/2 resend carries the full topic and no alias.** It reused the first send's empty topic and alias, so a reconnect while waiting for the acknowledgement resent with an alias the new connection had never set up — rejected by the broker, or routed to whatever the alias meant there.
- **Outbound aliasing no longer outlives the connection that set it up.** The alias manager was created only when CONNACK advertised a Topic Alias Maximum, so a reconnect to a broker that stopped advertising one kept the old limit and sent aliases the new broker had not allowed. It is now re-derived from every CONNACK.
- **A broker that breaks the alias rules has the connection closed**, as MQTT 5 requires of the receiver: an alias of 0 or above the limit the client sent gets DISCONNECT `0x94` (Topic Alias Invalid); an alias never established, or an empty topic with no alias, gets `0x82` (Protocol Error). The message is not acknowledged and `Events\ServerDisconnect` is dispatched. With auto-reconnect on, `loopOnce()` returns `false` and the next call reconnects; with it off, `ProtocolError` is thrown. On MQTT 3.1.1 an empty topic is the only case: no DISCONNECT reason can be sent, the connection is closed, and the same event and exception follow.
- The documentation described `withTopicAliasMaximum()` as a budget the broker could lower. It is the limit on aliases the *broker* may use; outbound aliasing has always followed the broker's CONNACK limit. README, `docs/topic-aliases.md`, the docblocks and the example now say so.

### Fixed — subscriptions the broker refuses

- **A refused subscription is no longer asked for again on every reconnect.** A filter the broker refused in its SUBACK (code `0x80` or above) was remembered exactly like a granted one, so each reconnect re-sent it and was refused again — and through `subscribe()`, which returns nothing, the application had no sign that its messages would never arrive. Refused filters are no longer remembered, a filter that starts being refused on resubscribe (an ACL changed while the client was away) is forgotten, and each refusal is logged with its reason, counted on the `subscriptions_refused` metric and reported through the new `Events\SubscriptionRefused`. `subscribe()` still returns normally; raising instead is a 3.0 change.
- **SUBACK codes are matched to the filters actually sent.** The encoders skip empty filters, so with an empty filter in a `subscribeWith()` list every code after it was matched to the wrong filter. A filter the SUBACK has no code for counts as refused.
- **`subscribeWith()` with only empty filters sends nothing** instead of a SUBSCRIBE with no filter at all, which is a protocol violation (MQTT-3.8.3-2) the broker answers by closing the connection — as `unsubscribe()` already did. It returns a `SubscribeResult` with packet identifier 0 and no codes.

A granted filter is resubscribed with the QoS that was *requested*, not the possibly lower one the broker granted, so the application's intent survives a failover to a broker that supports more. The granted levels are in `SubscribeResult` and `SubAck::getGrantedQoS()`. A refused request for a filter that is already subscribed leaves the earlier subscription in place; should the broker refuse that too, the next resubscribe forgets it.

Refusals on the automatic resubscribe are reported once it is over, with every filter sent again and the bookkeeping settled. A `SubscriptionRefused` listener may therefore subscribe to a fallback filter, which is remembered like any other subscription, and an exception it throws reaches the caller of `loopOnce()` (PSR-14) without cutting the resubscribe short.

### Fixed — QoS 2 publishing

- **Once PUBREC has arrived, a timeout resends the PUBREL, not the PUBLISH.** The client waited for PUBCOMP alone and, if it was late, resent the PUBLISH with DUP — after PUBREL had already gone out, which MQTT-4.3.3-6 forbids: a receiver that has released the Packet Identifier takes the resent PUBLISH as a new message and delivers it twice. The same happened on the connection that replaces a dropped one. The exchange is now tracked in two steps — the PUBLISH is resent until PUBREC arrives, the PUBREL from then on — under one resend budget.
- **A PUBREC with a reason code of 0x80 or above ends the exchange.** The client released the message anyway (MQTT-4.3.3-4 forbids a PUBREL there), then waited for a PUBCOMP that never came, resent the PUBLISH and threw `Timeout`. It now sends nothing more, `publish()` returns at once, and the flow-control slot is returned, as MQTT 5 §4.9 requires for an error PUBREC.

### Fixed — MQTT 5 properties

- **Every property the specification defines is decoded, whatever packet carries it.** Each packet type had its own parser for the properties it usually sees, and each stopped at the first identifier outside its list, dropping everything after it. A Subscription Identifier in a PUBLISH cost the message every User Property that followed; an Authentication Method in a CONNACK cost the Receive Maximum after it, so the client assumed 65,535 and could overrun the quota the broker had set. Subscription Identifiers arrive as a list under `subscription_identifiers` (a PUBLISH may carry several, MQTT-3.3.4-3).
- **Will Properties are sent.** The Will's property block was always empty, so `will_delay_interval` — which lets a device that reconnects quickly avoid being announced offline — and every other Will property set through `WillOptions::withProperties()` never reached the broker. New `WillOptions::withDelayInterval()`. (The broker holds a delayed Will only while the session lasts, so give the session an expiry — `Options::withSessionExpiry()` — at least as long.)
- **The CONNECT properties `Connect` documents are all encoded**: Request Response Information, Request Problem Information, User Properties, Authentication Method and Authentication Data were silently dropped.
- **Session state keeps `SubscribeOptions`.** Stored as JSON by `FileSessionStore`, they came back as arrays, and the first resubscribe after a restart failed with a `TypeError` — as did every reconnect after it. Files written by earlier versions read correctly.

### Fixed — messages in flight

- **A reconnect resends every unacknowledged message at once, in order** (MQTT-4.4.0-1, MQTT-4.6.0-1, MQTT-4.6.0-4): each PUBLISH again with DUP, in the order first sent, then a PUBREL for each QoS 2 message already received, in the order its PUBREC arrived. Each waiting `publish()` used to resend only when its own `ackTimeout` ran out — 3 s instead of 40 ms against Mosquitto — in whatever order the waits expired, and a message whose `publish()` had already given up was never resent at all, so the broker held a QoS 2 message it was never told to release.
- **MQTT 5 no longer resends on a live connection**, which MQTT-4.4.0-1 forbids: there `maxResendAttempts` is the number of extra `ackTimeout` periods `publish()` waits. MQTT 3.1.1, which allows it, still resends — the PUBLISH until PUBREC, the PUBREL after — and a message the reconnect has just resent is not resent again by the timer.
- **Without the session, waiting messages are published anew.** When the broker kept nothing (`sessionPresent` 0), messages a `publish()` still waits for go out again without DUP; those nobody waits for any more go with the old session.
- **Packet identifiers are allocated per client, skipping those still in use** (MQTT-2.2.1-3) by an unacknowledged message or a SUBSCRIBE/UNSUBSCRIBE awaiting its answer. A process-wide counter handed them out before, so after wrapping round a QoS 2 message could get the identifier of one the broker still held, and the broker dropped it as a duplicate. `QuotaExceeded` is thrown if all 65,535 are in use. `Util\RandomId::packetId()` remains for code that calls it directly.
- A message whose `publish()` gave up stays in flight — its identifier reserved, resent if the session resumes — until the broker acknowledges or refuses it, or the session is lost. Never on elapsed time alone: the broker may still hold it, and time does not end a session (MQTT 5 §4.1).
- **What Receive Maximum has no room for on a new connection is sent as slots free up**, in order and ahead of new messages, instead of never on that connection.
- **A message the broker refuses by ending the connection no longer uses up auto-reconnect** (MQTT 5). Resent first on every reconnect, it ended each new connection in turn until the client gave up altogether. When the broker ends a connection with a reason that blames a message — `0x87` Not authorized, `0x90`, `0x95`, `0x99`, `0x9A`, `0x9B` — the next connection resends one message at a time; refused again while one message alone is waiting, that message is given up on (warning, `inflight_dropped` metric) and the client carries on. A connection that merely drops gives up on nothing, and neither does MQTT 3.1.1, whose brokers give no reason: there auto-reconnect's attempt limit bounds the loop, and the message stays in flight.
- **A `publish()` waiting on a connection nothing will reopen fails at once with `TransportError`** — auto-reconnect off, a normal DISCONNECT, the session taken over, or the reconnect attempts used up — instead of waiting out every resend period first.
- An acknowledgement is recorded before the slot it frees is handed to a waiting resend, so a resend whose write fails no longer costs the waiting `publish()` an acknowledgement already read; a failed resend now closes the connection for the next `loopOnce()` to reopen. A message queued offline is kept for the next connection whenever its own PUBLISH never went out — resends going out during its wait no longer count as its own — and a wait inside a reconnect gives up at once when the connection drops again instead of after five seconds. The application's own `connect()` resends one message at a time after such a refusal, just as auto-reconnect does.
- **Messages in flight survive a restart.** With a persistent session and a session store, `disconnect()` now saves both halves of what is in flight — the QoS 1/2 messages not yet acknowledged, and the QoS 2 messages received but not yet released — and the next process resends the former and delivers the latter when the broker releases them. Only subscriptions came back before, so a replayed PUBREL was answered with PUBCOMP for a message the application never saw, and an unfinished QoS 2 publish was never released: against Mosquitto, which delivers on PUBREL, it never reached its subscribers. Once taken back, the messages are removed from the store — and when the broker kept no session, the stored ones are dropped — so a process that dies before its own `disconnect()` cannot leave the next one to deliver a QoS 2 message a second time. The session is saved under the Client Identifier the broker assigned, when it assigned one. Session files written by earlier versions still load, all-digit topic filters included.

### Fixed — limits the broker sets

- **A PUBLISH the broker said it will not accept is not sent.** Maximum QoS (MQTT-3.2.2-11), Retain Available (MQTT-3.2.2-14) and Maximum Packet Size (MQTT-3.2.2-15) from the CONNACK were decoded and ignored, so the broker closed the connection — and a reconnect resent the same message into the same refusal. `publish()` now throws before writing anything: the exception `ReasonCode::QoSNotSupported`, `RetainNotSupported` or `PacketTooLarge` maps to — and leaves no Topic Alias behind for a PUBLISH that never went out. A message already in flight that a reconnected broker no longer allows, or that can no longer be encoded, is given up on rather than resent.

### Fixed — after a connection ends

- **`connect()` clears the stop left behind by the previous connection, and nothing else.** When a connection ended with auto-reconnect off — an inbound DISCONNECT, a ping timeout, a protocol violation — `run()` and `messages()` were told to stop, and that outlived the connection: the first `run()` after a manual reconnect returned at once, and a supervisor's catch → connect → run loop spun without processing a message. A `stop()` the application asked for is now a separate flag that `connect()` leaves alone, so a SIGTERM handler that calls `stop()` between connections still ends the next loop.
- **Auto-reconnect no longer nests.** A wait inside a reconnect — the resubscribe, the offline-queue drain — that found the fresh connection closed again reconnected one level deeper, so a broker that broke the rules right after every CONNACK, or disconnected straight away, grew the stack without bound and a single `loopOnce()` never returned. A reconnect cannot start inside another; the inner wait times out, the attempt counts as failed, and the outermost loop tries again.
- **A message that could not be written while draining the offline queue is kept** at the head of the queue for the next connection — when nothing reached the wire for a passing reason (the connection was gone, or no flow-control slot freed up). A message that reached the broker, or can never be sent, such as one that cannot be encoded, is dropped with a warning and the `offline_queue_dropped` metric, as before: retrying the first could deliver a QoS 2 message twice, and retrying the second would block every message behind it. Draining stops as soon as the connection is gone.
- **A publish waiting for a flow-control slot survives the end of the connection** — a protocol violation, a DISCONNECT from the broker, or a missing PINGRESP. Closing the connection reset connection state, freeing the slot under the waiting `publish()`, which then wrote to the closed transport and lost the message even with auto-reconnect on. The state is left for the next connect to reset; the waiting publish goes out on the reconnected connection. The wait also backs off instead of spinning while the transport is closed.
- **Auto-reconnect does what `ServerDisconnect::$willReconnect` says.** After a broker's normal DISCONNECT (`0x00`) the event said no reconnect would follow, as the documentation does, but the next `loopOnce()` reconnected anyway. It no longer does, and `run()` / `messages()` return.
- **No auto-reconnect after `0x8E` Session taken over.** Another connection with the same Client Identifier has the session; reconnecting took it back and threw that client off, and when it reconnected too, the two took turns forever — 19 connections each in 12 seconds against Mosquitto. The client that lost the session now stays disconnected, and the event says so.
- **Auto-reconnect gives up on a broker that drops every new connection.** The attempt count reset on every successful CONNACK, so a broker that accepted each connection and closed it straight away — a ban applied after the handshake, another process with the same Client Identifier on MQTT 3.1.1 — was retried forever at the base delay. `reconnectMaxAttempts` now counts attempts in a row without a connection that lasted; a connection that stays up for ten seconds after its resubscribe, or the application's own `connect()`, starts the count over. A reconnect whose resubscribe the broker cuts off (as AWS IoT does for a filter its policy forbids) fails at once instead of waiting out the five-second SUBACK timeout. `ServerDisconnect::$willReconnect` takes the remaining attempts into account, so it no longer promises a reconnect the client will not make.
- **`disconnect()` stops auto-reconnect** until the next `connect()`. The next `loopOnce()` — `run()`'s own, or a message handler's — reconnected straight away.
- **`run()` and `messages()` return when a connection without auto-reconnect drops**, as they already did after a DISCONNECT, a missing PINGRESP or a protocol violation. They went on polling the dead socket for as long as the process lived.
- **`awaitMessage()` returns `null` as soon as nothing can arrive any more** — the connection has ended and nothing will reconnect it. It spun a CPU core until its timeout instead, and forever without one; `Client\RequestResponse` likewise. `messages()` still yields the messages that arrived before the connection ended, which the client has already acknowledged.
- **A `ServerDisconnect` listener that calls `connect()` is no longer undone** after a missing PINGRESP or a protocol violation: the "stop" those leave behind was set after the listeners ran.
- **A signal no longer tears down the connection.** A pcntl signal arriving while the client waited for data — a worker's SIGALRM timeout, a supervisor's SIGTERM — interrupts `stream_select()`, which was taken for a broken socket: the connection was closed and, without auto-reconnect, every later message lost. It is now a timeout like any other (`loopOnce()` returns `false`), so a handler that calls `stop()` ends `run()` at once and the connection survives a handler that does not. The interrupted call's warning no longer reaches the application's error handler either. Both transports.
- **A failed resubscribe after auto-reconnect no longer leaves later subscriptions unrecorded.** The internal "resubscribing" flag stayed set, so every `subscribe()` afterwards — even after a manual `connect()` — was not remembered for the next reconnect.
- **A resubscribe that fails part-way closes the new connection.** When a SUBACK never came, or a SUBSCRIBE could not be written, the attempt counted as failed but the connection stayed open — so `loopOnce()` never reconnected, and every filter after the failing one stayed unsubscribed on a live connection. The connection is now closed, and the next `loopOnce()` reconnects and resubscribes in full.
- **The session store is read on the first connection only.** With `withSessionStore()` and a persistent session, every reconnect replaced the in-memory subscriptions with the stored ones: filters subscribed since the last save were dropped, and filters unsubscribed or refused since came back. From the second connection on, memory is the newer record.

### Added

- `Events\SubscriptionRefused` — dispatched for each filter the broker refuses, including on the automatic resubscribe after a reconnect (`$resubscribing`), where it is held until the resubscribe is over.
- `OfflineQueue::requeue()` puts a message back at the head of the queue.
- `WillOptions::withDelayInterval()` — the MQTT 5 Will Delay Interval.
- `Options::withUserProperties()`, `withRequestResponseInformation()`, `withRequestProblemInformation()` — the MQTT 5 CONNECT properties they name.
- `SessionState::$outbound` and `$inboundQos2` — the messages in flight, for custom `SessionStoreInterface` implementations; `toArray()` / `fromArray()` handle them.
- Metrics: `publish_failed` (a PUBACK/PUBREC refusal) and `inflight_dropped` (a message in flight given up on).

## [2.1.0] - 2026-11-15

PHP 8.5, and a client that can hold a connection open: a timeout at any byte of a packet no
longer loses data, Keep Alive is driven by what the client sends, a silent link is detected,
and the receive path is faster. No source change is required to upgrade — see
**[UPGRADE.md](UPGRADE.md#20--21)** for the behaviour you can observe.

### Changed

- **PHP 8.5 is now required.** Under the compatibility promise this is a minor change: on PHP 8.4 Composer keeps installing 2.0.x. CI runs on 8.5, the stock-image install check uses `php:8.5-cli`, and PHP 8.6 runs as an allowed-to-fail job so its deprecations surface before users meet them.
- **Discarding a `with*()` result now warns.** Every wither on `Options`, `TlsOptions`, `SubscribeOptions` and `WillOptions` is a single PHP 8.5 `clone($this, [...])` and carries `#[\NoDiscard]`, so `$options->withKeepAlive(30);` — which never had any effect — emits a warning instead of silently doing nothing. A test pins the attribute on every wither.
- **`ping()` processes whatever arrives before the PINGRESP** rather than reading two raw bytes and throwing `ProtocolError` when they were not a PINGRESP. See the Fixed entry below.
- **`loopOnce()` may return `false` before its timeout** when that timeout runs past the moment the next PINGREQ is due, so the next call can send it. Callers that loop are unaffected.
- Development dependencies: Pest 5.3, PHPStan 2.3, Rector 2.8, Pint 1.32, Symfony components 8.1.11 (clears CVE-2026-109413 and CVE-2026-123792 in the dev-only `symfony/yaml`). PHPStan, Pint and Rector now target PHP 8.5. Rector reads the target from `composer.json`, applies its PHP 8.5 set, and now also covers `examples/` and `benchmarks/`.

### Added

- `Options::withPingResponseTimeout()` and `Options::$pingResponseTimeout` (default 10.0s).
- `Util\Clock` — the monotonic time source the whole library now reads.
- `Bytes::decodeVarInt()` takes an optional `$offset` (new trailing parameter).
- `benchmarks/inbound-throughput.php` — the whole receive path through public API over a real loopback socket, timing the client only; `benchmarks/hot-paths.php` — MQTT 5 decode and WebSocket masking.

### Deprecated

- `Transport\TransportScheme` — referenced nowhere; there is no URI parsing that selects a transport. Scheduled for removal in 3.0.

### Fixed — the client can now hold a connection open

Five defects that only show up on a long-lived connection, which is what this library is for.

- **Keep Alive was driven by inbound traffic.** `touchActivity()` ran after every read and the ping timer measured from it, but MQTT-3.1.2-23 obliges the *client* to send within the window — receiving does not count. A subscriber on a busy topic therefore never sent PINGREQ and was dropped by the broker every 1.5×keepAlive, then flapped forever. The clock is now split: `$lastSent` (updated in `trackWrite()`, so every outbound packet counts) drives the ping; `$lastReceived` is kept for liveness only.
- **A single lost PINGRESP disabled keepalive for the life of the process.** The outstanding-ping flag was set on send and cleared only on receipt, with no deadline — so after one unanswered PINGREQ the client sat on a dead socket that still reported `isOpen()`, receiving nothing, with auto-reconnect never firing because the transport looked healthy. This is the classic NAT-eviction / black-holed-broker failure. There is now a deadline (`Options::withPingResponseTimeout()`, default 10s). It is evaluated only after a poll that found the socket **silent**, never on elapsed wall time alone — judging it on the clock would tear down a healthy connection whenever the caller's loop interval exceeds the timeout, which is ordinary for a worker polling every 30s or a handler that blocks on a database write. Two silent polls are required, so one slow iteration cannot condemn a live link, and any inbound packet clears the ping since it proves the link is alive even if the PINGRESP itself was lost. On expiry the client dispatches `Events\ServerDisconnect`, closes the transport, and — when `autoReconnect` is off, which is the default — stops `run()`/`messages()` rather than spinning on a closed socket.
- **`server_keep_alive` from CONNACK was decoded and ignored**, violating MQTT-3.2.2-22. Requesting 300s against a broker that grants 60s produced an endless 90-second connect/drop loop. The granted value is now what the client pings on, and the override is logged.
- **Every deadline used wall-clock `microtime()`.** An NTP step of +45s expired all of them at once: an in-flight QoS 1 publish "timed out", resent with DUP, exhausted its retries and threw for a message the broker had already acknowledged. A backward step stalled keepalive past the broker's window. All 36 sites across the client, flow control, rate limiter and both transports now read `Util\Clock::now()`, which is monotonic `hrtime()`. `time()` is deliberately kept for `SessionState::$savedAt`, where wall-clock is the correct meaning.
- **Flow control leaked a slot on every failed publish.** The QoS 1/2 wait released its slot only via PUBACK/PUBCOMP, so a timeout kept it forever. With `receive_maximum = 10`, ten timeouts during a GC pause permanently exhausted the window and every later QoS 1/2 publish threw "timed out waiting for slot" until the process restarted. The slot is now reclaimed by a sweep in `loopOnce()` — deliberately not the instant `publish()` gives up, because MQTT-4.9.0-1 counts an unacknowledged message against Receive Maximum until the broker acknowledges it, and freeing it early would let the client exceed the window the broker granted. The sweep waits out the full resend budget, doubled.

### Fixed — the receive path

- **A timeout in the middle of a packet desynchronised the connection for good.** A frame was read in three or more socket reads, only the first inside the `try`, and `TcpTransport::readExact()` threw away whatever it had read when its deadline passed. One slow segment — a large payload over a congested link — and the next read started mid-packet, every later frame boundary was wrong, and `isOpen()` still said true so auto-reconnect never fired. `TcpTransport` and `WsTransport` now keep partially received bytes across a `Timeout`, `Client` keeps a half-read fixed header across `loopOnce()` calls, and a whole frame shares one deadline.
- **`tick()` never read a byte.** `readExact(n, 0.0)` checked its deadline before calling `select()`, so a zero timeout failed without looking at the socket and the documented non-blocking integration path delivered nothing. A zero timeout is now a poll.
- **`awaitMessage(null)` dropped quiet connections every five seconds.** `readExact(n, null)` skipped `select()` and relied on the 5-second stream timeout set in `open()`, which surfaced as a fatal `TransportError`; the client closed the transport. Even where it did not, the blocking read never returned to send a PINGREQ. An unbounded wait now blocks in `select()` and wakes when Keep Alive housekeeping is due.
- **`ping()` ate the head of the next packet.** On a subscribed connection a PUBLISH arriving before the PINGRESP lost its first two bytes and the stream desynchronised. `ping()` now pumps the receive loop, so the PUBLISH is delivered and the PINGRESP still completes the ping.
- **Acknowledgements went into four single-slot registers**, so a `publish()` from inside an `onMessage()` handler overwrote the outer `publish()`'s PUBACK; the outer call resent with DUP until it timed out for a message the broker had acknowledged. Acknowledgements are now matched by packet identifier, and one that nobody waits for — a PUBACK arriving after its `publish()` gave up — is dropped instead of kept.
- **A large packet arriving slowly could get a healthy connection condemned.** A PINGREQ sent while a body was still arriving has its PINGRESP queued behind that body, and the bytes already received were invisible to the liveness check until the last one landed — a slow transfer straddling a ping was torn down as a silent link. Once a body read times out, the rest is read in slices (256 B, doubling while data flows), each counting as proof of life. A link that stalls mid-packet is still condemned on the next Keep Alive cycle, like an idle one.
- **`ping()` let housekeeping apply a shorter deadline to its own PINGREQ.** With a `ping()` timeout longer than `pingResponseTimeout` the client closed the connection and `ping()` threw `TransportError`. `ping()` now owns the round trip: it throws `Timeout` after its own timeout and leaves the link up.
- **A failed `connect()` left the transport open.** After a CONNACK timeout or a malformed first packet, `isOpen()` stayed true, so auto-reconnect never retried and the next `loopOnce()` read on as if connected. Any failure after the socket opens now closes it.
- **WebSocket frames were lost the same way.** Header, length, mask and payload were separate blocking reads; a timeout during the payload discarded the header. Frames are now parsed only once complete.
- **Fragmented WebSocket messages desynchronised MQTT.** The first fragment entered the byte stream and its continuation frames were dropped. Binary continuations are now appended in order, a fragmented text message is kept out, and pings between fragments are answered. Reading many small packets out of one large frame is no longer quadratic.
- **`RandomId::clientId()` could return fewer characters than requested.** It base64-encoded random bytes, stripped `+`, `/` and `=`, then truncated; about 0.15% of 6-character IDs came back short (1 in 200,000 at the default 16). Characters are now drawn directly from `[A-Za-z0-9]`, so the length is exact.
- **`examples/connect_v3.php` used a hard-coded client ID** where every other example generates one, so two copies running at once kicked each other off the broker.
- **`InMemoryTransport` was quadratic in the bytes fed to it**: each read copied the whole remaining buffer. Consumer test suites that feed thousands of packets get faster.
- PHP 8.5 deprecates `chr()` outside 0–255. Every call site is now provably in range — `InMemoryTransport`'s `feed*()` helpers reject an out-of-range reason code with `InvalidArgumentException` rather than masking it into a different, valid one.

### Performance

Measured with the two new benchmarks on PHP 8.5, old and new code alternated and the best of each round kept:

| `benchmarks/inbound-throughput.php` — client µs per message | 2.0.0 | 2.1.0 |
|---|---|---|
| MQTT 3.1.1, QoS 0, 64 B | 4.6 | 3.8 |
| MQTT 3.1.1, QoS 0, 1 KiB | 7.0 | 5.6 |
| MQTT 5.0, QoS 0, 64 KiB | 126 | 95 |
| MQTT 5.0, QoS 0, 1 MiB | 2,574 | 1,837 |

| `benchmarks/hot-paths.php` | 2.0.0 | 2.1.0 |
|---|---|---|
| MQTT 5 PUBLISH decode, 1 MiB payload | 31.6 µs | 16.7 µs |
| WebSocket frame encode, 16 KiB | 956 µs | 7.2 µs |
| WebSocket frame encode, 1 MiB | 58.9 ms | 0.8 ms |

Where it came from:

- `TcpTransport` reads up to 64 KiB per system call into its own buffer, so the several small reads of a packet header are served from memory, and it asks PHP for 64 KiB stream chunks instead of 8 KiB — a 1 MiB packet took 128 `recv()` calls and now takes 16. A body read is returned without a copy when it fills the buffer exactly.
- The MQTT 5 decoder copied the rest of the packet — payload included — to read each one-to-four byte property length. `Bytes::decodeVarInt()` now takes an offset and reads in place.
- The `PacketReceived` event concatenated header and body, a full copy of every inbound packet, even with no dispatcher configured. Optional collaborators (`events`, `metrics`, flow control, topic aliases) are now called with `?->`, which also skips evaluating the arguments when they are absent.
- WebSocket masking was a `chr()`/`ord()` loop per byte in PHP; it is now one string XOR.

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

[Unreleased]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v2.1.0...HEAD
[2.1.0]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v1.3.0...v2.0.0
[1.3.0]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/UltraEmbeddedLab/mqtt-client/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/UltraEmbeddedLab/mqtt-client/releases/tag/v1.0.0
