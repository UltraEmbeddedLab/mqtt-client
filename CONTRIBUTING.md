# Contributing

Thanks for considering a contribution. This document is short on ceremony and long on the
things that actually save you a round-trip.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Report unacceptable
behaviour to gewaldb@gmail.com.

## What we're looking for

These are the real gaps, roughly in order of value. Any of them is welcome as a PR or as a
design discussion first — see [ROADMAP.md](ROADMAP.md) for the full picture.

- **Async adapter** — a ReactPHP or Amp driver. Blocked on splitting the blocking pump out
  of `Client`, which is the largest open piece of work.
- **Laravel or Symfony bridge** — service provider / bundle, config-to-`Options` mapping,
  a long-running listener command.
- **Inbound MQTT 5 topic-alias resolution** — the client negotiates aliases outbound but
  does not resolve them on received PUBLISH packets.
- **Acting on acknowledgement reason codes** — a PUBACK or SUBACK carrying a failure code
  is currently logged, not raised.
- **WebSocket transport** — continuation frames, and `wss://` (TLS must be enabled before
  the HTTP upgrade, which the current ordering makes impossible).
- **Typed MQTT 5 property objects** to replace `array<string, mixed>` on the public API.

Small, well-tested fixes are just as welcome as large features.

## Development setup

Requirements: PHP 8.5+, Composer, and Docker for the integration suite.

```bash
git clone https://github.com/UltraEmbeddedLab/mqtt-client.git
cd mqtt-client
composer install
docker compose up -d --wait   # Mosquitto on 1883, the integration tests need it
```

### One command before you push

```bash
composer ci
```

That runs, in order: `composer validate --strict`, `composer audit`, the workflow YAML
linter, Pint, PHPStan, Rector in dry-run mode, and the full test suite. It is exactly what
CI runs, so if it is green locally, CI will be green.

Individual pieces:

```bash
composer test              # every suite
composer test:unit         # unit only — no broker needed
composer test:integration  # needs the Docker broker
composer test:interop      # broker-dependent behaviour; see below for other brokers
composer test:coverage     # line coverage
composer type-coverage     # type coverage (CI enforces 85%)
composer stan              # PHPStan, level max
composer pint              # fix code style; `composer pint -- --test` to check
composer rector            # apply modernisation; `composer rector:check` to check
composer lint:workflows    # parse .github/workflows/*.yml
```

### About the integration suite

The integration tests skip themselves when no broker answers on `MQTT_HOST:MQTT_PORT`
(default `127.0.0.1:1883`). **If you see "MQTT broker not available", start Docker — you
are not running the tests that matter.** CI fails the build if they skip.

### Against other brokers

`tests/Interop` checks what depends on the broker — exactly-once delivery across a dropped
link, session takeover, a delayed Will, the broker's Maximum Packet Size, topic aliases it
sends, subscriptions it refuses, WebSocket. CI runs it against Mosquitto 2.1 (in the main
job), Mosquitto 2.0, EMQX and HiveMQ CE. `docker-compose.yml` has each of them behind a
profile, on its own port, so they can run side by side:

```bash
docker compose --profile interop up -d --wait          # all of them, beside Mosquitto

MQTT_BROKER=emqx MQTT_PORT=1884 MQTT_WS_PORT=8083 composer test:interop
MQTT_BROKER=hivemq MQTT_PORT=1885 composer test:interop
MQTT_BROKER=mosquitto MQTT_PORT=1886 MQTT_WS_PORT=9002 composer test:interop   # 2.0
```

`MQTT_BROKER` says which broker it is, for the tests whose expectations differ: only EMQX
sends topic aliases to clients or refuses a subscription (`#`) by default, so those tests
skip elsewhere and say why. A test that fails against one broker and passes against the
others is the point of the suite — look at what that broker does before changing the test.

For unit tests that need a scripted peer, use `ScienceStories\Mqtt\Testing\InMemoryTransport`
rather than a live broker. It is a six-method `TransportInterface` with `feedConnAck()`,
`feedPublish()`, `feedSubAck()` and friends for inbound scripting, plus `sentPackets()` and
`countSent()` for assertions. Every regression test in `tests/Unit/Client/` uses it.

## Pull requests

1. Fork and branch (`git checkout -b feature/amazing-feature`).
2. Make your change, with tests.
3. `composer ci` — green.
4. Commit **with sign-off**: `git commit -s -m 'Add amazing feature'` (see below).
5. Push and open a PR.

### Licensing of contributions

This project uses the [Developer Certificate of Origin](DCO). Sign off every commit with
`git commit -s`, which appends a `Signed-off-by: Your Name <you@example.com>` trailer.

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENSE.md) — the same licence that covers this project (inbound = outbound).

There is no CLA and no copyright assignment.

## Coding standards

The tooling enforces most of this; the rest is convention worth knowing:

- PSR-12 via Pint, plus `declare(strict_types=1)` in every file.
- Pint imports global functions and constants, hence the long `use function` blocks —
  do not remove them.
- `Options` and the `*Options` family are immutable by convention: every `with*()` is a
  single `clone($this, [...])` and carries `#[\NoDiscard]`, so calling one without using the
  result warns. A test enforces the attribute on every wither. New value objects should use
  `private(set)` (see `TopicAliasManager`).
- Anything that reads from a transport must tolerate a `Timeout` at any byte. The shipped
  transports keep partially received data, and `Client` keeps a half-read frame header
  across `loopOnce()` calls. Do not add a read that discards bytes on timeout.
- PHPStan runs at level max with `treatPhpDocTypesAsCertain` and `checkMissingTypehints`.
  Do not add `@phpstan-ignore` to silence an error — `reportUnmatchedIgnoredErrors` is on,
  so a suppression that stops being needed will fail the build.
- Anything touching the wire needs a byte-level test. Assert exact bytes, not
  `str_contains` — the latter cannot detect field reordering or a wrong Remaining Length.

## Backward compatibility

Read [docs/backward-compatibility.md](docs/backward-compatibility.md) before changing
anything public. It defines what is covered by semver, which behavioural changes count as
breaking, and the deprecation process. A change that keeps a signature but starts throwing
is still a break.

## Documentation

- Update `README.md` for user-facing changes.
- Add an entry to `CHANGELOG.md` under `[Unreleased]`, in the right section.
- PHPDoc on public methods; explain *why*, not *what*.
- Add an example under `examples/` for a new feature — CI lints all of them.

## Questions?

Use [Discussions](https://github.com/UltraEmbeddedLab/mqtt-client/discussions) for questions and
usage help. The issue tracker is for bugs and feature requests.

Thank you.
