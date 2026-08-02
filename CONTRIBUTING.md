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

Requirements: PHP 8.4+, Composer, and Docker for the integration suite.

```bash
git clone https://github.com/UltraEmbeddedLab/php-iot.git
cd php-iot
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
composer test              # both suites
composer test:unit         # unit only — no broker needed
composer test:integration  # needs the Docker broker
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
- `Options` and the `*Options` family are immutable by convention: `with*()` clones then
  assigns. New value objects should use PHP 8.4 `private(set)` (see `TopicAliasManager`).
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

Use [Discussions](https://github.com/UltraEmbeddedLab/php-iot/discussions) for questions and
usage help. The issue tracker is for bugs and feature requests.

Thank you.
