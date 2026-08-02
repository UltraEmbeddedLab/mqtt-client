# Backward Compatibility Promise

This project follows [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).
This page defines precisely what that covers, so you can pin a constraint and know what an
upgrade can do to you.

## What is public API

These are covered by the promise. Breaking any of them requires a major release.

| Surface                                                                          | What is guaranteed                                                                                              |
|----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `Contract\*`                                                                     | Method signatures. New methods on an interface are a major change, because you may implement it.                |
| `Client\Client`                                                                  | Public methods and their signatures                                                                             |
| `Client\Options` and the `*Options` family                                       | Constructor parameter **names** (they are used as named arguments), public property names, `with*()` signatures |
| `Client\ConnectResult`, `SubscribeResult`, `UnsubscribeResult`, `InboundMessage` | Public property names and types                                                                                 |
| `Easy\Mqtt`                                                                      | Static method signatures                                                                                        |
| `Events\*`                                                                       | Class names, constructor signatures, public properties                                                          |
| `Exception\*`                                                                    | Class names and the inheritance chain. An exception will not move to a different parent in a minor.             |
| `Protocol\QoS`, `MqttVersion`, `ReasonCode`, `Packet\PacketType`                 | Existing case names and backing values. New cases may be **added** in a minor.                                  |
| `Transport\TcpTransport`, `Transport\WsTransport`                                | Constructor and public method signatures                                                                        |
| `Testing\InMemoryTransport`                                                      | Public method signatures                                                                                        |
| `Session\FileSessionStore`                                                       | Public method signatures                                                                                        |
| `Util\Bytes`, `Util\TopicValidator`, `Util\RandomId`                             | Public static method signatures                                                                                 |

## What is not public API

Everything else, in particular:

- `Protocol\V311\*` and `Protocol\V5\*` internals. The `Encoder`/`Decoder` classes satisfy
  `EncoderInterface`/`DecoderInterface`; the interfaces are covered, the implementations'
  private structure and wire-level helper methods are not.
- `Protocol\Packet\*` constructor signatures. These mirror the wire format and will change
  as more of MQTT 5 is implemented. Consume them through `Client`, not directly.
- Anything `private` or `protected`, including behaviour reachable only through reflection.
- Log message text, log levels, and metric names. These are diagnostics, not contract.
- The exact wording of exception messages. The *type* is contract; the string is not.
- `docs/`, `examples/`, `benchmarks/`, `tools/`.

Extending a `final` class is impossible by design. Most classes here are `final`; if you
need to decorate one, decorate the `Contract\*` interface instead.

## Behavioural changes

Signature stability is not the whole story — a method that keeps its signature and starts
throwing has still broken you. This project treats the following as breaking, requiring a
major release:

- A method that returned a value starting to throw for input it previously accepted
  (this is why `connect()` throwing on a refused CONNACK shipped in 2.0, not 1.4).
- A new default that changes what reaches the application — for example the 16 MiB inbound
  packet cap and the 1000-message inbound queue bound, both new in 2.0.
- A change to what goes on the wire that a broker can reject.
- Tightening validation so that previously-accepted configuration now throws.

The following are **not** breaking and may ship in a minor:

- New optional parameters at the end of a signature.
- New `with*()` methods and new `Options` properties with backwards-compatible defaults.
- New enum cases (match on them defensively).
- Bug fixes that make the client conform to the MQTT specification where it previously did
  not, when the previous behaviour could not have been relied on deliberately.
- Performance changes, log output, metric names.

## Deprecation process

1. The old API keeps working and is marked `@deprecated` in the docblock, with the
   replacement named and the version that introduced the deprecation.
2. The deprecation is listed under `### Deprecated` in `CHANGELOG.md`.
3. It is removed no earlier than the next major release.

Nothing is removed without having been deprecated in a released minor first.

## Support window

Security fixes land on the latest minor of the current major. The previous minor receives
critical fixes for six months after its successor ships. See [SECURITY.md](../SECURITY.md).

## PHP version support

Raising the minimum PHP version is a **minor** change, not a major one — this follows the
convention used across the PHP ecosystem. Composer will simply not offer you a release your
runtime cannot satisfy. Dropping support for a PHP version that is still receiving security
support from php.net will not happen.

## What to pin

```
"ultraembeddedlab/mqtt-client": "^2.0"
```

This gets you bug fixes and new features, never a breaking change. If you need to be
stricter while evaluating, `~2.0.0` restricts you to patch releases.
