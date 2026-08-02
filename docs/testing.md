# Testing

This library ships a scriptable transport in `src/` — not in `tests/` — so that **your**
test suite can exercise MQTT code without a broker, a socket, or Docker.

```php
use ScienceStories\Mqtt\Testing\InMemoryTransport;
```

`TransportInterface` is six methods, and `InMemoryTransport` implements them against two
byte buffers: one holding what the "broker" will send, one recording what the client wrote.

## The shape of a test

```php
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Testing\InMemoryTransport;

$transport = new InMemoryTransport();
$transport->feedConnAck();                       // queue the broker's reply first

$client = new Client(new Options(host: 'fake.broker'), $transport);
$client->connect();                              // consumes the CONNACK

$transport->feedPublish('sensors/temp', '21.5');
$client->loopOnce(0.0);                          // consumes the PUBLISH

$message = $client->awaitMessage(0.0);
expect($message?->payload)->toBe('21.5');
expect($transport->countSent(PacketType::CONNECT))->toBe(1);
```

Feed before you pump. `readExact()` throws `Timeout` when the buffer holds fewer bytes than
asked for — the same contract `TcpTransport` has when no data arrives in time — so a client
call that expects a reply will fail rather than hang.

## Scripting the broker

Each helper builds a correct fixed header for you and returns `$this`, so they chain.

| Method | Sends |
|---|---|
| `feedConnAck(int $reasonCode = 0, bool $sessionPresent = false, string $properties = '')` | CONNACK |
| `feedPublish(string $topic, string $payload, QoS $qos = QoS::AtMostOnce, ?int $packetId = null, bool $dup = false, bool $retain = false, string $properties = '')` | PUBLISH |
| `feedSubAck(int $packetId, array $returnCodes = [0])` | SUBACK |
| `feedUnsubAck(int $packetId, array $reasonCodes = [])` | UNSUBACK |
| `feedPubAck(int $packetId)` / `feedPubRec()` / `feedPubRel()` / `feedPubComp()` | QoS 1/2 acknowledgements |
| `feedPingResp()` | PINGRESP |
| `feedDisconnect(int $reasonCode = 0)` | DISCONNECT |
| `feedPacket(PacketType $type, int $flags, string $body)` | any packet, header computed |
| `feed(string $bytes)` | raw bytes, for malformed-input tests |
| `closeByPeer()` | makes subsequent short reads raise `TransportError` instead of `Timeout` |

`$properties` takes a raw, already-length-prefixed MQTT 5 property block. Pass `"\x00"` for
"no properties" on an MQTT 5 connection.

## Asserting on what the client sent

| Method | Returns |
|---|---|
| `sentPackets()` | `list<array{type: int, flags: int, body: string}>` — every packet written, parsed |
| `sentPacketTypes()` | `list<int>` of packet type values, in order |
| `countSent(PacketType $type)` | how many of that type were written |
| `written()` / `takeWritten()` | the raw bytes; `takeWritten()` also clears the buffer |
| `pendingBytes()` | bytes fed but not yet consumed |
| `isTlsEnabled()` / `tlsOptions()` | whether `enableTls()` was called, and with what |
| `connectedHost()` / `connectedPort()` | what `open()` received |

A trailing partial packet is ignored by `sentPackets()`, so it is safe to call mid-exchange.

## Worked examples

### A protocol error must not escape as an engine error

```php
$transport = new InMemoryTransport();
$transport->feedConnAck();
$client = new Client(new Options(host: 'fake.broker'), $transport);
$client->connect();

// PUBLISH with both QoS bits set — malformed per MQTT-3.3.1-4.
$transport->feed("\x36\x08" . pack('n', 3) . 'a/b' . 'xx');

expect(fn () => $client->loopOnce(0.0))->toThrow(ProtocolError::class);
```

### A refused connection throws rather than returning

```php
$transport = new InMemoryTransport();
$transport->feedConnAck(5);   // 3.1.1 return code 5 = not authorized

$client = new Client(new Options(host: 'fake.broker'), $transport);

expect(fn () => $client->connect())->toThrow(AuthenticationError::class);
expect($transport->isOpen())->toBeFalse();
```

### Exactly-once delivery survives a reconnect

```php
$options = new Options(host: 'fake.broker', cleanSession: false);
$transport = new InMemoryTransport();
$transport->feedConnAck();
$client = new Client($options, $transport);
$client->connect();

$received = [];
$client->onMessage(function ($m) use (&$received) { $received[] = $m->payload; });

// PUBLISH arrives, the client answers PUBREC, then the connection drops.
$transport->feedPublish('a/b', 'exactly-once', QoS::ExactlyOnce, 42);
$client->loopOnce(0.0);

// The broker resumes the session and replays only the PUBREL, per §4.3.3.
$transport->feedConnAck(0, sessionPresent: true);
$client->connect();
$transport->feedPubRel(42);
$client->loopOnce(0.0);

expect($received)->toBe(['exactly-once']);
```

## Testing your own code

The same double works for application code that takes a `ClientInterface`:

```php
final class TelemetryPublisher
{
    public function __construct(private ClientInterface $client) {}

    public function send(float $celsius): void
    {
        $this->client->publish('sensors/temp', (string) $celsius);
    }
}

it('publishes the reading to the telemetry topic', function (): void {
    $transport = new InMemoryTransport();
    $transport->feedConnAck();
    $client = new Client(new Options(host: 'fake.broker'), $transport);
    $client->connect();
    $transport->takeWritten();   // discard the CONNECT

    new TelemetryPublisher($client)->send(21.5);

    $publish = $transport->sentPackets()[0];
    expect($publish['type'])->toBe(PacketType::PUBLISH->value)
        ->and($publish['body'])->toContain('sensors/temp')
        ->and($publish['body'])->toEndWith('21.5');
});
```

## What it does not do

`InMemoryTransport` serves a read only when the full requested length is already buffered.
A real socket delivers a frame in arbitrary chunks, so this double cannot reproduce
partial-read behaviour — which is exactly where the known mid-frame timeout bug lives (see
[ROADMAP.md](../ROADMAP.md)). For that class of test, use a `stream_socket_pair()` against
`TcpTransport` directly, or the Mosquitto in `docker-compose.yml`.

It also does not validate what the client sends. It records bytes; asserting they are
correct is your test's job.

## Running this project's own tests

```bash
composer test:unit          # no broker needed
docker compose up -d --wait # Mosquitto on 1883
composer test:integration   # needs the broker
composer ci                 # everything CI runs
```

The integration suite skips itself when no broker answers. If you see "MQTT broker not
available", start Docker — CI fails the build when those tests skip.
