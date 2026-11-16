# Server-Initiated Disconnect (MQTT 5.0)

In MQTT 5.0, the broker can send a DISCONNECT packet to the client with a reason code explaining why the connection is being closed.

The `ServerDisconnect` event also fires when the **client** ends the connection — a PINGRESP
that never arrived (`0x8D`), or a broker that broke the protocol, such as an invalid topic
alias (`0x94`, `0x82`). The event's `Disconnect` is then one the client built, and its
`reason_string` property begins `Closed by the client`:

```php
$reason       = $event->disconnect->properties['reason_string'] ?? '';
$closedByUs   = str_starts_with($reason, 'Closed by the client');
```

Whichever side ended the connection, the transport is already closed while listeners run, so
a listener's publish cannot go out on a connection that is being torn down: it is queued
offline if the offline queue is enabled, and otherwise throws `TransportError`.

## Overview

MQTT 3.1.1 only allowed clients to send DISCONNECT. MQTT 5.0 enables:

- Broker-initiated disconnects with reason codes
- Detailed error information via properties
- Server reference for failover scenarios

## Reason Codes

| Code | Hex | Description |
|------|-----|-------------|
| 0 | 0x00 | Normal disconnection |
| 4 | 0x04 | Disconnect with Will Message |
| 128 | 0x80 | Unspecified error |
| 129 | 0x81 | Malformed Packet |
| 130 | 0x82 | Protocol Error |
| 131 | 0x83 | Implementation specific error |
| 135 | 0x87 | Not authorized |
| 137 | 0x89 | Server busy |
| 139 | 0x8B | Server shutting down |
| 141 | 0x8D | Keep Alive timeout |
| 142 | 0x8E | Session taken over |
| 143 | 0x8F | Topic Filter invalid |
| 147 | 0x93 | Receive Maximum exceeded |
| 148 | 0x94 | Topic Alias invalid |
| 149 | 0x95 | Packet too large |
| 150 | 0x96 | Message rate too high |
| 151 | 0x97 | Quota exceeded |
| 152 | 0x98 | Administrative action |
| 153 | 0x99 | Payload format invalid |
| 154 | 0x9A | Retain not supported |
| 155 | 0x9B | QoS not supported |
| 156 | 0x9C | Use another server |
| 157 | 0x9D | Server moved |
| 158 | 0x9E | Shared Subscriptions not supported |
| 159 | 0x9F | Connection rate exceeded |
| 160 | 0xA0 | Maximum connect time |
| 161 | 0xA1 | Subscription Identifiers not supported |
| 162 | 0xA2 | Wildcard Subscriptions not supported |

## Properties

DISCONNECT packets may include:

| Property | Description |
|----------|-------------|
| `session_expiry_interval` | Updated session expiry (seconds) |
| `reason_string` | Human-readable explanation |
| `server_reference` | Alternative server to connect to |
| `user_properties` | Custom key-value metadata |

## Handling Disconnects

### Using Events (PSR-14)

```php
use ScienceStories\Mqtt\Events\ServerDisconnect;

$eventDispatcher->addListener(ServerDisconnect::class, function (ServerDisconnect $event) {
    $disconnect = $event->disconnect;

    echo "Server disconnect: " . $disconnect->getReasonDescription();

    if ($disconnect->isError()) {
        // Handle error scenarios
    }

    if ($ref = $disconnect->getServerReference()) {
        // Consider connecting to alternative server
        echo "Try: {$ref}";
    }
});

$client = new Client($options, $transport, events: $eventDispatcher);
```

### ServerDisconnect Event

```php
// Event properties
$event->disconnect;      // Disconnect packet object
$event->willReconnect;   // bool - will client attempt reconnect?

// Convenience methods
$event->getReasonCode();           // int
$event->isError();                 // bool (code >= 0x80)
$event->isNormal();                // bool (code 0x00 or 0x04)
$event->getReasonDescription();    // string
$event->getReasonString();         // string|null
$event->getServerReference();      // string|null
```

### Disconnect Packet API

```php
$disconnect = $event->disconnect;

// Reason code
$disconnect->reasonCode;                    // int
$disconnect->isNormal();                    // bool
$disconnect->isError();                     // bool
$disconnect->getReasonDescription();        // string

// Properties
$disconnect->getReasonString();             // string|null
$disconnect->getServerReference();          // string|null
$disconnect->getSessionExpiryInterval();    // int|null
$disconnect->getUserProperties();           // array<string,string>
$disconnect->hasProperty('key');            // bool
$disconnect->getProperty('key', $default);  // mixed
```

## Auto-Reconnect Behavior

When `autoReconnect` is enabled:

```php
$options = $options->withAutoReconnect(
    enable: true,
    maxAttempts: 5,
    baseDelay: 0.2,
    maxDelay: 5.0,
);
```

- On error disconnect (code >= 0x80): Auto-reconnect attempts
- On normal disconnect (code 0x00): No auto-reconnect
- On Session taken over (0x8E): No auto-reconnect — see below
- `ServerDisconnect::$willReconnect` says which; when it is `false`, `run()` and
  `messages()` stop and `loopOnce()` does not reconnect until you call `connect()`

`maxAttempts` counts attempts in a row without a connection that lasted: a reconnect that
succeeds but is dropped again within ten seconds of its resubscribe still counts. So a
broker that accepts every connection and closes it at once is given up on after
`maxAttempts`, rather than retried forever at `baseDelay`. A connection that stays up longer,
or a `connect()` of your own, starts the count over. `willReconnect` accounts for this: once
the attempts are used up it is `false`.

## Common Scenarios

### Session Taken Over (0x8E)

Another client connected with the same Client ID, and the broker gave it this session. The
client does not reconnect: taking the session back would throw the other client off, which
— reconnecting too — would take it back in turn, and the two would alternate for as long as
both run. (MQTT 3.1.1 brokers just close the socket, so there the client cannot tell a
takeover from a network failure; reconnecting is bounded by `maxAttempts` as above.)

```php
if ($disconnect->reasonCode === 0x8E) {
    // Two processes share a Client ID: give each its own.
    $newClientId = generateUniqueId();
}
```

### Server Shutting Down (0x8B)

Server is going offline for maintenance:

```php
if ($disconnect->reasonCode === 0x8B) {
    // Check for alternative server
    if ($ref = $disconnect->getServerReference()) {
        // Connect to alternative server
    } else {
        // Retry with exponential backoff
    }
}
```

### Quota Exceeded (0x97)

Client exceeded resource limits:

```php
if ($disconnect->reasonCode === 0x97) {
    // Reduce message rate
    // Contact administrator
}
```

### Keep Alive Timeout (0x8D)

Sent by the broker, it means the client went quiet for longer than 1.5× Keep Alive — the
client pings on its own, so this points at a process that blocked (a long handler, a GC
pause) or a network that dropped packets. Built by the client (`reason_string` starts with
`Closed by the client`), it means the broker did not answer a PINGREQ within
`Options::$pingResponseTimeout`: the link is dead, not slow.

```php
if ($disconnect->reasonCode === 0x8D) {
    // Broker side: keep loopOnce() running often enough, or raise Keep Alive.
    // Client side: the network path is gone; auto-reconnect handles it.
}
```

## Decoding DISCONNECT

The library automatically decodes DISCONNECT packets:

```php
// V5 Decoder decodes full packet
$disconnect = $decoder->decodeDisconnect($body);

// V311 Decoder returns normal disconnect (no body in 3.1.1)
$disconnect = $decoder->decodeDisconnect($body);
```

## Example

See `examples/server_disconnect_example.php` for a complete working example.

```bash
# Start the example
php examples/server_disconnect_example.php

# In another terminal, connect with same client ID to trigger 0x8E
```
