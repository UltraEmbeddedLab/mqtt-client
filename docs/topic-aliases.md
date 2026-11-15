# Topic Aliases (MQTT 5.0)

Topic aliases allow replacing a topic string with a 2-byte integer, significantly reducing bandwidth for repeated publishes to the same topic.

## Overview

When publishing messages repeatedly to the same topic, the topic string is sent with every message. Topic aliases optimize this by:

1. First publish: Send topic string + alias number (establishes mapping)
2. Subsequent publishes: Send only alias number (topic can be empty)

This reduces packet size from `len(topic) + 2` bytes to just `2` bytes per message.

## Configuration

```php
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;

$options = new Options(
    host: 'broker.example.com',
    version: MqttVersion::V5_0,
)
    ->withTopicAliasMaximum(10); // The broker may use up to 10 aliases when sending to us
```

## How It Works

### Connection Phase

Each direction has its own aliases and its own limit — they are not one budget negotiated
down:

| Limit | Sent in | Bounds |
|---|---|---|
| `Options::withTopicAliasMaximum()` | CONNECT | Aliases the **broker** may use when it sends to this client |
| The broker's `topic_alias_maximum` | CONNACK | Aliases **this client** may use when it publishes |

The same alias number can therefore mean one topic on the way out and another on the way
in; the client keeps the two maps apart.

### Receiving Phase

When the broker sends a PUBLISH with a topic and an alias, the client records the mapping;
when a later PUBLISH carries only the alias, the client fills the topic back in. Your
handler always sees the full topic:

```php
use ScienceStories\Mqtt\Client\InboundMessage;

$client->onMessage(function (InboundMessage $message): void {
    echo $message->topic; // never '' — aliases are already resolved
});
```

A broker that breaks the rules has the connection closed, as the specification requires:
an alias of 0 or above your limit gets DISCONNECT reason `0x94` (Topic Alias Invalid); an
alias it never established, or an empty topic with no alias, gets `0x82` (Protocol Error).
The message is not acknowledged, and a `ServerDisconnect` event is dispatched whose
`reason_string` begins "Closed by the client". With auto-reconnect on, the client then
reconnects; with it off, `loopOnce()` throws `ProtocolError`.

### Publishing Phase

The `TopicAliasManager` automatically handles alias assignment:

```php
// First publish - establishes alias
$client->publish('sensors/device1/temperature', '22.5');
// Packet contains: topic="sensors/device1/temperature", topic_alias=1

// Second publish - reuses alias
$client->publish('sensors/device1/temperature', '23.0');
// Packet contains: topic="", topic_alias=1 (topic omitted)
```

### Alias Assignment

- Aliases are assigned sequentially (1, 2, 3, ...)
- Each unique topic gets one alias
- When all aliases are used, new topics are published without aliases
- Aliases are connection-scoped (reset on disconnect)

## API Reference

### TopicAliasManager

Outbound aliasing is automatic: after every CONNACK the client builds its own
`TopicAliasManager` from the broker's advertised `topic_alias_maximum` — or none, if the
broker allows no aliases — and applies it to every PUBLISH. That instance is internal and
not reachable from `Client`. The class is public so you can drive it yourself:

```php
use ScienceStories\Mqtt\Client\TopicAliasManager;

$manager = new TopicAliasManager(maxAliases: 10);

// Allocate (or reuse) an alias for a topic
['alias' => $alias, 'isNew' => $isNew] = $manager->getOrCreateAlias('sensors/temp');

// Check current status
$count     = $manager->getAliasCount();       // Number of aliases used
$available = $manager->hasAvailableSlots();   // Can more aliases be created?

// Look up existing mappings — within this one direction
$alias    = $manager->getAlias('sensors/temp');  // int|null
$hasAlias = $manager->hasAlias('sensors/temp');  // bool
$topic    = $manager->resolveAlias(1);           // string|null

// Aliases are connection-scoped
$manager->reset();
```

One instance manages one direction. The client uses one for the aliases it sends and keeps
the broker's aliases in a separate map, since an alias number means different topics in
each direction.

## Limitations

1. **MQTT 5.0 only**: topic aliases do not exist in MQTT 3.1.1
2. **Broker support**: outbound aliasing needs the broker to advertise `topic_alias_maximum > 0`;
   inbound aliasing needs you to set `withTopicAliasMaximum()` above 0
3. **Connection-scoped**: both maps are cleared on every reconnect
4. **Per direction**: client→broker and broker→client aliases are separate, with separate limits

## Best Practices

1. **Size the inbound limit to the topics you receive**: each alias the broker may use costs
   one map entry on the client for the life of the connection
2. **Reuse topics**: outbound aliasing pays off when you publish to the same topics repeatedly
3. **Never set `topic_alias` yourself**: the client drops it from your properties, including
   when you forward a received message's properties — aliases belong to the connection
4. **Reconnects are handled**: both maps are cleared, and a QoS 1/2 resend always carries the
   full topic, so nothing depends on an alias the new connection never set up

## Example

See `examples/topic_alias_example.php` for a complete working example.

```php
// Run the example
php examples/topic_alias_example.php
```
