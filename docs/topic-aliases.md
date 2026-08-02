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
    ->withTopicAliasMaximum(10); // Request up to 10 topic aliases
```

## How It Works

### Connection Phase

1. Client sends `topic_alias_maximum` in CONNECT properties
2. Broker responds with its `topic_alias_maximum` in CONNACK
3. Client uses the broker's limit (may be lower than requested)

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

Outbound aliasing is automatic: after CONNACK the client builds its own
`TopicAliasManager` from the broker's advertised `topic_alias_maximum` and applies it to
every PUBLISH. That instance is internal and not reachable from `Client`. The class is
public so you can drive it yourself:

```php
use ScienceStories\Mqtt\Client\TopicAliasManager;

$manager = new TopicAliasManager(maxAliases: 10);

// Allocate (or reuse) an alias for a topic
['alias' => $alias, 'isNew' => $isNew] = $manager->getOrCreateAlias('sensors/temp');

// Check current status
$count     = $manager->getAliasCount();       // Number of aliases used
$available = $manager->hasAvailableSlots();   // Can more aliases be created?

// Look up existing mappings
$alias    = $manager->getAlias('sensors/temp');  // int|null
$hasAlias = $manager->hasAlias('sensors/temp');  // bool
$topic    = $manager->resolveAlias(1);           // string|null

// Aliases are connection-scoped
$manager->reset();
```

## Limitations

1. **MQTT 5.0 Only**: Topic aliases are not available in MQTT 3.1.1
2. **Broker Support**: Broker must advertise `topic_alias_maximum > 0`
3. **Connection-Scoped**: Aliases reset on disconnect/reconnect
4. **One-Way**: Each direction (client→broker, broker→client) has separate aliases
5. **Outbound only, for now**: the client aliases the topics it *publishes*. Aliases the
   broker uses on inbound PUBLISH packets are not yet resolved, so avoid setting
   `withTopicAliasMaximum()` above 0 until that lands — see the roadmap.

## Best Practices

1. **Set Appropriate Maximum**: Request only as many aliases as you need
2. **Reuse Topics**: Maximize benefit by publishing repeatedly to same topics
3. **Handle Reconnects**: Alias mappings reset after reconnect

## Example

See `examples/topic_alias_example.php` for a complete working example.

```php
// Run the example
php examples/topic_alias_example.php
```
