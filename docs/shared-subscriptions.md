# Shared Subscriptions (MQTT 5.0)

Shared subscriptions allow multiple clients to share message delivery, enabling load balancing across subscriber instances.

## Overview

With normal subscriptions, every subscriber receives every message. Shared subscriptions change this:

- Messages are distributed across subscribers in a share group
- Each message is delivered to **only one** client in the group
- Enables horizontal scaling and load balancing

## Topic Filter Format

```
$share/{ShareName}/{TopicFilter}
```

- `$share/` - Required prefix for shared subscriptions
- `{ShareName}` - Group name (alphanumeric, no wildcards)
- `{TopicFilter}` - Standard MQTT topic filter (can include wildcards)

### Examples

```
$share/workers/sensors/#           # All clients in "workers" group share sensors/#
$share/analytics/events/+/data     # All clients in "analytics" group share events/+/data
$share/processors/commands         # All clients in "processors" group share commands
```

## Usage

```php
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;

$options = new Options(
    host: 'broker.example.com',
    version: MqttVersion::V5_0, // Shared subscriptions require MQTT 5.0
);

$client = new Client($options, $transport);
$client->connect();

// Subscribe to shared subscription
$client->subscribe(['$share/mygroup/sensors/#'], qos: 1);

// Messages will be distributed across all clients in "mygroup"
```

## How It Works

### Message Distribution

1. Multiple clients subscribe to `$share/group/topic`
2. Publisher sends message to `topic`
3. Broker delivers message to **one** client in the group
4. Other clients in the group do not receive it

### Distribution Strategies

Brokers may use different strategies:
- **Round-robin**: Rotate through clients
- **Random**: Select client randomly
- **Sticky**: Same client for same topic (some brokers)

The specific strategy depends on the broker implementation.

## Checking Broker Support

```php
$result = $client->connect();

// The property is optional: absent means "supported" per MQTT 5 §3.2.2.3.13.
if ($result->connAck?->isSharedSubscriptionAvailable() === false) {
    echo "Broker does not support shared subscriptions";
}
```

> The property name is `connAck`, and it is `null` on MQTT 3.1.1 connections — a
> `$result->connack->...` chain (lower-case `a`, no null check) fatals or silently
> reports "supported" for every broker.

## Use Cases

### Load Balancing

Distribute message processing across multiple workers:

```php
// Worker 1
$client1->subscribe(['$share/workers/jobs/#']);

// Worker 2
$client2->subscribe(['$share/workers/jobs/#']);

// Worker 3
$client3->subscribe(['$share/workers/jobs/#']);

// Each job message goes to only one worker
```

### Horizontal Scaling

Scale subscribers independently:

```php
// Scale up: Add more instances subscribing to same share group
// Scale down: Remove instances without message loss
```

### Fault Tolerance

If one subscriber fails, messages automatically go to surviving subscribers.

## Limitations

1. **MQTT 5.0 Only**: Not supported in MQTT 3.1.1
2. **Broker Support Required**: Not all brokers support shared subscriptions
3. **No Ordering Guarantee**: Messages may arrive out of order across group
4. **No Redelivery on Failure**: If subscriber fails after receiving, message may be lost

## Best Practices

1. **Use QoS 1/2**: Ensure message acknowledgment for reliability
2. **Monitor Health**: Track subscriber health for the share group
3. **Unique Client IDs**: Each subscriber needs a unique client ID
4. **Handle Rebalancing**: Be prepared for message distribution changes

## Broker Support

Most major MQTT 5.0 brokers support shared subscriptions:
- Mosquitto 2.0+
- HiveMQ
- EMQX
- VerneMQ
- AWS IoT Core

Check your broker's documentation for specific details.

## Example

See `examples/shared_subscription_example.php` for a complete working example.

```bash
# Terminal 1 - Start first subscriber
php examples/shared_subscription_example.php

# Terminal 2 - Start second subscriber
php examples/shared_subscription_example.php

# Terminal 3 - Publish messages
php examples/simple_publish.php sensors/temperature "22.5"
```

Each message will be received by only one subscriber.
