# Flow Control (MQTT 5.0)

Flow control limits the number of unacknowledged QoS 1/2 messages in flight, preventing buffer overflow and ensuring fair resource allocation.

## Overview

MQTT 5.0 introduces `receive_maximum` to control the number of concurrent in-flight QoS 1/2 messages:

- **Client** sends its `receive_maximum` in CONNECT (how many messages it can receive)
- **Broker** sends its `receive_maximum` in CONNACK (how many messages client can send)

Neither side may have more than `receive_maximum` unacknowledged QoS 1/2 packets at any time.

## Configuration

```php
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;

$options = new Options(
    host: 'broker.example.com',
    version: MqttVersion::V5_0,
)
    ->withReceiveMaximum(10); // Limit to 10 concurrent in-flight messages
```

## How It Works

### During Publishing (QoS 1/2)

```php
// FlowControl automatically tracks in-flight messages
$packetId = $client->publish('topic', 'payload', new PublishOptions(
    qos: QoS::AtLeastOnce // QoS 1
));

// If at capacity, publish() will wait for ACKs before sending
```

### Automatic Waiting

When publishing at maximum capacity:

1. `publish()` checks if a slot is available
2. If not, it processes incoming packets (ACKs) while waiting
3. When a slot becomes available, the message is sent
4. Timeout throws `Timeout` exception after 5 seconds

### ACK Tracking

Flow control tracks:
- **PUBACK**: QoS 1 acknowledgment → releases slot
- **PUBCOMP**: QoS 2 final acknowledgment → releases slot

## API Reference

### FlowControl

The client creates and drives its own `FlowControl` from the broker's `receive_maximum`
in CONNACK; that instance is internal and not reachable from `Client`. The class is
public so you can use it directly when you are managing in-flight state yourself:

```php
use ScienceStories\Mqtt\Client\FlowControl;

$flow = new FlowControl(maxInFlight: 10);

// Check current status
$inFlight = $flow->currentInFlight;       // Current in-flight messages
$max      = $flow->maxInFlight;           // Maximum allowed
$canSend  = $flow->canSend();             // Is a slot available?

// Track a send and its acknowledgement
$flow->trackSend($packetId);
$flow->trackAck($packetId);

// Inspect pending state
$pending   = $flow->getPendingPacketIds(); // list<int>
$isPending = $flow->isPending($packetId);  // bool
$timedOut  = $flow->getTimedOutPackets(30.0); // Packets pending > 30 seconds

// Clear on reconnect
$flow->reset();
```

## Default Values

| Parameter | Default | Description |
|-----------|---------|-------------|
| `receive_maximum` | 65535 | Maximum concurrent QoS 1/2 messages |

If not specified in CONNECT/CONNACK, the default of 65535 effectively disables flow control.

## Benefits

1. **Prevents Overflow**: Slower clients won't be overwhelmed
2. **Fair Allocation**: Resources are distributed fairly among clients
3. **Backpressure**: Natural flow control mechanism
4. **Required for QoS**: Essential for reliable high-throughput scenarios

## Limitations

1. **MQTT 5.0 Only**: Not available in MQTT 3.1.1
2. **QoS 1/2 Only**: QoS 0 messages are not tracked
3. **Per-Connection**: Each connection has its own limits

## Best Practices

1. **Set Appropriate Limits**: Match your client's processing capacity
2. **Handle Timeouts**: Implement retry logic for timed-out messages
3. **Monitor In-Flight**: Track `currentInFlight` property for debugging
4. **Consider Broker Limits**: The effective limit is the broker's `receive_maximum`

## Example

See `examples/flow_control_example.php` for a complete working example.

```php
// Run the example
php examples/flow_control_example.php
```
