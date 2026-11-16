<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use NoDiscard;
use ScienceStories\Mqtt\Protocol\QoS;

/**
 * Immutable Last Will options for CONNECT.
 */
final class WillOptions
{
    public function __construct(
        public string $topic,
        public string $payload,
        public QoS $qos = QoS::AtMostOnce,
        public bool $retain = false,
        /**
         * @var array<string, mixed>|null MQTT 5 Will Properties (§3.1.3.2): will_delay_interval,
         *                                payload_format_indicator, message_expiry_interval, content_type,
         *                                response_topic, correlation_data, user_properties. Ignored on MQTT 3.1.1.
         */
        public ?array $properties = null,
    ) {
    }

    #[NoDiscard]
    public function withTopic(string $topic): self
    {
        return clone($this, ['topic' => $topic]);
    }

    #[NoDiscard]
    public function withPayload(string $payload): self
    {
        return clone($this, ['payload' => $payload]);
    }

    #[NoDiscard]
    public function withQos(QoS $qos): self
    {
        return clone($this, ['qos' => $qos]);
    }

    #[NoDiscard]
    public function withRetain(bool $retain = true): self
    {
        return clone($this, ['retain' => $retain]);
    }

    /**
     * @param  array<string, mixed>|null  $props
     */
    #[NoDiscard]
    public function withProperties(?array $props): self
    {
        return clone($this, ['properties' => $props]);
    }

    /**
     * Have the broker wait before publishing the Will (MQTT 5, Will Delay Interval).
     *
     * A device that reconnects within the delay is not announced offline at all — the usual
     * reason to give a presence Will one. The broker also publishes it early if the session
     * ends first. Ignored on MQTT 3.1.1, which publishes the Will at once.
     */
    #[NoDiscard]
    public function withDelayInterval(int $seconds): self
    {
        return clone($this, ['properties' => ['will_delay_interval' => max(0, $seconds)] + ($this->properties ?? [])]);
    }
}
