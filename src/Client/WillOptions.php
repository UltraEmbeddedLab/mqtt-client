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
        /** @var array<string, mixed>|null MQTT 5 Will properties (unused in v3 and MVP) */
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
}
