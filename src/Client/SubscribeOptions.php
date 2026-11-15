<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use NoDiscard;

/**
 * Immutable subscription options.
 * - For v3, only requested QoS per filter matters (carried in filters).
 * - For v5, flags can be set to adjust retain handling and local delivery semantics.
 */
final class SubscribeOptions
{
    /**
     * @param  array<string, mixed>|null  $properties  MQTT v5 SUBSCRIBE properties (send-side). Supported keys:
     *                                                 - user_properties: array<string, string>|list<array{0:string,1:string}>|list<array{key:string,value:string}>
     */
    public function __construct(
        public bool $noLocal = false,
        public bool $retainAsPublished = false,
        public int $retainHandling = 0, // 0,1,2
        public ?array $properties = null,
    ) {
        if ($this->retainHandling < 0 || $this->retainHandling > 2) {
            $this->retainHandling = 0;
        }
    }

    #[NoDiscard]
    public function withNoLocal(bool $flag = true): self
    {
        return clone($this, ['noLocal' => $flag]);
    }

    #[NoDiscard]
    public function withRetainAsPublished(bool $flag = true): self
    {
        return clone($this, ['retainAsPublished' => $flag]);
    }

    #[NoDiscard]
    public function withRetainHandling(int $mode): self
    {
        return clone($this, ['retainHandling' => ($mode < 0 || $mode > 2) ? 0 : $mode]);
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
