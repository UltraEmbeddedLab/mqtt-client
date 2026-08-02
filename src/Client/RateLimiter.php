<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use ScienceStories\Mqtt\Util\Clock;

use function max;
use function min;
use function usleep;

/**
 * Token bucket rate limiter for client-side publish throttling.
 *
 * Prevents flooding the broker by enforcing a maximum message rate.
 * Uses a token bucket algorithm: tokens are added at a steady rate,
 * and each publish consumes one token.
 */
final class RateLimiter
{
    private float $tokens;

    private float $lastRefill;

    /**
     * @param float $messagesPerSecond Maximum messages per second (e.g., 100.0)
     * @param int   $burstSize         Maximum burst size (tokens accumulated when idle)
     */
    public function __construct(
        private readonly float $messagesPerSecond,
        private readonly int $burstSize = 10,
    ) {
        $this->tokens     = (float) $burstSize;
        $this->lastRefill = Clock::now();
    }

    /**
     * Acquire a token, blocking until one is available.
     *
     * @return float Time waited in seconds (0.0 if no wait was needed)
     */
    public function acquire(): float
    {
        $this->refill();

        if ($this->tokens >= 1.0) {
            $this->tokens -= 1.0;

            return 0.0;
        }

        // Calculate wait time for next token
        $deficit     = 1.0 - $this->tokens;
        $waitSeconds = $deficit / $this->messagesPerSecond;
        usleep((int) ($waitSeconds * 1_000_000));

        $this->refill();
        $this->tokens = max(0.0, $this->tokens - 1.0);

        return $waitSeconds;
    }

    /**
     * Try to acquire a token without blocking.
     *
     * @return bool true if a token was acquired, false if rate limited
     */
    public function tryAcquire(): bool
    {
        $this->refill();

        if ($this->tokens >= 1.0) {
            $this->tokens -= 1.0;

            return true;
        }

        return false;
    }

    public function availableTokens(): float
    {
        $this->refill();

        return $this->tokens;
    }

    private function refill(): void
    {
        $now     = Clock::now();
        $elapsed = $now - $this->lastRefill;

        if ($elapsed > 0) {
            $this->tokens = min(
                (float) $this->burstSize,
                $this->tokens + ($elapsed * $this->messagesPerSecond),
            );
            $this->lastRefill = $now;
        }
    }
}
