<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Util;

use function hrtime;

/**
 * The monotonic time source every interval and deadline in this library reads.
 *
 * `microtime()` is wall-clock. An NTP step or a DST change moves it, and every outstanding
 * deadline expires at once: an in-flight QoS 1 publish "times out" and resends with DUP for
 * a message the broker already acknowledged, while a backward step stalls keepalive past
 * the broker's window. `hrtime()` counts from an arbitrary origin and only moves forward,
 * which is exactly what measuring an elapsed interval needs.
 *
 * Wall-clock time is still correct for a timestamp someone reads later — that is why
 * {@see \ScienceStories\Mqtt\Session\SessionState::$savedAt} keeps using `time()`.
 */
final class Clock
{
    /**
     * Monotonic seconds since an arbitrary origin.
     *
     * Only differences between two readings are meaningful; the absolute value is not.
     */
    public static function now(): float
    {
        return hrtime(true) / 1e9;
    }
}
