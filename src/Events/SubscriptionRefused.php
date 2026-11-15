<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Events;

/**
 * PSR-14 event dispatched when the broker refuses a subscription in its SUBACK.
 *
 * `subscribe()` returns nothing, and a refusal during the automatic resubscribe after a
 * reconnect has no caller to return to — this event is how the application finds out. The
 * filter is not remembered for later resubscription; subscribe again once the cause is
 * fixed (an ACL, a broker setting).
 *
 * `subscribeWith()` also reports the codes in its `SubscribeResult`.
 *
 * On your own call the event is dispatched before `subscribe()` returns. On the automatic
 * resubscribe it is held until every filter has been sent again, so a listener may subscribe
 * to a fallback filter — remembered like any other subscription — and an exception it throws
 * reaches the caller of `loopOnce()` without cutting the resubscribe short.
 *
 * DTO-only, immutable.
 */
final readonly class SubscriptionRefused
{
    /**
     * @param  string  $filter  The topic filter the broker refused
     * @param  int  $reasonCode  0x80 on MQTT 3.1.1; on MQTT 5 the SUBACK reason code, such as
     *                           0x87 Not authorized or 0x8F Topic Filter invalid
     * @param  bool  $resubscribing  True when the refusal answered the client's automatic
     *                               resubscribe after a reconnect, not a call of yours
     */
    public function __construct(
        public string $filter,
        public int $reasonCode,
        public bool $resubscribing = false,
    ) {
    }
}
