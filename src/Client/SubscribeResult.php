<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use ScienceStories\Mqtt\Protocol\Packet\SubAck;

/**
 * Result of a SUBSCRIBE exchange.
 *
 * Contains both high-level subscription result information and the raw SUBACK packet
 * for detailed inspection of broker response and capabilities.
 *
 * - packetId: identifier used in the SUBSCRIBE/SUBACK handshake; 0 when nothing was sent
 * - results: the SUBACK's return codes, one per filter actually sent, in the order given.
 *   Empty filters are never sent, so they have no code: with `['', 'a/b']` the only code,
 *   `results[0]`, is the one for `a/b`. Empty when every filter was empty.
 *   * For v3: 0,1,2 = granted QoS; 0x80 = failure
 *   * For v5: reason codes (0x00..0x9F); 0x00..0x02 still map to granted QoS
 *   A filter with a code of 0x80 or above was refused: the client does not remember it and
 *   dispatches `Events\SubscriptionRefused` for it.
 * - subAck: Full SUBACK packet for detailed inspection (return codes, properties, descriptions);
 *   null when nothing was sent
 */
final readonly class SubscribeResult
{
    /**
     * @param  int  $packetId  Packet identifier matching the SUBSCRIBE request, or 0 when nothing was sent
     * @param  list<int>  $results  Return/reason codes, one per non-empty filter, as the SUBACK listed them
     * @param  SubAck|null  $subAck  Full SUBACK packet for detailed inspection
     */
    public function __construct(
        public int $packetId,
        public array $results,
        public ?SubAck $subAck = null,
    ) {
    }
}
