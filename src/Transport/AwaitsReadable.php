<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Transport;

use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Util\Clock;

use function floor;
use function max;
use function stream_select;

/**
 * Waiting for a stream to become readable, shared by the stream-based transports.
 *
 * Both transports keep every byte they have read across a Timeout, and both depend on
 * the same two rules here: a zero timeout still looks at the socket once, and no
 * deadline at all blocks in select() rather than in a read.
 */
trait AwaitsReadable
{
    /**
     * Wait until $stream is readable or $deadline passes.
     *
     * Polls at least once even when the deadline has already passed, which is what makes a
     * zero timeout a poll rather than an instant failure. A null deadline blocks.
     *
     * @param  resource  $stream
     * @param  float|null  $deadline  monotonic seconds ({@see Clock::now()}), or null to block
     *
     * @throws Timeout
     * @throws TransportError if select() itself fails
     */
    private function awaitReadable($stream, ?float $deadline, string $errorPrefix = ''): void
    {
        $sec  = null;
        $usec = 0;
        if ($deadline !== null) {
            $timeLeft = max(0.0, $deadline - Clock::now());
            $sec      = (int) floor($timeLeft);
            $usec     = (int) floor(($timeLeft - $sec) * 1_000_000);
        }

        $r = [$stream];
        $w = null;
        $e = null;
        $n = @stream_select($r, $w, $e, $sec, $usec);
        if ($n === false) {
            throw new TransportError($errorPrefix.'stream_select failed');
        }
        if ($n === 0) {
            throw new Timeout($errorPrefix.'Read timed out');
        }
    }
}
