<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Transport;

use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Util\Clock;

use function defined;
use function floor;
use function max;
use function restore_error_handler;
use function set_error_handler;
use function str_contains;
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
     * @throws Timeout when the deadline passes, or early when a signal interrupts the wait
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

        $r       = [$stream];
        $w       = null;
        $e       = null;
        $warning = '';
        // Caught rather than silenced with @: a silenced warning still reaches every error
        // handler — the application's, an error tracker's, the test runner's — and EINTR is
        // routine for a process that uses signals, not something to report.
        set_error_handler(static function (int $errno, string $message) use (&$warning): bool {
            $warning = $message;

            return true;
        });
        try {
            $n = stream_select($r, $w, $e, $sec, $usec);
        } finally {
            restore_error_handler();
        }
        if ($n === false) {
            if (self::interruptedBySignal($warning)) {
                // A signal arrived (pcntl): nothing was read and the connection is fine.
                // Reported as a timeout, which callers already treat as "nothing yet", so
                // control returns to the application — a SIGTERM handler that called stop()
                // ends run() now rather than when the next packet happens to arrive. As a
                // TransportError it tore the connection down.
                throw new Timeout($errorPrefix.'Read interrupted by a signal');
            }

            throw new TransportError($errorPrefix.'stream_select failed'.($warning !== '' ? ": $warning" : ''));
        }
        if ($n === 0) {
            throw new Timeout($errorPrefix.'Read timed out');
        }
    }

    /**
     * Whether a failed stream_select() was a system call interrupted by a signal (EINTR).
     *
     * PHP reports it only through the warning text — "Unable to select [4]: Interrupted
     * system call". EINTR is 4 on Linux and macOS; ext-sockets or ext-pcntl say so when
     * loaded. Windows has no such signals.
     */
    private static function interruptedBySignal(string $warning): bool
    {
        $eintr = defined('SOCKET_EINTR') ? SOCKET_EINTR : (defined('PCNTL_EINTR') ? PCNTL_EINTR : 4);

        return str_contains($warning, "[$eintr]");
    }
}
