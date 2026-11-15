<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Tests\Support;

use Closure;
use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Testing\InMemoryTransport;

use function ord;

/**
 * An in-memory transport whose writes a test can react to, or make fail.
 *
 * Reacting to a write is how a test plays the broker mid-call: `publish()` blocks until it
 * is acknowledged, so an answer that depends on what the client just sent — a PUBCOMP for a
 * resent PUBREL, a dropped connection right after a given packet — can only be fed from
 * here.
 *
 * A failing write throws TransportError before anything reaches the inner transport, which
 * stays open: the client has to notice and close it itself, as it must with a socket whose
 * write failed but which the kernel has not reported closed yet.
 */
final class InterceptingTransport implements TransportInterface
{
    /** Writes still allowed to succeed; the one after them throws. */
    public int $writesBeforeFailure = PHP_INT_MAX;

    /**
     * Called after each successful write with the packet type (the high nibble of the first
     * byte) and the bytes written.
     *
     * @var (Closure(int, string): void)|null
     */
    public ?Closure $onWrite = null;

    public function __construct(public readonly InMemoryTransport $inner)
    {
    }

    public function open(string $host, int $port, float $timeoutSec = 5.0): void
    {
        $this->inner->open($host, $port, $timeoutSec);
    }

    public function write(string $bytes): int
    {
        if ($this->writesBeforeFailure-- <= 0) {
            throw new TransportError('Connection reset');
        }

        $written = $this->inner->write($bytes);
        if ($this->onWrite instanceof Closure && $bytes !== '') {
            ($this->onWrite)(ord($bytes[0]) >> 4, $bytes);
        }

        return $written;
    }

    public function readExact(int $length, ?float $timeoutSec = null): string
    {
        return $this->inner->readExact($length, $timeoutSec);
    }

    public function close(): void
    {
        $this->inner->close();
    }

    public function isOpen(): bool
    {
        return $this->inner->isOpen();
    }

    public function enableTls(?array $tlsOptions = null): void
    {
        $this->inner->enableTls($tlsOptions);
    }
}
