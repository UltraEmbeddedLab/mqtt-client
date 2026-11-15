<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Tests\Support;

use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Testing\InMemoryTransport;

/**
 * An in-memory transport whose writes start failing after a set number of them.
 *
 * The failure is a TransportError thrown before anything reaches the inner transport, which
 * stays open — the client has to notice and close it itself, as it must with a socket whose
 * write failed but which the kernel has not reported closed yet.
 */
final class FailingWrites implements TransportInterface
{
    /** Writes still allowed to succeed; the one after them throws. */
    public int $writesBeforeFailure = PHP_INT_MAX;

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

        return $this->inner->write($bytes);
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
