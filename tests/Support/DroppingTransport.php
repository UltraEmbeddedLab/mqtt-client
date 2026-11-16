<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Tests\Support;

use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;

use function ord;
use function usleep;

/**
 * A real transport whose link drops right after the client writes a given packet: the
 * bytes reach the broker, then the socket closes, as if the network went away before the
 * answer came back.
 */
final class DroppingTransport implements TransportInterface
{
    private int $seen = 0;

    /**
     * @param  int  $nth  drop after the nth such packet; 0 never drops
     */
    public function __construct(
        private readonly TransportInterface $inner,
        private readonly PacketType $after = PacketType::PUBLISH,
        private readonly int $nth = 0,
    ) {
    }

    public function open(string $host, int $port, float $timeoutSec = 5.0): void
    {
        $this->inner->open($host, $port, $timeoutSec);
    }

    public function write(string $bytes): int
    {
        $written = $this->inner->write($bytes);
        if ($bytes !== '' && (ord($bytes[0]) >> 4) === $this->after->value && ++$this->seen === $this->nth) {
            usleep(30_000);                                 // let the packet reach the broker first
            $this->inner->close();
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
