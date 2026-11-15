<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Tests\Support\Loopback;
use ScienceStories\Mqtt\Transport\TcpTransport;

/**
 * @return array{0: TcpTransport, 1: resource}
 */
function tcpPair(): array
{
    $loopback  = Loopback::listener();
    $transport = new TcpTransport();
    $transport->open($loopback->host, $loopback->port);

    return [$transport, $loopback->accept()];
}

/** Give the kernel a moment to move bytes across loopback. */
function settle(): void
{
    usleep(20_000);
}

describe('TcpTransport reads are atomic', function (): void {
    it('keeps bytes that arrived before a timeout instead of discarding them', function (): void {
        // The defect: a timeout halfway through a packet threw away what had been read,
        // so the next read started mid-packet and every later frame boundary was wrong.
        [$transport, $peer] = tcpPair();

        fwrite($peer, 'abc');
        settle();
        expect(fn (): string => $transport->readExact(5, 0.05))->toThrow(Timeout::class);

        fwrite($peer, 'de');
        expect($transport->readExact(5, 1.0))->toBe('abcde');
    });

    it('reassembles a body larger than one socket read across several timeouts', function (): void {
        [$transport, $peer] = tcpPair();
        $data               = random_bytes(200_000);
        $offset             = 0;
        $got                = null;

        // Feed the socket only as fast as the kernel accepts, reading in between — both
        // ends are in this process, so a blocking write of 200 KB could deadlock. Bounded,
        // so a transport that loses bytes on timeout fails here instead of hanging forever.
        $deadline = hrtime(true) + 5_000_000_000;
        while ($got === null && hrtime(true) < $deadline) {
            if ($offset < strlen($data)) {
                $offset += (int) fwrite($peer, substr($data, $offset, 32_768));
            }
            try {
                $got = $transport->readExact(strlen($data), 0.01);
            } catch (Timeout) {
                // partial — keep feeding
            }
        }

        expect($offset)->toBe(strlen($data))
            ->and($got)->toBe($data);
    });

    it('serves many small reads in order from a single socket read', function (): void {
        [$transport, $peer] = tcpPair();
        fwrite($peer, 'abcdefghij');
        settle();

        $out = '';
        for ($i = 0; $i < 10; $i++) {
            $out .= $transport->readExact(1, 0.0);
        }

        expect($out)->toBe('abcdefghij');
    });
});

describe('TcpTransport timeouts', function (): void {
    it('treats a zero timeout as a poll, returning data that is already waiting', function (): void {
        // tick() is loopOnce(0.0). The old deadline check ran before select(), so a zero
        // timeout failed without ever looking at the socket and tick() never read a byte.
        [$transport, $peer] = tcpPair();
        fwrite($peer, "\x01\x02");
        settle();

        expect($transport->readExact(2, 0.0))->toBe("\x01\x02");
    });

    it('fails a zero-timeout poll immediately when nothing is waiting', function (): void {
        [$transport, $peer] = tcpPair();

        $start = hrtime(true);
        expect(fn (): string => $transport->readExact(1, 0.0))->toThrow(Timeout::class)
            ->and((hrtime(true) - $start) / 1e9)->toBeLessThan(0.5);

        // Closed only now: had the peer gone away first, the poll would rightly have
        // reported EOF as a TransportError instead of a Timeout.
        fclose($peer);
    });

    it('blocks without a timeout until the data is there', function (): void {
        [$transport, $peer] = tcpPair();
        fwrite($peer, 'xyz');

        // No timeout argument: null, block until the bytes arrive.
        expect($transport->readExact(3))->toBe('xyz');
    });

    it('drops buffered bytes when the connection is reopened', function (): void {
        $loopback  = Loopback::listener();
        $transport = new TcpTransport();
        $transport->open($loopback->host, $loopback->port);
        $peer = $loopback->accept();
        fwrite($peer, 'stale-');
        settle();
        expect(fn (): string => $transport->readExact(100, 0.01))->toThrow(Timeout::class);

        $transport->open($loopback->host, $loopback->port);
        $fresh = $loopback->accept();
        fwrite($fresh, 'new');

        expect($transport->readExact(3, 1.0))->toBe('new');
    });
});
