<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Contract\TransportInterface;
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

/**
 * Run $wait with a SIGALRM due in one second — the way a worker's job timeout or a
 * supervisor's signal lands while the client is waiting.
 *
 * ext-pcntl is optional on purpose: Windows has none, so the tests that call this skip
 * without it, and requiring it in composer.json would break installs there.
 *
 * @return int how many signals the handler saw
 *
 * @noinspection PhpComposerExtensionStubsInspection
 */
function withAlarmSignal(Closure $wait): int
{
    $previous = pcntl_async_signals(true);
    $signals  = 0;
    pcntl_signal(SIGALRM, static function () use (&$signals): void {
        $signals++;
    });
    pcntl_alarm(1);
    try {
        $wait();
    } finally {
        pcntl_alarm(0);
        pcntl_signal(SIGALRM, SIG_DFL);
        pcntl_async_signals($previous);
    }

    return $signals;
}

describe('a signal while waiting to read', function (): void {
    it('is a timeout that leaves the connection usable, not a transport failure', function (): void {
        // select() fails with EINTR when a signal arrives. Taken for a broken socket, it tore
        // down the connection of any process that uses signals — a worker's SIGALRM timeout,
        // a supervisor's SIGTERM — whenever one landed while the client waited.
        [$transport, $peer] = tcpPair();
        $started            = hrtime(true);

        $signals = withAlarmSignal(static function () use ($transport): void {
            expect(fn (): string => $transport->readExact(1, 5.0))->toThrow(Timeout::class, 'interrupted by a signal');
        });

        expect($signals)->toBe(1)
            ->and((hrtime(true) - $started) / 1e9)->toBeLessThan(4.0)
            ->and($transport->isOpen())->toBeTrue();
        fwrite($peer, 'x');
        expect($transport->readExact(1, 1.0))->toBe('x');
    })->skip(! function_exists('pcntl_alarm'), 'needs ext-pcntl (not available on Windows)');

    it('leaves the client connected, and it delivers what arrives next', function (): void {
        $loopback  = Loopback::listener();
        $transport = new class (new TcpTransport(), $loopback) implements TransportInterface {
            /** @var resource|null the broker end, once the client has connected */
            public $peer;

            public function __construct(private readonly TcpTransport $inner, private readonly Loopback $loopback)
            {
            }

            public function open(string $host, int $port, float $timeoutSec = 5.0): void
            {
                $this->inner->open($host, $port, $timeoutSec);
                $this->peer = $this->loopback->accept();
                fwrite($this->peer, "\x20\x02\x00\x00");                        // CONNACK, accepted
            }

            public function write(string $bytes): int
            {
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
        };
        $client = new Client(new Options(host: $loopback->host, port: $loopback->port, clientId: 'signalled', keepAlive: 60), $transport);
        $client->connect();

        // Idle, the client blocks until Keep Alive is due — far longer than the alarm.
        withAlarmSignal(static function () use ($client): void {
            expect($client->loopOnce(null))->toBeFalse();
        });
        fwrite($transport->peer, "\x30\x04\x00\x01tx");                          // PUBLISH t: "x"

        expect($client->awaitMessage(1.0)?->payload)->toBe('x')
            ->and($transport->isOpen())->toBeTrue();
    })->skip(! function_exists('pcntl_alarm'), 'needs ext-pcntl (not available on Windows)');
});
