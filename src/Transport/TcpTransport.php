<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Transport;

use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;

use function is_int;
use function is_resource;
use function is_string;
use function sprintf;
use function strlen;

final class TcpTransport implements TransportInterface
{
    /** Owns $context and $tlsEnabled, and the whole TLS upgrade. */
    use NegotiatesTls;

    /** @var resource|null */
    private $stream;

    public function open(string $host, int $port, float $timeoutSec = 5.0): void
    {
        $this->close();

        $remote        = sprintf('tcp://%s:%d', $host, $port);
        $this->context = stream_context_create([]);

        $errno  = 0;
        $errstr = '';
        $stream = @stream_socket_client(
            $remote,
            $errno,
            $errstr,
            $timeoutSec,
            STREAM_CLIENT_CONNECT,
            $this->context
        );

        if ($stream === false) {
            $errNo  = is_int($errno) ? $errno : 0;
            $errStr = is_string($errstr) ? $errstr : '';
            throw new TransportError(sprintf('Failed to connect to %s:%d: [%d] %s', $host, $port, $errNo, $errStr));
        }

        // Blocking mode with a sane per-op timeout
        stream_set_blocking($stream, true);
        $sec  = (int) floor($timeoutSec);
        $usec = (int) floor(($timeoutSec - $sec) * 1_000_000);
        @stream_set_timeout($stream, $sec, $usec);

        $this->stream     = $stream;
        $this->tlsEnabled = false;
    }

    public function write(string $bytes): int
    {
        if (! $this->isOpen()) {
            throw new TransportError('Cannot write: transport is not open');
        }
        $stream = $this->stream;
        if (! is_resource($stream)) {
            throw new TransportError('Invalid stream resource');
        }

        $total = 0;
        $len   = strlen($bytes);
        while ($total < $len) {
            $written = @fwrite($stream, substr($bytes, $total));
            if ($written === false) {
                throw new TransportError('Write failed');
            }
            if ($written === 0) {
                // Check for EOF/closed connection
                if (feof($stream)) {
                    throw new TransportError('Connection closed by peer during write');
                }
                // Briefly yield to avoid busy loop
                usleep(1000);

                continue;
            }
            $total += $written;
        }

        return $total;
    }

    public function readExact(int $length, ?float $timeoutSec = null): string
    {
        if ($length < 0) {
            throw new TransportError('readExact length cannot be negative');
        }
        if ($length === 0) {
            return '';
        }
        if (! $this->isOpen()) {
            throw new TransportError('Cannot read: transport is not open');
        }
        $stream = $this->stream;
        if (! is_resource($stream)) {
            throw new TransportError('Invalid stream resource');
        }

        $data     = '';
        $deadline = $timeoutSec !== null ? (microtime(true) + $timeoutSec) : null;

        while (strlen($data) < $length) {
            $remaining = $length - strlen($data);
            if ($remaining <= 0) {
                break;
            }

            // Handle timeout via stream_select when a timeout is provided
            if ($deadline !== null) {
                $now      = microtime(true);
                $timeLeft = $deadline - $now;
                if ($timeLeft <= 0) {
                    throw new Timeout('Read timed out');
                }

                $sec  = (int) floor($timeLeft);
                $usec = (int) floor(($timeLeft - $sec) * 1_000_000);

                $r = [$stream];
                $w = null;
                $e = null;
                $n = @stream_select($r, $w, $e, $sec, $usec);
                if ($n === false) {
                    throw new TransportError('stream_select failed');
                }
                if ($n === 0) {
                    throw new Timeout('Read timed out');
                }
            }

            $toRead = max(1, $remaining);
            $chunk  = @fread($stream, $toRead);
            if ($chunk === false) {
                throw new TransportError('Read failed');
            }
            if ($chunk === '') {
                if (feof($stream)) {
                    throw new TransportError('Connection closed by peer during read');
                }
                // No data but not EOF: brief wait to avoid spin
                usleep(1000);

                continue;
            }

            $data .= $chunk;
        }

        return $data;
    }

    public function close(): void
    {
        if (is_resource($this->stream)) {
            @fclose($this->stream);
        }
        $this->stream     = null;
        $this->context    = null;
        $this->tlsEnabled = false;
    }

    public function isOpen(): bool
    {
        return is_resource($this->stream);
    }

    /**
     * @param  array<string, mixed>|null  $tlsOptions
     */
    public function enableTls(?array $tlsOptions = null): void
    {
        $this->assertTlsAvailable();

        if (! $this->isOpen()) {
            throw new TransportError('Cannot enable TLS: transport is not open');
        }
        if ($this->tlsEnabled) {
            return; // already enabled
        }

        $stream = $this->stream;
        if (! is_resource($stream)) {
            throw new TransportError('Invalid stream resource');
        }

        $this->negotiateTls($stream, $tlsOptions);
    }
}
