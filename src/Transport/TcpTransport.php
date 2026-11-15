<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Transport;

use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Util\Clock;

use function fclose;
use function feof;
use function floor;
use function fread;
use function fwrite;
use function is_int;
use function is_resource;
use function is_string;
use function max;
use function sprintf;
use function stream_context_create;
use function stream_set_blocking;
use function stream_set_chunk_size;
use function stream_set_timeout;
use function stream_socket_client;
use function strlen;
use function substr;
use function usleep;

final class TcpTransport implements TransportInterface
{
    use AwaitsReadable;

    /** Owns $context and $tlsEnabled, and the whole TLS upgrade. */
    use NegotiatesTls;

    /**
     * Bytes requested from the socket for a small read.
     *
     * Large enough that a burst of small packets arrives in one system call, small enough
     * that an idle connection costs little memory.
     */
    private const int READ_AHEAD = 65_536;

    /** @var resource|null */
    private $stream;

    /** Bytes received from the socket and not yet returned by readExact(). */
    private string $buffer = '';

    /** Position of the first unread byte in $buffer. */
    private int $offset = 0;

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
        // PHP refills its own read buffer 8 KiB at a time by default, so a 1 MiB packet
        // cost 128 recv() calls. Reading in READ_AHEAD-sized chunks cuts that eightfold.
        stream_set_chunk_size($stream, self::READ_AHEAD);

        $this->stream     = $stream;
        $this->tlsEnabled = false;
        $this->buffer     = '';
        $this->offset     = 0;
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

    /**
     * Read exactly $length bytes, or none.
     *
     * Atomic with respect to timeouts: bytes that arrive before the deadline are kept in
     * the receive buffer, not discarded, so a Timeout halfway through a packet leaves the
     * stream exactly where it was and the next call resumes from the same byte. Without
     * that, one slow segment desynchronised the MQTT framing for the rest of the connection.
     *
     * A timeout of 0.0 polls once — it returns data that is already waiting — and null
     * blocks until the bytes arrive.
     */
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

        if (strlen($this->buffer) - $this->offset < $length) {
            $this->fill($length, $timeoutSec === null ? null : Clock::now() + $timeoutSec);
        }

        // The common large-packet case: the buffer holds exactly what was asked for, so it
        // is handed over without copying.
        if ($this->offset === 0 && strlen($this->buffer) === $length) {
            $out          = $this->buffer;
            $this->buffer = '';

            return $out;
        }

        $out = substr($this->buffer, $this->offset, $length);
        $this->offset += $length;
        if ($this->offset === strlen($this->buffer)) {
            $this->buffer = '';
            $this->offset = 0;
        }

        return $out;
    }

    public function close(): void
    {
        if (is_resource($this->stream)) {
            @fclose($this->stream);
        }
        $this->stream     = null;
        $this->context    = null;
        $this->tlsEnabled = false;
        $this->buffer     = '';
        $this->offset     = 0;
    }

    /**
     * Grow the receive buffer until it holds at least $length unread bytes.
     *
     * Small reads ask the socket for up to READ_AHEAD bytes, so the many tiny reads of a
     * packet header are served from memory instead of one select()+fread() each. Large
     * reads ask for exactly what is missing, so a big packet body is not over-read and can
     * be returned without a copy.
     *
     * @throws Timeout leaving every byte already received in the buffer
     */
    private function fill(int $length, ?float $deadline): void
    {
        $stream = $this->stream;
        if (! is_resource($stream)) {
            throw new TransportError('Invalid stream resource');
        }

        // Drop the consumed prefix once, here, rather than on every read.
        if ($this->offset > 0) {
            $this->buffer = substr($this->buffer, $this->offset);
            $this->offset = 0;
        }

        while (strlen($this->buffer) < $length) {
            $this->awaitReadable($stream, $deadline);

            $missing = $length - strlen($this->buffer);
            $chunk   = @fread($stream, max($missing, self::READ_AHEAD));
            if ($chunk === false) {
                throw new TransportError('Read failed');
            }
            if ($chunk === '') {
                if (feof($stream)) {
                    throw new TransportError('Connection closed by peer during read');
                }
                // Readable but nothing decoded yet — e.g. a TLS record still incomplete.
                // Yield briefly rather than spin on select().
                usleep(1000);

                continue;
            }

            $this->buffer .= $chunk;
        }
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
        // Bytes already read ahead arrived in plaintext; once the stream is encrypted they
        // could never be interpreted correctly. MQTT has no STARTTLS, so this only fires on
        // misuse — but silently mixing the two would be undiagnosable.
        if (strlen($this->buffer) > $this->offset) {
            throw new TransportError('Cannot enable TLS: plaintext bytes are already buffered');
        }

        $this->negotiateTls($stream, $tlsOptions);
    }
}
