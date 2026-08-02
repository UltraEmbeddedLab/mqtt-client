<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Transport;

use ScienceStories\Mqtt\Client\TlsOptions;
use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;

use function array_key_exists;
use function is_array;
use function is_int;
use function is_resource;
use function is_string;
use function sprintf;
use function strlen;

final class TcpTransport implements TransportInterface
{
    /** @var resource|null */
    private $stream;

    /** @var resource|null */
    private $context;

    private bool $tlsEnabled = false;

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
        if (! extension_loaded('openssl')) {
            // ext-openssl is only suggested, not required, so plain TCP works without it.
            // Fail with something actionable rather than a cryptic crypto error.
            throw new TransportError('Cannot enable TLS: ext-openssl is not loaded');
        }
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

        // Apply TLS/SSL context options if provided (e.g., ['ssl' => ['verify_peer' => true, ...]])
        if ($tlsOptions) {
            if (! is_resource($this->context)) {
                $this->context = stream_context_create([]);
            }
            // Support both nested wrapper array and flat options under 'ssl'
            foreach ($tlsOptions as $wrapper => $opts) {
                if ($wrapper !== 'ssl' || ! is_array($opts)) {
                    // If user passed flat array, map it under 'ssl'
                    $wrapper = 'ssl';
                    $opts    = $tlsOptions;
                }
                foreach ($opts as $k => $v) {
                    @stream_context_set_option($this->context, $wrapper, (string) $k, $v);
                }
            }
        }

        // Attach the context to the stream if not already. Some PHP versions
        // allow passing a stream instead of context to set options directly.
        if ($this->context) {
            // Try to set a couple of sane defaults if not explicitly provided
            $opts = stream_context_get_options($this->context);
            $ssl  = is_array($opts['ssl'] ?? null) ? $opts['ssl'] : [];
            if (! array_key_exists('SNI_enabled', $ssl)) {
                @stream_context_set_option($this->context, 'ssl', 'SNI_enabled', true);
            }
            if (! array_key_exists('verify_peer', $ssl)) {
                @stream_context_set_option($this->context, 'ssl', 'verify_peer', true);
            }
            if (! array_key_exists('verify_peer_name', $ssl)) {
                @stream_context_set_option($this->context, 'ssl', 'verify_peer_name', true);
            }
        }

        // Initiate TLS handshake. STREAM_CRYPTO_METHOD_TLS_CLIENT would also permit
        // TLS 1.0/1.1 (RFC 8996: MUST NOT), so default to 1.2+1.3 unless the caller
        // widened it through TlsOptions::withCryptoMethod().
        $method = TlsOptions::DEFAULT_CRYPTO_METHOD;
        if (is_resource($this->context)) {
            $ctxOpts    = stream_context_get_options($this->context);
            $ssl        = is_array($ctxOpts['ssl'] ?? null) ? $ctxOpts['ssl'] : [];
            $configured = $ssl['crypto_method'] ?? null;
            if (is_int($configured)) {
                $method = $configured;
            }
        }

        $result = @stream_socket_enable_crypto($stream, true, $method);
        if ($result !== true) {
            $detail = $this->tlsErrorDetail();
            // Leave no half-open socket behind: isOpen() would otherwise keep reporting
            // true, which suppresses auto-reconnect and hides the failure.
            $this->close();

            throw new TransportError('TLS negotiation failed'.($detail === '' ? '' : ": $detail"));
        }

        $this->tlsEnabled = true;
    }

    /**
     * Collect whatever OpenSSL and PHP recorded about the failed handshake.
     *
     * Without this the caller cannot tell "certificate verify failed" from "unknown ca"
     * from "wrong version number" (i.e. connected to the plaintext port).
     */
    private function tlsErrorDetail(): string
    {
        $parts = [];
        if (function_exists('openssl_error_string')) {
            while (($err = openssl_error_string()) !== false) {
                $parts[] = $err;
            }
        }
        $last = error_get_last();
        if ($last !== null && $last['message'] !== '') {
            $parts[] = $last['message'];
        }

        return implode('; ', $parts);
    }
}
