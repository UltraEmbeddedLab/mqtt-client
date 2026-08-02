<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Transport;

use ScienceStories\Mqtt\Client\TlsOptions;
use ScienceStories\Mqtt\Exception\TransportError;

use function array_key_exists;
use function extension_loaded;
use function function_exists;
use function implode;
use function is_array;
use function is_int;
use function is_resource;
use function stream_context_create;
use function stream_context_get_options;
use function stream_context_set_option;
use function stream_socket_enable_crypto;

/**
 * The TLS half of a stream-based transport.
 *
 * TcpTransport and WsTransport both wrap a PHP stream and both upgrade it the same way, so
 * this was ~70 lines duplicated verbatim between them. Worse than the repetition: fixes
 * landed in one copy and not the other — the CHANGELOG records a `verify_peer` hardening
 * applied only to TcpTransport, and the TLS 1.0/1.1 default had to be corrected twice.
 *
 * The using class owns the stream and supplies it; everything else lives here.
 */
trait NegotiatesTls
{
    /** @var resource|null */
    private $context;

    private bool $tlsEnabled = false;

    /**
     * Upgrade an open stream to TLS.
     *
     * @param  resource  $stream
     * @param  array<string, mixed>|null  $tlsOptions  Stream-context options, either nested
     *                                                 under an 'ssl' key or flat
     * @param  string  $errorPrefix  Prepended to every thrown message, so each transport
     *                               keeps its own wording
     *
     * @throws TransportError if OpenSSL is unavailable or the handshake fails
     */
    private function negotiateTls($stream, ?array $tlsOptions, string $errorPrefix = ''): void
    {
        $this->applyTlsContextOptions($tlsOptions);
        $this->applyTlsContextDefaults();

        // STREAM_CRYPTO_METHOD_TLS_CLIENT would also permit TLS 1.0/1.1 (RFC 8996: MUST
        // NOT), so default to 1.2+1.3 unless the caller widened it via
        // TlsOptions::withCryptoMethod().
        $result = @stream_socket_enable_crypto($stream, true, $this->resolveCryptoMethod());

        if ($result === true) {
            $this->tlsEnabled = true;

            return;
        }

        $detail = self::tlsErrorDetail();
        // Leave no half-open socket behind: isOpen() would otherwise keep reporting true,
        // which suppresses auto-reconnect and hides the failure.
        $this->close();

        throw new TransportError($errorPrefix.'TLS negotiation failed'.($detail === '' ? '' : ": $detail"));
    }

    /**
     * Guard clauses shared by both transports' enableTls().
     *
     * @throws TransportError if TLS cannot be attempted at all
     */
    private function assertTlsAvailable(string $errorPrefix = ''): void
    {
        if (! extension_loaded('openssl')) {
            // ext-openssl is only suggested, not required, so plain TCP works without it.
            // Fail with something actionable rather than a cryptic crypto error.
            throw new TransportError($errorPrefix.'Cannot enable TLS: ext-openssl is not loaded');
        }
    }

    /**
     * Copy caller-supplied options onto the stream context.
     *
     * Accepts both the nested `['ssl' => [...]]` shape and a flat array of ssl options.
     *
     * @param  array<string, mixed>|null  $tlsOptions
     */
    private function applyTlsContextOptions(?array $tlsOptions): void
    {
        if ($tlsOptions === null || $tlsOptions === []) {
            return;
        }

        if (! is_resource($this->context)) {
            $this->context = stream_context_create([]);
        }

        foreach ($tlsOptions as $wrapper => $opts) {
            if ($wrapper !== 'ssl' || ! is_array($opts)) {
                // Flat array: treat the whole thing as ssl options and stop, so a second
                // key cannot re-apply the outer array under a bogus name.
                $this->setSslOptions($tlsOptions);

                return;
            }
            $this->setSslOptions($opts);
        }
    }

    /**
     * @param  array<array-key, mixed>  $opts
     */
    private function setSslOptions(array $opts): void
    {
        foreach ($opts as $key => $value) {
            @stream_context_set_option($this->context, 'ssl', (string) $key, $value);
        }
    }

    /**
     * Certificate verification stays on unless the caller explicitly turned it off.
     */
    private function applyTlsContextDefaults(): void
    {
        if (! is_resource($this->context)) {
            return;
        }

        $ssl = $this->sslContextOptions();

        foreach (['SNI_enabled', 'verify_peer', 'verify_peer_name'] as $key) {
            if (! array_key_exists($key, $ssl)) {
                @stream_context_set_option($this->context, 'ssl', $key, true);
            }
        }
    }

    private function resolveCryptoMethod(): int
    {
        $configured = $this->sslContextOptions()['crypto_method'] ?? null;

        return is_int($configured) ? $configured : TlsOptions::DEFAULT_CRYPTO_METHOD;
    }

    /**
     * @return array<array-key, mixed>
     */
    private function sslContextOptions(): array
    {
        if (! is_resource($this->context)) {
            return [];
        }

        $opts = stream_context_get_options($this->context);

        return is_array($opts['ssl'] ?? null) ? $opts['ssl'] : [];
    }

    /**
     * Collect whatever OpenSSL and PHP recorded about a failed handshake.
     *
     * Without this the caller cannot tell "certificate verify failed" from "unknown ca"
     * from "wrong version number" (i.e. connected to the plaintext port).
     */
    private static function tlsErrorDetail(): string
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
