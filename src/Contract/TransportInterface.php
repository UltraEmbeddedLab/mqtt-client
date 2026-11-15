<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Contract;

use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;

/**
 * TransportInterface defines the low-level I/O contract for MQTT communication.
 * Different implementations may use stream sockets, ext-sockets, TLS, or WebSockets.
 */
interface TransportInterface
{
    /**
     * Open a connection to the MQTT broker.
     *
     * @throws TransportError if the connection fails
     */
    public function open(string $host, int $port, float $timeoutSec = 5.0): void;

    /**
     * @return int Number of bytes written
     *
     * @throws TransportError on failure
     */
    public function write(string $bytes): int;

    /**
     * Read exactly $length bytes from the transport.
     *
     * The client relies on three properties, which every shipped transport has:
     * - Atomic: on Timeout nothing is consumed. Bytes that arrived before the deadline are
     *   kept and returned by a later call, so a timeout mid-packet loses nothing.
     * - A $timeoutSec of 0.0 is a poll: look at what is already available, then give up.
     *   It must not fail before looking.
     * - null blocks until the bytes arrive.
     *
     * @throws Timeout if the read times out
     * @throws TransportError if the connection is closed or invalid
     */
    public function readExact(int $length, ?float $timeoutSec = null): string;

    /**
     * Close the connection if open.
     */
    public function close(): void;

    /**
     * Returns true if the connection is open and ready.
     */
    public function isOpen(): bool;

    /**
     * Upgrade the transport to TLS if supported.
     *
     * @param  array<string, mixed>|null  $tlsOptions
     *
     * @throws TransportError if TLS negotiation fails
     */
    public function enableTls(?array $tlsOptions = null): void;
}
