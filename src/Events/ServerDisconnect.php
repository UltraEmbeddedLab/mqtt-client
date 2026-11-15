<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Events;

use ScienceStories\Mqtt\Protocol\Packet\Disconnect;

/**
 * PSR-14 event dispatched when the connection to the broker ends unexpectedly.
 *
 * Usually because the broker sent a DISCONNECT. It is also dispatched when the client ends
 * the connection itself — a PINGRESP that never came (0x8D), or a broker that broke the
 * protocol, such as an invalid topic alias (0x94, 0x82). Then `$disconnect` is a packet the
 * client built, and its `reason_string` property begins "Closed by the client", so a
 * listener can tell which side gave up. In that case the transport is already closed when
 * listeners run: anything they publish goes to the offline queue, if one is configured.
 *
 * MQTT 5.0 allows brokers to send DISCONNECT packets to clients with:
 * - Reason codes explaining why the connection is being closed
 * - Properties providing additional context (reason string, server reference)
 *
 * Common server-initiated disconnect reasons:
 * - 0x8B (139): Server shutting down
 * - 0x8D (141): Keep Alive timeout
 * - 0x8E (142): Session taken over (another client connected with same ID)
 * - 0x93 (147): Receive Maximum exceeded
 * - 0x95 (149): Packet too large
 * - 0x96 (150): Message rate too high
 * - 0x97 (151): Quota exceeded
 *
 * Usage in event listener:
 * ```php
 * $dispatcher->addListener(ServerDisconnect::class, function (ServerDisconnect $event) {
 *     $disconnect = $event->disconnect;
 *
 *     If ($disconnect->isError()) {
 *         echo "Server disconnected due to error: " . $disconnect->getReasonDescription();
 *
 *         // Check for server reference (alternative server)
 *         if ($ref = $disconnect->getServerReference()) {
 *             echo "Try connecting to: $ref";
 *         }
 *     }
 * });
 * ```
 */
final readonly class ServerDisconnect
{
    /**
     * @param Disconnect $disconnect The decoded DISCONNECT packet
     * @param bool $willReconnect Whether the client will attempt to reconnect
     */
    public function __construct(
        public Disconnect $disconnect,
        public bool $willReconnect = false,
    ) {
    }

    /**
     * Get the reason code from the DISCONNECT packet.
     */
    public function getReasonCode(): int
    {
        return $this->disconnect->reasonCode;
    }

    /**
     * Check if the disconnect was an error (reason code >= 0x80).
     */
    public function isError(): bool
    {
        return $this->disconnect->isError();
    }

    /**
     * Check if the disconnect was normal (reason code 0x00 or 0x04).
     */
    public function isNormal(): bool
    {
        return $this->disconnect->isNormal();
    }

    /**
     * Get the human-readable reason description.
     */
    public function getReasonDescription(): string
    {
        return $this->disconnect->getReasonDescription();
    }

    /**
     * Get the reason string property (if present).
     */
    public function getReasonString(): ?string
    {
        return $this->disconnect->getReasonString();
    }

    /**
     * Get the server reference (alternative server) if present.
     */
    public function getServerReference(): ?string
    {
        return $this->disconnect->getServerReference();
    }
}
