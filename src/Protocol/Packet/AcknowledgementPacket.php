<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\Packet;

use function array_filter;
use function is_array;
use function is_string;

/**
 * Shared model for the four QoS acknowledgement packets: PUBACK, PUBREC, PUBREL, PUBCOMP.
 *
 * All four have the same shape on the wire — a Packet Identifier, an optional MQTT 5
 * Reason Code, and optional Properties — and therefore the same accessors. They were
 * previously four byte-identical 150-line classes; the differences were entirely in the
 * documentation, which now lives on each subclass where it belongs.
 *
 * MQTT 3.1.1 structure:
 * - Fixed Header: packet type, flags, Remaining Length (2)
 * - Variable Header: Packet Identifier (2 bytes)
 *
 * MQTT 5.0 structure:
 * - Fixed Header: packet type, flags, Remaining Length (2+)
 * - Variable Header: Packet Identifier (2 bytes) + Reason Code (1 byte) + Properties
 *
 * On MQTT 3.1.1 there are no reason codes, so `$reasonCode` is always 0 and
 * {@see isSuccess()} always returns true.
 */
abstract class AcknowledgementPacket
{
    /**
     * MQTT 5.0 reason code descriptions.
     *
     * The superset across the four packet types. PUBREL and PUBCOMP only ever carry 0x00
     * or 0x92 in practice; the extra entries are inert for them.
     *
     * @var array<int, string>
     */
    private const array V5_REASON_CODES = [
        0x00 => 'Success',
        0x10 => 'No matching subscribers',
        0x80 => 'Unspecified error',
        0x83 => 'Implementation specific error',
        0x87 => 'Not authorized',
        0x90 => 'Topic Name invalid',
        0x91 => 'Packet Identifier in use',
        0x92 => 'Packet Identifier not found',
        0x97 => 'Quota exceeded',
        0x99 => 'Payload format invalid',
    ];

    /**
     * @param  int  $packetId  Packet identifier matching the packet being acknowledged (1-65535)
     * @param  int  $reasonCode  MQTT 5.0 reason code (0 = success, 0x80+ = error). Always 0 for MQTT 3.1.1.
     * @param  array<string, mixed>|null  $properties  MQTT 5.0 properties. Possible keys:
     *                                                  - reason_string: string (human-readable explanation)
     *                                                  - user_properties: array<string, string> (custom metadata)
     */
    public function __construct(
        public int $packetId,
        public int $reasonCode = 0,
        public ?array $properties = null,
    ) {
    }

    /**
     * Whether the acknowledgement indicates success.
     *
     * True for MQTT 3.1.1, which has no reason codes. For MQTT 5.0, true for 0x00 (Success)
     * and 0x10 (No matching subscribers) — the latter is below 0x80 and is explicitly not
     * an error: the message was accepted, nobody was subscribed.
     */
    public function isSuccess(): bool
    {
        return $this->reasonCode === 0x00 || $this->reasonCode === 0x10;
    }

    /**
     * Whether the acknowledgement indicates an error.
     *
     * False for MQTT 3.1.1. For MQTT 5.0, true for reason codes 0x80 and above.
     */
    public function isError(): bool
    {
        return $this->reasonCode >= 0x80;
    }

    /**
     * Human-readable description for the reason code (MQTT 5.0).
     *
     * @return string Description, or "Unknown" if the code is not recognised
     */
    public function getReasonDescription(): string
    {
        return self::V5_REASON_CODES[$this->reasonCode] ?? 'Unknown';
    }

    /**
     * Read a property from the MQTT 5.0 property block.
     *
     * @param  string  $key  Property name
     * @param  mixed  $default  Returned when the property is absent
     */
    public function getProperty(string $key, mixed $default = null): mixed
    {
        return $this->properties[$key] ?? $default;
    }

    /**
     * Whether a property is present in the MQTT 5.0 property block.
     */
    public function hasProperty(string $key): bool
    {
        return isset($this->properties[$key]);
    }

    /**
     * The Reason String property (MQTT 5.0), if the broker sent one.
     */
    public function getReasonString(): ?string
    {
        $val = $this->getProperty('reason_string');

        return is_string($val) ? $val : null;
    }

    /**
     * The User Properties (MQTT 5.0), filtered to string keys and values.
     *
     * @return array<string, string>
     */
    public function getUserProperties(): array
    {
        $val = $this->getProperty('user_properties');

        if (! is_array($val)) {
            return [];
        }

        return array_filter($val, fn ($value, $key): bool => is_string($key) && is_string($value), ARRAY_FILTER_USE_BOTH);
    }
}
