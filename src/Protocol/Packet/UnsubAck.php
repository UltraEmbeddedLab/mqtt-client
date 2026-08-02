<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\Packet;

use ScienceStories\Mqtt\Protocol\MqttVersion;

use function is_array;
use function is_string;

/**
 * UNSUBACK packet model for MQTT 3.1.1 and 5.0.
 *
 * This class represents the broker's response to an UNSUBSCRIBE packet.
 *
 * MQTT 3.1.1:
 * - Contains only the Packet Identifier
 * - No reason codes (acknowledgment is implicit success)
 * - No properties field
 *
 * MQTT 5.0:
 * - Contains Packet Identifier
 * - Reason codes (1 byte per unsubscribed topic filter):
 *   * 0x00 (0): Success - subscription deleted
 *   * 0x11 (17): No subscription existed
 *   * 0x80 (128): Unspecified error
 *   * 0x83 (131): Implementation specific error
 *   * 0x87 (135): Not authorized
 *   * 0x8F (143): Topic Filter invalid
 *   * 0x91 (145): Packet Identifier in use
 * - Properties field (reason_string, user_properties)
 *
 * The reason codes indicate whether each unsubscription was successful.
 * Unlike SUBACK, UNSUBACK reason codes are all success/failure indicators
 * (no granted QoS levels since unsubscribing doesn't involve QoS negotiation).
 */
final class UnsubAck
{
    /**
     * MQTT 5.0 reason code descriptions for UNSUBACK.
     *
     * @var array<int, string>
     */
    private const array V5_REASON_CODES = [
        0x00 => 'Success',
        0x11 => 'No subscription existed',
        0x80 => 'Unspecified error',
        0x83 => 'Implementation specific error',
        0x87 => 'Not authorized',
        0x8F => 'Topic Filter invalid',
        0x91 => 'Packet Identifier in use',
    ];

    /**
     * @param  int  $packetId  Packet identifier matching the UNSUBSCRIBE request
     * @param  list<int>|null  $reasonCodes  MQTT 5.0 reason codes (1 per topic filter). Null for MQTT 3.1.1.
     *                                        - 0x00: Success
     *                                        - 0x11: No subscription existed
     *                                        - 0x80+: Various error codes
     * @param  array<string, mixed>|null  $properties  MQTT 5.0 UNSUBACK properties. Possible keys:
     *                                                  - reason_string: string (human-readable reason)
     *                                                  - user_properties: array<string, string> (custom metadata)
     */
    public function __construct(
        public int $packetId,
        public ?array $reasonCodes = null,
        public ?array $properties = null,
    ) {
    }

    /**
     * Check if all unsubscriptions were successful.
     * For MQTT 3.1.1, always returns true (no reason codes, implicit success).
     * For MQTT 5.0, returns true if all reason codes are 0x00 or 0x11 (success/no subscription).
     */
    public function isSuccess(): bool
    {
        // MQTT 3.1.1: no reason codes, always success
        if ($this->reasonCodes === null) {
            return true;
        }
        return array_all($this->reasonCodes, fn (int $code): bool => $code === 0x00 || $code === 0x11);
    }

    /**
     * Check if any unsubscriptions failed.
     * For MQTT 3.1.1, always returns false.
     * For MQTT 5.0, returns true if any reason code indicates failure (>= 0x80).
     */
    public function hasFailures(): bool
    {
        if ($this->reasonCodes === null) {
            return false;
        }
        return array_any($this->reasonCodes, fn (int $code): bool => $code >= 0x80);
    }

    /**
     * Get a human-readable description for a specific reason code.
     *
     * @param  int  $code  Reason code to describe
     * @param  MqttVersion|string  $version  MQTT version (for future compatibility)
     * @return string Human-readable description
     */
    public function getReasonDescription(int $code, MqttVersion|string $version = MqttVersion::V5_0): string
    {
        // MQTT 3.1.1 has no reason codes
        if ($version === MqttVersion::V3_1_1 || $version === '3.1.1') {
            return 'Success (implicit)';
        }

        return self::V5_REASON_CODES[$code] ?? 'Unknown';
    }

    /**
     * Get all reason code descriptions as a list.
     * For MQTT 3.1.1, returns an empty array.
     * For MQTT 5.0, returns descriptions for all reason codes.
     *
     * @param  MqttVersion|string  $version  MQTT version
     * @return list<string> List of human-readable descriptions
     */
    public function getAllReasonDescriptions(MqttVersion|string $version = MqttVersion::V5_0): array
    {
        if ($this->reasonCodes === null) {
            return [];
        }

        $descriptions = [];
        foreach ($this->reasonCodes as $code) {
            $descriptions[] = $this->getReasonDescription($code, $version);
        }

        return $descriptions;
    }

    /**
     * Get indices of failed unsubscriptions (reason code >= 0x80).
     * For MQTT 3.1.1, always returns an empty array.
     * For MQTT 5.0, returns array indices where unsubscription failed.
     *
     * @return list<int> List of indices (0-based) that failed
     */
    public function getFailedIndices(): array
    {
        if ($this->reasonCodes === null) {
            return [];
        }

        $failed = [];
        foreach ($this->reasonCodes as $idx => $code) {
            if ($code >= 0x80) {
                $failed[] = $idx;
            }
        }

        return $failed;
    }

    /**
     * Get a property value from MQTT 5.0 UNSUBACK properties.
     *
     * @param  string  $key  Property name
     * @param  mixed  $default  Default value if property doesn't exist
     * @return mixed Property value or default
     */
    public function getProperty(string $key, mixed $default = null): mixed
    {
        return $this->properties[$key] ?? $default;
    }

    /**
     * Check if a property exists in MQTT 5.0 UNSUBACK properties.
     */
    public function hasProperty(string $key): bool
    {
        return isset($this->properties[$key]);
    }

    /**
     * Get the reason string (MQTT 5.0).
     * Provides additional human-readable information about the unsubscription result.
     */
    public function getReasonString(): ?string
    {
        $val = $this->getProperty('reason_string');

        return is_string($val) ? $val : null;
    }

    /**
     * Get user properties (MQTT 5.0).
     *
     * @return array<string, string>
     */
    public function getUserProperties(): array
    {
        $val = $this->getProperty('user_properties');

        if (! is_array($val)) {
            return [];
        }

        // Ensure all keys and values are strings for type safety
        return array_filter($val, fn ($value, $key): bool => is_string($key) && is_string($value), ARRAY_FILTER_USE_BOTH);
    }
}
