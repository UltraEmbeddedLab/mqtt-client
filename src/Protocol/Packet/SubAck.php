<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\Packet;

use ScienceStories\Mqtt\Protocol\MqttVersion;

use function is_array;
use function is_string;

/**
 * SUBACK packet model for MQTT 3.1.1 and 5.0.
 *
 * This class represents the broker's response to a SUBSCRIBE packet, containing:
 * - Packet Identifier: matches the SUBSCRIBE packet's identifier
 * - Return/Reason Codes: one code per topic filter in the SUBSCRIBE request
 * - Properties (MQTT 5.0 only): additional metadata like reason string and user properties
 *
 * MQTT 3.1.1 Return Codes:
 * - 0x00 (0): Success - Maximum QoS 0
 * - 0x01 (1): Success - Maximum QoS 1
 * - 0x02 (2): Success - Maximum QoS 2
 * - 0x80 (128): Failure - Subscription rejected
 *
 * MQTT 5.0 Reason Codes (subset):
 * - 0x00 (0): Granted QoS 0
 * - 0x01 (1): Granted QoS 1
 * - 0x02 (2): Granted QoS 2
 * - 0x11 (17): No subscription existed (used for unsubscribe)
 * - 0x80 (128): Unspecified error
 * - 0x83 (131): Implementation specific error
 * - 0x87 (135): Not authorized
 * - 0x8F (143): Topic Filter invalid
 * - 0x91 (145): Packet Identifier in use
 * - 0x97 (151): Quota exceeded
 * - 0x9E (158): Shared Subscriptions not supported
 * - 0xA1 (161): Subscription Identifiers not supported
 * - 0xA2 (162): Wildcard Subscriptions not supported
 */
final class SubAck
{
    /**
     * MQTT 3.1.1 return code descriptions.
     *
     * @var array<int, string>
     */
    private const array V3_RETURN_CODES = [
        0x00 => 'Success - Maximum QoS 0',
        0x01 => 'Success - Maximum QoS 1',
        0x02 => 'Success - Maximum QoS 2',
        0x80 => 'Failure',
    ];

    /**
     * MQTT 5.0 reason code descriptions.
     *
     * @var array<int, string>
     */
    private const array V5_REASON_CODES = [
        0x00 => 'Granted QoS 0',
        0x01 => 'Granted QoS 1',
        0x02 => 'Granted QoS 2',
        0x11 => 'No subscription existed',
        0x80 => 'Unspecified error',
        0x83 => 'Implementation specific error',
        0x87 => 'Not authorized',
        0x8F => 'Topic Filter invalid',
        0x91 => 'Packet Identifier in use',
        0x97 => 'Quota exceeded',
        0x9E => 'Shared Subscriptions not supported',
        0xA1 => 'Subscription Identifiers not supported',
        0xA2 => 'Wildcard Subscriptions not supported',
    ];

    /**
     * @param  int  $packetId  Packet identifier matching the SUBSCRIBE request
     * @param  list<int>  $returnCodes  List of return/reason codes (one per topic filter)
     * @param  array<string, mixed>|null  $properties  MQTT 5.0 SUBACK properties. Possible keys:
     *                                                  - reason_string: string (human-readable error explanation)
     *                                                  - user_properties: array<string, string> (custom metadata)
     */
    public function __construct(
        public int $packetId,
        public array $returnCodes,
        public ?array $properties = null,
    ) {
    }

    /**
     * Check if all subscriptions were successful (no failures).
     */
    public function isSuccess(): bool
    {
        return array_all($this->returnCodes, fn (int $code): bool => $code < 0x80);
    }

    /**
     * Check if any subscription failed.
     */
    public function hasFailures(): bool
    {
        return ! $this->isSuccess();
    }

    /**
     * Get a human-readable description for a specific return/reason code.
     *
     * @param  int  $code  Return/reason code to describe
     * @param  MqttVersion|string  $version  MQTT version (MqttVersion enum or "3.1.1"/"5.0" string)
     */
    public function getReasonDescription(int $code, MqttVersion|string $version): string
    {
        if (is_string($version)) {
            $version = $version === '5.0' ? MqttVersion::V5_0 : MqttVersion::V3_1_1;
        }

        $codes = $version === MqttVersion::V5_0 ? self::V5_REASON_CODES : self::V3_RETURN_CODES;

        return $codes[$code] ?? 'Unknown';
    }

    /**
     * Get descriptions for all return/reason codes.
     *
     * @param  MqttVersion|string  $version  MQTT version (MqttVersion enum or "3.1.1"/"5.0" string)
     * @return list<string> Human-readable descriptions matching the order of returnCodes
     */
    public function getAllReasonDescriptions(MqttVersion|string $version): array
    {
        $descriptions = [];
        foreach ($this->returnCodes as $code) {
            $descriptions[] = $this->getReasonDescription($code, $version);
        }

        return $descriptions;
    }

    /**
     * Get the granted QoS levels (filters out failures).
     *
     * @return list<int> List of granted QoS levels (0, 1, or 2)
     */
    public function getGrantedQoS(): array
    {
        $granted = [];
        foreach ($this->returnCodes as $code) {
            if ($code >= 0x00 && $code <= 0x02) {
                $granted[] = $code;
            }
        }

        return $granted;
    }

    /**
     * Get the indices of failed subscriptions.
     *
     * @return list<int> List of indices (0-based) where subscription failed
     */
    public function getFailedIndices(): array
    {
        $failed = [];
        foreach ($this->returnCodes as $index => $code) {
            if ($code >= 0x80) {
                $failed[] = $index;
            }
        }

        return $failed;
    }

    /**
     * Get a property value from MQTT 5.0 SUBACK properties.
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
     * Check if a property exists in MQTT 5.0 SUBACK properties.
     */
    public function hasProperty(string $key): bool
    {
        return isset($this->properties[$key]);
    }

    /**
     * Get the reason string (MQTT 5.0).
     * Provides additional human-readable information about the subscription result.
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
