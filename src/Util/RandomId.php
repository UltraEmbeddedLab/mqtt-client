<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Util;

use InvalidArgumentException;
use Random\RandomException;

use function base64_encode;
use function preg_replace;
use function random_bytes;
use function substr;

/**
 * RandomId generates unique MQTT identifiers.
 * - Client IDs: random strings, alphanumeric (safe for brokers).
 * - Packet IDs: sequential numbers in [1..65535], with wrap-around.
 */
final class RandomId
{
    private static int $packetIdCounter = 1;

    /**
     * Generate a random client ID.
     *
     * @param  int  $length  Length of the ID (default: 16)
     *
     * @throws RandomException
     */
    public static function clientId(int $length = 16): string
    {
        if ($length < 1) {
            throw new InvalidArgumentException('Client ID length must be >= 1');
        }
        $bytes = random_bytes($length);

        // Convert bytes to safe alphanumeric
        $base  = base64_encode($bytes);
        $clean = preg_replace('/[^a-zA-Z0-9]/', '', $base);
        $clean ??= '';

        return substr($clean, 0, $length);
    }

    /**
     * Generate the next packet ID (1..65535).
     * Rolls over to 1 after reaching 65535.
     */
    public static function packetId(): int
    {
        $id = self::$packetIdCounter++;
        if (self::$packetIdCounter > 0xFFFF) {
            self::$packetIdCounter = 1;
        }

        return $id;
    }

    /**
     * Reset the packet ID counter (mainly for tests).
     */
    public static function resetPacketId(): void
    {
        self::$packetIdCounter = 1;
    }
}
