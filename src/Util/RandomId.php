<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Util;

use InvalidArgumentException;
use Random\RandomException;

use function random_int;
use function strlen;

/**
 * RandomId generates unique MQTT identifiers.
 * - Client IDs: random strings, alphanumeric (safe for brokers).
 * - Packet IDs: sequential numbers in [1..65535], with wrap-around.
 */
final class RandomId
{
    /** Characters every broker accepts in a Client Identifier (MQTT-3.1.3-5). */
    private const string CLIENT_ID_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

    private static int $packetIdCounter = 1;

    /**
     * Generate a random client ID of exactly $length alphanumeric characters.
     *
     * Each character is drawn uniformly from [A-Za-z0-9]. The previous implementation
     * base64-encoded random bytes, stripped `+`, `/` and `=`, then truncated — so it could
     * return fewer characters than asked for (about 0.15% of 6-character IDs).
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

        $last = strlen(self::CLIENT_ID_ALPHABET) - 1;
        $id   = '';
        for ($i = 0; $i < $length; $i++) {
            $id .= self::CLIENT_ID_ALPHABET[random_int(0, $last)];
        }

        return $id;
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
