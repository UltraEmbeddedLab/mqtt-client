<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Util;

use ScienceStories\Mqtt\Exception\ProtocolError;

use function chr;
use function is_int;
use function ord;
use function strlen;

/**
 * Utility functions for encoding/decoding MQTT binary formats.
 */
final class Bytes
{
    /**
     * Encode a variable length integer (MQTT Remaining Length).
     *
     * @throws ProtocolError if value is out of the allowed range
     */
    public static function encodeVarInt(int $value): string
    {
        if ($value < 0 || $value > 268_435_455) { // max 0x0FFFFFFF
            throw new ProtocolError("VarInt out of range: $value");
        }

        $out = '';
        do {
            $byte  = $value % 128;
            $value = intdiv($value, 128);
            if ($value > 0) {
                $byte |= 0x80;
            }
            $out .= chr($byte);
        } while ($value > 0);

        return $out;
    }

    /**
     * Decode a variable length integer (MQTT Remaining Length).
     *
     * @param  int  $consumed  Number of bytes consumed (output)
     *
     * @throws ProtocolError if malformed
     */
    public static function decodeVarInt(string $data, int &$consumed = 0): int
    {
        $multiplier = 1;
        $value      = 0;
        $consumed   = 0;

        for ($i = 0; $i < 4; $i++) {
            if (! isset($data[$i])) {
                throw new ProtocolError('Malformed VarInt: data too short');
            }

            $byte = ord($data[$i]);
            $consumed++;

            $value += ($byte & 0x7F) * $multiplier;

            if (($byte & 0x80) === 0) {
                return $value;
            }

            $multiplier *= 128;
        }

        throw new ProtocolError('Malformed VarInt: exceeds 4 bytes');
    }

    /**
     * Encode a two-byte integer field, rejecting values that would silently truncate.
     *
     * `pack('n', ...)` wraps modulo 65536, which turns an out-of-range Keep Alive or
     * Packet Identifier into a wire-legal but wrong value. Every two-byte field should
     * go through here.
     *
     * @param  string  $field  Field name used in the error message
     *
     * @throws ProtocolError if the value cannot be represented
     */
    public static function encodeUint16(int $value, string $field = 'Value'): string
    {
        if ($value < 0 || $value > 65535) {
            throw new ProtocolError("$field must be between 0 and 65535, got $value");
        }

        return pack('n', $value);
    }

    /**
     * Encode an MQTT UTF-8 string (2-byte length prefix and UTF-8 bytes).
     *
     * @throws ProtocolError if string too long
     */
    public static function encodeString(string $value): string
    {
        $len = strlen($value);
        if ($len > 65535) {
            throw new ProtocolError("String too long: $len bytes");
        }

        return pack('n', $len).$value;
    }

    /**
     * Decode an MQTT UTF-8 string (2-byte length prefix).
     *
     * @param  int  $offset  Reference to offset in the data
     *
     * @throws ProtocolError if malformed
     */
    public static function decodeString(string $data, int &$offset = 0): string
    {
        if ($offset + 2 > strlen($data)) {
            throw new ProtocolError('Malformed string: missing length');
        }

        $lenArr = @unpack('n', substr($data, $offset, 2));
        if ($lenArr === false || ! isset($lenArr[1]) || ! is_int($lenArr[1])) {
            throw new ProtocolError('Malformed string: invalid length prefix');
        }
        $len = $lenArr[1];
        $offset += 2;

        if ($offset + $len > strlen($data)) {
            throw new ProtocolError('Malformed string: not enough bytes');
        }

        $str = substr($data, $offset, $len);
        $offset += $len;

        return $str;
    }
}
