<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\V5;

use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Util\Bytes;

use function ord;
use function strlen;
use function substr;
use function unpack;

/**
 * Decodes an MQTT 5 property block (§2.2.2) into named values.
 *
 * It knows every property the specification defines (§2.2.2.2), whatever packet carries
 * it. Each packet type used to have its own parser for the properties it usually sees, and
 * each stopped at the first identifier outside that list, dropping everything after it — a
 * Subscription Identifier in a PUBLISH cost the message every User Property that followed.
 *
 * An identifier the specification does not define cannot be skipped, because its length
 * is unknown; decoding stops there and keeps what came before. A value cut short by the
 * end of the block stops decoding the same way; a string or binary value cut short raises
 * ProtocolError, as Bytes::decodeString() always has.
 *
 * User Properties are returned as a map, so a name sent twice keeps its last value
 * (ordered pairs are a 3.0 change). Subscription Identifiers, of which a PUBLISH may carry
 * several (MQTT-3.3.4-3), are returned as a list under `subscription_identifiers`.
 *
 * @internal
 */
final class PropertyDecoder
{
    private const int BYTE = 1;

    private const int TWO_BYTE_INTEGER = 2;

    private const int FOUR_BYTE_INTEGER = 4;

    private const int VARIABLE_BYTE_INTEGER = 5;

    private const int UTF8_STRING = 6;

    private const int BINARY_DATA = 7;

    private const int UTF8_STRING_PAIR = 8;

    /** @var array<int, array{0: string, 1: int}> identifier => [name, type], MQTT 5 §2.2.2.2 */
    private const array PROPERTIES = [
        0x01 => ['payload_format_indicator', self::BYTE],
        0x02 => ['message_expiry_interval', self::FOUR_BYTE_INTEGER],
        0x03 => ['content_type', self::UTF8_STRING],
        0x08 => ['response_topic', self::UTF8_STRING],
        0x09 => ['correlation_data', self::BINARY_DATA],
        0x0B => ['subscription_identifiers', self::VARIABLE_BYTE_INTEGER],
        0x11 => ['session_expiry_interval', self::FOUR_BYTE_INTEGER],
        0x12 => ['assigned_client_identifier', self::UTF8_STRING],
        0x13 => ['server_keep_alive', self::TWO_BYTE_INTEGER],
        0x15 => ['authentication_method', self::UTF8_STRING],
        0x16 => ['authentication_data', self::BINARY_DATA],
        0x17 => ['request_problem_information', self::BYTE],
        0x18 => ['will_delay_interval', self::FOUR_BYTE_INTEGER],
        0x19 => ['request_response_information', self::BYTE],
        0x1A => ['response_information', self::UTF8_STRING],
        0x1C => ['server_reference', self::UTF8_STRING],
        0x1F => ['reason_string', self::UTF8_STRING],
        0x21 => ['receive_maximum', self::TWO_BYTE_INTEGER],
        0x22 => ['topic_alias_maximum', self::TWO_BYTE_INTEGER],
        0x23 => ['topic_alias', self::TWO_BYTE_INTEGER],
        0x24 => ['maximum_qos', self::BYTE],
        0x25 => ['retain_available', self::BYTE],
        0x26 => ['user_properties', self::UTF8_STRING_PAIR],
        0x27 => ['maximum_packet_size', self::FOUR_BYTE_INTEGER],
        0x28 => ['wildcard_subscription_available', self::BYTE],
        0x29 => ['subscription_identifier_available', self::BYTE],
        0x2A => ['shared_subscription_available', self::BYTE],
    ];

    /**
     * @param  string  $block  the properties themselves, without the length prefix
     * @return array<string, mixed>
     *
     * @throws ProtocolError for a string, binary value or variable byte integer cut short
     */
    public static function decode(string $block): array
    {
        $out                     = [];
        $userProperties          = [];
        $subscriptionIdentifiers = [];
        $i                       = 0;
        $n                       = strlen($block);
        while ($i < $n) {
            $property = self::PROPERTIES[ord($block[$i++])] ?? null;
            if ($property === null) {
                break;
            }
            [$name, $type] = $property;

            if ($type === self::BYTE) {
                if ($i >= $n) {
                    break;
                }
                $out[$name] = ord($block[$i++]);
            } elseif ($type === self::TWO_BYTE_INTEGER) {
                if ($i + 2 > $n) {
                    break;
                }
                $out[$name] = (ord($block[$i]) << 8) | ord($block[$i + 1]);
                $i += 2;
            } elseif ($type === self::FOUR_BYTE_INTEGER) {
                if ($i + 4 > $n) {
                    break;
                }
                $out[$name] = unpack('N', substr($block, $i, 4))[1] ?? 0;
                $i += 4;
            } elseif ($type === self::VARIABLE_BYTE_INTEGER) {
                $consumed                  = 0;
                $subscriptionIdentifiers[] = Bytes::decodeVarInt($block, $consumed, $i);
                $i += $consumed;
            } elseif ($type === self::UTF8_STRING_PAIR) {
                $key                  = Bytes::decodeString($block, $i);
                $userProperties[$key] = Bytes::decodeString($block, $i);
            } else {
                $out[$name] = Bytes::decodeString($block, $i);
            }
        }

        if ($userProperties !== []) {
            $out['user_properties'] = $userProperties;
        }
        if ($subscriptionIdentifiers !== []) {
            $out['subscription_identifiers'] = $subscriptionIdentifiers;
        }

        return $out;
    }
}
