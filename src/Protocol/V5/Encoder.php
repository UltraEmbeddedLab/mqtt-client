<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\V5;

use InvalidArgumentException;
use LogicException;
use ScienceStories\Mqtt\Client\SubscribeOptions;
use ScienceStories\Mqtt\Client\WillOptions; // reuse DTO
use ScienceStories\Mqtt\Contract\EncoderInterface; // reuse DTO
use ScienceStories\Mqtt\Protocol\Packet\Connect; // codes are identical in v5
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\Packet\Publish;
use ScienceStories\Mqtt\Util\Bytes;

use function array_key_exists;
use function chr;
use function count;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_object;
use function is_resource;
use function is_string;
use function sprintf;
use function strlen;

/**
 * Encoder for MQTT 5.0 packets.
 *
 * Encodes packets according to the MQTT 5.0 specification (protocol level 5).
 * - Supports properties field for enhanced features
 * - Clean Start flag instead of Clean Session
 * - Extended authentication, topic aliases, user properties, and more
 */
final class Encoder implements EncoderInterface
{
    /**
     * Build a CONNECT packet byte for MQTT 5.0.
     * - Empty properties (MVP)
     */
    public function encodeConnect(Connect $pkt): string
    {
        // Variable header
        $vh = Bytes::encodeString('MQTT'); // Protocol Name
        $vh .= chr(5);                    // Protocol Level = 5 (MQTT 5.0)

        // Connect Flags (same bit layout as v3.1.1, Clean Start replaces Clean Session)
        $flags = 0;
        if ($pkt->cleanSession) { // maps to Clean Start
            $flags |= 0x02;
        }
        $hasUser = $pkt->username !== null;
        $hasPass = $pkt->password !== null;
        if ($hasUser) {
            $flags |= 0x80;
        }
        if ($hasPass) {
            $flags |= 0x40;
        }
        // Will support (v5): set Will Flag, QoS, Retain in flags; will properties in payload
        $hasWill = $pkt->will instanceof WillOptions;
        $will    = $pkt->will;
        if ($hasWill && $will instanceof WillOptions) {
            $flags |= 0x04; // Will Flag
            $q = $will->qos->value & 0x03;
            $flags |= ($q << 3);
            if ($will->retain) {
                $flags |= 0x20;
            }
        }
        $vh .= chr($flags);

        // Keep Alive (2 bytes)
        $vh .= Bytes::encodeUint16($pkt->keepAlive, 'Keep Alive');

        // Properties (varint length). Include known properties when provided on packet.
        $props = '';
        if (is_array($pkt->properties)) {
            // Session Expiry Interval (0x11) - four byte integer
            if (array_key_exists('session_expiry_interval', $pkt->properties)) {
                $val = $pkt->properties['session_expiry_interval'];
                $u32 = is_int($val) ? $val : (int) (is_string($val) && is_numeric($val) ? $val : 0);
                if ($u32 < 0) {
                    $u32 = 0;
                }
                if ($u32 > 0xFFFFFFFF) {
                    $u32 = 0xFFFFFFFF;
                }
                $props .= chr(0x11).pack('N', $u32);
            }

            // Receive Maximum (0x21) - two byte integer
            if (array_key_exists('receive_maximum', $pkt->properties)) {
                $val = $pkt->properties['receive_maximum'];
                $u16 = is_int($val) ? $val : (int) (is_string($val) && is_numeric($val) ? $val : 65535);
                if ($u16 < 1) {
                    $u16 = 1;
                }
                if ($u16 > 65535) {
                    $u16 = 65535;
                }
                $props .= chr(0x21).pack('n', $u16);
            }

            // Topic Alias Maximum (0x22) - two byte integer
            if (array_key_exists('topic_alias_maximum', $pkt->properties)) {
                $val = $pkt->properties['topic_alias_maximum'];
                $u16 = is_int($val) ? $val : (int) (is_string($val) && is_numeric($val) ? $val : 0);
                if ($u16 < 0) {
                    $u16 = 0;
                }
                if ($u16 > 65535) {
                    $u16 = 65535;
                }
                $props .= chr(0x22).pack('n', $u16);
            }

            // Maximum Packet Size (0x27) - four byte integer.
            // Tells the broker the largest packet we will accept, per MQTT 5 §3.1.2.11.4.
            // Without it the broker is entitled to send packets of any size.
            if (array_key_exists('maximum_packet_size', $pkt->properties)) {
                $val = $pkt->properties['maximum_packet_size'];
                $u32 = is_int($val) ? $val : (int) (is_string($val) && is_numeric($val) ? $val : 0);
                if ($u32 < 1) {
                    $u32 = 1;
                }
                if ($u32 > 268_435_455) {
                    $u32 = 268_435_455;
                }
                $props .= chr(0x27).pack('N', $u32);
            }
        }
        $vh .= Bytes::encodeVarInt(strlen($props)).$props;

        // Payload
        $payload = Bytes::encodeString($pkt->clientId);
        if ($hasWill && $will instanceof WillOptions) {
            // Will Properties (varint length). MVP: none
            $payload .= Bytes::encodeVarInt(0);
            // Will Topic and Will Payload
            $payload .= Bytes::encodeString($will->topic);
            $payload .= Bytes::encodeString($will->payload);
        }
        if ($hasUser) {
            $username = $pkt->username ?? '';
            $payload .= Bytes::encodeString($username);
        }
        if ($hasPass) {
            $password = $pkt->password ?? '';
            $payload .= Bytes::encodeString($password);
        }

        // Fixed header
        $remaining = strlen($vh) + strlen($payload);
        $fixed     = chr(PacketType::CONNECT->value << 4).Bytes::encodeVarInt($remaining);

        return $fixed.$vh.$payload;
    }

    /**
     * Encode a PUBLISH packet for MQTT 5.0.
     *
     * Packet structure:
     * - Fixed Header: Type (3), DUP, QoS, RETAIN flags
     * - Variable Header: Topic name, Packet Identifier (if QoS > 0), Properties
     * - Payload: Application message
     *
     * MQTT 5.0 includes a property field for enhanced features like content type,
     * message expiry, topic aliases, response topic, correlation data, and user properties.
     *
     * @throws LogicException If QoS > 0 and packetId is not provided
     */
    public function encodePublish(Publish $pkt): string
    {
        // Fixed Header: construct flags byte with DUP, QoS, and RETAIN
        $fixedHeader = ($pkt->dup ? 0x08 : 0x00);
        $fixedHeader |= ($pkt->qos->value << 1);
        $fixedHeader |= ($pkt->retain ? 0x01 : 0x00);
        $fixedHeader |= (PacketType::PUBLISH->value << 4);

        // Variable Header: topic name
        $variableHeader = Bytes::encodeString($pkt->topic);

        // For QoS 1/2, include Packet Identifier before properties
        if ($pkt->qos->value > 0) {
            if ($pkt->packetId === null) {
                throw new LogicException('QoS>0 requires packetId in Publish packet');
            }
            $variableHeader .= pack('n', $pkt->packetId);
        }

        // MQTT 5.0 requires a Properties field in the PUBLISH variable header (after packetId if QoS>0)
        $props = $this->encodePublishProperties($pkt->properties ?? []);
        $variableHeader .= Bytes::encodeVarInt(strlen($props)).$props;

        // Payload: application message (can be empty)
        $payload = $pkt->payload;

        // Calculate the remaining length
        $remainingLength = strlen($variableHeader) + strlen($payload);

        return chr($fixedHeader).
            Bytes::encodeVarInt($remainingLength).
            $variableHeader.
            $payload;
    }

    /**
     * Encode a SUBSCRIBE packet for MQTT 5.0.
     *
     * Packet structure:
     * - Fixed Header: Type (8), reserved flags (0x02)
     * - Variable Header: Packet Identifier (2 bytes) + Properties
     * - Payload: List of topic filters with subscription options
     *   * Each entry: UTF-8 topic filter + 1 byte subscription options
     *
     * MQTT 5.0 extends subscriptions with:
     * - Properties field (user_properties supported)
     * - Subscription options byte combining QoS and three flags:
     *   * Bits 0-1: QoS level (0, 1, or 2)
     *   * Bit 2: No Local flag (don't send back own publications)
     *   * Bit 3: Retain As Published flag (forward retain flag as-is)
     *   * Bits 4-5: Retain Handling (0=send, 1=send if new, 2=don't send)
     *   * Bits 6-7: Reserved (must be 0)
     *
     * @param  non-empty-list<array{filter:string,qos:int}>  $filters  Topic filters with QoS levels
     * @param  int  $packetId  Packet identifier (1-65535) for tracking SUBACK response
     * @param  SubscribeOptions|null  $options  Subscription options (noLocal, retainAsPublished, retainHandling, properties)
     * @return string Binary-encoded SUBSCRIBE packet
     */
    public function encodeSubscribe(array $filters, int $packetId, ?SubscribeOptions $options = null): string
    {
        // Variable header: Packet Identifier (2 bytes, big-endian)
        $vh = pack('n', $packetId);

        // Properties for SUBSCRIBE (v5). Support: user_properties (0x26)
        // Other properties like subscription_identifier (0x0B) can be added later
        $props = '';
        if ($options instanceof SubscribeOptions && is_array($options->properties) && array_key_exists('user_properties', $options->properties)) {
            $up = $options->properties['user_properties'];
            if (is_array($up)) {
                foreach ($this->normalizeUserProperties($up) as [$k, $v]) {
                    // User Property (0x26): key-value string pair
                    $props .= chr(0x26).Bytes::encodeString($k).Bytes::encodeString($v);
                }
            }
        }
        // Append properties with varint length prefix
        $vh .= Bytes::encodeVarInt(strlen($props)).$props;

        // Payload: Topic filters with subscription options
        $payload = '';
        foreach ($filters as $f) {
            $filter = (string) $f['filter'];
            $qos    = (int) $f['qos'];

            // Skip empty filters
            if ($filter === '') {
                continue;
            }

            // Clamp QoS to valid range (0-2)
            if ($qos < 0) {
                $qos = 0;
            }
            if ($qos > 2) {
                $qos = 2;
            }

            // Build subscription options byte (MQTT 5.0 feature)
            $opts = 0;

            // Bits 0-1: QoS level
            $opts |= ($qos & 0x03);

            // Apply MQTT 5.0 subscription options if provided
            if ($options instanceof SubscribeOptions) {
                // Bit 2: No Local flag (don't receive own publications)
                if ($options->noLocal) {
                    $opts |= 0x04;
                }
                // Bit 3: Retain As Published flag (forward retain flag)
                if ($options->retainAsPublished) {
                    $opts |= 0x08;
                }
                // Bits 4-5: Retain Handling (0=send, 1=send if new, 2=don't send)
                $rh = $options->retainHandling & 0x03;
                $opts |= ($rh << 4);
            }

            // Encode: UTF-8 string (2-byte length + bytes) + 1-byte subscription options
            $payload .= Bytes::encodeString($filter).chr($opts);
        }

        // Calculate remaining length
        $remaining = strlen($vh) + strlen($payload);

        // Fixed header: type SUBSCRIBE (8) with reserved flags 0b0010 (0x02)
        $fixed = chr((PacketType::SUBSCRIBE->value << 4) | 0x02).Bytes::encodeVarInt($remaining);

        return $fixed.$vh.$payload;
    }

    /**
     * Encode an UNSUBSCRIBE packet for MQTT 5.0.
     *
     * Packet structure:
     * - Fixed Header: Type (10), reserved flags (0x02)
     * - Variable Header: Packet Identifier (2 bytes) + Properties
     * - Payload: List of topic filters to unsubscribe from
     *   * Each entry: UTF-8 topic filter string
     *
     * MQTT 5.0 includes a properties field in the variable header for enhanced features.
     * Currently, no properties are encoded (empty properties with length 0).
     * Future enhancements can add user_properties for custom metadata.
     *
     * @param  non-empty-list<string>  $filters  List of topic filters to unsubscribe from
     * @param  int  $packetId  Packet identifier (1-65535) for tracking UNSUBACK response
     * @return string Binary-encoded UNSUBSCRIBE packet
     */
    public function encodeUnsubscribe(array $filters, int $packetId): string
    {
        // Variable header: Packet Identifier (2 bytes, big-endian) + Properties
        $vh = pack('n', $packetId);

        // Properties for UNSUBSCRIBE (v5). Currently empty (MVP).
        // Future: user_properties (0x26) can be added here
        $props = '';
        $vh .= Bytes::encodeVarInt(strlen($props)).$props;

        // Payload: List of topic filters (UTF-8 strings)
        $payload = '';
        foreach ($filters as $filter) {
            $f = $filter;
            if ($f === '') {
                continue;
            }
            $payload .= Bytes::encodeString($f);
        }

        // Calculate remaining length
        $remaining = strlen($vh) + strlen($payload);

        // Fixed header: type UNSUBSCRIBE (10) with reserved flags 0b0010 (0x02)
        $fixed = chr((PacketType::UNSUBSCRIBE->value << 4) | 0x02).Bytes::encodeVarInt($remaining);

        return $fixed.$vh.$payload;
    }

    /**
     * Build properties for MQTT 5 PUBLISH from a normalized map.
     * Supported keys:
     *  - Payload_format_indicator: 0|1|bool
     *  - Message_expiry_interval: int (u32)
     *  - Topic_alias: int (u16)
     *  - Response_topic: string
     *  - Correlation_data: string (binary)
     *  - user_properties: array<string,string>|list<array{0:string,1:string}>|list<array{key:string,value:string}>
     *  - Content_type: string
     */
    /**
     * @param  array<string,mixed>|null  $properties
     */
    private function encodePublishProperties(?array $properties): string
    {
        if (! $properties) {
            return '';
        }

        $out = '';

        // Payload Format Indicator (0x01) - byte
        if (array_key_exists('payload_format_indicator', $properties)) {
            $out .= chr(0x01).chr($this->toByte($properties['payload_format_indicator']));
        }

        // Message Expiry Interval (0x02) - four byte integer
        if (array_key_exists('message_expiry_interval', $properties)) {
            $out .= chr(0x02).pack('N', $this->toUInt32($properties['message_expiry_interval']));
        }

        // Content Type (0x03) - UTF-8 string
        if (array_key_exists('content_type', $properties)) {
            $out .= chr(0x03).Bytes::encodeString($this->toString($properties['content_type']));
        }

        // Response Topic (0x08) - UTF-8 string
        if (array_key_exists('response_topic', $properties)) {
            $out .= chr(0x08).Bytes::encodeString($this->toString($properties['response_topic']));
        }

        // Correlation Data (0x09) - Binary Data (2-byte length + bytes)
        if (array_key_exists('correlation_data', $properties)) {
            $out .= chr(0x09).Bytes::encodeString($this->toBinary($properties['correlation_data']));
        }

        // Topic Alias (0x23) - two-byte integer
        if (array_key_exists('topic_alias', $properties)) {
            $out .= chr(0x23).pack('n', $this->toUInt16($properties['topic_alias']));
        }

        // User Property (0x26) - can appear multiple times
        if (array_key_exists('user_properties', $properties) && is_array($properties['user_properties'])) {
            foreach ($this->normalizeUserProperties($properties['user_properties']) as [$k, $v]) {
                $out .= chr(0x26).Bytes::encodeString($k).Bytes::encodeString($v);
            }
        }

        return $out;
    }

    /**
     * @param array<array-key, mixed> $up
     * @return list<array{0:string,1:string}>
     */
    private function normalizeUserProperties(array $up): array
    {
        $pairs   = [];
        $isAssoc = ! array_is_list($up);
        if ($isAssoc) {
            foreach ($up as $k => $v) {
                $pairs[] = [$this->toString($k), $this->toString($v)];
            }
        } else {
            foreach ($up as $item) {
                if (is_array($item)) {
                    if (array_is_list($item) && count($item) >= 2) {
                        $pairs[] = [$this->toString($item[0] ?? ''), $this->toString($item[1] ?? '')];
                    } elseif (isset($item['key'], $item['value'])) {
                        $pairs[] = [$this->toString($item['key']), $this->toString($item['value'])];
                    }
                }
            }
        }

        return $pairs;
    }

    private function toString(mixed $v): string
    {
        if (is_string($v)) {
            return $v;
        }
        if (is_int($v) || is_float($v) || is_bool($v) || $v === null) {
            return (string) $v;
        }
        if (is_object($v) && method_exists($v, '__toString')) {
            return (string) $v;
        }

        return '';
    }

    private function toBinary(mixed $v): string
    {
        if (is_string($v)) {
            return $v;
        }
        if (is_int($v) || is_float($v) || is_bool($v) || $v === null) {
            return (string) $v;
        }

        return '';
    }

    private function toUInt16(mixed $v): int
    {
        $this->rejectInvalidType($v);
        $i = is_int($v) ? $v : (int) (is_string($v) && is_numeric($v) ? $v : 0);
        if ($i < 0) {
            $i = 0;
        }
        if ($i > 0xFFFF) {
            return 0xFFFF;
        }

        return $i;
    }

    private function toUInt32(mixed $v): int
    {
        $this->rejectInvalidType($v);
        $i = is_int($v) ? $v : (int) (is_string($v) && is_numeric($v) ? $v : 0);
        if ($i < 0) {
            $i = 0;
        }
        // pack('N', $i) will take lower 32 bits; clamp to 32-bit range
        if ($i > 0xFFFFFFFF) {
            return 0xFFFFFFFF;
        }

        return $i;
    }

    private function toByte(mixed $v): int
    {
        $this->rejectInvalidType($v);
        if (is_bool($v)) {
            return $v ? 1 : 0;
        }
        $i = is_int($v) ? $v : (int) (is_string($v) && is_numeric($v) ? $v : 0);
        if ($i < 0) {
            $i = 0;
        }
        if ($i > 255) {
            return 255;
        }

        return $i;
    }

    private function rejectInvalidType(mixed $v): void
    {
        if (is_array($v) || is_resource($v) || (is_object($v) && ! method_exists($v, '__toString'))) {
            throw new InvalidArgumentException(sprintf(
                'MQTT property value must be a scalar type, got %s',
                get_debug_type($v),
            ));
        }
    }
}
