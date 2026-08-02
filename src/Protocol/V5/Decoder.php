<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\V5;

use ScienceStories\Mqtt\Client\InboundMessage;
use ScienceStories\Mqtt\Contract\DecoderInterface;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Protocol\Packet\ConnAck;
use ScienceStories\Mqtt\Protocol\Packet\Disconnect;
use ScienceStories\Mqtt\Protocol\Packet\PubAck;
use ScienceStories\Mqtt\Protocol\Packet\PubComp;
use ScienceStories\Mqtt\Protocol\Packet\PubRec;
use ScienceStories\Mqtt\Protocol\Packet\PubRel;
use ScienceStories\Mqtt\Protocol\Packet\SubAck;
use ScienceStories\Mqtt\Protocol\Packet\UnsubAck;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Util\Bytes;

use function is_int;
use function ord;
use function strlen;

/**
 * Decoder for MQTT 5.0 packets.
 *
 * Decodes packets according to the MQTT 5.0 specification (protocol level 5).
 * - Supports properties field with extensive metadata
 * - Reason codes instead of simple return codes
 * - Enhanced error information with reason strings and user properties
 */
final class Decoder implements DecoderInterface
{
    /**
     * Decode bytes of a CONNACK packet body for v5.
     * Body layout (v5):
     *  - Byte1: Acknowledge Flags (bit0 = session present)
     *  - Byte2: Reason Code
     *  - Properties: VarInt length and properties (ignored in MVP)
     */
    public function decodeConnAck(string $packetBody): ConnAck
    {
        if (strlen($packetBody) < 2) {
            throw new ProtocolError('CONNACK too short');
        }

        $ackFlags   = ord($packetBody[0]);
        $reasonCode = ord($packetBody[1]);

        // Properties
        $offset   = 2;
        $propsMap = null;
        if (isset($packetBody[$offset])) {
            $rest     = substr($packetBody, $offset);
            $consumed = 0;
            $len      = Bytes::decodeVarInt($rest, $consumed);
            if ($consumed + $offset + $len > strlen($packetBody)) {
                throw new ProtocolError('Malformed CONNACK: properties truncated');
            }
            $propsRaw = substr($packetBody, $offset + $consumed, $len);
            $propsMap = $this->parseConnAckProperties($propsRaw);
        }

        $sessionPresent = (bool) ($ackFlags & 0x01);

        return new ConnAck($sessionPresent, $reasonCode, $propsMap);
    }

    /**
     * Parse a subset of MQTT 5 CONNACK properties into an associative array.
     * Recognized keys:
     *  - Assigned_client_identifier (0x12) string
     *  - Server_keep_alive (0x13) u16
     *  - Receive_maximum (0x21) u16
     *  - Topic_alias_maximum (0x22) u16
     *  - Maximum_qos (0x24) byte
     *  - Retain_available (0x25) byte
     *  - Maximum_packet_size (0x27) u32
     *  - Wildcard_subscription_available (0x28) byte
     *  - Subscription_identifier_available (0x29) byte
     *  - Shared_subscription_available (0x2A) byte
     *  - Response_information (0x1A) string
     *  - Reason_string (0x1F) string
     *  - Server_reference (0x1C) string
     *  - User_properties (0x26) map<string,string>
     *
     * @return array<string, mixed>
     */
    private function parseConnAckProperties(string $props): array
    {
        $out = [];
        $i   = 0;
        $n   = strlen($props);
        while ($i < $n) {
            $id = ord($props[$i++]);
            switch ($id) {
                case 0x12: // Assigned Client Identifier
                    $off                               = $i;
                    $out['assigned_client_identifier'] = Bytes::decodeString($props, $off);
                    $i                                 = $off;
                    break;
                case 0x13: // Server Keep Alive (u16)
                    if ($i + 2 > $n) {
                        $i = $n;
                        break;
                    }
                    $out['server_keep_alive'] = unpack('n', substr($props, $i, 2))[1] ?? 0;
                    $i += 2;
                    break;
                case 0x21: // Receive Maximum (u16)
                    if ($i + 2 > $n) {
                        $i = $n;
                        break;
                    }
                    $out['receive_maximum'] = unpack('n', substr($props, $i, 2))[1] ?? 0;
                    $i += 2;
                    break;
                case 0x22: // Topic Alias Maximum (u16)
                    if ($i + 2 > $n) {
                        $i = $n;
                        break;
                    }
                    $out['topic_alias_maximum'] = unpack('n', substr($props, $i, 2))[1] ?? 0;
                    $i += 2;
                    break;
                case 0x24: // Maximum QoS (byte)
                    if ($i >= $n) {
                        $i = $n;
                        break;
                    }
                    $out['maximum_qos'] = ord($props[$i++]);
                    break;
                case 0x25: // Retain Available (byte)
                    if ($i >= $n) {
                        $i = $n;
                        break;
                    }
                    $out['retain_available'] = ord($props[$i++]);
                    break;
                case 0x27: // Maximum Packet Size (u32)
                    if ($i + 4 > $n) {
                        $i = $n;
                        break;
                    }
                    $out['maximum_packet_size'] = unpack('N', substr($props, $i, 4))[1] ?? 0;
                    $i += 4;
                    break;
                case 0x28: // Wildcard Subscription Available (byte)
                    if ($i >= $n) {
                        $i = $n;
                        break;
                    }
                    $out['wildcard_subscription_available'] = ord($props[$i++]);
                    break;
                case 0x29: // Subscription Identifier Available (byte)
                    if ($i >= $n) {
                        $i = $n;
                        break;
                    }
                    $out['subscription_identifier_available'] = ord($props[$i++]);
                    break;
                case 0x2A: // Shared Subscription Available (byte)
                    if ($i >= $n) {
                        $i = $n;
                        break;
                    }
                    $out['shared_subscription_available'] = ord($props[$i++]);
                    break;
                case 0x1A: // Response Information (string)
                    $off                         = $i;
                    $out['response_information'] = Bytes::decodeString($props, $off);
                    $i                           = $off;
                    break;
                case 0x1F: // Reason String (string)
                    $off                  = $i;
                    $out['reason_string'] = Bytes::decodeString($props, $off);
                    $i                    = $off;
                    break;
                case 0x1C: // Server Reference (string)
                    $off                     = $i;
                    $out['server_reference'] = Bytes::decodeString($props, $off);
                    $i                       = $off;
                    break;
                case 0x26: // User Property (key,value)
                    $off                        = $i;
                    $k                          = Bytes::decodeString($props, $off);
                    $v                          = Bytes::decodeString($props, $off);
                    $i                          = $off;
                    $out['user_properties'][$k] = $v;
                    break;
                default:
                    // Unknown property: stop parsing for safety
                    $i = $n;
                    break;
            }
        }

        return $out;
    }

    /**
     * Parse common MQTT 5.0 acknowledgment properties (reason_string + user_properties).
     * Used by SUBACK, UNSUBACK, PUBACK, PUBREC, PUBREL, PUBCOMP.
     *
     * @return array<string, mixed>
     */
    private function parseAckProperties(string $props): array
    {
        $out = [];
        $i   = 0;
        $n   = strlen($props);
        while ($i < $n) {
            $id = ord($props[$i++]);
            switch ($id) {
                case 0x1F: // Reason String (string)
                    $off                  = $i;
                    $out['reason_string'] = Bytes::decodeString($props, $off);
                    $i                    = $off;
                    break;
                case 0x26: // User Property (key,value)
                    $off                        = $i;
                    $k                          = Bytes::decodeString($props, $off);
                    $v                          = Bytes::decodeString($props, $off);
                    $i                          = $off;
                    $out['user_properties'][$k] = $v;
                    break;
                default:
                    // Unknown property: stop parsing for safety
                    $i = $n;
                    break;
            }
        }

        return $out;
    }

    /**
     * Decode SUBACK: packetId (2) + properties(varint+props) + payload reason codes.
     *
     * MQTT 5.0 SUBACK structure:
     * - Packet Identifier (2 bytes)
     * - Properties (varint length and properties)
     *   * reason_string (0x1F): string
     *   * user_properties (0x26): key-value pairs
     * - Reason codes (1 byte per subscription)
     *   * 0x00-0x02: Granted QoS 0, 1, or 2
     *   * 0x80+: Various failure codes
     */
    public function decodeSubAck(string $packetBody): SubAck
    {
        if (strlen($packetBody) < 4) { // minimal: id(2)+props_len(1)+empty
            throw new ProtocolError('SUBACK too short');
        }
        $arr = unpack('n', substr($packetBody, 0, 2));
        if ($arr === false || ! isset($arr[1]) || ! is_int($arr[1])) {
            throw new ProtocolError('SUBACK malformed packet id');
        }
        $pid    = $arr[1];
        $offset = 2;
        // Properties
        $rest     = substr($packetBody, $offset);
        $consumed = 0;
        $propLen  = Bytes::decodeVarInt($rest, $consumed);
        $offset += $consumed;
        if ($offset + $propLen > strlen($packetBody)) {
            throw new ProtocolError('SUBACK properties truncated');
        }
        $propsRaw = substr($packetBody, $offset, $propLen);
        $offset += $propLen;
        $propsMap = $propLen > 0 ? $this->parseAckProperties($propsRaw) : null;

        // Reason codes
        $codes = [];
        for ($i = $offset, $n = strlen($packetBody); $i < $n; $i++) {
            $codes[] = ord($packetBody[$i]);
        }

        return new SubAck($pid, $codes, $propsMap);
    }

    /**
     * Decode inbound PUBLISH with v5 Properties between topic and payload.
     */
    public function decodePublish(int $flags, string $packetBody): InboundMessage
    {
        $dup    = (bool) (($flags & 0x08) >> 3);
        $qosVal = ($flags & 0x06) >> 1;
        $retain = (bool) ($flags & 0x01);
        // MQTT-3.3.1-4: both QoS bits set is a Malformed Packet. QoS::from() would raise
        // a \ValueError, which sits outside MqttException and escapes every documented
        // catch block, killing a long-running loop.
        $qos = QoS::tryFrom($qosVal) ?? throw new ProtocolError("Malformed PUBLISH: invalid QoS bits ($qosVal)");

        $offset   = 0;
        $topic    = Bytes::decodeString($packetBody, $offset);
        $packetId = null;
        if ($qosVal > 0) {
            if ($offset + 2 > strlen($packetBody)) {
                throw new ProtocolError('PUBLISH missing packet id');
            }
            $arr = unpack('n', substr($packetBody, $offset, 2));
            if ($arr === false || ! isset($arr[1]) || ! is_int($arr[1])) {
                throw new ProtocolError('PUBLISH invalid packet id');
            }
            $packetId = $arr[1];
            $offset += 2;
        }

        // Properties
        $rest     = substr($packetBody, $offset);
        $consumed = 0;
        $propLen  = Bytes::decodeVarInt($rest, $consumed);
        $offset += $consumed;
        if ($offset + $propLen > strlen($packetBody)) {
            throw new ProtocolError('PUBLISH properties truncated');
        }
        $propsRaw = substr($packetBody, $offset, $propLen);
        $offset += $propLen;

        $properties = $this->parsePublishProperties($propsRaw);
        $payload    = substr($packetBody, $offset);

        return new InboundMessage(
            topic: $topic,
            payload: $payload,
            qos: $qos,
            retain: $retain,
            dup: $dup,
            packetId: $packetId,
            properties: $properties,
        );
    }

    /**
     * Decode UNSUBACK: packetId + properties + reason codes.
     *
     * MQTT 5.0 UNSUBACK structure:
     * - Packet Identifier (2 bytes)
     * - Properties (varint length and properties)
     *   * reason_string (0x1F): string
     *   * user_properties (0x26): key-value pairs
     * - Reason codes (1 byte per unsubscribed topic filter)
     *   * 0x00: Success
     *   * 0x11: No subscription existed
     *   * 0x80+: Various failure codes
     */
    public function decodeUnsubAck(string $packetBody): UnsubAck
    {
        if (strlen($packetBody) < 4) {
            throw new ProtocolError('UNSUBACK too short');
        }
        $arr = unpack('n', substr($packetBody, 0, 2));
        if ($arr === false || ! isset($arr[1]) || ! is_int($arr[1])) {
            throw new ProtocolError('UNSUBACK malformed packet id');
        }
        $pid    = $arr[1];
        $offset = 2;

        // Properties
        $rest     = substr($packetBody, $offset);
        $consumed = 0;
        $propLen  = Bytes::decodeVarInt($rest, $consumed);
        $offset += $consumed;
        if ($offset + $propLen > strlen($packetBody)) {
            throw new ProtocolError('UNSUBACK properties truncated');
        }
        $propsRaw = substr($packetBody, $offset, $propLen);
        $offset += $propLen;
        $propsMap = $propLen > 0 ? $this->parseAckProperties($propsRaw) : null;

        // Reason codes
        $codes = [];
        for ($i = $offset, $n = strlen($packetBody); $i < $n; $i++) {
            $codes[] = ord($packetBody[$i]);
        }

        return new UnsubAck($pid, $codes, $propsMap);
    }


    /**
     * Parse a subset of v5 PUBLISH properties into an associative array.
     *
     * @return array<string,mixed>
     */
    private function parsePublishProperties(string $props): array
    {
        $out = [];
        $i   = 0;
        $len = strlen($props);
        while ($i < $len) {
            $id = ord($props[$i++]);
            switch ($id) {
                case 0x01: // payload_format_indicator (byte)
                    if ($i >= $len) {
                        break 2;
                    }
                    $out['payload_format_indicator'] = ord($props[$i++]);
                    break;
                case 0x02: // message_expiry_interval (u32)
                    if ($i + 4 > $len) {
                        break 2;
                    }
                    $out['message_expiry_interval'] = unpack('N', substr($props, $i, 4))[1] ?? 0;
                    $i += 4;
                    break;
                case 0x03: // content_type (string)
                    $offset              = $i;
                    $out['content_type'] = Bytes::decodeString($props, $offset);
                    $i                   = $offset;
                    break;
                case 0x08: // response_topic (string)
                    $offset                = $i;
                    $out['response_topic'] = Bytes::decodeString($props, $offset);
                    $i                     = $offset;
                    break;
                case 0x09: // correlation_data (binary)
                    $offset                  = $i;
                    $out['correlation_data'] = Bytes::decodeString($props, $offset);
                    $i                       = $offset;
                    break;
                case 0x23: // topic_alias (u16)
                    if ($i + 2 > $len) {
                        break 2;
                    }
                    $out['topic_alias'] = unpack('n', substr($props, $i, 2))[1] ?? 0;
                    $i += 2;
                    break;
                case 0x26: // user_property (key,value)
                    $offset                       = $i;
                    $key                          = Bytes::decodeString($props, $offset);
                    $val                          = Bytes::decodeString($props, $offset);
                    $i                            = $offset;
                    $out['user_properties'][$key] = $val;
                    break;
                default:
                    // Unknown property id: break loop for safety
                    $i = $len; // stop parsing
                    break;
            }
        }

        return $out;
    }


    public function decodePubAck(string $packetBody): PubAck
    {
        [$pid, $reasonCode, $propsMap] = $this->decodeQoSAck($packetBody, 'PUBACK');

        return new PubAck($pid, $reasonCode, $propsMap);
    }

    public function decodePubRec(string $packetBody): PubRec
    {
        [$pid, $reasonCode, $propsMap] = $this->decodeQoSAck($packetBody, 'PUBREC');

        return new PubRec($pid, $reasonCode, $propsMap);
    }

    public function decodePubRel(string $packetBody): PubRel
    {
        [$pid, $reasonCode, $propsMap] = $this->decodeQoSAck($packetBody, 'PUBREL');

        return new PubRel($pid, $reasonCode, $propsMap);
    }

    public function decodePubComp(string $packetBody): PubComp
    {
        [$pid, $reasonCode, $propsMap] = $this->decodeQoSAck($packetBody, 'PUBCOMP');

        return new PubComp($pid, $reasonCode, $propsMap);
    }

    /**
     * Shared decoder for QoS acknowledgment packets (PUBACK, PUBREC, PUBREL, PUBCOMP).
     *
     * Structure: Packet Identifier (2 bytes) + optional Reason Code (1 byte) + optional Properties.
     *
     * @return array{0: int, 1: int, 2: array<string, mixed>|null} [packetId, reasonCode, properties]
     */
    private function decodeQoSAck(string $packetBody, string $packetName): array
    {
        if (strlen($packetBody) < 2) {
            throw new ProtocolError("$packetName too short");
        }
        $arr = unpack('n', substr($packetBody, 0, 2));
        if ($arr === false || ! isset($arr[1]) || ! is_int($arr[1])) {
            throw new ProtocolError("$packetName malformed packet id");
        }
        $pid = $arr[1];

        if (strlen($packetBody) === 2) {
            return [$pid, 0, null];
        }

        $reasonCode = ord($packetBody[2]);
        $offset     = 3;

        $propsMap = null;
        if ($offset < strlen($packetBody)) {
            $rest     = substr($packetBody, $offset);
            $consumed = 0;
            $propLen  = Bytes::decodeVarInt($rest, $consumed);
            $offset += $consumed;
            if ($offset + $propLen > strlen($packetBody)) {
                throw new ProtocolError("$packetName properties truncated");
            }
            $propsRaw = substr($packetBody, $offset, $propLen);
            $propsMap = $propLen > 0 ? $this->parseAckProperties($propsRaw) : null;
        }

        return [$pid, $reasonCode, $propsMap];
    }

    /**
     * Decode DISCONNECT body for MQTT 5.0.
     *
     * Structure:
     * - Reason Code (1 byte) - optional, defaults to 0x00 if omitted
     * - Properties (varint length and data) - optional
     *
     * Note: MQTT 5.0 allows servers to send DISCONNECT to clients with
     * reason codes explaining why the connection is being closed.
     */
    public function decodeDisconnect(string $packetBody): Disconnect
    {
        // Empty packet = normal disconnection (reason code 0x00)
        if ($packetBody === '') {
            return new Disconnect(0x00);
        }

        // Reason code (1 byte)
        $reasonCode = ord($packetBody[0]);
        $offset     = 1;

        // Properties
        $propsMap = null;
        if ($offset < strlen($packetBody)) {
            $rest     = substr($packetBody, $offset);
            $consumed = 0;
            $propLen  = Bytes::decodeVarInt($rest, $consumed);
            $offset += $consumed;
            if ($offset + $propLen > strlen($packetBody)) {
                throw new ProtocolError('DISCONNECT properties truncated');
            }
            $propsRaw = substr($packetBody, $offset, $propLen);
            $propsMap = $propLen > 0 ? $this->parseDisconnectProperties($propsRaw) : null;
        }

        return new Disconnect($reasonCode, $propsMap);
    }

    /**
     * Parse MQTT 5.0 DISCONNECT properties.
     * Recognized keys:
     *  - Session_expiry_interval (0x11): u32
     *  - Reason_string (0x1F): string
     *  - User_properties (0x26): array<string,string>
     *  - Server_reference (0x1C): string
     *
     * @return array<string, mixed>
     */
    private function parseDisconnectProperties(string $props): array
    {
        $out = [];
        $i   = 0;
        $n   = strlen($props);
        while ($i < $n) {
            $id = ord($props[$i++]);
            switch ($id) {
                case 0x11: // Session Expiry Interval (u32)
                    if ($i + 4 > $n) {
                        $i = $n;
                        break;
                    }
                    $out['session_expiry_interval'] = unpack('N', substr($props, $i, 4))[1] ?? 0;
                    $i += 4;
                    break;
                case 0x1C: // Server Reference (string)
                    $off                     = $i;
                    $out['server_reference'] = Bytes::decodeString($props, $off);
                    $i                       = $off;
                    break;
                case 0x1F: // Reason String (string)
                    $off                  = $i;
                    $out['reason_string'] = Bytes::decodeString($props, $off);
                    $i                    = $off;
                    break;
                case 0x26: // User Property (key,value)
                    $off                        = $i;
                    $k                          = Bytes::decodeString($props, $off);
                    $v                          = Bytes::decodeString($props, $off);
                    $i                          = $off;
                    $out['user_properties'][$k] = $v;
                    break;
                default:
                    // Unknown property: stop parsing for safety
                    $i = $n;
                    break;
            }
        }

        return $out;
    }
}
