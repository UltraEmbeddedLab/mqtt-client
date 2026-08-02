<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\V311;

use LogicException;
use ScienceStories\Mqtt\Client\SubscribeOptions;
use ScienceStories\Mqtt\Client\WillOptions;
use ScienceStories\Mqtt\Contract\EncoderInterface;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Protocol\Packet\Connect;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\Packet\Publish;
use ScienceStories\Mqtt\Util\Bytes;

use function chr;
use function strlen;

/**
 * Encoder for MQTT 3.1.1 packets.
 *
 * Encodes packets according to the MQTT 3.1.1 specification (protocol level 4).
 * - No properties field (properties are MQTT 5.0 only)
 * - Clean Session flag instead of Clean Start
 * - Maximum packet size is implementation-dependent
 */
final class Encoder implements EncoderInterface
{
    /**
     * Build a CONNECT packet byte for MQTT 3.1.1.
     */
    public function encodeConnect(Connect $pkt): string
    {
        // Variable header
        $vh = Bytes::encodeString('MQTT'); // Protocol Name
        $vh .= chr(4);                    // Protocol Level = 4 (MQTT 3.1.1)

        // Connect Flags
        $flags = 0;
        // Clean Session
        if ($pkt->cleanSession) {
            $flags |= 0x02;
        }
        // Username / Password
        $hasUser = $pkt->username !== null;
        $hasPass = $pkt->password !== null;
        // MQTT-3.1.2-22: on 3.1.1 a Password requires a User Name. Brokers close the
        // connection without a CONNACK when this is violated, which is undiagnosable
        // from the client side, so reject it here. (MQTT 5 lifts the restriction.)
        if ($hasPass && ! $hasUser) {
            throw new ProtocolError('MQTT 3.1.1 does not allow a password without a username (MQTT-3.1.2-22)');
        }
        if ($hasUser) {
            $flags |= 0x80;
        }
        if ($hasPass) {
            $flags |= 0x40;
        }
        // Will settings (v3 payload Will Topic + Message precede username/password)
        $hasWill = $pkt->will instanceof WillOptions;
        $will    = $pkt->will;
        if ($hasWill && $will instanceof WillOptions) {
            $flags |= 0x04; // Will Flag
            $q = $will->qos->value & 0x03;
            $flags |= ($q << 3); // Will QoS bits 3-4
            if ($will->retain) {
                $flags |= 0x20; // Will Retain
            }
        }

        $vh .= chr($flags);
        // Keep Alive (2 bytes)
        $vh .= Bytes::encodeUint16($pkt->keepAlive, 'Keep Alive');

        // Payload
        $payload = Bytes::encodeString($pkt->clientId);
        if ($hasWill && $will instanceof WillOptions) {
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
     * Encode a PUBLISH packet for MQTT 3.1.1.
     *
     * Packet structure:
     * - Fixed Header: Type (3), DUP, QoS, RETAIN flags
     * - Variable Header: Topic name, Packet Identifier (if QoS > 0)
     * - Payload: Application message
     *
     * MQTT 3.1.1 does not support the properties field (that's MQTT 5.0 only).
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

        // For QoS 1/2, include Packet Identifier in variable header
        if ($pkt->qos->value > 0) {
            if ($pkt->packetId === null) {
                throw new LogicException('QoS>0 requires packetId in Publish packet');
            }
            $variableHeader .= pack('n', $pkt->packetId);
        }

        // Payload: application message (can be empty)
        $payload = $pkt->payload;

        // Calculate the remaining length
        $remainingLength = strlen($variableHeader) + strlen($payload);

        return chr($fixedHeader)
            .Bytes::encodeVarInt($remainingLength)
            .$variableHeader
            .$payload;
    }

    /**
     * Encode a SUBSCRIBE packet for MQTT 3.1.1.
     *
     * Packet structure:
     * - Fixed Header: Type (8), reserved flags (0x02)
     * - Variable Header: Packet Identifier (2 bytes)
     * - Payload: List of topic filters with requested QoS
     *   * Each entry: UTF-8 topic filter + 1 byte QoS (0, 1, or 2)
     *
     * MQTT 3.1.1 only supports basic subscription with topic filter and QoS level.
     * No subscription options like No Local, Retain As Published, or Retain Handling.
     * The $options parameter is ignored in v3.1.1 (provided for interface compatibility).
     *
     * @param  non-empty-list<array{filter:string,qos:int}>  $filters  Topic filters with QoS levels
     * @param  int  $packetId  Packet identifier (1-65535) for tracking SUBACK response
     * @param  SubscribeOptions|null  $options  Ignored in MQTT 3.1.1 (MQTT 5.0 only)
     * @return string Binary-encoded SUBSCRIBE packet
     */
    public function encodeSubscribe(array $filters, int $packetId, ?SubscribeOptions $options = null): string
    {
        // Variable header: Packet Identifier (2 bytes, big-endian)
        $vh = pack('n', $packetId);

        // Payload: Topic Filter + Requested QoS (only QoS byte in v3.1.1, no subscription options)
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

            // Encode: UTF-8 string (2-byte length + bytes) + 1-byte QoS
            $payload .= Bytes::encodeString($filter).chr($qos);
        }

        // Calculate remaining length
        $remaining = strlen($vh) + strlen($payload);

        // Fixed header: type SUBSCRIBE (8) with reserved flags 0b0010 (0x02)
        $fixed = chr((PacketType::SUBSCRIBE->value << 4) | 0x02).Bytes::encodeVarInt($remaining);

        return $fixed.$vh.$payload;
    }

    /**
     * Encode an UNSUBSCRIBE packet for MQTT 3.1.1.
     *
     * Packet structure:
     * - Fixed Header: Type (10), reserved flags (0x02)
     * - Variable Header: Packet Identifier (2 bytes)
     * - Payload: List of topic filters to unsubscribe from
     *   * Each entry: UTF-8 topic filter string
     *
     * MQTT 3.1.1 only supports basic unsubscription with topic filter list.
     * No properties or options (properties are MQTT 5.0 only).
     *
     * @param  non-empty-list<string>  $filters  List of topic filters to unsubscribe from
     * @param  int  $packetId  Packet identifier (1-65535) for tracking UNSUBACK response
     * @return string Binary-encoded UNSUBSCRIBE packet
     */
    public function encodeUnsubscribe(array $filters, int $packetId): string
    {
        // Variable header: Packet Identifier (2 bytes, big-endian)
        $vh = pack('n', $packetId);

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
}
