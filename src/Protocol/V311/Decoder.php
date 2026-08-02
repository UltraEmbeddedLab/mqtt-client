<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\V311;

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
 * Decoder for MQTT 3.1.1 packets.
 *
 * Decodes packets according to the MQTT 3.1.1 specification (protocol level 4).
 * - No properties field (properties are MQTT 5.0 only)
 * - Simple return codes (0 = accepted, 1-5 = various error conditions)
 */
final class Decoder implements DecoderInterface
{
    /**
     * Decode bytes of a CONNACK packet body (variable header and payload).
     * Caller should pass only the remaining bytes after fixed header.
     */
    public function decodeConnAck(string $packetBody): ConnAck
    {
        if (strlen($packetBody) < 2) {
            throw new ProtocolError('CONNACK too short');
        }

        $ackFlags   = ord($packetBody[0]);
        $returnCode = ord($packetBody[1]);

        $sessionPresent = (bool) ($ackFlags & 0x01);

        return new ConnAck($sessionPresent, $returnCode);
    }

    /**
     * Decode SUBACK body: packetId (2) + payload (list of return codes).
     *
     * MQTT 3.1.1 SUBACK structure:
     * - Packet Identifier (2 bytes)
     * - Return codes (1 byte per subscription)
     *   * 0x00-0x02: Success with granted QoS 0, 1, or 2
     *   * 0x80: Failure
     */
    public function decodeSubAck(string $packetBody): SubAck
    {
        if (strlen($packetBody) < 2) {
            throw new ProtocolError('SUBACK too short');
        }
        $packetId = unpack('n', substr($packetBody, 0, 2));
        if ($packetId === false || ! isset($packetId[1]) || ! is_int($packetId[1])) {
            throw new ProtocolError('SUBACK malformed packet id');
        }
        $pid   = $packetId[1];
        $codes = [];
        $rest  = substr($packetBody, 2);
        $len   = strlen($rest);
        for ($i = 0; $i < $len; $i++) {
            $codes[] = ord($rest[$i]);
        }

        return new SubAck($pid, $codes);
    }

    /**
     * Decode inbound PUBLISH packet body (topic[,packetId]) + payload.
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

        $payload = substr($packetBody, $offset);

        return new InboundMessage(
            topic: $topic,
            payload: $payload,
            qos: $qos,
            retain: $retain,
            dup: $dup,
            packetId: $packetId,
        );
    }

    /**
     * Decode UNSUBACK body: packetId only in v3.1.1.
     *
     * MQTT 3.1.1 UNSUBACK structure:
     * - Packet Identifier (2 bytes)
     * - No reason codes (acknowledgment is implicit success)
     * - No properties
     */
    public function decodeUnsubAck(string $packetBody): UnsubAck
    {
        if (strlen($packetBody) < 2) {
            throw new ProtocolError('UNSUBACK too short');
        }
        $arr = unpack('n', substr($packetBody, 0, 2));
        if ($arr === false || ! isset($arr[1]) || ! is_int($arr[1])) {
            throw new ProtocolError('UNSUBACK malformed packet id');
        }

        $pid = $arr[1];

        return new UnsubAck($pid);
    }

    /**
     * Decode PUBACK body: packetId only in v3.1.1.
     *
     * MQTT 3.1.1 PUBACK structure:
     * - Packet Identifier (2 bytes)
     * - No reason code (implicit success)
     * - No properties
     */
    public function decodePubAck(string $packetBody): PubAck
    {
        if (strlen($packetBody) < 2) {
            throw new ProtocolError('PUBACK too short');
        }
        $arr = unpack('n', substr($packetBody, 0, 2));
        if ($arr === false || ! isset($arr[1]) || ! is_int($arr[1])) {
            throw new ProtocolError('PUBACK malformed packet id');
        }

        $pid = $arr[1];

        return new PubAck($pid, 0);
    }

    /**
     * Decode PUBREC body: packetId only in v3.1.1.
     *
     * MQTT 3.1.1 PUBREC structure:
     * - Packet Identifier (2 bytes)
     * - No reason code (implicit success)
     * - No properties
     */
    public function decodePubRec(string $packetBody): PubRec
    {
        if (strlen($packetBody) < 2) {
            throw new ProtocolError('PUBREC too short');
        }
        $arr = unpack('n', substr($packetBody, 0, 2));
        if ($arr === false || ! isset($arr[1]) || ! is_int($arr[1])) {
            throw new ProtocolError('PUBREC malformed packet id');
        }

        $pid = $arr[1];

        return new PubRec($pid, 0);
    }

    /**
     * Decode PUBREL body: packetId only in v3.1.1.
     *
     * MQTT 3.1.1 PUBREL structure:
     * - Packet Identifier (2 bytes)
     * - No reason code (implicit success)
     * - No properties
     */
    public function decodePubRel(string $packetBody): PubRel
    {
        if (strlen($packetBody) < 2) {
            throw new ProtocolError('PUBREL too short');
        }
        $arr = unpack('n', substr($packetBody, 0, 2));
        if ($arr === false || ! isset($arr[1]) || ! is_int($arr[1])) {
            throw new ProtocolError('PUBREL malformed packet id');
        }

        $pid = $arr[1];

        return new PubRel($pid, 0);
    }

    /**
     * Decode PUBCOMP body: packetId only in v3.1.1.
     *
     * MQTT 3.1.1 PUBCOMP structure:
     * - Packet Identifier (2 bytes)
     * - No reason code (implicit success)
     * - No properties
     */
    public function decodePubComp(string $packetBody): PubComp
    {
        if (strlen($packetBody) < 2) {
            throw new ProtocolError('PUBCOMP too short');
        }
        $arr = unpack('n', substr($packetBody, 0, 2));
        if ($arr === false || ! isset($arr[1]) || ! is_int($arr[1])) {
            throw new ProtocolError('PUBCOMP malformed packet id');
        }

        $pid = $arr[1];

        return new PubComp($pid, 0);
    }

    /**
     * Decode DISCONNECT body for MQTT 3.1.1.
     *
     * MQTT 3.1.1 DISCONNECT structure:
     * - No variable header or payload (always normal disconnection)
     *
     * Note: In MQTT 3.1.1, DISCONNECT is only sent by the client to the broker,
     * but we implement decoding for completeness and protocol testing.
     */
    public function decodeDisconnect(string $packetBody): Disconnect
    {
        // MQTT 3.1.1 DISCONNECT has no body - always normal disconnection
        return new Disconnect(0x00);
    }
}
