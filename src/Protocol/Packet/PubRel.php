<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\Packet;

/**
 * PUBREL packet model for MQTT 3.1.1 and 5.0. Packet type 6.
 *
 * PUBREL is sent in response to a PUBREC — the third packet in the QoS 2 handshake,
 * releasing the message for delivery.
 *
 * QoS 2 flow:
 * 1. Sender sends PUBLISH with QoS 2 and a Packet Identifier
 * 2. Receiver sends PUBREC
 * 3. Sender sends PUBREL to release the message  ← this packet
 * 4. Receiver sends PUBCOMP to confirm completion
 *
 * **PUBREL is the only QoS 2 packet with mandatory fixed-header flags**: bits 3-0 of the
 * first byte must be 0b0010 (0x02) per MQTT-3.6.1-1. A broker that receives any other
 * value treats it as a Malformed Packet.
 *
 * MQTT 5.0 reason codes are limited to 0x00 (Success) and 0x92 (Packet Identifier not
 * found).
 *
 * @see AcknowledgementPacket for the shared structure and accessors
 */
final class PubRel extends AcknowledgementPacket
{
}
