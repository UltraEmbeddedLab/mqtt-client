<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\Packet;

/**
 * PUBCOMP packet model for MQTT 3.1.1 and 5.0. Packet type 7.
 *
 * PUBCOMP is sent in response to a PUBREL — the fourth and final packet in the QoS 2
 * handshake, confirming completion.
 *
 * QoS 2 flow:
 * 1. Sender sends PUBLISH with QoS 2 and a Packet Identifier
 * 2. Receiver sends PUBREC
 * 3. Sender sends PUBREL to release the message
 * 4. Receiver sends PUBCOMP to confirm completion  ← this packet
 *
 * Once PUBCOMP has been sent and received, both parties discard the Packet Identifier and
 * the exactly-once guarantee is fulfilled.
 *
 * MQTT 5.0 reason codes are limited to 0x00 (Success) and 0x92 (Packet Identifier not
 * found).
 *
 * @see AcknowledgementPacket for the shared structure and accessors
 */
final class PubComp extends AcknowledgementPacket
{
}
