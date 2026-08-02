<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\Packet;

/**
 * PUBACK packet model for MQTT 3.1.1 and 5.0. Packet type 4.
 *
 * PUBACK is the response to a PUBLISH with QoS 1 (At least once delivery); it confirms
 * receipt to the sender.
 *
 * QoS 1 flow:
 * 1. Sender sends PUBLISH with QoS 1 and a Packet Identifier
 * 2. Receiver sends PUBACK with the matching Packet Identifier  ← this packet
 * 3. Sender considers the message delivered
 *
 * MQTT 5.0 reason codes seen here: 0x00 Success, 0x10 No matching subscribers,
 * 0x80 Unspecified error, 0x83 Implementation specific error, 0x87 Not authorized,
 * 0x90 Topic Name invalid, 0x91 Packet Identifier in use, 0x97 Quota exceeded,
 * 0x99 Payload format invalid.
 *
 * Note that 0x10 is *not* an error — the message was accepted, nobody was subscribed.
 *
 * @see AcknowledgementPacket for the shared structure and accessors
 */
final class PubAck extends AcknowledgementPacket
{
}
