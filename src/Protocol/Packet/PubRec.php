<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Protocol\Packet;

/**
 * PUBREC packet model for MQTT 3.1.1 and 5.0. Packet type 5.
 *
 * PUBREC is the response to a PUBLISH with QoS 2 (Exactly once delivery) — the first
 * acknowledgement in the four-packet handshake.
 *
 * QoS 2 flow:
 * 1. Sender sends PUBLISH with QoS 2 and a Packet Identifier
 * 2. Receiver sends PUBREC with the matching Packet Identifier  ← this packet
 * 3. Sender sends PUBREL to release the message
 * 4. Receiver sends PUBCOMP to confirm completion
 *
 * MQTT 5.0 reason codes here match PUBACK's set. Per MQTT-4.3.3-4 a PUBREC carrying a code
 * of 0x80 or above ends the exchange: the sender must not send PUBREL, and both parties
 * release the Packet Identifier.
 *
 * @see AcknowledgementPacket for the shared structure and accessors
 */
final class PubRec extends AcknowledgementPacket
{
}
