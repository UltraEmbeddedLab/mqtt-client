<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use ScienceStories\Mqtt\Protocol\Packet\Publish;
use ScienceStories\Mqtt\Protocol\QoS;

use function array_keys;
use function count;

/**
 * The QoS 1 and 2 messages this client has published that the broker has not finished
 * acknowledging — the client's half of the Session State for outbound messages
 * (MQTT-4.1.0-1).
 *
 * Kept in the order the protocol needs them resent after a reconnect: PUBLISH packets in
 * the order they were first sent (MQTT-4.6.0-1), PUBREL packets in the order their PUBRECs
 * arrived (MQTT-4.6.0-4) — which is why releasing a message moves it to the end.
 *
 * A message stays here after `publish()` stops waiting for it ("abandoned"): the broker may
 * still hold it, so its Packet Identifier must not be reused, and a resumed session still
 * owes it a resend. It leaves when the broker acknowledges or refuses it, or when the session
 * is gone — never on elapsed time alone, which does not end a session.
 *
 * @internal
 */
final class OutboundInflight
{
    /**
     * @var array<int, array{publish: Publish, released: bool, abandonedAt: float|null, sends: int}>
     *                                                                                               keyed by Packet Identifier, in resend order; `sends` counts every transmission
     */
    private array $messages = [];

    /**
     * Record a PUBLISH just written. The stored packet carries the full topic and no Topic
     * Alias: an alias belongs to one connection, and a resend may go out on another.
     */
    public function add(Publish $publish): void
    {
        if ($publish->packetId === null) {
            return;
        }
        $this->messages[$publish->packetId] = ['publish' => $publish, 'released' => false, 'abandonedAt' => null, 'sends' => 1];
    }

    /** PUBREC arrived and PUBREL went out: from now on only the PUBREL may be resent. */
    public function release(int $packetId): void
    {
        $message = $this->messages[$packetId] ?? null;
        if ($message === null) {
            return;
        }
        unset($this->messages[$packetId]);
        $message['released']       = true;
        $this->messages[$packetId] = $message;
    }

    /** The exchange is over — acknowledged, refused with an error PUBREC, or given up on. */
    public function remove(int $packetId): void
    {
        unset($this->messages[$packetId]);
    }

    /** publish() stopped waiting; the broker may still finish the exchange. */
    public function abandon(int $packetId, float $now): void
    {
        if (isset($this->messages[$packetId])) {
            $this->messages[$packetId]['abandonedAt'] = $now;
        }
    }

    /** The message went out again. */
    public function noteSent(int $packetId): void
    {
        if (isset($this->messages[$packetId])) {
            $this->messages[$packetId]['sends']++;
        }
    }

    /** How many times the message has gone out, or 0 if it is no longer in flight. */
    public function sends(int $packetId): int
    {
        return $this->messages[$packetId]['sends'] ?? 0;
    }

    public function has(int $packetId): bool
    {
        return isset($this->messages[$packetId]);
    }

    /**
     * The message under $packetId, if it is still in flight.
     *
     * @return array{publish: Publish, released: bool}|null
     */
    public function find(int $packetId): ?array
    {
        $message = $this->messages[$packetId] ?? null;

        return $message === null ? null : ['publish' => $message['publish'], 'released' => $message['released']];
    }

    /**
     * The Packet Identifiers to resend after a reconnect, in the order to resend them.
     *
     * @return list<int>
     */
    public function resendOrder(): array
    {
        return array_keys($this->messages);
    }

    /** The session is gone: messages nobody is waiting for go with it. */
    public function dropAbandoned(): void
    {
        foreach ($this->messages as $packetId => $message) {
            if ($message['abandonedAt'] !== null) {
                unset($this->messages[$packetId]);
            }
        }
    }

    /**
     * Everything in flight, in resend order, as plain data for the session store.
     *
     * @return list<array{packetId: int, topic: string, payload: string, qos: int, retain: bool, properties: array<string, mixed>|null, released: bool}>
     */
    public function export(): array
    {
        $out = [];
        foreach ($this->messages as $packetId => $message) {
            $publish = $message['publish'];
            $out[]   = [
                'packetId'   => $packetId,
                'topic'      => $publish->topic,
                'payload'    => $publish->payload,
                'qos'        => $publish->qos->value,
                'retain'     => $publish->retain,
                'properties' => $publish->properties,
                'released'   => $message['released'],
            ];
        }

        return $out;
    }

    /**
     * Take back what a previous process left in flight. Nobody is waiting for these, so they
     * count as abandoned from now: resent if the session resumed, forgotten if it did not.
     *
     * @param  list<array{packetId: int, topic: string, payload: string, qos: int, retain: bool, properties: array<string, mixed>|null, released: bool}>  $messages
     */
    public function restore(array $messages, float $now): void
    {
        foreach ($messages as $message) {
            $qos = QoS::tryFrom($message['qos']);
            if ($qos === null || $qos === QoS::AtMostOnce) {
                continue;
            }
            $this->messages[$message['packetId']] = [
                'publish'     => new Publish($message['topic'], $message['payload'], $qos, $message['retain'], packetId: $message['packetId'], properties: $message['properties']),
                'released'    => $message['released'],
                'abandonedAt' => $now,
                'sends'       => 1,
            ];
        }
    }

    public function count(): int
    {
        return count($this->messages);
    }
}
