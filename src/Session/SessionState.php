<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Session;

use ScienceStories\Mqtt\Client\SubscribeOptions;

use function array_filter;
use function base64_decode;
use function base64_encode;
use function count;
use function is_array;
use function is_bool;
use function is_int;
use function is_string;
use function restore_error_handler;
use function serialize;
use function set_error_handler;
use function time;
use function unserialize;

use const ARRAY_FILTER_USE_KEY;

/**
 * Data transfer object representing MQTT session state for persistence.
 *
 * Stores the client's half of the session (MQTT-4.1.0-1), so that a process restarted with
 * a persistent session can carry on where the previous one stopped:
 * - Subscriptions: topic filters, their QoS and options — resubscribed after a reconnect
 * - Outbound: QoS 1/2 messages published and not yet fully acknowledged — resent, in order,
 *   once the broker confirms it still has the session
 * - Inbound QoS 2: messages received and answered with PUBREC, awaiting the broker's
 *   PUBREL — delivered when it arrives, instead of being acknowledged unseen
 * - Timestamp: when the session was last saved
 *
 * Usage:
 * ```php
 * $state = new SessionState(
 *     subscriptions: [
 *         'sensors/+/temp' => ['qos' => 1, 'options' => null],
 *         'alerts/#' => ['qos' => 2, 'options' => $opts],
 *     ],
 *     savedAt: time(),
 * );
 *
 * // Check if expired
 * if ($state->isExpired(3600)) { // 1 hour expiry
 *     $store->delete($clientId);
 * }
 * ```
 *
 * toArray() is JSON-safe: payloads are base64, and MQTT 5 properties — which may hold
 * binary Correlation Data — are serialized and base64-encoded. A store that keeps the array
 * as JSON keeps message payloads on disk; keep that location private.
 */
final class SessionState
{
    /**
     * @param array<string, array{qos: int, options: SubscribeOptions|null}> $subscriptions Active subscriptions (filter => settings)
     * @param list<int> $pendingQos2 Packet Identifiers of $inboundQos2, kept for stores and readers written before it
     * @param int $savedAt Unix timestamp when session was saved
     * @param list<array{packetId: int, topic: string, payload: string, qos: int, retain: bool, properties: array<string, mixed>|null, released: bool}> $outbound
     *                                                                                                                                                           Published and not fully acknowledged, in the order to resend them;
     *                                                                                                                                                           `released`: PUBREC arrived, so only the PUBREL is resent
     * @param list<array{packetId: int, topic: string, payload: string, qos: int, retain: bool, properties: array<string, mixed>|null}> $inboundQos2
     *                                                                                                                                         QoS 2 messages received (PUBREC sent) and not yet released by the broker
     */
    public function __construct(
        public array $subscriptions = [],
        public array $pendingQos2 = [],
        public int $savedAt = 0,
        public array $outbound = [],
        public array $inboundQos2 = [],
    ) {
        if ($this->savedAt === 0) {
            $this->savedAt = time();
        }
    }

    /**
     * Check if the session has expired.
     *
     * @param int $expirySeconds Session expiry interval in seconds. 0 = never expires.
     */
    public function isExpired(int $expirySeconds): bool
    {
        if ($expirySeconds === 0) {
            return false;
        }

        return time() - $this->savedAt > $expirySeconds;
    }

    /**
     * Get the age of the session in seconds.
     */
    public function getAge(): int
    {
        return time() - $this->savedAt;
    }

    /**
     * Check if there are any active subscriptions.
     */
    public function hasSubscriptions(): bool
    {
        return count($this->subscriptions) > 0;
    }

    /**
     * Check if there are any pending QoS 2 messages.
     */
    public function hasPendingQos2(): bool
    {
        return count($this->pendingQos2) > 0 || count($this->inboundQos2) > 0;
    }

    /**
     * Get subscription count.
     */
    public function getSubscriptionCount(): int
    {
        return count($this->subscriptions);
    }

    /**
     * Serialize state to a JSON-safe array for storage.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $subscriptions = [];
        foreach ($this->subscriptions as $filter => $subscription) {
            $options                = $subscription['options'];
            $subscriptions[$filter] = [
                'qos' => $subscription['qos'],
                // The shape json_encode() gave the object before, so older files still read.
                'options' => $options instanceof SubscribeOptions ? [
                    'noLocal'           => $options->noLocal,
                    'retainAsPublished' => $options->retainAsPublished,
                    'retainHandling'    => $options->retainHandling,
                    'properties'        => $this->encodeProperties($options->properties),
                ] : null,
            ];
        }

        $outbound = [];
        foreach ($this->outbound as $message) {
            $outbound[] = $this->encodeMessage($message) + ['released' => $message['released']];
        }

        $inbound = [];
        foreach ($this->inboundQos2 as $message) {
            $inbound[] = $this->encodeMessage($message);
        }

        return [
            'subscriptions' => $subscriptions,
            'pending_qos2'  => $this->pendingQos2,
            'saved_at'      => $this->savedAt,
            'outbound'      => $outbound,
            'inbound_qos2'  => $inbound,
        ];
    }

    /**
     * Create state from array (deserialization). Entries of the wrong shape are skipped
     * rather than trusted.
     *
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $subscriptions = [];
        foreach (is_array($data['subscriptions'] ?? null) ? $data['subscriptions'] : [] as $filter => $subscription) {
            // A filter such as "42" comes back from JSON as an int key; it is still a filter.
            if (! is_array($subscription) || ! is_int($subscription['qos'] ?? null)) {
                continue;
            }
            $subscriptions[(string) $filter] = ['qos' => $subscription['qos'], 'options' => self::decodeOptions($subscription['options'] ?? null)];
        }

        $pendingQos2 = [];
        foreach (is_array($data['pending_qos2'] ?? null) ? $data['pending_qos2'] : [] as $packetId) {
            if (is_int($packetId)) {
                $pendingQos2[] = $packetId;
            }
        }

        $outbound = [];
        foreach (is_array($data['outbound'] ?? null) ? $data['outbound'] : [] as $entry) {
            if (! is_array($entry) || ! is_bool($entry['released'] ?? null)) {
                continue;
            }
            $message = self::decodeMessage($entry);
            if ($message !== null) {
                $outbound[] = $message + ['released' => $entry['released']];
            }
        }

        $inbound = [];
        foreach (is_array($data['inbound_qos2'] ?? null) ? $data['inbound_qos2'] : [] as $entry) {
            $message = is_array($entry) ? self::decodeMessage($entry) : null;
            if ($message !== null) {
                $inbound[] = $message;
            }
        }

        return new self(
            subscriptions: $subscriptions,
            pendingQos2: $pendingQos2,
            savedAt: is_int($data['saved_at'] ?? null) ? $data['saved_at'] : 0,
            outbound: $outbound,
            inboundQos2: $inbound,
        );
    }

    /**
     * @param array{packetId: int, topic: string, payload: string, qos: int, retain: bool, properties: array<string, mixed>|null} $message
     * @return array{packet_id: int, topic: string, payload: string, qos: int, retain: bool, properties: string|null}
     */
    private function encodeMessage(array $message): array
    {
        return [
            'packet_id'  => $message['packetId'],
            'topic'      => $message['topic'],
            'payload'    => base64_encode($message['payload']),
            'qos'        => $message['qos'],
            'retain'     => $message['retain'],
            'properties' => $this->encodeProperties($message['properties']),
        ];
    }

    /**
     * @return array{packetId: int, topic: string, payload: string, qos: int, retain: bool, properties: array<string, mixed>|null}|null
     * @param array<array-key, mixed> $entry  one stored message, as decoded from JSON
     */
    private static function decodeMessage(array $entry): ?array
    {
        $packetId = $entry['packet_id'] ?? null;
        $topic    = $entry['topic']     ?? null;
        $payload  = is_string($entry['payload'] ?? null) ? base64_decode($entry['payload'], true) : false;
        $qos      = $entry['qos']    ?? null;
        $retain   = $entry['retain'] ?? null;
        if (! is_int($packetId) || $packetId < 1 || $packetId > 0xFFFF || ! is_string($topic) || $topic === ''
                                || $payload                                                             === false || ($qos !== 1 && $qos !== 2) || ! is_bool($retain)) {
            return null;
        }

        return [
            'packetId'   => $packetId,
            'topic'      => $topic,
            'payload'    => $payload,
            'qos'        => $qos,
            'retain'     => $retain,
            'properties' => self::decodeProperties($entry['properties'] ?? null),
        ];
    }

    private static function decodeOptions(mixed $options): ?SubscribeOptions
    {
        if ($options instanceof SubscribeOptions) {
            return $options;
        }
        if (! is_array($options)) {
            return null;
        }

        return new SubscribeOptions(
            noLocal: ($options['noLocal'] ?? false)                     === true,
            retainAsPublished: ($options['retainAsPublished'] ?? false) === true,
            retainHandling: is_int($options['retainHandling'] ?? null) ? $options['retainHandling'] : 0,
            properties: self::decodeProperties($options['properties'] ?? null),
        );
    }

    /**
     * MQTT 5 properties can hold binary data (Correlation Data), which JSON cannot carry, so
     * they are serialized and base64-encoded as a whole.
     *
     * @param array<string, mixed>|null $properties
     */
    private function encodeProperties(?array $properties): ?string
    {
        return $properties === null ? null : base64_encode(serialize($properties));
    }

    /**
     * Reverses encodeProperties(). Objects are never revived: properties are scalars and
     * arrays, and a stored file is data, not code. Also accepts a plain array, as files
     * written before properties were encoded hold.
     *
     * @return array<string, mixed>|null
     */
    private static function decodeProperties(mixed $encoded): ?array
    {
        if (is_string($encoded)) {
            $raw = base64_decode($encoded, true);
            // Damaged data is skipped, not reported: a warning — even one silenced with @ —
            // reaches the application's error handler, which may well make it fatal.
            set_error_handler(static fn (): bool => true);
            try {
                $encoded = $raw === false ? null : unserialize($raw, ['allowed_classes' => false]);
            } finally {
                restore_error_handler();
            }
        }
        if (! is_array($encoded)) {
            return null;
        }

        // Properties are named; anything else in the array is not one.
        return array_filter($encoded, is_string(...), ARRAY_FILTER_USE_KEY);
    }
}
