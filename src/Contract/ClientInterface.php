<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Contract;

use Generator;
use ScienceStories\Mqtt\Client\ConnectResult;
use ScienceStories\Mqtt\Client\InboundMessage;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Client\SubscribeOptions;
use ScienceStories\Mqtt\Client\SubscribeResult;

interface ClientInterface
{
    public function connect(): ConnectResult;

    public function disconnect(string $reason = ''): void;

    public function publish(string $topic, string $payload, ?PublishOptions $options = null): int;

    /**
     * Send a PINGREQ and wait for PINGRESP.
     *
     * @return bool true on successful round-trip
     */
    public function ping(?float $timeoutSec = 5.0): bool;

    /**
     * Subscribe to topic filters at one QoS.
     *
     * A filter the broker refuses in its SUBACK is not remembered for resubscription after a
     * reconnect; an `Events\SubscriptionRefused` is dispatched for it. Use subscribeWith() to
     * get the SUBACK codes back.
     *
     * @param non-empty-list<string> $topics
     */
    public function subscribe(array $topics, int $qos = 0): void;

    /**
     * Subscribe to topic filters, each with its own QoS, and return the SUBACK codes.
     *
     * Codes are in the order of the filters sent; empty filters are never sent. A refused
     * filter (code 0x80 or above) is not remembered for resubscription — an earlier grant of
     * the same filter is kept — and dispatches an `Events\SubscriptionRefused`. A granted one
     * is resubscribed after a reconnect with the QoS requested here, not the possibly lower
     * one the broker granted.
     *
     * @param  non-empty-list<array{filter:string,qos:int}>  $filters
     */
    public function subscribeWith(array $filters, ?SubscribeOptions $options = null): SubscribeResult;

    /** @param non-empty-list<string> $topics */
    public function unsubscribe(array $topics): void;

    /**
     * Poll underlying transport once and process a single packet if available.
     *
     * @return bool true if a packet was processed, false on timeout
     */
    public function loopOnce(?float $timeoutSec = 0.1): bool;

    /** Short alias for loopOnce(0.0). */
    public function tick(): bool;

    /** Request cooperative stop for run()/message generators. */
    public function stop(): void;

    /**
     * Blocks up to timeoutSec for next message; returns null on timeout.
     */
    public function awaitMessage(?float $timeoutSec = null): ?InboundMessage;

    /**
     * Generator that yields inbound messages until stop() is called or the generator is broken.
     * Each iteration waits up to timeoutSec for a message; if none arrives, it continues.
     *
     * @return Generator<int, InboundMessage, mixed, void>
     */
    public function messages(?float $timeoutSec = 0.2): Generator;

    /** Set a message handler; invoked for each inbound PUBLISH. */
    public function onMessage(callable $handler): void;

    /** Optional convenience loop. */
    public function run(callable $onMessage, ?float $idleSleep = 0.01): void;

    /** Total bytes sent to the broker across all connections. */
    public function bytesSent(): int;

    /** Total bytes received from the broker across all connections. */
    public function bytesReceived(): int;
}
