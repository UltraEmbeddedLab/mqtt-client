<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use SplQueue;

/**
 * Offline message queue for buffering publishes during disconnects.
 *
 * Messages are queued when the transport is not connected and drained
 * automatically upon successful reconnection.
 */
final class OfflineQueue
{
    /** @var SplQueue<array{topic: string, payload: string, options: PublishOptions}> */
    private readonly SplQueue $queue;

    private int $count = 0;

    /**
     * @param int $maxSize Maximum number of queued messages (0 = unlimited)
     */
    public function __construct(
        private readonly int $maxSize = 1000,
    ) {
        $this->queue = new SplQueue();
    }

    /**
     * Enqueue a message for later delivery.
     *
     * @return bool true if queued, false if queue is full
     */
    public function enqueue(string $topic, string $payload, PublishOptions $options): bool
    {
        if ($this->maxSize > 0 && $this->count >= $this->maxSize) {
            return false;
        }

        $this->queue->enqueue(['topic' => $topic, 'payload' => $payload, 'options' => $options]);
        $this->count++;

        return true;
    }

    /**
     * Dequeue the next message.
     *
     * @return array{topic: string, payload: string, options: PublishOptions}|null
     */
    public function dequeue(): ?array
    {
        if ($this->queue->isEmpty()) {
            return null;
        }

        $this->count--;

        /** @var array{topic: string, payload: string, options: PublishOptions} */
        return $this->queue->dequeue();
    }

    /**
     * Put a message back at the head of the queue.
     *
     * For a message just dequeued that could not be sent: it keeps its place in line and is
     * not lost. The bound is not checked — the slot was the message's own a moment ago.
     *
     * @param  array{topic: string, payload: string, options: PublishOptions}  $message
     */
    public function requeue(array $message): void
    {
        $this->queue->unshift($message);
        $this->count++;
    }

    public function isEmpty(): bool
    {
        return $this->queue->isEmpty();
    }

    public function count(): int
    {
        return $this->count;
    }

    public function clear(): void
    {
        while (!$this->queue->isEmpty()) {
            $this->queue->dequeue();
        }
        $this->count = 0;
    }
}
