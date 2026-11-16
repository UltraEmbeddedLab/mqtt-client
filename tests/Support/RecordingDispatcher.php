<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Tests\Support;

use Psr\EventDispatcher\EventDispatcherInterface;

use function array_filter;
use function array_values;

/** Keeps every event the client dispatches, for assertions on what it reported. */
final class RecordingDispatcher implements EventDispatcherInterface
{
    /** @var list<object> */
    public array $events = [];

    public function dispatch(object $event): object
    {
        $this->events[] = $event;

        return $event;
    }

    /**
     * The recorded events of one class, in order.
     *
     * @template T of object
     *
     * @param  class-string<T>  $class
     * @return list<T>
     */
    public function of(string $class): array
    {
        return array_values(array_filter($this->events, static fn (object $event): bool => $event instanceof $class));
    }
}
