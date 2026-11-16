<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Tests\Support;

use Psr\Log\AbstractLogger;
use Stringable;

/** Keeps every message logged to it, whatever its level. */
final class RecordingLogger extends AbstractLogger
{
    /** @var list<string> */
    public array $messages = [];

    public function log($level, string|Stringable $message, array $context = []): void
    {
        $this->messages[] = (string) $message;
    }
}
