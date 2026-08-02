<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use function count;

/**
 * Manages MQTT 5.0 Topic Aliases for efficient bandwidth usage.
 *
 * Topic aliases allow replacing a topic string with a 2-byte integer,
 * significantly reducing bandwidth for repeated publishes to the same topic.
 *
 * MQTT 5.0 Topic Alias Behavior:
 * - The broker advertises topic_alias_maximum in CONNACK
 * - Client can assign aliases from 1 to topic_alias_maximum
 * - First publish with topic and alias establishes the mapping
 * - Subsequent publishes can use alias only (empty topic)
 * - Aliases are connection-scoped and reset on reconnect
 *
 * Usage:
 * ```php
 * $manager = new TopicAliasManager(10); // max 10 aliases
 *
 * // First publish - establishes alias
 * $alias = $manager->getOrCreateAlias('sensors/temperature'); // returns 1
 * // Publish with topic='sensors/temperature', alias=1
 *
 * // Second publish - reuses alias
 * $alias = $manager->getOrCreateAlias('sensors/temperature'); // returns 1
 * // Publish with topic='', alias=1 (topic can be omitted)
 *
 * // Resolve incoming alias
 * $topic = $manager->resolveAlias(1); // returns 'sensors/temperature'
 * ```
 */
final class TopicAliasManager
{
    /** @var array<string, int> Topic to alias mapping */
    private array $topicToAlias = [];

    /** @var array<int, string> Alias to topic mapping (for inbound resolution) */
    private array $aliasToTopic = [];

    /** @var int Next alias to assign */
    private int $nextAlias = 1;

    /**
     * @param int $maxAliases Maximum topic aliases (from broker's topic_alias_maximum).
     *                        Use 0 to disable topic aliases.
     */
    public function __construct(
        public private(set) int $maxAliases = 0 {
            set => max(0, $value);
        },
    ) {
    }

    /**
     * Get or create an alias for a topic.
     *
     * Behavior:
     * - If topic already has an alias, returns it (for reuse)
     * - If no alias exists and slots available, creates new alias
     * - If no slots available, returns null (publish without alias)
     *
     * @param string $topic The topic to get/create alias for
     * @return array{alias: int|null, isNew: bool} The alias (if any) and whether it's newly created
     */
    public function getOrCreateAlias(string $topic): array
    {
        // Disabled or empty topic
        if ($this->maxAliases === 0 || $topic === '') {
            return ['alias' => null, 'isNew' => false];
        }

        // Already has an alias
        if (isset($this->topicToAlias[$topic])) {
            return ['alias' => $this->topicToAlias[$topic], 'isNew' => false];
        }

        // No slots available
        if ($this->nextAlias > $this->maxAliases) {
            return ['alias' => null, 'isNew' => false];
        }

        // Create new alias
        $alias                      = $this->nextAlias++;
        $this->topicToAlias[$topic] = $alias;
        $this->aliasToTopic[$alias] = $topic;

        return ['alias' => $alias, 'isNew' => true];
    }

    /**
     * Resolve an alias to its topic (for inbound messages).
     *
     * @param int $alias The alias to resolve
     * @return string|null The topic, or null if alias not found
     */
    public function resolveAlias(int $alias): ?string
    {
        return $this->aliasToTopic[$alias] ?? null;
    }

    /**
     * Register an alias for a topic (for inbound aliases from broker).
     *
     * @param int $alias The alias number
     * @param string $topic The topic string
     */
    public function registerAlias(int $alias, string $topic): void
    {
        if ($alias < 1 || $topic === '') {
            return;
        }

        // Clean up old topic mapping if alias was previously mapped to a different topic
        if (isset($this->aliasToTopic[$alias])) {
            $oldTopic = $this->aliasToTopic[$alias];
            if ($oldTopic !== $topic) {
                unset($this->topicToAlias[$oldTopic]);
            }
        }

        // Clean up old alias mapping if topic was previously mapped to a different alias
        if (isset($this->topicToAlias[$topic])) {
            $oldAlias = $this->topicToAlias[$topic];
            if ($oldAlias !== $alias) {
                unset($this->aliasToTopic[$oldAlias]);
            }
        }

        $this->aliasToTopic[$alias] = $topic;
        $this->topicToAlias[$topic] = $alias;
    }

    /**
     * Check if a topic has an existing alias.
     */
    public function hasAlias(string $topic): bool
    {
        return isset($this->topicToAlias[$topic]);
    }

    /**
     * Get the alias for a topic without creating one.
     */
    public function getAlias(string $topic): ?int
    {
        return $this->topicToAlias[$topic] ?? null;
    }

    /**
     * Reset all aliases.
     * Should be called on disconnect/reconnect as aliases are connection-scoped.
     */
    public function reset(): void
    {
        $this->topicToAlias = [];
        $this->aliasToTopic = [];
        $this->nextAlias    = 1;
    }

    /**
     * Get current alias count.
     */
    public function getAliasCount(): int
    {
        return count($this->topicToAlias);
    }

    /**
     * Check if more aliases can be created.
     */
    public function hasAvailableSlots(): bool
    {
        return $this->nextAlias <= $this->maxAliases;
    }

}
