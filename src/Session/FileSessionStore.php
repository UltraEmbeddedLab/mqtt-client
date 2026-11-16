<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Session;

use JsonException;
use RuntimeException;
use ScienceStories\Mqtt\Contract\SessionStoreInterface;

use function chmod;
use function file_exists;
use function file_get_contents;
use function file_put_contents;
use function is_array;
use function is_dir;
use function is_writable;
use function json_decode;
use function json_encode;
use function mkdir;
use function preg_match;
use function sha1;
use function unlink;

use const JSON_THROW_ON_ERROR;
use const LOCK_EX;

/**
 * File-based session persistence store.
 *
 * Stores MQTT session state as JSON files in a directory.
 * Each client gets its own file based on a hash of its client ID.
 *
 * File Format (see SessionState::toArray()):
 * ```json
 * {
 *     "subscriptions": {
 *         "sensors/+/temp": {"qos": 1, "options": null}
 *     },
 *     "pending_qos2": [1234],
 *     "saved_at": 1699999999,
 *     "outbound": [{"packet_id": 7, "topic": "a", "payload": "<base64>", "qos": 2, "retain": false, "properties": null, "released": true}],
 *     "inbound_qos2": [{"packet_id": 1234, "topic": "b", "payload": "<base64>", "qos": 2, "retain": false, "properties": null}]
 * }
 * ```
 *
 * Usage:
 * ```php
 * $store = new FileSessionStore('/var/mqtt/sessions');
 *
 * // Or with custom expiry (cleanup stale sessions)
 * $store = new FileSessionStore('/var/mqtt/sessions', 86_400); // 24h expiry
 *
 * // Use with client options
 * $options = (new Options('broker.local'))
 *     ->withSessionStore($store)
 *     ->withCleanSession(false);
 * ```
 *
 * Security Notes:
 * - Session directory should not be web-accessible
 * - Files hold topic filters and the payloads of messages still in flight when the client
 *   disconnected — QoS 1/2 messages not yet acknowledged, in either direction — so give the
 *   directory the same protection as the data itself
 */
final readonly class FileSessionStore implements SessionStoreInterface
{
    public string $directory;

    public int $defaultExpirySeconds;

    /**
     * @param string $directory Directory to store session files
     * @param int $defaultExpirySeconds Default session expiry (0 = never expires)
     */
    public function __construct(string $directory, int $defaultExpirySeconds = 0)
    {
        $this->directory            = rtrim($directory, '/\\');
        $this->defaultExpirySeconds = max(0, $defaultExpirySeconds);
        $this->ensureDirectory();
    }

    /**
     * @throws JsonException
     */
    public function save(string $clientId, SessionState $state): void
    {
        $path = $this->getPath($clientId);
        $data = json_encode($state->toArray(), JSON_THROW_ON_ERROR);

        $written = file_put_contents($path, $data, LOCK_EX);
        if ($written === false) {
            throw new RuntimeException("Failed to write session file: $path");
        }
        // It may hold message payloads: readable by the owner only.
        @chmod($path, 0600);
    }

    public function load(string $clientId): ?SessionState
    {
        $path = $this->getPath($clientId);

        if (! file_exists($path)) {
            return null;
        }

        $contents = file_get_contents($path);
        if ($contents === false) {
            return null;
        }

        try {
            $data = json_decode($contents, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return null;
        }

        if (! is_array($data)) {
            return null;
        }

        /** @var array<string, mixed> $data */
        $state = SessionState::fromArray($data);

        // Check expiry
        if ($this->defaultExpirySeconds > 0 && $state->isExpired($this->defaultExpirySeconds)) {
            $this->delete($clientId);

            return null;
        }

        return $state;
    }

    public function delete(string $clientId): void
    {
        $path = $this->getPath($clientId);

        if (file_exists($path)) {
            unlink($path);
        }
    }

    public function exists(string $clientId): bool
    {
        $path = $this->getPath($clientId);

        if (! file_exists($path)) {
            return false;
        }

        // Also check expiry
        if ($this->defaultExpirySeconds > 0) {
            $state = $this->load($clientId);

            return $state instanceof SessionState;
        }

        return true;
    }

    /**
     * Get the file path for a client's session.
     */
    private function getPath(string $clientId): string
    {
        // Use hash to ensure valid filename and avoid directory traversal
        $safeId = $this->sanitizeClientId($clientId);

        return $this->directory.'/'.$safeId.'.json';
    }

    /**
     * Sanitize client ID to a safe filename.
     */
    private function sanitizeClientId(string $clientId): string
    {
        // If client ID is simple alphanumeric, use it directly (max 64 chars)
        if (preg_match('/^[a-zA-Z0-9_-]{1,64}$/', $clientId)) {
            return $clientId;
        }

        // Otherwise hash it for safety
        return 'mqtt_'.sha1($clientId);
    }

    /**
     * Ensure the storage directory exists and is writable.
     */
    private function ensureDirectory(): void
    {
        if (! is_dir($this->directory)) {
            // Session files may hold message payloads: the directory is the owner's alone.
            $created = mkdir($this->directory, 0700, true);
            if (! $created) {
                throw new RuntimeException("Failed to create session directory: $this->directory");
            }
        }

        if (! is_writable($this->directory)) {
            throw new RuntimeException("Session directory is not writable: $this->directory");
        }
    }

    /**
     * Clean up expired sessions from the directory.
     *
     * @return int Number of expired sessions removed
     */
    public function cleanupExpired(): int
    {
        if ($this->defaultExpirySeconds === 0) {
            return 0;
        }

        $removed = 0;
        $files   = glob($this->directory.'/*.json');

        if ($files === false) {
            return 0;
        }

        foreach ($files as $file) {
            $contents = file_get_contents($file);
            if ($contents === false) {
                continue;
            }

            try {
                $data = json_decode($contents, true, 512, JSON_THROW_ON_ERROR);
                if (! is_array($data)) {
                    continue;
                }

                /** @var array<string, mixed> $data */
                $state = SessionState::fromArray($data);
                if ($state->isExpired($this->defaultExpirySeconds)) {
                    unlink($file);
                    $removed++;
                }
            } catch (JsonException) {
                // Skip malformed files
            }
        }

        return $removed;
    }
}
