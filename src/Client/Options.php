<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use InvalidArgumentException;
use ScienceStories\Mqtt\Contract\SessionStoreInterface;
use ScienceStories\Mqtt\Protocol\MqttVersion;

use function max;

/**
 * Options encapsulate connection settings for the MQTT client.
 * Immutable builder-style API.
 */
final class Options
{
    /** Largest Remaining Length the MQTT specification permits. */
    public const int PROTOCOL_MAXIMUM_PACKET_SIZE = 268_435_455;

    /** Conservative default: well above any broker's own limit, far below an OOM. */
    public const int DEFAULT_MAXIMUM_PACKET_SIZE = 16 * 1024 * 1024;

    /** Largest Keep Alive representable in the two-byte CONNECT field. */
    public const int MAXIMUM_KEEP_ALIVE = 65535;

    /** Generous enough that a consumer keeping up never sees it; small enough to bound memory. */
    public const int DEFAULT_INBOUND_QUEUE_SIZE = 1000;

    /**
     * @param  TlsOptions|array<string, mixed>|null  $tlsOptions  TlsOptions object or raw stream context array
     * @param  list<string>  $messageFilters
     */
    public function __construct(
        public string $host,
        public int $port = 1883,
        public string $clientId = '',
        public int $keepAlive = 60,
        public bool $cleanSession = true,
        public ?string $username = null,
        public ?string $password = null,
        public MqttVersion $version = MqttVersion::V3_1_1,
        public bool $useTls = false,
        public TlsOptions|array|null $tlsOptions = null,
        public ?WillOptions $will = null,
        // MQTT 5 session expiry (CONNECT property). Null = do not send property.
        public ?int $sessionExpiry = null,
        // Auto reconnect options
        public bool $autoReconnect = false,
        public int $reconnectMaxAttempts = 5,
        public float $reconnectBaseDelay = 0.2, // seconds
        public float $reconnectMaxDelay = 5.0,  // seconds
        public float $reconnectJitter = 0.2,    // +/- 20%
        public array $messageFilters = [],
        // MQTT 5.0 Topic Alias Maximum (0 = disabled, max 65535)
        public int $topicAliasMaximum = 0,
        // MQTT 5.0 Receive Maximum for flow control (1-65535, default 65535)
        public int $receiveMaximum = 65535,
        // Session persistence store
        public ?SessionStoreInterface $sessionStore = null,
        // QoS 1 de-duplication cache size (default 256)
        public int $qos1DeduplicationSize = 256,
        // Rate limiter (null = no rate limiting)
        public ?RateLimiter $rateLimiter = null,
        // Offline queue max size (0 = disabled, >0 = max buffered messages during disconnect)
        public int $offlineQueueSize = 0,
        // Timeout for QoS 1/2 ACK wait in seconds (default 5.0)
        public float $ackTimeout = 5.0,
        // Number of resend attempts for unacknowledged QoS 1/2 messages (default 3)
        public int $maxResendAttempts = 3,
        // Largest inbound packet accepted, in bytes of Remaining Length (default 16 MiB).
        // Enforced before the body is read, so a hostile broker cannot force a large
        // allocation by declaring one. Also advertised to MQTT 5 brokers as property 0x27.
        public int $maximumPacketSize = self::DEFAULT_MAXIMUM_PACKET_SIZE,
        // Bound on the queue backing awaitMessage()/messages() (0 = unlimited).
        // Bounded by default: in the push-only pattern (onMessage() plus a hand-rolled
        // loopOnce() loop) nothing drains it, so an unbounded queue is a slow OOM.
        public int $inboundQueueSize = self::DEFAULT_INBOUND_QUEUE_SIZE,
    ) {
        // The with*() setters validate, but the constructor is a public entry point too:
        // `new Options(host: 'x', maximumPacketSize: 0)` would otherwise reject the
        // client's own CONNACK, and a bad keepAlive would only surface at encode time.
        if ($keepAlive < 0 || $keepAlive > self::MAXIMUM_KEEP_ALIVE) {
            throw new InvalidArgumentException('Keep Alive must be between 0 and '.self::MAXIMUM_KEEP_ALIVE." seconds, got $keepAlive");
        }
        if ($maximumPacketSize < 1 || $maximumPacketSize > self::PROTOCOL_MAXIMUM_PACKET_SIZE) {
            throw new InvalidArgumentException('Maximum packet size must be between 1 and '.self::PROTOCOL_MAXIMUM_PACKET_SIZE." bytes, got $maximumPacketSize");
        }
        if ($inboundQueueSize < 0) {
            throw new InvalidArgumentException("Inbound queue size cannot be negative, got $inboundQueueSize");
        }
    }

    public function withHost(string $host, int $port = 1883): self
    {
        $c       = clone $this;
        $c->host = $host;
        $c->port = $port;

        return $c;
    }

    public function withClientId(string $id): self
    {
        $c           = clone $this;
        $c->clientId = $id;

        return $c;
    }

    public function withUser(string $username, ?string $password = null): self
    {
        $c           = clone $this;
        $c->username = $username;
        $c->password = $password;

        return $c;
    }

    /**
     * Keep Alive interval in seconds. 0 disables the mechanism.
     *
     * The CONNECT field is two bytes wide, so values above 65535 cannot be represented
     * and are rejected rather than silently truncated.
     *
     * @throws InvalidArgumentException if out of range
     */
    public function withKeepAlive(int $seconds): self
    {
        if ($seconds < 0 || $seconds > self::MAXIMUM_KEEP_ALIVE) {
            throw new InvalidArgumentException(
                'Keep Alive must be between 0 and '.self::MAXIMUM_KEEP_ALIVE." seconds, got $seconds",
            );
        }

        $c            = clone $this;
        $c->keepAlive = $seconds;

        return $c;
    }

    public function withCleanSession(bool $flag): self
    {
        $c               = clone $this;
        $c->cleanSession = $flag;

        return $c;
    }

    /**
     * MQTT 5 Session Expiry Interval (seconds). Null to omit property; 0 to expire immediately; >0 for persistent session.
     */
    public function withSessionExpiry(?int $seconds): self
    {
        $c = clone $this;
        if ($seconds !== null) {
            if ($seconds < 0) {
                $seconds = 0;
            }
            // Clamp to 32-bit unsigned max per MQTT 5
            if ($seconds > 0xFFFFFFFF) {
                $seconds = 0xFFFFFFFF;
            }
        }
        $c->sessionExpiry = $seconds;

        return $c;
    }

    /**
     * @param  TlsOptions|array<string, mixed>|null  $options  TlsOptions object or raw stream context array
     */
    public function withTls(TlsOptions|array|null $options = null): self
    {
        $c             = clone $this;
        $c->useTls     = true;
        $c->tlsOptions = $options;

        return $c;
    }

    /**
     * Timeout in seconds for waiting on QoS 1/2 acknowledgments before resending.
     */
    public function withAckTimeout(float $seconds): self
    {
        $c             = clone $this;
        $c->ackTimeout = max(0.1, $seconds);

        return $c;
    }

    /**
     * Maximum number of resend attempts for unacknowledged QoS 1/2 messages.
     */
    public function withMaxResendAttempts(int $attempts): self
    {
        $c                    = clone $this;
        $c->maxResendAttempts = max(0, $attempts);

        return $c;
    }

    public function withWill(?WillOptions $will): self
    {
        $c       = clone $this;
        $c->will = $will;

        return $c;
    }

    public function withAutoReconnect(
        bool $enable = true,
        int $maxAttempts = 5,
        float $baseDelay = 0.2,
        float $maxDelay = 5.0,
        float $jitter = 0.2,
    ): self {
        $c                       = clone $this;
        $c->autoReconnect        = $enable;
        $c->reconnectMaxAttempts = $maxAttempts;
        $c->reconnectBaseDelay   = $baseDelay;
        $c->reconnectMaxDelay    = $maxDelay;
        $c->reconnectJitter      = $jitter;

        return $c;
    }

    /**
     * Add a single client-side delivery filter (MQTT topic filter syntax: + and # supported).
     */
    public function withMessageFilter(string $filter): self
    {
        $f = trim($filter);
        $c = clone $this;
        if ($f !== '') {
            $c->messageFilters ??= [];
            $c->messageFilters[] = $f;
        }

        return $c;
    }

    /**
     * Replace client-side delivery filters with the provided list.
     *
     * @param  list<string>  $filters
     */
    public function withMessageFilters(array $filters): self
    {
        $out = [];
        foreach ($filters as $f) {
            $s = trim($f);
            if ($s !== '') {
                $out[] = $s;
            }
        }
        $c                 = clone $this;
        $c->messageFilters = $out;

        return $c;
    }

    /**
     * MQTT 5.0 Topic Alias Maximum.
     *
     * Sets the maximum number of topic aliases the client wants to use.
     * The broker may advertise a lower limit in CONNACK which takes precedence.
     *
     * @param int $maximum Maximum topic aliases (0 = disabled, max 65535)
     */
    public function withTopicAliasMaximum(int $maximum): self
    {
        $c = clone $this;
        if ($maximum < 0) {
            $maximum = 0;
        }
        if ($maximum > 65535) {
            $maximum = 65535;
        }
        $c->topicAliasMaximum = $maximum;

        return $c;
    }

    /**
     * MQTT 5.0 Receive Maximum for flow control.
     *
     * Limits the number of concurrent unacknowledged QoS 1/2 messages.
     * Sent in CONNECT properties. The broker's receive_maximum in CONNACK
     * determines the limit for messages sent to the broker.
     *
     * @param int $maximum Maximum concurrent QoS 1/2 messages (1-65535)
     */
    public function withReceiveMaximum(int $maximum): self
    {
        $c = clone $this;
        if ($maximum < 1) {
            $maximum = 1;
        }
        if ($maximum > 65535) {
            $maximum = 65535;
        }
        $c->receiveMaximum = $maximum;

        return $c;
    }

    /**
     * Set session persistence store.
     *
     * When set, session state (subscriptions, pending QoS 2 messages) will be
     * saved and restored across client restarts. Useful with cleanSession=false.
     *
     * @param SessionStoreInterface|null $store Session storage implementation
     */
    public function withSessionStore(?SessionStoreInterface $store): self
    {
        $c               = clone $this;
        $c->sessionStore = $store;

        return $c;
    }

    /**
     * Set the QoS 1 de-duplication cache size.
     *
     * Controls how many recently-seen QoS 1 Packet Identifiers are tracked
     * to suppress duplicate deliveries. Higher values use more memory but
     * reduce duplicate risk under high throughput.
     *
     * @param int $size Cache size (minimum 16, default 256)
     */
    public function withQos1DeduplicationSize(int $size): self
    {
        $c                        = clone $this;
        $c->qos1DeduplicationSize = max(16, $size);

        return $c;
    }

    /**
     * Set a rate limiter for outbound publishes.
     *
     * @param RateLimiter|null $limiter Rate limiter instance (null to disable)
     */
    public function withRateLimiter(?RateLimiter $limiter): self
    {
        $c              = clone $this;
        $c->rateLimiter = $limiter;

        return $c;
    }

    /**
     * Enable offline message queue for buffering publishes during disconnects.
     *
     * @param int $maxSize Maximum number of queued messages (0 = disabled)
     */
    public function withOfflineQueue(int $maxSize): self
    {
        $c                   = clone $this;
        $c->offlineQueueSize = max(0, $maxSize);

        return $c;
    }

    /**
     * Largest inbound packet the client will accept, in bytes of Remaining Length.
     *
     * Checked against the declared Remaining Length before the body is read, so a broker
     * cannot force a large allocation by lying about it. On MQTT 5 the value is also sent
     * as the Maximum Packet Size property (0x27) so the broker knows not to exceed it.
     *
     * @param int $bytes 1..268435455
     *
     * @throws InvalidArgumentException if out of range
     */
    public function withMaximumPacketSize(int $bytes): self
    {
        if ($bytes < 1 || $bytes > self::PROTOCOL_MAXIMUM_PACKET_SIZE) {
            throw new InvalidArgumentException(
                'Maximum packet size must be between 1 and '.self::PROTOCOL_MAXIMUM_PACKET_SIZE." bytes, got $bytes",
            );
        }

        $c                    = clone $this;
        $c->maximumPacketSize = $bytes;

        return $c;
    }

    /**
     * Bound the queue that backs awaitMessage() and messages().
     *
     * Only relevant for pull-style consumption: when an onMessage() handler is registered
     * the handler consumes each message and nothing is queued. When the bound is reached
     * the oldest message is dropped and counted on the `inbound_queue_dropped` metric.
     *
     * @param int $maxMessages Maximum buffered messages (0 = unlimited)
     *
     * @throws InvalidArgumentException if negative
     */
    public function withInboundQueueSize(int $maxMessages): self
    {
        if ($maxMessages < 0) {
            throw new InvalidArgumentException("Inbound queue size cannot be negative, got $maxMessages");
        }

        $c                   = clone $this;
        $c->inboundQueueSize = $maxMessages;

        return $c;
    }
}
