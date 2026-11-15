<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use InvalidArgumentException;
use NoDiscard;
use ScienceStories\Mqtt\Contract\SessionStoreInterface;
use ScienceStories\Mqtt\Protocol\MqttVersion;

use function max;
use function min;
use function trim;

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

    /** Long enough to ride out a slow broker, short enough to notice a black-holed link. */
    public const float DEFAULT_PING_RESPONSE_TIMEOUT = 10.0;

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
        // How long to wait for a PINGRESP before declaring the connection dead.
        // Without a deadline a single lost PINGRESP latches the outstanding-ping flag and
        // keepalive stops for the life of the process, on a socket that still reports open.
        public float $pingResponseTimeout = self::DEFAULT_PING_RESPONSE_TIMEOUT,
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
        if ($pingResponseTimeout <= 0) {
            throw new InvalidArgumentException("Ping response timeout must be greater than 0, got $pingResponseTimeout");
        }
    }

    /**
     * How long to wait for a PINGRESP before treating the connection as dead.
     *
     * On expiry the client closes the transport, which is what lets auto-reconnect take
     * over. Without this a black-holed link (NAT eviction, a hard-killed broker) leaves the
     * client sitting on a socket that still reports open, receiving nothing, forever.
     *
     * @throws InvalidArgumentException if not greater than zero
     */
    #[NoDiscard]
    public function withPingResponseTimeout(float $seconds): self
    {
        if ($seconds <= 0) {
            throw new InvalidArgumentException("Ping response timeout must be greater than 0, got $seconds");
        }

        return clone($this, ['pingResponseTimeout' => $seconds]);
    }

    #[NoDiscard]
    public function withHost(string $host, int $port = 1883): self
    {
        return clone($this, [
            'host' => $host,
            'port' => $port,
        ]);
    }

    #[NoDiscard]
    public function withClientId(string $id): self
    {
        return clone($this, ['clientId' => $id]);
    }

    #[NoDiscard]
    public function withUser(string $username, ?string $password = null): self
    {
        return clone($this, [
            'username' => $username,
            'password' => $password,
        ]);
    }

    /**
     * Keep Alive interval in seconds. 0 disables the mechanism.
     *
     * The CONNECT field is two bytes wide, so values above 65535 cannot be represented
     * and are rejected rather than silently truncated.
     *
     * @throws InvalidArgumentException if out of range
     */
    #[NoDiscard]
    public function withKeepAlive(int $seconds): self
    {
        if ($seconds < 0 || $seconds > self::MAXIMUM_KEEP_ALIVE) {
            throw new InvalidArgumentException(
                'Keep Alive must be between 0 and '.self::MAXIMUM_KEEP_ALIVE." seconds, got $seconds",
            );
        }

        return clone($this, ['keepAlive' => $seconds]);
    }

    #[NoDiscard]
    public function withCleanSession(bool $flag): self
    {
        return clone($this, ['cleanSession' => $flag]);
    }

    /**
     * MQTT 5 Session Expiry Interval (seconds). Null to omit property; 0 to expire immediately; >0 for persistent session.
     */
    #[NoDiscard]
    public function withSessionExpiry(?int $seconds): self
    {
        // Clamp to the 32-bit unsigned range of the MQTT 5 property.
        return clone($this, ['sessionExpiry' => $seconds === null ? null : max(0, min($seconds, 0xFFFFFFFF))]);
    }

    /**
     * @param  TlsOptions|array<string, mixed>|null  $options  TlsOptions object or raw stream context array
     */
    #[NoDiscard]
    public function withTls(TlsOptions|array|null $options = null): self
    {
        return clone($this, [
            'useTls'     => true,
            'tlsOptions' => $options,
        ]);
    }

    /**
     * Timeout in seconds for waiting on QoS 1/2 acknowledgments before resending.
     */
    #[NoDiscard]
    public function withAckTimeout(float $seconds): self
    {
        return clone($this, ['ackTimeout' => max(0.1, $seconds)]);
    }

    /**
     * Maximum number of resend attempts for unacknowledged QoS 1/2 messages.
     */
    #[NoDiscard]
    public function withMaxResendAttempts(int $attempts): self
    {
        return clone($this, ['maxResendAttempts' => max(0, $attempts)]);
    }

    #[NoDiscard]
    public function withWill(?WillOptions $will): self
    {
        return clone($this, ['will' => $will]);
    }

    #[NoDiscard]
    public function withAutoReconnect(
        bool $enable = true,
        int $maxAttempts = 5,
        float $baseDelay = 0.2,
        float $maxDelay = 5.0,
        float $jitter = 0.2,
    ): self {
        return clone($this, [
            'autoReconnect'        => $enable,
            'reconnectMaxAttempts' => $maxAttempts,
            'reconnectBaseDelay'   => $baseDelay,
            'reconnectMaxDelay'    => $maxDelay,
            'reconnectJitter'      => $jitter,
        ]);
    }

    /**
     * Add a single client-side delivery filter (MQTT topic filter syntax: + and # supported).
     */
    #[NoDiscard]
    public function withMessageFilter(string $filter): self
    {
        $f = trim($filter);

        return $f === '' ? clone $this : clone($this, ['messageFilters' => [...$this->messageFilters, $f]]);
    }

    /**
     * Replace client-side delivery filters with the provided list.
     *
     * @param  list<string>  $filters
     */
    #[NoDiscard]
    public function withMessageFilters(array $filters): self
    {
        $out = [];
        foreach ($filters as $f) {
            $s = trim($f);
            if ($s !== '') {
                $out[] = $s;
            }
        }
        return clone($this, ['messageFilters' => $out]);
    }

    /**
     * MQTT 5.0 Topic Alias Maximum.
     *
     * Sets the maximum number of topic aliases the client wants to use.
     * The broker may advertise a lower limit in CONNACK which takes precedence.
     *
     * @param int $maximum Maximum topic aliases (0 = disabled, max 65535)
     */
    #[NoDiscard]
    public function withTopicAliasMaximum(int $maximum): self
    {
        return clone($this, ['topicAliasMaximum' => max(0, min($maximum, 65535))]);
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
    #[NoDiscard]
    public function withReceiveMaximum(int $maximum): self
    {
        return clone($this, ['receiveMaximum' => max(1, min($maximum, 65535))]);
    }

    /**
     * Set session persistence store.
     *
     * When set, session state (subscriptions, pending QoS 2 messages) will be
     * saved and restored across client restarts. Useful with cleanSession=false.
     *
     * @param SessionStoreInterface|null $store Session storage implementation
     */
    #[NoDiscard]
    public function withSessionStore(?SessionStoreInterface $store): self
    {
        return clone($this, ['sessionStore' => $store]);
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
    #[NoDiscard]
    public function withQos1DeduplicationSize(int $size): self
    {
        return clone($this, ['qos1DeduplicationSize' => max(16, $size)]);
    }

    /**
     * Set a rate limiter for outbound publishes.
     *
     * @param RateLimiter|null $limiter Rate limiter instance (null to disable)
     */
    #[NoDiscard]
    public function withRateLimiter(?RateLimiter $limiter): self
    {
        return clone($this, ['rateLimiter' => $limiter]);
    }

    /**
     * Enable offline message queue for buffering publishes during disconnects.
     *
     * @param int $maxSize Maximum number of queued messages (0 = disabled)
     */
    #[NoDiscard]
    public function withOfflineQueue(int $maxSize): self
    {
        return clone($this, ['offlineQueueSize' => max(0, $maxSize)]);
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
    #[NoDiscard]
    public function withMaximumPacketSize(int $bytes): self
    {
        if ($bytes < 1 || $bytes > self::PROTOCOL_MAXIMUM_PACKET_SIZE) {
            throw new InvalidArgumentException(
                'Maximum packet size must be between 1 and '.self::PROTOCOL_MAXIMUM_PACKET_SIZE." bytes, got $bytes",
            );
        }

        return clone($this, ['maximumPacketSize' => $bytes]);
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
    #[NoDiscard]
    public function withInboundQueueSize(int $maxMessages): self
    {
        if ($maxMessages < 0) {
            throw new InvalidArgumentException("Inbound queue size cannot be negative, got $maxMessages");
        }

        return clone($this, ['inboundQueueSize' => $maxMessages]);
    }
}
