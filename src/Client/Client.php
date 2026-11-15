<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use Closure;
use Generator;
use LogicException;
use Psr\EventDispatcher\EventDispatcherInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ScienceStories\Mqtt\Contract\ClientInterface;
use ScienceStories\Mqtt\Contract\DecoderInterface;
use ScienceStories\Mqtt\Contract\EncoderInterface;
use ScienceStories\Mqtt\Contract\MetricsInterface;
use ScienceStories\Mqtt\Contract\SessionStoreInterface;
use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Events\MessageReceived as EvMessageReceived;
use ScienceStories\Mqtt\Events\PacketReceived as EvPacketReceived;
use ScienceStories\Mqtt\Events\PacketSent as EvPacketSent;
use ScienceStories\Mqtt\Events\ServerDisconnect as EvServerDisconnect;
use ScienceStories\Mqtt\Exception\AuthenticationError;
use ScienceStories\Mqtt\Exception\MqttException;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Exception\ServerError;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\ConnAck;
use ScienceStories\Mqtt\Protocol\Packet\Connect as ConnectPacket;
use ScienceStories\Mqtt\Protocol\Packet\Disconnect;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\Packet\PubAck;
use ScienceStories\Mqtt\Protocol\Packet\PubComp;
use ScienceStories\Mqtt\Protocol\Packet\Publish;
use ScienceStories\Mqtt\Protocol\Packet\SubAck;
use ScienceStories\Mqtt\Protocol\Packet\UnsubAck;
use ScienceStories\Mqtt\Protocol\ReasonCode;
use ScienceStories\Mqtt\Protocol\V311\Decoder as V311Decoder;
use ScienceStories\Mqtt\Protocol\V311\Encoder as V311Encoder;
use ScienceStories\Mqtt\Protocol\V5\Decoder as V5Decoder;
use ScienceStories\Mqtt\Protocol\V5\Encoder as V5Encoder;
use ScienceStories\Mqtt\Session\SessionState;
use ScienceStories\Mqtt\Util\Bytes;
use ScienceStories\Mqtt\Util\Clock;
use ScienceStories\Mqtt\Util\RandomId;
use ScienceStories\Mqtt\Util\TopicValidator;
use SplQueue;
use Throwable;

use function array_any;
use function array_key_exists;
use function array_key_first;
use function array_keys;
use function bin2hex;
use function chr;
use function chunk_split;
use function count;
use function explode;
use function floor;
use function is_array;
use function is_int;
use function is_string;
use function max;
use function min;
use function mt_getrandmax;
use function mt_rand;
use function ord;
use function pack;
use function round;
use function strlen;
use function strtoupper;
use function substr;
use function time;
use function trim;
use function usleep;

/**
 * Client supporting CONNECT/DISCONNECT, PUBLISH QoS0 and basic SUBSCRIBE/receive for MQTT 3.1.1 and 5.0.
 */
final class Client implements ClientInterface
{
    /**
     * Silent polls required before an unanswered PINGREQ condemns the connection.
     *
     * More than one, so a single long-blocking iteration — a handler waiting on a database,
     * a GC pause — cannot tear down a healthy link on its own.
     */
    private const int MIN_SILENT_POLLS = 2;

    /**
     * Body slice right after a frame times out (see $frameBodySlice). Small, so the next
     * read completes from bytes the transport already holds and liveness sees them; it
     * doubles on every read that succeeds, up to MAX_FRAME_SLICE, while data keeps flowing.
     */
    private const int MIN_FRAME_SLICE = 256;

    private const int MAX_FRAME_SLICE = 65_536;

    private EncoderInterface $encoder;

    private DecoderInterface $decoder;

    /** @var SplQueue<InboundMessage> */
    private readonly SplQueue $inbound;

    /** @var callable|null */
    private $messageHandler;

    /**
     * The connection ended and nothing will re-establish it — an inbound DISCONNECT, a ping
     * timeout, a protocol violation, reconnect attempts exhausted — so run() and messages()
     * stop instead of spinning on a closed transport. Belongs to that connection: connect()
     * clears it.
     */
    private bool $shouldStop = false;

    /**
     * The application called stop(). Kept apart from $shouldStop so that connect() — which
     * clears what the previous connection left behind — never discards a stop the
     * application asked for, e.g. from a SIGTERM handler between connections.
     */
    private bool $stopRequested = false;

    /** Set while attemptReconnect() runs, so a wait inside it cannot reconnect again. */
    private bool $reconnecting = false;

    /**
     * Acknowledgements for requests still being waited on, keyed by ackKey().
     *
     * A waiter registers its key (value null) before sending, loopOnce() fills it in, and
     * the waiter removes it however it ends. Two properties follow from that:
     * - a nested wait — publish() from inside an onMessage() handler — cannot overwrite the
     *   outer wait's acknowledgement, which the single "last PUBACK" slot this replaces did;
     * - an acknowledgement nobody is waiting for (a PUBACK that arrives after its publish()
     *   gave up) is dropped rather than accumulating.
     *
     * @var array<int, PubAck|PubComp|SubAck|UnsubAck|null>
     */
    private array $acks = [];

    /**
     * First byte of a frame whose remainder has not arrived yet, or null between frames.
     *
     * Partial-frame state lives here, across loopOnce() calls, so that a timeout halfway
     * through a packet header resumes where it stopped instead of reading the next byte as
     * a new packet type and desynchronising the stream for good.
     *
     * @var int<0, 255>|null
     */
    private ?int $frameHeader = null;

    /** Remaining Length bytes of that frame read so far. */
    private string $frameLength = '';

    /** Body bytes of that frame read so far, once it is being read in slices. */
    private string $frameBody = '';

    /**
     * Bytes per body read once the frame has timed out at least once; 0 reads it whole.
     *
     * Reading the body whole is fastest — on a healthy link it arrives in one call, without
     * a copy. But it gives liveness no signal until the last byte: a large body trickling
     * in over a slow link while a PINGREQ is out, its PINGRESP queued behind that very
     * body, looked like a silent link and was condemned. After the first timeout the body
     * is read in slices instead, each through trackRead(), so arriving bytes count as life:
     * a link that delivers MIN_FRAME_SLICE bytes per Ping Response Timeout stays up.
     */
    private int $frameBodySlice = 0;

    /**
     * True while ping() owns the outstanding PINGREQ. Its deadline is then the caller's,
     * not housekeeping's: a health check must not condemn the link it is checking.
     */
    private bool $pingOwnedByCaller = false;

    /** PINGRESPs received on this client — ping() waits for this to move. */
    private int $pingResponses = 0;

    /** Monotonic seconds at the last packet we sent — this drives Keep Alive. */
    private float $lastSent;

    /** Monotonic seconds at the last packet we received — liveness only, not Keep Alive. */
    private float $lastReceived;

    /**
     * Monotonic seconds at the outstanding PINGREQ, or null when none is in flight.
     *
     * One field rather than a boolean plus a timestamp: the pair could disagree — a caller
     * that set the flag without the time left the deadline comparing against a sentinel —
     * and this makes that state unrepresentable.
     */
    private ?float $pingSentAt = null;

    /** Consecutive polls that found the socket silent while a PINGREQ was outstanding. */
    private int $silentPolls = 0;

    /**
     * Keep Alive actually in force: the broker's Server Keep Alive when it sends one
     * (MQTT-3.2.2-22 makes honouring it mandatory), otherwise the configured value.
     */
    private int $effectiveKeepAlive;

    /** @var array<int, InboundMessage> QoS2 inbound pending messages by Packet Identifier */
    private array $qos2InboundPending = [];

    /** @var array<string, array{qos:int, options:?SubscribeOptions}> Map of topic filter => subscription settings */
    private array $subscriptions = [];

    private int $reconnectAttempts = 0;

    private bool $isResubscribing = false;

    /** @var array<int, float> Recently seen QoS1 Packet Identifiers for de-duplication */
    private array $qos1Seen = [];

    private readonly int $qos1SeenMax;

    /**
     * Topic aliases this client may *send*, bounded by the broker's CONNACK Topic Alias
     * Maximum. Null when the broker allows none.
     */
    private ?TopicAliasManager $topicAliasManager = null;

    /**
     * Topic aliases the broker has established on this connection, alias => topic.
     *
     * Kept apart from $topicAliasManager on purpose. The two directions are independent
     * alias namespaces with independent limits (MQTT-3.3.2.3.4): the broker may use aliases
     * 1..Options::$topicAliasMaximum when talking to us, while we use 1..its CONNACK limit
     * when talking to it. Sharing one map would let an alias the broker assigns overwrite
     * one we assigned to a different topic.
     *
     * @var array<int, string>
     */
    private array $inboundTopicAliases = [];

    /** MQTT 5.0 Flow Control for QoS 1/2 in-flight message limiting */
    private ?FlowControl $flowControl = null;

    /** Offline message queue for buffering during disconnects */
    private ?OfflineQueue $offlineQueue = null;

    private int $bytesSent = 0;

    private int $bytesReceived = 0;

    public function bytesSent(): int
    {
        return $this->bytesSent;
    }

    public function bytesReceived(): int
    {
        return $this->bytesReceived;
    }

    private function trackWrite(string $bytes): void
    {
        $written = $this->transport->write($bytes);
        $this->bytesSent += $written;
        // Every outbound packet resets the Keep Alive window — that is the whole point of
        // measuring it here rather than at individual call sites, which is how the old
        // code came to measure inbound traffic instead.
        $this->touchSent();
    }

    private function trackRead(int $length, ?float $timeoutSec = null): string
    {
        $data = $this->transport->readExact($length, $timeoutSec);
        $this->bytesReceived += strlen($data);
        // Bytes from the peer are the only proof the link is alive. Recording it here
        // rather than at call sites is what keeps that true: the previous single
        // "activity" clock was touched after writes too, so our own outbound packets
        // masqueraded as evidence the broker was still there.
        $this->touchReceived();

        return $data;
    }

    /** Record that we sent something — this is what the Keep Alive window measures. */
    private function touchSent(): void
    {
        $this->lastSent = Clock::now();
    }

    /** Record that the peer said something. Used for liveness, never for Keep Alive. */
    private function touchReceived(): void
    {
        $this->lastReceived = Clock::now();
    }

    /**
     * Read one complete MQTT frame, resuming a partial one left by an earlier call.
     *
     * The whole frame shares one deadline. A Timeout leaves every byte already read in
     * $frameHeader/$frameLength or in the transport, which keeps {@see readExact()}
     * atomic, so nothing is lost and the next call picks up mid-frame.
     *
     * Returns null when the declared Remaining Length exceeds the configured maximum: the
     * transport has been closed by then, because the stream sits mid-frame and cannot be
     * resynchronised.
     *
     * @return array{type: int, flags: int, header: string, body: string}|null
     *
     * @throws Timeout       nothing complete arrived in time; partial state is kept
     * @throws ProtocolError the Remaining Length is malformed; the transport is closed
     */
    private function readFrame(?float $timeoutSec, string $what = 'packet'): ?array
    {
        $deadline = $timeoutSec === null ? null : Clock::now() + $timeoutSec;

        $this->frameHeader ??= ord($this->trackRead(1, $timeoutSec)[0]);

        // Remaining Length: one to four bytes, high bit set on all but the last.
        while ($this->frameLength === '' || (ord($this->frameLength[-1]) & 0x80) !== 0) {
            if (strlen($this->frameLength) === 4) {
                $this->transport->close();
                $this->resetFrameState();

                throw new ProtocolError('Malformed Remaining Length: exceeds 4 bytes');
            }
            $this->frameLength .= $this->trackRead(1, $this->remainingTime($deadline));
        }

        $remainingLength = Bytes::decodeVarInt($this->frameLength);
        if (! $this->packetSizeAllowed($remainingLength, $what)) {
            $this->resetFrameState();

            return null;
        }

        while (strlen($this->frameBody) < $remainingLength) {
            $missing = $remainingLength - strlen($this->frameBody);
            try {
                $piece = $this->trackRead(
                    $this->frameBodySlice === 0 ? $missing : min($missing, $this->frameBodySlice),
                    $this->remainingTime($deadline),
                );
            } catch (Timeout $e) {
                $this->frameBodySlice = self::MIN_FRAME_SLICE;

                throw $e;
            }
            $this->frameBody = $this->frameBody === '' ? $piece : $this->frameBody.$piece;
            if ($this->frameBodySlice > 0) {
                $this->frameBodySlice = min(self::MAX_FRAME_SLICE, $this->frameBodySlice * 2);
            }
        }

        $body  = $this->frameBody;
        $frame = [
            'type'   => $this->frameHeader >> 4,
            'flags'  => $this->frameHeader & 0x0F,
            'header' => chr($this->frameHeader).$this->frameLength,
            'body'   => $body,
        ];
        $this->resetFrameState();

        return $frame;
    }

    private function resetFrameState(): void
    {
        $this->frameHeader    = null;
        $this->frameLength    = '';
        $this->frameBody      = '';
        $this->frameBodySlice = 0;
    }

    /** Seconds left until $deadline, never negative; null means no deadline. */
    private function remainingTime(?float $deadline): ?float
    {
        return $deadline === null ? null : max(0.0, $deadline - Clock::now());
    }

    /**
     * Cap a poll so it wakes in time for Keep Alive housekeeping.
     *
     * Without this, loopOnce(null) — what awaitMessage(null) runs — blocked in the read
     * for as long as the connection stayed quiet, so no PINGREQ was ever sent and the
     * broker dropped the client after 1.5x Keep Alive. Returning early is harmless: every
     * caller that waits longer loops, and the next iteration sends the ping.
     */
    private function pollTimeout(?float $requested): ?float
    {
        $keepAlive = $this->effectiveKeepAlive;
        if ($keepAlive <= 0 || $this->pingOwnedByCaller) {
            // ping() passes its own remaining time and owns the deadline.
            return $requested;
        }

        $now = Clock::now();
        $due = $this->pingSentAt === null
            ? max(1.0, $keepAlive * 0.9)          - ($now - $this->lastSent)
            : $this->options->pingResponseTimeout - ($now - $this->pingSentAt);
        $due = max(0.0, $due);

        return $requested === null ? $due : min($requested, $due);
    }

    private function sendPingReq(): void
    {
        // Build PINGREQ without awaiting response; loopOnce() or ping() will handle PINGRESP
        $pkt = chr(PacketType::PINGREQ->value << 4) . chr(0);
        $this->logger->debug('>> PINGREQ (auto)');
        $this->trackWrite($pkt);
        $this->pingSentAt  = Clock::now();
        $this->silentPolls = 0;
    }

    /**
     * Send a PINGREQ when the Keep Alive window is nearly up.
     *
     * The window is measured from the last packet we **sent**. MQTT-3.1.2-23 obliges the
     * client to send within it; receiving does not count. Measuring inbound traffic — which
     * the single "activity" clock used to do — means a busy subscriber never pings and is
     * dropped by the broker every 1.5x Keep Alive.
     */
    private function maybeAutoPing(): void
    {
        if (! $this->transport->isOpen() || $this->pingSentAt !== null) {
            return;
        }

        $keep = $this->effectiveKeepAlive;
        if ($keep <= 0) {
            return;
        }

        // Ping slightly before the window closes rather than exactly on it.
        $threshold = max(1.0, $keep * 0.9);
        if (Clock::now() - $this->lastSent >= $threshold) {
            $this->sendPingReq();
        }
    }

    /**
     * Decide whether an unanswered PINGREQ means the connection is dead.
     *
     * Called only after a poll that found the socket silent — never on wall-clock alone.
     * That distinction matters: judging "time since the ping" would tear down a perfectly
     * healthy connection whenever the caller's loop interval exceeds the timeout, which is
     * ordinary for a worker polling every 30s or a handler that blocks on a database write.
     * The question is "did we listen and hear nothing", not "how long has it been".
     *
     * Without any deadline at all the outstanding ping latches, keepalive stops for the
     * life of the process, and the client sits on a dead socket that still reports
     * isOpen() — so auto-reconnect never fires either. That is the classic NAT-eviction /
     * black-holed-broker failure.
     */
    private function expirePingIfSilent(): void
    {
        if ($this->pingSentAt === null || $this->pingOwnedByCaller || ! $this->transport->isOpen()) {
            return;
        }

        // Any inbound packet proves the link is alive, even if the PINGRESP itself was lost
        // or reordered.
        if ($this->lastReceived > $this->pingSentAt) {
            $this->pingSentAt  = null;
            $this->silentPolls = 0;

            return;
        }

        $this->silentPolls++;

        $waited = Clock::now() - $this->pingSentAt;
        // Require both a real elapsed timeout and more than one silent poll, so a single
        // long-blocking iteration cannot condemn the connection on its own.
        if ($waited < $this->options->pingResponseTimeout || $this->silentPolls < self::MIN_SILENT_POLLS) {
            return;
        }

        $this->logger->warning('No PINGRESP within the deadline; treating the connection as dead', [
            'waitedSeconds' => round($waited, 3),
            'timeout'       => $this->options->pingResponseTimeout,
            'silentPolls'   => $this->silentPolls,
        ]);
        $this->metrics?->increment('ping_timeouts');

        // Tell the application — a connection dying silently is how a supervisor ends up
        // watching a live process that will never deliver another message. Closed first, as
        // whenever the client is the side giving up, so listeners all see the same thing: a
        // transport that is already closed.
        $this->transport->close();
        $this->resetConnectionState();

        $willReconnect = $this->options->autoReconnect;
        $this->events?->dispatch(new EvServerDisconnect(
            new Disconnect(ReasonCode::KeepAliveTimeout->value, ['reason_string' => 'Closed by the client: no PINGRESP within the Ping Response Timeout']),
            $willReconnect,
        ));

        if (! $willReconnect) {
            // Nothing will re-establish this connection, so stop run()/messages() rather
            // than spinning on a closed transport forever.
            $this->shouldStop = true;
        }
    }

    public function __construct(private readonly Options $options, private readonly TransportInterface $transport, private readonly LoggerInterface $logger = new NullLogger(), private readonly ?EventDispatcherInterface $events = null, private readonly ?MetricsInterface $metrics = null)
    {
        $this->inbound = new SplQueue();
        if ($this->options->version === MqttVersion::V5_0) {
            // Choose by protocol version
            $this->encoder = new V5Encoder();
            $this->decoder = new V5Decoder();
        } else {
            $this->encoder = new V311Encoder();
            $this->decoder = new V311Decoder();
        }
        $this->qos1SeenMax = $this->options->qos1DeduplicationSize;
        if ($this->options->offlineQueueSize > 0) {
            $this->offlineQueue = new OfflineQueue($this->options->offlineQueueSize);
        }
        $this->effectiveKeepAlive = $this->options->keepAlive;
        // Start both clocks so nothing reads an unset timestamp before connect().
        $this->touchSent();
        $this->touchReceived();
    }

    /**
     * Connect, as the application asks to — which also clears the "connection ended" stop
     * left behind by the previous connection.
     *
     * When a connection ends with auto-reconnect off (an inbound DISCONNECT, a ping timeout,
     * a protocol violation), run() and messages() are told to stop. That belongs to the
     * connection that died: left in place, the first run() after a manual reconnect returned
     * at once, and a supervisor's catch → connect → run loop spun without ever processing a
     * message. A stop() the application asked for is a separate flag and survives.
     */
    public function connect(): ConnectResult
    {
        $this->shouldStop = false;

        return $this->establishConnection();
    }

    /**
     * Open the transport and complete the MQTT handshake. Shared by connect() and the
     * auto-reconnect path, which must not clear a stop the application asked for.
     */
    private function establishConnection(): ConnectResult
    {
        $connectStart = Clock::now();
        // Nothing from a previous connection may leak into this one: Packet Identifiers,
        // topic aliases and flow-control quota are all connection-scoped.
        $this->resetConnectionState();
        $this->logger->info('Opening connection', ['host' => $this->options->host, 'port' => $this->options->port]);
        $this->transport->open($this->options->host, $this->options->port);

        $connack = null;
        try {
            $connack = $this->handshake();
        } finally {
            // Whatever failed after open() — TLS, a timeout waiting for CONNACK, a malformed
            // or refused one — must not leave a half-open transport behind. isOpen() would
            // stay true, so auto-reconnect would never retry, and the next loopOnce() would
            // treat whatever the broker sent next as traffic on a live session. A finally
            // rather than catch-and-rethrow: every exception propagates untouched.
            if (! $connack instanceof ConnAck) {
                $this->transport->close();
                $this->resetFrameState();
            }
        }
        $versionStr = $this->options->version->value;

        // MQTT-3.2.2-22: when the broker sends a Server Keep Alive, the client MUST use it
        // instead of the value it asked for. Ignoring it means pinging on a schedule the
        // broker has already rejected — e.g. asking for 300s against a broker that grants
        // 60s produces an endless 90-second connect/drop loop.
        $serverKeepAlive          = $connack->getServerKeepAlive();
        $this->effectiveKeepAlive = $serverKeepAlive ?? $this->options->keepAlive;
        if ($serverKeepAlive !== null && $serverKeepAlive !== $this->options->keepAlive) {
            $this->logger->info('Broker overrode Keep Alive', [
                'requested' => $this->options->keepAlive,
                'granted'   => $serverKeepAlive,
            ]);
        }

        // Inbound QoS 2 messages awaiting PUBREL are Session state. Discard them only
        // when the broker has discarded its half of the session too, otherwise a replayed
        // PUBREL would be answered with PUBCOMP for a message never delivered upstream.
        if ($this->options->cleanSession || ! $connack->sessionPresent) {
            $this->qos2InboundPending = [];
        }

        $assignedId = null;
        if ($this->options->version === MqttVersion::V5_0) {
            $properties = is_array($connack->properties) ? $connack->properties : [];
            $assignedId = $properties['assigned_client_identifier'] ?? null;
            if (! is_string($assignedId)) {
                $assignedId = null;
            }

            // Both limits are re-derived from every CONNACK, absent property included: they
            // belong to this connection, and a broker that stops advertising one — a
            // reconnect to another cluster node, a config change — must not inherit the
            // previous connection's value.

            // The broker's Topic Alias Maximum bounds the aliases this client may *send*.
            // (The one in Options is the other direction; see $inboundTopicAliases.)
            $brokerTopicAliasMax     = $properties['topic_alias_maximum'] ?? 0;
            $this->topicAliasManager = is_int($brokerTopicAliasMax) && $brokerTopicAliasMax > 0
                ? new TopicAliasManager($brokerTopicAliasMax)
                : null;
            $this->logger->debug('Outbound topic aliases', ['max' => $this->topicAliasManager->maxAliases ?? 0]);

            // Receive Maximum defaults to 65,535 when the broker does not send it.
            $brokerReceiveMax  = $properties['receive_maximum'] ?? 65535;
            $this->flowControl = new FlowControl(is_int($brokerReceiveMax) && $brokerReceiveMax > 0 ? $brokerReceiveMax : 65535);
            $this->logger->debug('Flow Control initialized', ['max' => $this->flowControl->maxInFlight]);
        }

        // Restore session state if session store is configured and session was present
        $clientId = $assignedId ?? $this->options->clientId;
        if ($this->options->sessionStore instanceof SessionStoreInterface && $connack->sessionPresent && ! $this->options->cleanSession) {
            $this->restoreSession($clientId);
        }

        $this->metrics?->increment('connections', 1.0, ['version' => $versionStr]);
        $this->metrics?->observe('connect_duration_seconds', Clock::now() - $connectStart);

        // Drain offline queue after successful connect
        $this->drainOfflineQueue();

        return new ConnectResult(
            $connack->sessionPresent,
            'MQTT',
            $versionStr,
            $connack->returnCode,
            $assignedId,
            $connack,
        );
    }

    /**
     * TLS, CONNECT, and a successful CONNACK — everything between open() and a usable
     * session. Any failure throws, and connect() closes the transport.
     *
     * @throws MqttException
     */
    private function handshake(): ConnAck
    {
        if ($this->options->useTls) {
            $tlsOpts = $this->options->tlsOptions;
            if ($tlsOpts instanceof TlsOptions) {
                // Set peer_name from host if not explicitly configured
                if ($tlsOpts->peerName === null) {
                    $tlsOpts = $tlsOpts->withPeerName($this->options->host);
                }
                $tls = $tlsOpts->toStreamContext();
            } else {
                // Legacy array support
                $tls = $tlsOpts ?? [];
                $ssl = is_array($tls['ssl'] ?? null) ? $tls['ssl'] : $tls;
                if (! array_key_exists('peer_name', $ssl)) {
                    $tls = ['ssl' => $ssl + ['peer_name' => $this->options->host]];
                }
            }
            $this->logger->info('Enabling TLS');
            $this->transport->enableTls($tls);
        }

        // Build CONNECT packet with MQTT 5.0 properties
        $connectProps = null;
        if ($this->options->version === MqttVersion::V5_0) {
            $connectProps = [];
            if ($this->options->sessionExpiry !== null) {
                $connectProps['session_expiry_interval'] = $this->options->sessionExpiry;
            }
            if ($this->options->receiveMaximum !== 65535) {
                $connectProps['receive_maximum'] = $this->options->receiveMaximum;
            }
            if ($this->options->topicAliasMaximum > 0) {
                $connectProps['topic_alias_maximum'] = $this->options->topicAliasMaximum;
            }
            if ($this->options->maximumPacketSize < Options::PROTOCOL_MAXIMUM_PACKET_SIZE) {
                // Make the local limit a negotiated one rather than a unilateral cut-off.
                $connectProps['maximum_packet_size'] = $this->options->maximumPacketSize;
            }
            if ($connectProps === []) {
                $connectProps = null;
            }
        }
        $connect = new ConnectPacket(
            $this->options->clientId,
            $this->options->keepAlive,
            $this->options->cleanSession,
            $this->options->username,
            $this->options->password,
            $this->options->will,
            $connectProps,
        );
        $data = $this->encoder->encodeConnect($connect);
        $this->logger->debug('>> CONNECT', ['bytes' => strlen($data), 'preview' => $this->hexPreview($data)]);
        $this->trackWrite($data);

        $this->logger->debug('<< waiting for CONNACK');
        // The size bound in readFrame() runs before the body is read: this read is
        // pre-authentication, so an unauthenticated peer must not be able to size the
        // allocation. connect() has no bool contract to honour, so a violation is fatal
        // here rather than a skipped packet.
        $frame = $this->readFrame(5.0, 'CONNACK')
            ?? throw new ProtocolError("Declared CONNACK size exceeds the maximum of {$this->options->maximumPacketSize} bytes");
        if ($frame['type'] !== PacketType::CONNACK->value) {
            throw new ProtocolError("Expected CONNACK, got type {$frame['type']}");
        }
        $body = $frame['body'];
        if (strlen($body) < 2) {
            // v3: at least 2 bytes (ack flags + return code), v5: also followed by properties
            throw new ProtocolError('Invalid CONNACK length: '.strlen($body));
        }
        $this->logger->debug('<< CONNACK', ['bytes' => strlen($body), 'preview' => $this->hexPreview($frame['header'].$body)]);
        $connack = $this->decoder->decodeConnAck($body);

        $this->logger->info('CONNACK', ['sessionPresent' => $connack->sessionPresent, 'reasonCode' => $connack->returnCode, 'version' => $this->options->version->value]);

        if ($connack->returnCode !== 0) {
            // MQTT-3.2.2-7: the broker closes the network connection after a refusal.
            // Returning normally here would make the caller replay buffered publishes
            // into a dead socket and surface an auth failure as a later read timeout.
            // connect() closes the transport on the way out.
            $exception = $this->connectRefusedException($connack);
            $this->logger->warning('CONNECT refused', ['reasonCode' => $connack->returnCode, 'error' => $exception->getMessage()]);

            throw $exception;
        }

        return $connack;
    }

    public function disconnect(string $reason = ''): void
    {
        // Save session state before disconnecting (if persistent session)
        if ($this->options->sessionStore instanceof SessionStoreInterface && ! $this->options->cleanSession) {
            $this->saveSession($this->options->clientId);
        }

        if (! $this->transport->isOpen()) {
            $this->resetConnectionState();

            return;
        }

        // DISCONNECT fixed header: type 14, flags 0, length 0
        $packet = chr(PacketType::DISCONNECT->value << 4) . chr(0);
        $this->logger->debug('>> DISCONNECT');
        $this->trackWrite($packet);

        $this->transport->close();
        $this->resetConnectionState();
        $this->metrics?->increment('disconnections', 1.0, ['reason' => $reason !== '' ? 'client' : 'normal']);
        $this->logger->info('Disconnected', ['reason' => $reason]);
    }

    /**
     * Reset every piece of state scoped to a single network connection.
     *
     * Packet Identifiers, topic aliases and flow-control quota are all defined per
     * connection, so anything surviving a reconnect corrupts the next session — a stale
     * `$pingSentAt` silently disables keepalive for the life of the process, and a
     * stale de-duplication cache drops messages the new session has never seen.
     *
     * Two things are deliberately *not* cleared here:
     * - the inbound message queue, which is the caller's data and may still be waiting
     *   for awaitMessage();
     * - `$qos2InboundPending`, which MQTT-4.1.0-1 defines as *Session* state. After we
     *   answer a QoS 2 PUBLISH with PUBREC the broker's recovery procedure (§4.3.3)
     *   replays only the PUBREL, so dropping the stored message here would make us
     *   PUBCOMP a message the application never received. It is discarded in connect()
     *   instead, and only when the broker has discarded its half too.
     */
    private function resetConnectionState(): void
    {
        $this->topicAliasManager?->reset();
        $this->flowControl?->reset();
        $this->inboundTopicAliases = [];
        $this->qos1Seen            = [];
        $this->pingSentAt          = null;
        $this->silentPolls         = 0;
        // A half-read frame belongs to the byte stream of the connection that just ended.
        $this->resetFrameState();
        // $acks is deliberately not touched. Each key belongs to a waiter still on the call
        // stack, which resends on the new connection and must be able to receive the answer;
        // one already received is a fact the broker stated, and dropping it would only force
        // a needless DUP resend. Each waiter removes its own key however it ends.
        // Start both clocks now, so a fresh connection is not immediately "overdue".
        $this->touchSent();
        $this->touchReceived();
    }

    /**
     * Whether a declared Remaining Length is within the configured maximum.
     *
     * Called before the body is read: `Bytes::decodeVarInt()` legitimately returns values
     * up to 268,435,455, so five header bytes would otherwise be enough for any peer to
     * request a 256 MiB allocation.
     *
     * On rejection the transport is closed, because the stream is positioned mid-frame
     * and there is no way to resynchronise it. Callers decide whether to throw.
     */
    private function packetSizeAllowed(int $remainingLength, string $what): bool
    {
        $max = $this->options->maximumPacketSize;
        if ($remainingLength <= $max) {
            return true;
        }

        $this->logger->warning('Inbound packet exceeds maximum packet size', [
            'what'     => $what,
            'declared' => $remainingLength,
            'maximum'  => $max,
        ]);
        $this->transport->close();
        $this->metrics?->increment('oversized_packets_rejected');

        return false;
    }

    /**
     * Map a refused CONNACK onto the most specific exception type.
     *
     * MQTT 3.1.1 return codes 1-5 and MQTT 5 reason codes are different namespaces and
     * must not be mapped through the same table.
     */
    private function connectRefusedException(ConnAck $connack): MqttException
    {
        $code = $connack->returnCode;

        if ($this->options->version === MqttVersion::V5_0) {
            return ReasonCode::tryFromInt($code)?->toException('CONNECT')
                ?? new ProtocolError('CONNECT refused with unknown reason code '.$code, $code);
        }

        $message = 'CONNECT refused: '.$connack->getReasonDescription(MqttVersion::V3_1_1);

        return match ($code) {
            3       => new ServerError($message, $code),
            4, 5    => new AuthenticationError($message, $code),
            default => new ProtocolError($message, $code),
        };
    }

    public function publish(string $topic, string $payload, ?PublishOptions $options = null): int
    {
        TopicValidator::validatePublishTopic($topic);
        $options ??= new PublishOptions();

        // Offline queue: buffer messages when transport is disconnected
        if (! $this->transport->isOpen() && $this->offlineQueue instanceof OfflineQueue) {
            if ($this->offlineQueue->enqueue($topic, $payload, $options)) {
                $this->logger->debug('Message queued offline', ['topic' => $topic, 'queued' => $this->offlineQueue->count()]);
                $this->metrics?->increment('offline_queue_enqueued');

                return 0;
            }
            $this->logger->warning('Offline queue full, message dropped', ['topic' => $topic]);
            $this->metrics?->increment('offline_queue_dropped');

            return 0;
        }

        // Rate limiting
        $rateLimiter = $this->options->rateLimiter;
        if ($rateLimiter instanceof RateLimiter) {
            $waited = $rateLimiter->acquire();
            if ($waited > 0) {
                $this->metrics?->observe('rate_limit_wait_seconds', $waited);
            }
        }

        $qos      = $options->qos->value;
        $packetId = null;
        if ($qos > 0) {
            $packetId = RandomId::packetId();

            // Flow control: wait for slot if using QoS 1/2
            if ($this->flowControl instanceof FlowControl && ! $this->flowControl->canSend()) {
                $this->logger->debug('Flow control: waiting for slot', [
                    'inFlight' => $this->flowControl->currentInFlight,
                    'max'      => $this->flowControl->maxInFlight,
                ]);
                // Poll loopOnce to process incoming ACKs while waiting
                $deadline = Clock::now() + 5.0;
                // @phpstan-ignore-next-line booleanNot.alwaysTrue - canSend() state changes via loopOnce()
                while (! $this->flowControl->canSend()) {
                    $timeLeft = $deadline - Clock::now();
                    if ($timeLeft <= 0) {
                        throw new Timeout('Flow control: timed out waiting for slot');
                    }
                    if (! $this->loopOnce(max(0.01, $timeLeft)) && ! $this->transport->isOpen()) {
                        // Nothing arrives on a closed transport until something reconnects
                        // it; back off instead of spinning a core until the deadline.
                        usleep(10_000);
                    }
                }
            }
        }

        // Topic aliases (MQTT 5) belong to the client and to this connection. A topic_alias in
        // the caller's properties — typically copied from a received message to forward it —
        // is dropped: passed through, it re-pointed an alias the client had assigned to a
        // different topic, and the client's next publish to that topic went somewhere else.
        $properties = $options->properties;
        if ($properties !== null) {
            unset($properties['topic_alias']);
        }
        $publishTopic = $topic;
        $publishProps = $properties;
        if ($this->topicAliasManager instanceof TopicAliasManager && $this->options->version === MqttVersion::V5_0) {
            $aliasResult = $this->topicAliasManager->getOrCreateAlias($topic);
            if ($aliasResult['alias'] !== null) {
                $publishProps ??= [];
                $publishProps['topic_alias'] = $aliasResult['alias'];
                // If alias already established, we can omit the topic
                if (! $aliasResult['isNew']) {
                    $publishTopic = '';
                    $this->logger->debug('Using topic alias', ['topic' => $topic, 'alias' => $aliasResult['alias']]);
                } else {
                    $this->logger->debug('Establishing topic alias', ['topic' => $topic, 'alias' => $aliasResult['alias']]);
                }
            }
        }

        $pkt = new Publish(
            $publishTopic,
            $payload,
            $options->qos,
            $options->retain,
            $options->dup,
            packetId: $packetId,
            properties: $publishProps,
        );

        $data = $this->encoder->encodePublish($pkt);
        $this->logger->debug('>> PUBLISH', [
            'topic'         => $topic,
            'qos'           => $options->qos->value,
            'retain'        => $options->retain,
            'bytes'         => strlen($data),
            'payload_bytes' => strlen($payload),
            'properties'    => $publishProps ?? [],
            'preview'       => $this->hexPreview($data),
            'packetId'      => $packetId,
        ]);
        // PSR-14: emit PacketSent event for PUBLISH
        $this->events?->dispatch(new EvPacketSent($data, PacketType::PUBLISH->value));
        $this->trackWrite($data);

        // Track in-flight for flow control (a packet identifier exists only for QoS 1/2)
        if ($packetId !== null) {
            $this->flowControl?->trackSend($packetId);
        }

        $this->metrics?->increment('publish_sent', 1.0, ['qos' => $qos]);
        $this->metrics?->size('publish_payload_bytes', strlen($payload), ['qos' => $qos]);

        return $this->awaitPublishAck($qos, (int) $packetId, $topic, $payload, $options, $properties);
    }

    /**
     * Return flow-control quota held by messages the broker will plainly never acknowledge.
     *
     * The slot for an in-flight PUBLISH belongs to the broker until it acknowledges, so it
     * must NOT be released the moment `publish()` gives up waiting: MQTT-4.9.0-1 counts the
     * message against Receive Maximum while it is still unacknowledged, and freeing it
     * early lets the client exceed the window the broker granted.
     *
     * But holding it forever is the leak this sweep exists to prevent — a handful of
     * timeouts against a broker advertising `receive_maximum: 1` would otherwise exhaust
     * the quota for the life of the process. So entries are reclaimed only well past the
     * point where an acknowledgement could still plausibly arrive: the full resend budget,
     * doubled.
     */
    private function sweepStalledFlowControlSlots(): void
    {
        if (! $this->flowControl instanceof FlowControl) {
            return;
        }

        $resendBudget = $this->options->ackTimeout * ($this->options->maxResendAttempts + 1);
        $grace        = max(1.0, $resendBudget * 2);

        foreach ($this->flowControl->getTimedOutPackets($grace) as $packetId) {
            $this->logger->warning('Reclaiming flow-control slot for an unacknowledged packet', [
                'packetId'     => $packetId,
                'graceSeconds' => round($grace, 3),
            ]);
            $this->flowControl->trackAck($packetId);
            $this->metrics?->increment('flow_control_slots_reclaimed');
        }
    }

    /**
     * Block until the broker acknowledges a QoS 1/2 publish, resending with DUP on timeout.
     *
     * QoS 1 completes on PUBACK, QoS 2 on PUBCOMP; the PUBREC/PUBREL step in between is
     * answered by loopOnce() itself.
     *
     * A resend carries the full topic and no Topic Alias. If the connection was
     * re-established while waiting, an alias would belong to the old connection — the new
     * broker would reject it, or route it to whatever topic the alias now means.
     *
     * @param  array<string, mixed>|null  $properties  the caller's properties, alias removed
     * @return int The packet identifier, or 0 for QoS 0
     *
     * @throws Timeout if the acknowledgement never arrives within the resend budget
     */
    private function awaitPublishAck(int $qos, int $packetId, string $topic, string $payload, PublishOptions $options, ?array $properties): int
    {
        if ($qos === 0) {
            return 0;
        }

        [$type, $class, $what] = $qos === 1
            ? [PacketType::PUBACK, PubAck::class, 'PUBACK']
            : [PacketType::PUBCOMP, PubComp::class, 'QoS2 handshake'];

        $resends = 0;

        try {
            $ack = $this->awaitAck(
                $type,
                $packetId,
                $class,
                $this->options->ackTimeout,
                $what,
                function () use (&$resends, $qos, $packetId, $topic, $payload, $options, $properties): bool {
                    if ($resends >= $this->options->maxResendAttempts) {
                        return false;
                    }
                    $resends++;
                    $this->logger->debug($qos === 1 ? '>> PUBLISH (resend)' : '>> PUBLISH (QoS2 resend)', ['packetId' => $packetId, 'attempt' => $resends]);
                    $this->trackWrite($this->encoder->encodePublish(new Publish(
                        $topic,
                        $payload,
                        $options->qos,
                        $options->retain,
                        dup: true,
                        packetId: $packetId,
                        properties: $properties,
                    )));

                    return true;
                },
            );
        } catch (Timeout) {
            throw new Timeout("Timed out waiting for $what after $resends resend(s)");
        }

        $this->logger->info($qos === 1 ? 'PUBACK' : 'PUBCOMP', ['packetId' => $packetId, 'reasonCode' => $ack->reasonCode, 'success' => $ack->isSuccess()]);

        return $packetId;
    }

    /**
     * What has arrived for $key so far. A method rather than an inline read, because
     * loopOnce() fills the slot behind the caller's back.
     */
    private function receivedAck(int $key): PubAck|PubComp|SubAck|UnsubAck|null
    {
        return $this->acks[$key] ?? null;
    }

    /** One integer key per (packet type, Packet Identifier) pair. */
    private function ackKey(PacketType $type, int $packetId): int
    {
        return ($type->value << 16) | $packetId;
    }

    /** Hand an inbound acknowledgement to whoever is waiting for it, if anyone is. */
    private function recordAck(PacketType $type, PubAck|PubComp|SubAck|UnsubAck $ack): void
    {
        $key = $this->ackKey($type, $ack->packetId);
        if (array_key_exists($key, $this->acks)) {
            $this->acks[$key] = $ack;

            return;
        }

        $this->logger->debug('Acknowledgement nobody is waiting for', ['type' => $type->name, 'packetId' => $ack->packetId]);
    }

    /**
     * Pump loopOnce() until the acknowledgement for $packetId arrives.
     *
     * Must be called after the request is written and before anything else reads: nothing
     * can read in between, so an acknowledgement cannot arrive before the key exists.
     *
     * @template T of PubAck|PubComp|SubAck|UnsubAck
     *
     * @param  class-string<T>  $class
     * @param  (Closure(): bool)|null  $onExpiry  called when a wait expires; return true after
     *                                            resending to wait again, false to give up
     * @return T
     *
     * @throws Timeout once the wait expires and $onExpiry declines to retry
     */
    private function awaitAck(PacketType $type, int $packetId, string $class, float $timeout, string $what, ?Closure $onExpiry = null): PubAck|PubComp|SubAck|UnsubAck
    {
        $key              = $this->ackKey($type, $packetId);
        $this->acks[$key] = null;

        try {
            $deadline = Clock::now() + $timeout;
            while (true) {
                $ack = $this->receivedAck($key);
                if ($ack instanceof $class) {
                    return $ack;
                }

                $timeLeft = $deadline - Clock::now();
                if ($timeLeft <= 0) {
                    if ($onExpiry instanceof Closure && $onExpiry()) {
                        $deadline = Clock::now() + $timeout;

                        continue;
                    }

                    throw new Timeout("Timed out waiting for $what");
                }

                if (! $this->loopOnce(max(0.01, $timeLeft)) && ! $this->transport->isOpen()) {
                    // Nothing will arrive on a closed transport until something reconnects
                    // it; back off instead of spinning a core until the deadline.
                    usleep(10_000);
                }
            }
        } finally {
            unset($this->acks[$key]);
        }
    }

    /**
     * Send PINGREQ and wait for the PINGRESP.
     *
     * The wait pumps loopOnce(), so anything the broker sends first — a PUBLISH on a
     * subscribed connection, an acknowledgement — is processed and delivered as usual.
     * The previous implementation read two raw bytes off the socket instead, which ate the
     * head of whatever packet happened to arrive first and desynchronised the stream.
     *
     * @throws Timeout        if no PINGRESP arrives in time
     * @throws TransportError if the connection closes while waiting
     */
    public function ping(?float $timeoutSec = 5.0): bool
    {
        if (! $this->transport->isOpen()) {
            throw new LogicException('Cannot PING: transport not open');
        }
        $pkt = chr(PacketType::PINGREQ->value << 4) . chr(0);
        $this->logger->debug('>> PINGREQ');

        $deadline  = Clock::now() + ($timeoutSec ?? 5.0);
        $responses = $this->pingResponses;

        try {
            $this->pingOwnedByCaller = true;
            $this->trackWrite($pkt);
            $this->pingSentAt = Clock::now();

            while ($this->pingResponses === $responses) {
                if (! $this->transport->isOpen()) {
                    throw new TransportError('Connection closed while waiting for PINGRESP');
                }
                $timeLeft = $deadline - Clock::now();
                if ($timeLeft <= 0) {
                    throw new Timeout('Timed out waiting for PINGRESP');
                }
                $this->loopOnce($timeLeft);
            }
        } finally {
            // This call owns its own request/response round trip, so the outstanding-ping
            // state must not survive it however it ends. Leaving it set would arm the
            // auto-ping deadline against a ping this method already gave up on — a health
            // check that manufactures the outage it exists to detect.
            $this->pingSentAt        = null;
            $this->silentPolls       = 0;
            $this->pingOwnedByCaller = false;
        }

        $this->logger->info('PINGRESP OK');
        $this->metrics?->increment('pings');

        return true;
    }

    public function subscribe(array $topics, int $qos = 0): void
    {
        $filters = [];
        foreach ($topics as $t) {
            TopicValidator::validateSubscribeFilter((string) $t);
            $filters[] = ['filter' => (string) $t, 'qos' => $qos];
        }
        $this->subscribeWith($filters);
    }

    public function subscribeWith(array $filters, ?SubscribeOptions $options = null): SubscribeResult
    {
        $pid  = RandomId::packetId();
        $data = $this->encoder->encodeSubscribe($filters, $pid, $options);
        $this->logger->debug('>> SUBSCRIBE', ['packetId' => $pid, 'filters' => $filters, 'bytes' => strlen($data), 'preview' => $this->hexPreview($data)]);
        $this->trackWrite($data);

        // Retained messages may arrive before the SUBACK; awaitAck() delivers them as usual.
        $subAck = $this->awaitAck(PacketType::SUBACK, $pid, SubAck::class, 5.0, 'SUBACK');
        $this->logger->info('SUBACK', ['packetId' => $pid, 'codes' => $subAck->returnCodes]);

        // Record subscriptions if this is a user-initiated subscription (avoid duplicate records during resubscribe)
        if (! $this->isResubscribing) {
            $this->recordSubscriptionsFromFilters($filters, $options);
        }

        $this->metrics?->increment('subscriptions', (float) count($filters));

        return new SubscribeResult($pid, $subAck->returnCodes, $subAck);
    }

    public function unsubscribe(array $topics): void
    {
        $pid = RandomId::packetId();
        // Build filters list of strings; encoders accept list<string>
        $filters = [];
        foreach ($topics as $t) {
            $f = (string) $t;
            if ($f !== '') {
                $filters[] = $f;
            }
        }
        if ($filters === []) {
            return;
        }
        $data = $this->encoder->encodeUnsubscribe($filters, $pid);
        $this->logger->debug('>> UNSUBSCRIBE', ['packetId' => $pid, 'filters' => $filters, 'bytes' => strlen($data), 'preview' => $this->hexPreview($data)]);
        $this->trackWrite($data);

        $unsuback = $this->awaitAck(PacketType::UNSUBACK, $pid, UnsubAck::class, 5.0, 'UNSUBACK');
        $this->logger->info('UNSUBACK', ['packetId' => $pid, 'codes' => $unsuback->reasonCodes ?? []]);

        $this->removeSubscriptions($filters);
        $this->metrics?->increment('unsubscriptions', (float) count($filters));
    }

    public function loopOnce(?float $timeoutSec = 0.1): bool
    {
        // Auto-reconnect if transport is closed and the feature enabled
        if (! $this->transport->isOpen() && $this->options->autoReconnect) {
            $this->logger->info('Transport not open; attempting auto-reconnect');
            $ok = $this->attemptReconnect();
            if (! $ok) {
                return false;
            }
        }

        // Before attempting to read, send an automatic PING if needed and reclaim any
        // flow-control quota held by messages the broker has long since abandoned.
        $this->maybeAutoPing();
        $this->sweepStalledFlowControlSlots();
        try {
            $frame = $this->readFrame($this->pollTimeout($timeoutSec));
        } catch (Timeout) {
            // We listened and heard nothing complete. Only now may an outstanding PINGREQ
            // count against its deadline — judging it before the read would condemn a
            // healthy connection whenever the caller polls less often than the timeout.
            $this->expirePingIfSilent();

            return false; // nothing available
        } catch (TransportError) {
            // Transport broke; attempt to reconnect next iteration
            $this->logger->info('Transport error on read; will try to reconnect');
            $this->transport->close();
            $this->resetFrameState();

            return false;
        }
        if ($frame === null) {
            // Oversized: the stream is positioned mid-frame and cannot be resynchronised,
            // so the transport is already closed. Report it the way every other
            // transport-level failure in this method does — as "no packet processed" — so
            // the documented bool contract holds and auto-reconnect can take over.
            return false;
        }

        ['type' => $type, 'flags' => $flags, 'header' => $header, 'body' => $body] = $frame;
        $rl                                                                        = strlen($body);
        $this->metrics?->increment('packets_received', 1.0, ['type' => $type]);
        $this->metrics?->size('packet_bytes', strlen($header) + $rl, ['dir' => 'in', 'type' => $type]);
        // PSR-14: emit PacketReceived with the full frame. The nullsafe call skips evaluating
        // its argument when nobody listens — concatenating header and body copies the whole
        // packet, which for a large payload was a measurable share of the receive cost.
        $this->events?->dispatch(new EvPacketReceived($header.$body, $type, $flags, $rl));

        switch ($type) {
            case PacketType::PUBLISH->value:
                // Before anything is acknowledged or delivered: an invalid alias ends the
                // connection, and the message it carried must not be PUBACKed.
                $msg = $this->resolveInboundTopic($this->decoder->decodePublish($flags, $body));
                if (! $msg instanceof InboundMessage) {
                    // The broker broke the alias rules and the connection is closed; with
                    // auto-reconnect on, the next call re-establishes it.
                    return false;
                }
                // QoS1: immediately acknowledge with PUBACK (v3/v5 minimal form)
                if ($msg->qos->value === 1 && $msg->packetId !== null) {
                    $puback = chr(PacketType::PUBACK->value << 4) . chr(2) . pack('n', $msg->packetId);
                    $this->logger->debug('>> PUBACK', ['packetId' => $msg->packetId]);
                    $this->trackWrite($puback);
                    // QoS1 idempotency. MQTT-3.3.1-3: only a re-delivery (DUP=1) can be a
                    // copy of something already handed to the application. The broker is
                    // free to reuse a Packet Identifier the moment we PUBACK it, so
                    // matching on the identifier alone would discard unrelated messages.
                    $pid = $msg->packetId;
                    if ($msg->dup && isset($this->qos1Seen[$pid])) {
                        $this->logger->debug('QoS1 duplicate PUBLISH suppressed', ['packetId' => $pid, 'dup' => $msg->dup]);

                        return true;
                    }
                    $this->qos1Seen[$pid] = Clock::now();
                    if (count($this->qos1Seen) > $this->qos1SeenMax) {
                        // Evict the oldest by key. array_shift() would renumber the
                        // integer Packet Identifier keys from 0 and corrupt the cache.
                        unset($this->qos1Seen[array_key_first($this->qos1Seen)]);
                    }
                    // QoS1: decide delivery based on client-side filters
                    $this->logger->debug('<< PUBLISH', ['topic' => $msg->topic, 'bytes' => $rl, 'qos' => $msg->qos->value, 'retain' => $msg->retain]);
                    $this->deliverIfMatches($msg);

                    return true;
                }
                // QoS2: respond with PUBREC and store pending until PUBREL
                if ($msg->qos->value === 2 && $msg->packetId !== null) {
                    // If duplicate PUBLISH (a DUP flag may be set), resend PUBREC but do not duplicate store
                    $pid    = $msg->packetId;
                    $pubrec = chr(PacketType::PUBREC->value << 4) . chr(2) . pack('n', $pid);
                    $this->logger->debug('>> PUBREC', ['packetId' => $pid]);
                    $this->trackWrite($pubrec);
                    // Store message only if not already pending
                    $this->qos2InboundPending[$pid] ??= $msg;

                    return true;
                }
                // QoS0: deliver immediately (subject to client-side filters)
                $this->logger->debug('<< PUBLISH', ['topic' => $msg->topic, 'bytes' => $rl, 'qos' => $msg->qos->value, 'retain' => $msg->retain]);
                $this->deliverIfMatches($msg);

                return true;
            case PacketType::PINGRESP->value:
                $this->logger->debug('<< PINGRESP');
                // Clear outstanding flag to allow future auto PINGs
                $this->pingSentAt = null;
                $this->pingResponses++;

                return true;
            case PacketType::SUBACK->value:
                $this->recordAck(PacketType::SUBACK, $this->decoder->decodeSubAck($body));

                return true;
            case PacketType::UNSUBACK->value:
                $this->recordAck(PacketType::UNSUBACK, $this->decoder->decodeUnsubAck($body));

                return true;
            case PacketType::PUBACK->value:
                // QoS 1 acknowledgment: decode full packet with reason code and properties
                $pubAck = $this->decoder->decodePubAck($body);
                $this->logger->debug('<< PUBACK', [
                    'packetId'   => $pubAck->packetId,
                    'reasonCode' => $pubAck->reasonCode,
                    'success'    => $pubAck->isSuccess(),
                ]);
                $this->flowControl?->trackAck($pubAck->packetId);
                $this->recordAck(PacketType::PUBACK, $pubAck);

                return true;
            case PacketType::PUBREL->value:
                // QoS 2 step 2 (Rx): decode PUBREL, respond with PUBCOMP, and deliver stored message
                $pubrel = $this->decoder->decodePubRel($body);
                $pid    = $pubrel->packetId;
                $this->logger->debug('<< PUBREL', [
                    'packetId'   => $pid,
                    'reasonCode' => $pubrel->reasonCode,
                    'success'    => $pubrel->isSuccess(),
                ]);
                // Send PUBCOMP response
                $pubcomp = chr(PacketType::PUBCOMP->value << 4) . chr(2) . pack('n', $pid);
                $this->logger->debug('>> PUBCOMP', ['packetId' => $pid]);
                $this->trackWrite($pubcomp);
                // Deliver stored message if pending
                if (isset($this->qos2InboundPending[$pid])) {
                    $msg = $this->qos2InboundPending[$pid];
                    unset($this->qos2InboundPending[$pid]);
                    $this->logger->debug('<< PUBLISH (QoS2 complete)', ['topic' => $msg->topic, 'packetId' => $pid]);
                    $this->deliverIfMatches($msg);
                }

                return true;
            case PacketType::PUBREC->value:
                // QoS 2 step 1 (Tx): decode PUBREC, respond with PUBREL
                $pubrec = $this->decoder->decodePubRec($body);
                $pid    = $pubrec->packetId;
                $this->logger->debug('<< PUBREC', [
                    'packetId'   => $pid,
                    'reasonCode' => $pubrec->reasonCode,
                    'success'    => $pubrec->isSuccess(),
                ]);
                // Send PUBREL response (flags 0x02)
                $pubrel = chr((PacketType::PUBREL->value << 4) | 0x02) . chr(2) . pack('n', $pid);
                $this->logger->debug('>> PUBREL', ['packetId' => $pid]);
                $this->trackWrite($pubrel);

                return true;
            case PacketType::PUBCOMP->value:
                // QoS 2 step 3 (Tx): decode final PUBCOMP acknowledgment
                $pubComp = $this->decoder->decodePubComp($body);
                $this->logger->debug('<< PUBCOMP', [
                    'packetId'   => $pubComp->packetId,
                    'reasonCode' => $pubComp->reasonCode,
                    'success'    => $pubComp->isSuccess(),
                ]);
                $this->flowControl?->trackAck($pubComp->packetId);
                $this->recordAck(PacketType::PUBCOMP, $pubComp);

                return true;
            case PacketType::DISCONNECT->value:
                // Inbound DISCONNECT: decode full packet and dispatch event
                $disconnect = $this->decoder->decodeDisconnect($body);
                $this->logger->info('<< DISCONNECT', [
                    'reasonCode' => $disconnect->reasonCode,
                    'isError'    => $disconnect->isError(),
                    'reason'     => $disconnect->getReasonDescription(),
                ]);

                // Dispatch ServerDisconnect event before closing transport
                $willReconnect = $this->options->autoReconnect && $disconnect->isError();
                $this->events?->dispatch(new EvServerDisconnect($disconnect, $willReconnect));

                // Reset connection-scoped state
                $this->resetConnectionState();
                $this->transport->close();

                if ($this->options->autoReconnect) {
                    // Keep running; loopOnce will try to reconnect on the next iteration
                    $this->shouldStop = false;
                    $this->logger->info('Will attempt auto-reconnect');
                } else {
                    $this->shouldStop = true;
                }

                return true;
            default:
                // ignore others for MVP
                $this->logger->debug('<< IGNORED', ['type' => $type, 'bytes' => $rl, 'preview' => $this->hexPreview($header.$body)]);

                return true;
        }
    }

    public function tick(): bool
    {
        return $this->loopOnce(0.0);
    }

    public function stop(): void
    {
        $this->stopRequested = true;
    }

    public function awaitMessage(?float $timeoutSec = null): ?InboundMessage
    {
        if (! $this->inbound->isEmpty()) {
            /** @var InboundMessage $m */
            $m = $this->inbound->dequeue();

            return $m;
        }

        if ($timeoutSec === 0.0) {
            return null;
        }

        if ($timeoutSec === null) {
            // Block until we get something
            for (; ;) {
                // @phpstan-ignore-next-line - queue state may change after loopOnce()
                if (! $this->inbound->isEmpty()) {
                    /** @var InboundMessage $m */
                    $m = $this->inbound->dequeue();

                    return $m;
                }
                $this->loopOnce(null);
            }
        }

        $deadline = Clock::now() + $timeoutSec;
        for (; ;) {
            // @phpstan-ignore-next-line - queue state may change after loopOnce()
            if (! $this->inbound->isEmpty()) {
                /** @var InboundMessage $m */
                $m = $this->inbound->dequeue();

                return $m;
            }

            $left = $deadline - Clock::now();
            if ($left <= 0) {
                return null;
            }

            $this->loopOnce($left);
        }
    }

    public function onMessage(callable $handler): void
    {
        $this->messageHandler = $handler;
    }

    public function run(callable $onMessage, ?float $idleSleep = 0.01): void
    {
        $this->onMessage($onMessage);
        for (; ;) {
            if ($this->shouldStop || $this->stopRequested) {
                break;
            }
            $processed = $this->loopOnce(0.2);
            // The handler registered above has already been invoked for anything queued,
            // so drain it here. Without this the queue only ever grows in this loop.
            while (! $this->inbound->isEmpty()) {
                $this->inbound->dequeue();
            }
            if ($processed === false && ($idleSleep !== null && $idleSleep > 0)) {
                usleep((int) floor($idleSleep * 1_000_000));
            }
        }
    }

    /**
     * @return Generator<int, InboundMessage, mixed, void>
     */
    public function messages(?float $timeoutSec = 0.2): Generator
    {
        for (; ;) {
            if ($this->shouldStop || $this->stopRequested) {
                break;
            }
            $msg = $this->awaitMessage($timeoutSec);
            if ($msg instanceof InboundMessage) {
                yield $msg;
            }
        }
    }

    /**
     * @param  list<array{filter:string,qos:int}>  $filters
     */
    private function recordSubscriptionsFromFilters(array $filters, ?SubscribeOptions $options): void
    {
        foreach ($filters as $f) {
            $filter = $f['filter'];
            if ($filter === '') {
                continue;
            }
            $qos = $f['qos'];
            if ($qos < 0) {
                $qos = 0;
            }
            if ($qos > 2) {
                $qos = 2;
            }
            $this->subscriptions[$filter] = ['qos' => $qos, 'options' => $options];
        }
    }

    /**
     * @param  non-empty-list<string>  $filters
     */
    private function removeSubscriptions(array $filters): void
    {
        foreach ($filters as $filter) {
            $f = $filter;
            unset($this->subscriptions[$f]);
        }
    }

    private function attemptReconnect(): bool
    {
        if (! $this->options->autoReconnect) {
            return false;
        }
        if ($this->reconnecting) {
            // A wait inside the reconnect itself — the resubscribe, the offline-queue drain —
            // found the new connection closed again. Reconnecting from in here nested one
            // level per failure: a broker that broke the rules right after every CONNACK grew
            // the stack without bound, and loopOnce() never returned. The wait times out
            // instead, this attempt counts as failed, and the outermost loop tries again.
            return false;
        }
        if ($this->reconnectAttempts >= $this->options->reconnectMaxAttempts) {
            $this->logger->warning('Auto-reconnect: max attempts reached; stopping');
            $this->shouldStop = true;

            return false;
        }

        $delay = $this->computeBackoffDelay();
        if ($delay > 0) {
            usleep((int) floor($delay * 1_000_000));
        }

        $this->reconnecting = true;

        try {
            $this->logger->info('Attempting reconnect', ['attempt' => $this->reconnectAttempts + 1]);
            $this->metrics?->increment('reconnect_attempts');
            $this->establishConnection();
            $this->reconnectAttempts = 0;
            // Resubscribe
            if ($this->subscriptions !== []) {
                $this->isResubscribing = true;
                try {
                    foreach ($this->subscriptions as $topic => $s) {
                        $this->subscribeWith([
                            ['filter' => $topic, 'qos' => $s['qos']],
                        ], $s['options']);
                    }
                } finally {
                    // However the resubscribe ends. Left set, every later subscribe() — even
                    // after a manual connect() — would go unrecorded and not survive the
                    // next reconnect.
                    $this->isResubscribing = false;
                }
            }

            return true;
        } catch (Throwable $e) {
            $this->reconnectAttempts++;
            $this->logger->warning('Reconnect failed', ['error' => $e->getMessage(), 'attempts' => $this->reconnectAttempts]);

            return false;
        } finally {
            $this->reconnecting = false;
        }
    }

    private function computeBackoffDelay(): float
    {
        $attempt = $this->reconnectAttempts;
        $base    = $this->options->reconnectBaseDelay;
        $max     = $this->options->reconnectMaxDelay;
        $delay   = $base * (2 ** max(0, $attempt));
        if ($delay > $max) {
            $delay = $max;
        }
        $jitter = $this->options->reconnectJitter;
        if ($jitter > 0) {
            $r     = mt_rand() / max(1, mt_getrandmax());
            $delta = (2 * $r - 1) * $jitter; // [-jitter, +jitter]
            $delay *= (1.0 + $delta);
        }
        if ($delay < 0) {
            return 0.0;
        }

        return $delay;
    }

    private function hexPreview(string $bytes): string
    {
        $s   = substr($bytes, 0, 64);
        $hex = strtoupper(bin2hex($s));

        return trim(chunk_split($hex, 2, ' ')) . (strlen($bytes) > 64 ? ' …' : '');
    }

    /**
     * Give an inbound PUBLISH its real topic, applying the broker's Topic Alias (MQTT 5).
     *
     * A PUBLISH carrying both a topic and an alias establishes — or replaces — the mapping;
     * one carrying only an alias uses it. Without this, every aliased PUBLISH after the
     * first reached the application with an empty topic.
     *
     * The alias itself is removed from the delivered message's properties. It is a detail of
     * this connection in the broker-to-client direction, meaningless to the application —
     * and harmful when a message is forwarded with its properties, since the number would
     * then be sent back in the other direction.
     *
     * Returns null when the broker broke the rules and auto-reconnect will take over; see
     * abortConnection().
     *
     * @throws ProtocolError for an alias the broker may not use, one it never established,
     *                       or an empty topic with no alias, when nothing will reconnect
     */
    private function resolveInboundTopic(InboundMessage $msg): ?InboundMessage
    {
        $alias = $msg->properties['topic_alias'] ?? null;

        if (! is_int($alias)) {
            if ($msg->topic === '') {
                // §3.3.2.1: an empty Topic Name is only valid together with a Topic Alias.
                return $this->abortConnection(ReasonCode::ProtocolError, 'Inbound PUBLISH has an empty topic and no Topic Alias');
            }

            return $msg;
        }

        // MQTT-3.3.2-8 and -11: 0 is never a valid alias, and the broker may not exceed the
        // Topic Alias Maximum this client sent in CONNECT — where 0 means none at all.
        $maximum = $this->options->topicAliasMaximum;
        if ($alias < 1 || $alias > $maximum) {
            return $this->abortConnection(ReasonCode::TopicAliasInvalid, "Inbound Topic Alias $alias is outside the permitted range 1..$maximum");
        }

        $topic = $msg->topic;
        if ($topic !== '') {
            $this->inboundTopicAliases[$alias] = $topic;
        } else {
            $topic = $this->inboundTopicAliases[$alias] ?? '';
            if ($topic === '') {
                return $this->abortConnection(ReasonCode::ProtocolError, "Inbound Topic Alias $alias was never established on this connection");
            }
        }

        $properties = $msg->properties ?? [];
        unset($properties['topic_alias']);

        return new InboundMessage(
            topic: $topic,
            payload: $msg->payload,
            qos: $msg->qos,
            retain: $msg->retain,
            dup: $msg->dup,
            packetId: $msg->packetId,
            properties: $properties,
        );
    }

    /**
     * End the connection because the broker broke the protocol.
     *
     * Per §4.13 the receiver of a Protocol Error closes the Network Connection; MQTT 5 lets
     * it say why with a DISCONNECT reason code first, MQTT 3.1.1 has no way to. Listeners get
     * a ServerDisconnect event, as for a ping timeout, so the connection never dies silently
     * — not even when the violation surfaces inside a message handler's own publish(). Its
     * reason string says the client closed the connection, not the broker.
     *
     * The transport is closed before the event is dispatched, so a listener that publishes
     * cannot put a packet on the wire after the client's own DISCONNECT. Connection state is
     * not reset here: the next connect does that, and resetting now freed flow-control slots
     * under a publish() still waiting for one, which then wrote to the closed transport.
     *
     * With auto-reconnect on this returns null and loopOnce() reports "nothing processed",
     * so the next call reconnects and a pending publish() resends. With it off, nothing would
     * re-establish the connection, so the violation is thrown.
     *
     * @throws ProtocolError when auto-reconnect is off
     */
    private function abortConnection(ReasonCode $reason, string $message): null
    {
        $this->logger->warning('Closing the connection: the broker violated the protocol', ['reason' => $reason->name, 'detail' => $message]);
        if ($this->options->version === MqttVersion::V5_0 && $this->transport->isOpen()) {
            try {
                // DISCONNECT, Remaining Length 1, the reason code; no properties.
                $this->trackWrite(pack('CCC', PacketType::DISCONNECT->value << 4, 1, $reason->value));
            } catch (MqttException) {
                // Best effort: the connection is being torn down either way.
            }
        }
        $this->transport->close();
        $this->resetFrameState();

        $willReconnect = $this->options->autoReconnect;
        $this->events?->dispatch(new EvServerDisconnect(
            new Disconnect($reason->value, ['reason_string' => "Closed by the client: $message"]),
            $willReconnect,
        ));
        $this->metrics?->increment('protocol_violations', 1.0, ['reason' => $reason->value]);

        if ($willReconnect) {
            return null;
        }

        // As for an inbound DISCONNECT: nothing will re-establish this connection, so
        // run()/messages() stop instead of spinning on a closed transport.
        $this->shouldStop = true;

        throw new ProtocolError($message, $reason->value);
    }

    private function deliverIfMatches(InboundMessage $msg): void
    {
        $filters = $this->options->messageFilters;
        if ($filters !== [] && ! $this->topicMatchesAny($msg->topic, $filters)) {
            $this->logger->debug('Message filtered out by client filters', ['topic' => $msg->topic]);

            return;
        }

        $this->metrics?->increment('messages_delivered', 1.0, ['qos' => $msg->qos->value, 'retain' => $msg->retain ? 1 : 0]);
        $this->metrics?->size('message_payload_bytes', strlen($msg->payload), ['dir' => 'in', 'qos' => $msg->qos->value]);
        // PSR-14: emit MessageReceived when the message is accepted for delivery
        $this->events?->dispatch(new EvMessageReceived($msg));

        $hasHandler = $this->messageHandler !== null;

        // Pull delivery: awaitMessage(), messages() and Client\RequestResponse drain this
        // queue. It is fed even when a handler is registered, because the two channels are
        // documented as independent — but it is bounded, because nothing drains it in the
        // push-only pattern.
        $limit = $this->options->inboundQueueSize;
        if ($limit > 0 && count($this->inbound) >= $limit) {
            $this->inbound->dequeue();
            $this->metrics?->increment('inbound_queue_dropped');
            if ($hasHandler) {
                // Expected in the push-only pattern: the handler already consumed it.
                $this->logger->debug('Inbound queue full; oldest message discarded', ['limit' => $limit]);
            } else {
                $this->logger->warning('Inbound queue full; oldest message dropped before delivery', ['limit' => $limit, 'topic' => $msg->topic]);
            }
        }
        $this->inbound->enqueue($msg);

        // Push delivery.
        if ($hasHandler) {
            try {
                ($this->messageHandler)($msg);
            } catch (Throwable $e) {
                $this->logger->error('Message handler threw exception', [
                    'topic' => $msg->topic,
                    'error' => $e->getMessage(),
                ]);
            }
        }
    }

    /**
     * @param  list<string>  $filters
     */
    private function topicMatchesAny(string $topic, array $filters): bool
    {
        return array_any($filters, fn (string $filter): bool => $this->topicMatchesFilter($topic, $filter));

    }

    private function topicMatchesFilter(string $topic, string $filter): bool
    {
        if ($filter === '#') {
            return true;
        }
        $tLevels = explode('/', $topic);
        $fLevels = explode('/', $filter);
        $ti      = 0;
        $fi      = 0;
        $tn      = count($tLevels);
        $fn      = count($fLevels);
        while ($fi < $fn) {
            $f = $fLevels[$fi];
            if ($f === '#') {
                // multi-level must be last
                return $fi === $fn - 1;
            }
            if ($ti >= $tn) {
                return false;
            }
            $t = $tLevels[$ti];
            if ($f !== '+' && $f !== $t) {
                return false;
            }
            $fi++;
            $ti++;
        }

        return $ti === $tn && $fi === $fn;
    }

    private function sessionStore(): ?SessionStoreInterface
    {
        $store = $this->options->sessionStore ?? null;

        return $store instanceof SessionStoreInterface ? $store : null;
    }

    /**
     * Save session state to the configured session store.
     */
    private function saveSession(string $clientId): void
    {
        $store = $this->sessionStore();
        if (! $store instanceof SessionStoreInterface) {
            return;
        }

        $pendingQos2 = array_keys($this->qos2InboundPending);

        $state = new SessionState(
            subscriptions: $this->subscriptions,
            pendingQos2: $pendingQos2,
            savedAt: time(),
        );

        try {
            $store->save($clientId, $state);
            $this->logger->debug('Session saved', [
                'clientId'      => $clientId,
                'subscriptions' => count($this->subscriptions),
                'pendingQos2'   => count($pendingQos2),
            ]);
        } catch (Throwable $e) {
            $this->logger->warning('Failed to save session', ['error' => $e->getMessage()]);
        }
    }

    /**
     * Restore session state from the configured session store.
     */
    private function restoreSession(string $clientId): void
    {
        $store = $this->sessionStore();
        if (! $store instanceof SessionStoreInterface) {
            return;
        }

        try {
            $state = $store->load($clientId);
            if (!$state instanceof SessionState) {
                $this->logger->debug('No saved session found', ['clientId' => $clientId]);

                return;
            }

            // Restore subscriptions
            /** @var array<string, array{qos: int, options: ?SubscribeOptions}> $restoredSubs */
            $restoredSubs        = $state->subscriptions;
            $this->subscriptions = $restoredSubs;

            $this->logger->debug('Session restored', [
                'clientId'      => $clientId,
                'subscriptions' => count($state->subscriptions),
                'age'           => $state->getAge(),
            ]);
        } catch (Throwable $e) {
            $this->logger->warning('Failed to restore session', ['error' => $e->getMessage()]);
        }
    }

    /**
     * Drain the offline message queue, publishing all buffered messages.
     */
    private function drainOfflineQueue(): void
    {
        if (! $this->offlineQueue instanceof OfflineQueue || $this->offlineQueue->isEmpty()) {
            return;
        }

        $drained = 0;
        // Only while the connection holds: publish() on a closed transport puts the message
        // straight back into this queue, so draining on past a disconnect could only cycle
        // messages through it.
        while ($this->transport->isOpen() && ($msg = $this->offlineQueue->dequeue()) !== null) {
            $sentBefore = $this->bytesSent;
            try {
                $this->publish($msg['topic'], $msg['payload'], $msg['options']);
                $drained++;
            } catch (Throwable $e) {
                if (($e instanceof TransportError || $e instanceof Timeout) && $this->bytesSent === $sentBefore) {
                    // Not a byte reached the wire, for a passing reason — the connection was
                    // gone, or no flow-control slot freed up in time — so it is safe to retry
                    // on the next connection, at the head of the queue.
                    $this->offlineQueue->requeue($msg);
                    $this->logger->warning('Offline message kept for the next connection', ['topic' => $msg['topic'], 'error' => $e->getMessage()]);

                    break;
                }

                // Either it reached the broker — retrying could deliver a QoS 2 message twice —
                // or it can never be sent, such as one that cannot be encoded, and retrying it
                // would block every message behind it for good. Dropped, as it always was.
                $this->logger->warning('Offline message dropped', ['topic' => $msg['topic'], 'error' => $e->getMessage()]);
                $this->metrics?->increment('offline_queue_dropped');
            }
        }

        if ($drained > 0) {
            $this->logger->info('Drained offline queue', ['count' => $drained]);
            $this->metrics?->increment('offline_queue_drained', (float) $drained);
        }
    }
}
