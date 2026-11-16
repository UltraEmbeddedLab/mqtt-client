<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use Closure;
use Generator;
use InvalidArgumentException;
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
use ScienceStories\Mqtt\Events\SubscriptionRefused as EvSubscriptionRefused;
use ScienceStories\Mqtt\Exception\AuthenticationError;
use ScienceStories\Mqtt\Exception\MqttException;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Exception\QuotaExceeded;
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
use ScienceStories\Mqtt\Protocol\Packet\PubRec;
use ScienceStories\Mqtt\Protocol\Packet\SubAck;
use ScienceStories\Mqtt\Protocol\Packet\UnsubAck;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Protocol\ReasonCode;
use ScienceStories\Mqtt\Protocol\V311\Decoder as V311Decoder;
use ScienceStories\Mqtt\Protocol\V311\Encoder as V311Encoder;
use ScienceStories\Mqtt\Protocol\V5\Decoder as V5Decoder;
use ScienceStories\Mqtt\Protocol\V5\Encoder as V5Encoder;
use ScienceStories\Mqtt\Session\SessionState;
use ScienceStories\Mqtt\Util\Bytes;
use ScienceStories\Mqtt\Util\Clock;
use ScienceStories\Mqtt\Util\TopicValidator;
use SplQueue;
use Throwable;

use function array_any;
use function array_filter;
use function array_key_exists;
use function array_key_first;
use function array_keys;
use function array_values;
use function bin2hex;
use function chr;
use function chunk_split;
use function count;
use function explode;
use function floor;
use function in_array;
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
use function sprintf;
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

    /** How long a connection must last for the reconnect count to start over. */
    private const float STABLE_CONNECTION_SECONDS = 10.0;

    /**
     * DISCONNECT reasons that blame a message rather than the connection (MQTT 5). A broker
     * that ends a connection with one has refused something just sent — see resendInflight().
     */
    private const array MESSAGE_REJECTIONS = [
        ReasonCode::NotAuthorized,
        ReasonCode::TopicNameInvalid,
        ReasonCode::PacketTooLarge,
        ReasonCode::PayloadFormatInvalid,
        ReasonCode::RetainNotSupported,
        ReasonCode::QoSNotSupported,
    ];

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
     * When the current connection's CONNACK arrived ({@see Clock::now()}); null once a
     * reconnect has judged it. How long a connection lasted decides whether the reconnect
     * backoff starts over — see attemptReconnect().
     */
    private ?float $connectedAt = null;

    /** QoS 1/2 messages the broker has not finished acknowledging, in resend order. */
    private readonly OutboundInflight $outbound;

    /** Where allocatePacketId() looks next. */
    private int $nextPacketId = 1;

    /**
     * Counts CONNACKs, so a publish() still waiting can tell that its message has already
     * been resent on a new connection by resendInflight().
     */
    private int $connectionEpoch = 0;

    /**
     * In-flight messages still to resend on this connection, in order, because Receive
     * Maximum had no room when it opened. They go out as acknowledgements free slots, ahead
     * of anything new (drainResendBacklog()).
     *
     * @var list<int>
     */
    private array $resendBacklog = [];

    /** Whether this connection resumed a session, which decides the DUP flag on resends. */
    private bool $resendDup = false;

    /**
     * The reason the broker gave for ending the last connection, when it blamed a message
     * (MESSAGE_REJECTIONS); null otherwise. Read, and cleared, by the next resendInflight().
     */
    private ?ReasonCode $rejectedWith = null;

    /**
     * Whether this connection resends one message at a time — after a connection the broker
     * ended over a message, so that a refusal can be pinned on the message that caused it.
     */
    private bool $probing = false;

    /** While probing: the message sent alone and not yet accepted. */
    private ?int $probe = null;

    /** Maximum QoS the broker accepts for a PUBLISH on this connection (MQTT 5 CONNACK). */
    private int $brokerMaximumQos = 2;

    /** Whether the broker accepts retained messages on this connection (MQTT 5 CONNACK). */
    private bool $brokerRetainAvailable = true;

    /** The largest packet the broker accepts on this connection, if it said (MQTT 5 CONNACK). */
    private ?int $brokerMaximumPacketSize = null;

    /**
     * The Client Identifier the broker knows this session by — the one it assigned, if the
     * application left it empty. The session store is read and written under this key.
     */
    private ?string $sessionClientId = null;

    /**
     * SubscriptionRefused events from the automatic resubscribe, held until it is over.
     *
     * Dispatched mid-resubscribe, a listener that threw was taken for a failed reconnect —
     * leaving a live connection with the rest of the filters never sent — and one that
     * subscribed to a fallback filter was not remembered, because the client was still
     * flagged as resubscribing.
     *
     * @var list<EvSubscriptionRefused>
     */
    private array $deferredRefusals = [];

    /**
     * Whether the session store has been consulted. Only the first connection restores
     * from it; after that the in-memory subscriptions are at least as fresh as anything
     * stored, and restoring on every reconnect brought back filters the broker had refused
     * and dropped ones subscribed since the last save.
     */
    private bool $sessionRestoreDone = false;

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
     * @var array<int, PubAck|PubRec|PubComp|SubAck|UnsubAck|null>
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
        // transport that is already closed. Connection state is left for the next connect to
        // reset: resetting it here freed the flow-control slot under a waiting publish().
        $this->transport->close();
        $this->resetFrameState();

        $willReconnect = $this->willReconnect();
        if (! $willReconnect) {
            // Nothing will re-establish this connection, so stop run()/messages() rather
            // than spinning on a closed transport forever. Before the event, so a listener
            // that calls connect() in response is not undone.
            $this->shouldStop = true;
        }
        $this->events?->dispatch(new EvServerDisconnect(
            new Disconnect(ReasonCode::KeepAliveTimeout->value, ['reason_string' => 'Closed by the client: no PINGRESP within the Ping Response Timeout']),
            $willReconnect,
        ));
    }

    public function __construct(private readonly Options $options, private readonly TransportInterface $transport, private readonly LoggerInterface $logger = new NullLogger(), private readonly ?EventDispatcherInterface $events = null, private readonly ?MetricsInterface $metrics = null)
    {
        $this->inbound  = new SplQueue();
        $this->outbound = new OutboundInflight();
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
        // The application starting over is a fresh start for auto-reconnect too.
        $this->reconnectAttempts = 0;

        $result            = $this->establishConnection();
        $this->connectedAt = Clock::now();

        return $result;
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

        // The limits the broker sets on what this client may send (MQTT 5), re-derived from
        // every CONNACK; MQTT 3.1.1 brokers announce none.
        $this->brokerMaximumQos        = $connack->getMaximumQoS()     ?? 2;
        $this->brokerRetainAvailable   = $connack->isRetainAvailable() ?? true;
        $this->brokerMaximumPacketSize = $connack->getMaximumPacketSize();

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
        $clientId              = $assignedId ?? $this->options->clientId;
        $this->sessionClientId = $clientId;
        if (! $this->sessionRestoreDone && $this->options->sessionStore instanceof SessionStoreInterface) {
            if ($connack->sessionPresent && ! $this->options->cleanSession) {
                $this->restoreSession($clientId);
            } else {
                // The broker has no session to finish these in; kept, they would be replayed
                // into whichever later session happened to be present.
                $this->discardStoredInflight($clientId);
            }
        }
        // Whatever the first connection found, from here on memory is the newer record.
        $this->sessionRestoreDone = true;

        $this->connectionEpoch++;
        $this->metrics?->increment('connections', 1.0, ['version' => $versionStr]);
        $this->metrics?->observe('connect_duration_seconds', Clock::now() - $connectStart);

        $resent = false;
        try {
            $this->resendInflight($connack->sessionPresent);
            $resent = true;
        } finally {
            if (! $resent) {
                // As for a failed handshake: a connection that could not take the resend is
                // not one to keep, or isOpen() would hide it from auto-reconnect.
                $this->transport->close();
                $this->resetFrameState();
            }
        }

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
            if ($this->options->requestResponseInformation !== null) {
                $connectProps['request_response_information'] = $this->options->requestResponseInformation;
            }
            if ($this->options->requestProblemInformation !== null) {
                $connectProps['request_problem_information'] = $this->options->requestProblemInformation;
            }
            if ($this->options->userProperties !== null && $this->options->userProperties !== []) {
                $connectProps['user_properties'] = $this->options->userProperties;
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
        // The application ended the connection: nothing reconnects it until connect(). Left
        // unset, the next loopOnce() — inside run(), or a handler's own — auto-reconnected.
        $this->shouldStop = true;

        // Save session state before disconnecting (if persistent session)
        if ($this->options->sessionStore instanceof SessionStoreInterface && ! $this->options->cleanSession) {
            // Under the identifier the broker keeps the session by: saved under an empty one
            // while the broker had assigned another, it was never found again.
            $this->saveSession($this->sessionClientId ?? $this->options->clientId);
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
        $written = false;

        return $this->sendPublish($topic, $payload, $options, $written);
    }

    /**
     * publish(), telling the caller whether its PUBLISH reached the wire — which the offline
     * drain needs to know, and cannot learn from the byte counters: resends owed to the
     * broker may go out during the same call.
     *
     * @param  bool  $written  set to true once this message's PUBLISH has been written
     */
    private function sendPublish(string $topic, string $payload, ?PublishOptions $options, bool &$written): int
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

        // What the broker said it will not take is not sent: it would end the connection
        // (MQTT-3.2.2-11, MQTT-3.2.2-14), and a reconnect would resend it straight into the same
        // refusal. The size is checked once the packet is encoded, below.
        $breach = $this->brokerLimitBreach($options->qos, $options->retain);
        if ($breach instanceof ReasonCode) {
            throw $this->brokerLimitException($breach, $options->qos, 0);
        }

        $qos      = $options->qos->value;
        $packetId = null;
        if ($qos > 0) {
            // Wait for a flow-control slot, and for the resends owed on this connection to go
            // out first: a new message must not overtake them.
            if ($this->mustWaitToPublish()) {
                $this->logger->debug('Waiting to publish', [
                    'inFlight'       => $this->flowControl?->currentInFlight,
                    'max'            => $this->flowControl?->maxInFlight,
                    'resendsWaiting' => count($this->resendBacklog),
                ]);
                // Poll loopOnce to process incoming ACKs while waiting
                $deadline = Clock::now() + 5.0;
                while ($this->mustWaitToPublish()) {
                    $timeLeft = $deadline - Clock::now();
                    if ($timeLeft <= 0) {
                        throw new Timeout('Flow control: timed out waiting for slot');
                    }
                    if (! $this->loopOnce(max(0.01, $timeLeft)) && ! $this->transport->isOpen()) {
                        if ($this->reconnecting || $this->connectionEnded()) {
                            // As in awaitAck(): nothing will reopen it from here.
                            throw new TransportError('Connection closed while waiting for a flow-control slot');
                        }
                        // Nothing arrives on a closed transport until something reconnects
                        // it; back off instead of spinning a core until the deadline.
                        usleep(10_000);
                    }
                }
            }
            // Only once the PUBLISH can go out: a wait that times out uses none up.
            $packetId = $this->allocatePacketId();
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
        $newAlias     = false;
        if ($this->topicAliasManager instanceof TopicAliasManager && $this->options->version === MqttVersion::V5_0) {
            $aliasResult = $this->topicAliasManager->getOrCreateAlias($topic);
            $newAlias    = $aliasResult['isNew'];
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

        $encoded = false;
        try {
            $data   = $this->encoder->encodePublish($pkt);
            $breach = $this->brokerLimitBreach($options->qos, $options->retain, strlen($data));
            if ($breach instanceof ReasonCode) {
                throw $this->brokerLimitException($breach, $options->qos, strlen($data));    // MQTT-3.2.2-15
            }
            $encoded = true;
        } finally {
            if (! $encoded && $newAlias) {
                // The PUBLISH that would have set the alias up never goes out: the broker must
                // not be sent the alias alone next time, for a mapping it never received.
                $this->topicAliasManager->forgetNew($topic);
            }
        }
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
        $written = true;

        if ($packetId !== null) {
            $this->flowControl?->trackSend($packetId);
        }

        $this->metrics?->increment('publish_sent', 1.0, ['qos' => $qos]);
        $this->metrics?->size('publish_payload_bytes', strlen($payload), ['qos' => $qos]);

        // In flight from the start of the wait (a packet identifier exists only for QoS 1/2).
        // Recorded as it must be resent: the full topic and no Topic Alias, which a new
        // connection would not know.
        return $this->awaitPublishAck($qos, $packetId === null ? null : new Publish($topic, $payload, $options->qos, $options->retain, packetId: $packetId, properties: $properties));
    }

    /**
     * Whether a new QoS 1/2 PUBLISH must wait: for a flow-control slot, or for the resends
     * owed on this connection, which go first.
     */
    private function mustWaitToPublish(): bool
    {
        $this->drainResendBacklog();

        return $this->resendBacklog !== [] || ($this->flowControl instanceof FlowControl && ! $this->flowControl->canSend());
    }

    /**
     * Which limit the broker set for this connection a PUBLISH would break, if any.
     *
     * @param  int|null  $size  the encoded packet's length, once known
     */
    private function brokerLimitBreach(QoS $qos, bool $retain, ?int $size = null): ?ReasonCode
    {
        if ($qos->value > $this->brokerMaximumQos) {
            return ReasonCode::QoSNotSupported;
        }
        if ($retain && ! $this->brokerRetainAvailable) {
            return ReasonCode::RetainNotSupported;
        }
        if ($size !== null && $this->brokerMaximumPacketSize !== null && $size > $this->brokerMaximumPacketSize) {
            return ReasonCode::PacketTooLarge;
        }

        return null;
    }

    /** The exception a PUBLISH the broker would refuse is turned away with, before sending. */
    private function brokerLimitException(ReasonCode $breach, QoS $qos, int $size): MqttException
    {
        return $breach->toException(match ($breach) {
            ReasonCode::QoSNotSupported    => "PUBLISH not sent: QoS $qos->value is above the broker's Maximum QoS of $this->brokerMaximumQos",
            ReasonCode::RetainNotSupported => 'PUBLISH not sent: the broker does not accept retained messages',
            default                        => "PUBLISH not sent: $size bytes is above the broker's Maximum Packet Size of $this->brokerMaximumPacketSize",
        });
    }

    /**
     * Return flow-control quota held by messages the broker will plainly never acknowledge.
     *
     * The slot for an in-flight PUBLISH belongs to the broker until it acknowledges, so it
     * must NOT be released the moment `publish()` gives up waiting: MQTT-4.9.0-1 counts the
     * message against Receive Maximum while it is still unacknowledged, and freeing it
     * early lets the client exceed the window the broker granted.
     *
     * But holding the quota forever is the leak this sweep exists to prevent — a handful of
     * timeouts against a broker advertising `receive_maximum: 1` would otherwise exhaust it
     * for the life of the connection. So slots are reclaimed only well past the point where
     * an acknowledgement could still plausibly arrive: the longest a publish() can wait,
     * doubled. That is a QoS 2 exchange — one ackTimeout per resend plus one for each of its
     * two steps, PUBREC and PUBCOMP.
     *
     * The messages themselves are never let go of on time alone: the broker may still hold
     * them, and elapsed time does not end a session (MQTT 5 §4.1). They stay in flight — their
     * identifiers reserved, resent on the next connection — until the broker answers, refuses
     * them, or the session is gone.
     */
    private function sweepStalledInflight(): void
    {
        $longestWait = $this->options->ackTimeout * ($this->options->maxResendAttempts + 2);
        $grace       = max(1.0, $longestWait * 2);

        if (! $this->flowControl instanceof FlowControl) {
            return;
        }
        foreach ($this->flowControl->getTimedOutPackets($grace) as $packetId) {
            $this->logger->warning('Reclaiming flow-control slot for an unacknowledged packet', [
                'packetId'     => $packetId,
                'graceSeconds' => round($grace, 3),
            ]);
            $this->flowControl->trackAck($packetId);
            $this->metrics?->increment('flow_control_slots_reclaimed');
        }
        $this->drainResendBacklog();
    }

    /**
     * Block until the broker acknowledges a QoS 1/2 publish.
     *
     * QoS 1 completes on PUBACK. QoS 2 (MQTT-4.3.3) completes in two steps: PUBREC, which
     * loopOnce() answers with PUBREL, then PUBCOMP. A PUBREC with a reason code of 0x80 or
     * above ends the exchange: no PUBREL is sent (MQTT-4.3.3-4), and the error is logged, as
     * a failed PUBACK is.
     *
     * When to send the message again is the protocol's call, not a timer's. A reconnect
     * resends everything still in flight at once, in order (resendInflight()). On a live
     * connection MQTT 5 forbids resending at all (MQTT-4.4.0-1), so there an expired wait is
     * simply waited again; MQTT 3.1.1 allows it, so there the message goes again — the
     * PUBLISH with DUP until PUBREC arrives, only the PUBREL after (MQTT-4.3.3-6). Either way
     * at most `maxResendAttempts` extra periods of `ackTimeout` are waited.
     *
     * Giving up does not end the exchange for the broker. The message stays in flight — its
     * Packet Identifier reserved, owed a resend if the session resumes — until it is
     * acknowledged, the session is lost, or sweepStalledInflight() gives up on it too.
     *
     * @param  Publish|null  $inflight  the message as it must be resent; null for QoS 0
     * @return int The packet identifier, or 0 for QoS 0
     *
     * @throws Timeout if the acknowledgement never arrives
     * @throws TransportError if the connection ends and nothing will reopen it
     */
    private function awaitPublishAck(int $qos, ?Publish $inflight): int
    {
        if ($qos === 0 || ! $inflight instanceof Publish || $inflight->packetId === null) {
            return 0;
        }

        $packetId   = $inflight->packetId;
        $ackTimeout = $this->options->ackTimeout;
        $periods    = 0;                       // extra periods waited, at most maxResendAttempts
        $epoch      = $this->connectionEpoch;
        $onExpiry   = function () use (&$periods, &$epoch, $packetId): bool {
            if ($periods >= $this->options->maxResendAttempts) {
                return false;
            }
            $periods++;
            if ($epoch !== $this->connectionEpoch) {
                // Reconnected meanwhile, and resendInflight() resent it there (or will, as
                // slots free). Resending again now would only duplicate that.
                $epoch = $this->connectionEpoch;

                return true;
            }
            if ($this->options->version === MqttVersion::V3_1_1 && $this->transport->isOpen() && $this->outbound->has($packetId)) {
                $this->resendOutbound($packetId, dup: true);
            }

            return true;
        };

        $waitingFor = PacketType::PUBACK;
        $pubCompKey = null;
        $settled    = false;
        try {
            $this->outbound->add($inflight);
            if ($qos === 1) {
                $this->logPublishOutcome($this->awaitAck(PacketType::PUBACK, $packetId, PubAck::class, $ackTimeout, 'PUBACK', $onExpiry));
                $settled = true;

                return $packetId;
            }

            // Registered now rather than after PUBREC: a publish() from inside an onMessage()
            // handler pumps loopOnce() as well, and can read this exchange's PUBREC and PUBCOMP
            // before control comes back here. Unregistered, the PUBCOMP was dropped as
            // unsolicited and this wait sat out a full ackTimeout to resend a PUBREL nobody needed.
            $pubCompKey              = $this->ackKey(PacketType::PUBCOMP, $packetId);
            $this->acks[$pubCompKey] = null;

            $waitingFor = PacketType::PUBREC;
            $pubRec     = $this->awaitAck(PacketType::PUBREC, $packetId, PubRec::class, $ackTimeout, 'PUBREC', $onExpiry);
            if ($pubRec->isError()) {
                // loopOnce() sent no PUBREL, returned the flow-control slot and forgot the
                // message; the Packet Identifier is free again.
                $this->logPublishOutcome($pubRec);
                $settled = true;

                return $packetId;
            }

            $waitingFor = PacketType::PUBCOMP;
            $this->logPublishOutcome($this->awaitAck(PacketType::PUBCOMP, $packetId, PubComp::class, $ackTimeout, 'PUBCOMP', $onExpiry));
            $settled = true;

            return $packetId;
        } catch (Timeout) {
            $sends = $this->outbound->sends($packetId);

            throw new Timeout($sends === 0
                ? "Timed out waiting for $waitingFor->name; the message was given up on"
                : "Timed out waiting for $waitingFor->name after ".($sends - 1).' resend(s)');
        } finally {
            if (! $settled) {
                // However the wait ended — a timeout, a closed connection, a listener's
                // exception — publish() stops waiting; the broker may not have.
                $this->outbound->abandon($packetId, Clock::now());
            }
            if ($pubCompKey !== null) {
                unset($this->acks[$pubCompKey]);
            }
        }
    }

    /**
     * Write an in-flight message again: its PUBREL once released, otherwise the PUBLISH —
     * with DUP when the broker may have seen it before (MQTT-3.3.1-1).
     *
     * With $onNewConnection, a PUBLISH that the limits the broker announced for this
     * connection forbid is not sent but given up on: sending it would only get the connection
     * closed again.
     */
    private function resendOutbound(int $packetId, bool $dup, bool $onNewConnection = false): void
    {
        $message = $this->outbound->find($packetId);
        if ($message === null) {
            return;
        }

        if ($message['released']) {
            $this->logger->debug('>> PUBREL (resend)', ['packetId' => $packetId]);
            $this->trackWrite($this->pubRelPacket($packetId));
        } else {
            $publish = $message['publish'];
            try {
                $data = $this->encoder->encodePublish(new Publish(
                    $publish->topic,
                    $publish->payload,
                    $publish->qos,
                    $publish->retain,
                    $dup,
                    $packetId,
                    $publish->properties,
                ));
            } catch (InvalidArgumentException|MqttException $e) {
                // Restored from a store this version of the protocol cannot encode, say. Left
                // in, it would fail every connection it was resent on.
                $this->outbound->remove($packetId);
                $this->logger->warning('Giving up on a message in flight that cannot be encoded', ['packetId' => $packetId, 'error' => $e->getMessage()]);
                $this->metrics?->increment('inflight_dropped', 1.0, ['reason' => 'unencodable']);

                return;
            }
            $breach = $onNewConnection ? $this->brokerLimitBreach($publish->qos, $publish->retain, strlen($data)) : null;
            if ($breach instanceof ReasonCode) {
                $this->outbound->remove($packetId);
                $this->logger->warning('Giving up on a message in flight that this connection does not allow', [
                    'packetId' => $packetId,
                    'reason'   => $breach->name,
                ]);
                $this->metrics?->increment('inflight_dropped', 1.0, ['reason' => $breach->value]);

                return;
            }
            $this->logger->debug('>> PUBLISH (resend)', ['packetId' => $packetId, 'qos' => $publish->qos->value, 'dup' => $dup]);
            $this->trackWrite($data);
        }
        $this->outbound->noteSent($packetId);
        $this->flowControl?->trackSend($packetId);
    }

    /**
     * Resend what the broker has not finished acknowledging, first thing on a new connection
     * (MQTT-4.4.0-1), in the order the protocol requires (MQTT-4.6.0-1, MQTT-4.6.0-4).
     *
     * With the session resumed the broker still has its half: each PUBLISH goes again with
     * DUP, and a PUBREL for each message already released. Without it the broker kept
     * nothing: messages no publish() is waiting for go with the old session, and the rest are
     * published anew, for the application still waiting on them. A released message gets its
     * PUBREL either way — the broker answers PUBCOMP even for an identifier it does not know
     * (0x92 on MQTT 5), which is what ends that publish()'s wait.
     *
     * What Receive Maximum has no room for yet waits in the backlog and goes out, still in
     * order, as acknowledgements free slots.
     *
     * A message the broker refuses by ending the connection — MQTT 5 says so in the
     * DISCONNECT reason (MESSAGE_REJECTIONS) — would otherwise be resent first on every new
     * connection and end each one, until auto-reconnect gave up. So after such a connection
     * the next one probes: it resends one message at a time, each once the previous was
     * accepted. Refused again while one message alone was waiting, that message is the one,
     * and only then is it given up on. A connection that merely dropped proves nothing about
     * any message, and gives up on none: elapsed time and network failures do not end a
     * session. (MQTT 3.1.1 has no DISCONNECT reasons, so there nothing is ever given up on;
     * auto-reconnect's attempt limit bounds the loop instead.)
     */
    private function resendInflight(bool $sessionPresent): void
    {
        $rejection = $this->rejectedWith;
        $culprit   = null;
        if ($rejection instanceof ReasonCode && $this->probe !== null && $this->outbound->has($this->probe)) {
            // Refused again while it waited alone: this message is the one.
            $culprit = $this->probe;
            $this->outbound->remove($culprit);
            $this->logger->warning('Giving up on a message the broker refused by ending the connection', [
                'packetId' => $culprit,
                'reason'   => $rejection->name,
            ]);
            $this->metrics?->increment('inflight_dropped', 1.0, ['reason' => $rejection->value]);
        }
        $this->probing      = $rejection instanceof ReasonCode && $culprit === null;
        $this->probe        = null;
        $this->rejectedWith = null;

        $this->resendBacklog = [];
        $this->resendDup     = $sessionPresent;
        if (! $sessionPresent) {
            $this->outbound->dropAbandoned();
        }

        $this->resendBacklog = $this->outbound->resendOrder();
        $this->drainResendBacklog();
        if ($this->resendBacklog !== []) {
            $this->logger->info('Receive Maximum reached while resending; the rest follow as slots free up', [
                'waiting' => count($this->resendBacklog),
            ]);
        }
    }

    /**
     * Resend what the backlog holds, in order, for as long as Receive Maximum allows. Runs
     * whenever a slot frees up, and before a new message takes one, so a message owed a
     * resend is never overtaken by a newer one.
     */
    private function drainResendBacklog(): void
    {
        while ($this->resendBacklog !== [] && $this->transport->isOpen()) {
            if ($this->flowControl instanceof FlowControl && ! $this->flowControl->canSend()) {
                return;
            }
            if ($this->probe !== null) {
                $probe = $this->outbound->find($this->probe);
                if ($probe !== null && ! $probe['released']) {
                    return;                                 // the probe is not accepted yet
                }
                $this->probe = null;
            }
            $packetId = array_shift($this->resendBacklog);
            $message  = $this->outbound->find($packetId);
            if ($this->probing && $message !== null && ! $message['released']) {
                $this->probe = $packetId;
            }
            if ($message !== null && $message['released'] && ! $this->resendDup) {
                $this->logger->warning('Releasing a QoS 2 message on a session the broker did not keep; whether it was delivered is unknown', ['packetId' => $packetId]);
            }
            try {
                $this->resendOutbound($packetId, $this->resendDup, onNewConnection: true);
            } catch (TransportError $e) {
                // Called from the receive loop and from publish(): a write that fails means the
                // connection is gone. Close it so the next loopOnce() reconnects, rather than
                // throwing from wherever the drain happened to run.
                $this->logger->info('Transport error while resending; will try to reconnect', ['error' => $e->getMessage()]);
                $this->resendBacklog = [];
                $this->transport->close();
                $this->resetFrameState();
                $this->noteConnectionLost();

                return;
            }
        }
    }

    /**
     * The next Packet Identifier not in use (MQTT-2.2.1-3): not held by a message still in
     * flight, nor by a SUBSCRIBE or UNSUBSCRIBE waiting for its answer. Sequential for each
     * client, from 1 up to 65,535 and round again. The process-wide counter it replaces could
     * hand a new QoS 2 message the identifier of one the broker still held, and the broker
     * then took the new message for a duplicate and dropped it.
     *
     * @throws QuotaExceeded when every identifier is in use
     */
    private function allocatePacketId(): int
    {
        for ($tries = 0; $tries < 0xFFFF; $tries++) {
            $packetId           = $this->nextPacketId;
            $this->nextPacketId = $packetId >= 0xFFFF ? 1 : $packetId + 1;
            if (! $this->outbound->has($packetId)
                && ! array_key_exists($this->ackKey(PacketType::SUBACK, $packetId), $this->acks)
                && ! array_key_exists($this->ackKey(PacketType::UNSUBACK, $packetId), $this->acks)) {
                return $packetId;
            }
        }

        throw new QuotaExceeded('Every Packet Identifier is in use by an unacknowledged request');
    }

    /** Log how the broker answered a publish; a refusal is a warning and a metric. */
    private function logPublishOutcome(PubAck|PubRec|PubComp $ack): void
    {
        $type = match (true) {
            $ack instanceof PubAck => 'PUBACK',
            $ack instanceof PubRec => 'PUBREC',
            default                => 'PUBCOMP',
        };
        if (! $ack->isError()) {
            $this->logger->info($type, ['packetId' => $ack->packetId, 'reasonCode' => $ack->reasonCode, 'success' => $ack->isSuccess()]);

            return;
        }

        if ($ack instanceof PubComp) {
            // The broker accepted the message at PUBREC; PUBCOMP only retires the Packet
            // Identifier. Its one error code, 0x92 Packet Identifier not found, is what a
            // resent PUBREL gets after the first PUBCOMP was lost — "not an error during
            // recovery" (MQTT 5 §3.7.2.1) — so it is not counted as a failed publish.
            $this->logger->notice('PUBCOMP: the broker no longer knows this Packet Identifier', [
                'packetId'   => $ack->packetId,
                'reasonCode' => sprintf('0x%02X', $ack->reasonCode),
            ]);

            return;
        }

        $this->logger->warning('The broker reported a failure for a publish', [
            'packet'       => $type,
            'packetId'     => $ack->packetId,
            'reasonCode'   => sprintf('0x%02X', $ack->reasonCode),
            'reason'       => $ack->getReasonDescription(),
            'reasonString' => $ack->getReasonString(),
        ]);
        $this->metrics?->increment('publish_failed', 1.0, ['packet' => $type, 'reason' => $ack->reasonCode]);
    }

    /** PUBREL: fixed-header flags 0b0010 (MQTT-3.6.1-1); no Reason Code means Success. */
    private function pubRelPacket(int $packetId): string
    {
        return chr((PacketType::PUBREL->value << 4) | 0x02) . chr(2) . pack('n', $packetId);
    }

    /**
     * What has arrived for $key so far. A method rather than an inline read, because
     * loopOnce() fills the slot behind the caller's back.
     */
    private function receivedAck(int $key): PubAck|PubRec|PubComp|SubAck|UnsubAck|null
    {
        return $this->acks[$key] ?? null;
    }

    /** One integer key per (packet type, Packet Identifier) pair. */
    private function ackKey(PacketType $type, int $packetId): int
    {
        return ($type->value << 16) | $packetId;
    }

    /** Hand an inbound acknowledgement to whoever is waiting for it, if anyone is. */
    private function recordAck(PacketType $type, PubAck|PubRec|PubComp|SubAck|UnsubAck $ack): void
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
     * can read in between, so an acknowledgement cannot arrive before the key exists. A wait
     * that starts later than that registers its key in advance (see awaitPublishAck()).
     *
     * @template T of PubAck|PubRec|PubComp|SubAck|UnsubAck
     *
     * @param  class-string<T>  $class
     * @param  (Closure(): bool)|null  $onExpiry  called when a wait expires; return true after
     *                                            resending to wait again, false to give up
     * @return T
     *
     * @throws Timeout once the wait expires and $onExpiry declines to retry
     * @throws TransportError when the connection closes during a reconnect's own wait
     */
    private function awaitAck(PacketType $type, int $packetId, string $class, float $timeout, string $what, ?Closure $onExpiry = null): PubAck|PubRec|PubComp|SubAck|UnsubAck
    {
        $key = $this->ackKey($type, $packetId);
        // Keep an acknowledgement that has already arrived: a caller may register a key ahead
        // of the wait — the QoS 2 sender registers PUBCOMP's while it still waits for PUBREC.
        $this->acks[$key] ??= null;

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
                    if ($this->reconnecting || $this->connectionEnded()) {
                        // Nothing will reopen the connection: inside a reconnect (they do not
                        // nest), or once it has ended for good — auto-reconnect off or done
                        // with. Say so now rather than when the timeout runs out.
                        throw new TransportError("Connection closed while waiting for $what");
                    }
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

    /**
     * @param  non-empty-list<array{filter: string, qos: int}>  $filters
     */
    public function subscribeWith(array $filters, ?SubscribeOptions $options = null): SubscribeResult
    {
        // Exactly the filters that go on the wire, in order. The encoders skip empty ones, and
        // the SUBACK carries one code per filter actually sent (MQTT-3.9.3-1) — matching codes
        // against the list as given would shift every code after an empty filter.
        $sent = array_values(array_filter($filters, static fn (array $f): bool => $f['filter'] !== ''));
        if ($sent === []) {
            // A SUBSCRIBE without a single filter is a protocol violation (MQTT-3.8.3-2): the
            // broker would close the connection. As unsubscribe() does, send nothing.
            $this->logger->debug('SUBSCRIBE skipped: no non-empty topic filter');

            return new SubscribeResult(0, []);
        }

        $pid  = $this->allocatePacketId();
        $data = $this->encoder->encodeSubscribe($sent, $pid, $options);
        $this->logger->debug('>> SUBSCRIBE', ['packetId' => $pid, 'filters' => $sent, 'bytes' => strlen($data), 'preview' => $this->hexPreview($data)]);
        $this->trackWrite($data);

        // Retained messages may arrive before the SUBACK; awaitAck() delivers them as usual.
        $subAck = $this->awaitAck(PacketType::SUBACK, $pid, SubAck::class, 5.0, 'SUBACK');
        $codes  = $subAck->returnCodes;
        $this->logger->info('SUBACK', ['packetId' => $pid, 'codes' => $codes]);
        if (count($codes) !== count($sent)) {
            $this->logger->warning('SUBACK code count does not match the filters sent; a missing code counts as refused', [
                'filters' => count($sent),
                'codes'   => count($codes),
            ]);
        }

        $granted = [];
        $refused = [];
        foreach ($sent as $i => $filter) {
            $code = $codes[$i] ?? 0x80;
            if ($code < 0x80) {
                $granted[] = $filter;
            } else {
                $refused[] = [$filter['filter'], $code];
            }
        }

        // Every state change first, events last: a listener may throw, or subscribe and
        // unsubscribe itself, and must find the bookkeeping already settled.

        // Only granted filters are remembered for resubscription — with the QoS that was
        // *requested*. A broker may grant less (its Maximum QoS); asking again for what the
        // application wanted, not what one broker allowed, keeps that intent across a
        // reconnect to a broker that can do more. The granted levels are in the result.
        if (! $this->isResubscribing) {
            $this->recordSubscriptionsFromFilters($granted, $options);
        }
        foreach ($refused as [$filter, $code]) {
            $this->noteRefusal($filter, $code, $subAck);
        }
        $this->metrics?->increment('subscriptions', (float) count($granted));

        foreach ($refused as [$filter, $code]) {
            $event = new EvSubscriptionRefused($filter, $code, $this->isResubscribing);
            if ($this->isResubscribing) {
                $this->deferredRefusals[] = $event;
            } else {
                $this->events?->dispatch($event);
            }
        }

        return new SubscribeResult($pid, $codes, $subAck);
    }

    /**
     * The broker said no to one filter: record it, and stop asserting it.
     *
     * A refused filter used to be remembered like a granted one, so every reconnect asked
     * for it again — and through `subscribe()`, which returns nothing, the application had
     * no sign that messages would never arrive.
     *
     * Refused during the automatic resubscribe, the broker no longer accepts a filter the
     * client was holding, so it is forgotten. Refused on the application's own call, any
     * earlier subscription to the same filter is left alone: a refused replacement does not
     * remove the subscription the broker already has, and a filter never granted was never
     * remembered in the first place.
     */
    private function noteRefusal(string $filter, int $code, SubAck $subAck): void
    {
        if ($this->isResubscribing) {
            unset($this->subscriptions[$filter]);
        }
        $this->logger->warning('Subscription refused by the broker', [
            'filter'        => $filter,
            'reasonCode'    => sprintf('0x%02X', $code),
            'reason'        => $subAck->getReasonDescription($code, $this->options->version),
            'resubscribing' => $this->isResubscribing,
        ]);
        $this->metrics?->increment('subscriptions_refused', 1.0, ['reason' => $code]);
    }

    /** Report what the automatic resubscribe was refused, now that it is over. */
    private function dispatchDeferredRefusals(): void
    {
        $refusals               = $this->deferredRefusals;
        $this->deferredRefusals = [];
        foreach ($refusals as $event) {
            $this->events?->dispatch($event);
        }
    }

    public function unsubscribe(array $topics): void
    {
        $pid = $this->allocatePacketId();
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
        // Auto-reconnect if transport is closed and the feature enabled — unless the connection
        // ended in a way nothing should undo (a normal DISCONNECT, the session taken over).
        if (! $this->transport->isOpen() && $this->options->autoReconnect && ! $this->shouldStop) {
            $this->logger->info('Transport not open; attempting auto-reconnect');
            $ok = $this->attemptReconnect();
            if (! $ok) {
                return false;
            }
        }

        // Before attempting to read, send an automatic PING if needed and reclaim any
        // flow-control quota held by messages the broker has long since abandoned.
        $this->maybeAutoPing();
        $this->sweepStalledInflight();
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
            $this->noteConnectionLost();

            return false;
        }
        if ($frame === null) {
            // Oversized: the stream is positioned mid-frame and cannot be resynchronised,
            // so the transport is already closed. Report it the way every other
            // transport-level failure in this method does — as "no packet processed" — so
            // the documented bool contract holds and auto-reconnect can take over.
            $this->noteConnectionLost();

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
                $this->outbound->remove($pubAck->packetId);
                // Recorded before the freed slot is used: a resend that fails to write must
                // not cost the waiting publish() an acknowledgement already read.
                $this->recordAck(PacketType::PUBACK, $pubAck);
                $this->drainResendBacklog();

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
                // QoS 2 step 1 (Tx). Below 0x80 the broker has the message: release it with
                // PUBREL — also for a publish() that has stopped waiting, so the broker's
                // side of the exchange can finish. From 0x80 the exchange is over: no PUBREL
                // (MQTT-4.3.3-4), and the flow-control slot comes back as it would on PUBCOMP
                // (MQTT 5 §4.9).
                $pubrec = $this->decoder->decodePubRec($body);
                $pid    = $pubrec->packetId;
                $this->logger->debug('<< PUBREC', [
                    'packetId'   => $pid,
                    'reasonCode' => $pubrec->reasonCode,
                    'success'    => $pubrec->isSuccess(),
                ]);
                $this->recordAck(PacketType::PUBREC, $pubrec);
                if ($pubrec->isError()) {
                    $this->flowControl?->trackAck($pid);
                    $this->outbound->remove($pid);
                    $this->drainResendBacklog();
                } else {
                    $this->logger->debug('>> PUBREL', ['packetId' => $pid]);
                    $this->trackWrite($this->pubRelPacket($pid));
                    $this->outbound->release($pid);
                }

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
                $this->outbound->remove($pubComp->packetId);
                $this->recordAck(PacketType::PUBCOMP, $pubComp);
                $this->drainResendBacklog();

                return true;
            case PacketType::DISCONNECT->value:
                // Inbound DISCONNECT: decode full packet and dispatch event
                $disconnect = $this->decoder->decodeDisconnect($body);
                $this->logger->info('<< DISCONNECT', [
                    'reasonCode' => $disconnect->reasonCode,
                    'isError'    => $disconnect->isError(),
                    'reason'     => $disconnect->getReasonDescription(),
                ]);

                // Closed before anyone hears of it, as whenever a connection ends: a listener
                // that publishes must not write to a connection the broker is tearing down.
                // Connection state is left for the next connect to reset — resetting it here
                // freed the flow-control slot under a publish() still waiting for one, which
                // then wrote to the closed transport.
                $this->transport->close();
                $this->resetFrameState();
                $reason             = ReasonCode::tryFrom($disconnect->reasonCode);
                $this->rejectedWith = in_array($reason, self::MESSAGE_REJECTIONS, true) ? $reason : null;

                $willReconnect = $this->reconnectsAfter($disconnect) && $this->willReconnect();
                if ($willReconnect) {
                    $this->logger->info('Will attempt auto-reconnect');
                } else {
                    // Also with auto-reconnect on: the event used to say "no reconnect" for a
                    // normal disconnection while the next loopOnce() reconnected anyway.
                    $this->shouldStop = true;
                }
                // Last, so a listener that calls connect() in response is not undone.
                $this->events?->dispatch(new EvServerDisconnect($disconnect, $willReconnect));

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

        $deadline = $timeoutSec === null ? null : Clock::now() + $timeoutSec;
        for (; ;) {
            // @phpstan-ignore-next-line - queue state may change after loopOnce()
            if (! $this->inbound->isEmpty()) {
                /** @var InboundMessage $m */
                $m = $this->inbound->dequeue();

                return $m;
            }
            // Nothing more can arrive. Waiting on, every loopOnce() failed at once and this
            // spun a core until the deadline — forever, with no timeout — after the session
            // was taken over, a normal DISCONNECT, or any drop without auto-reconnect.
            if ($this->connectionEnded()) {
                return null;
            }

            $left = $deadline === null ? null : $deadline - Clock::now();
            if ($left !== null && $left <= 0) {
                return null;
            }

            if (! $this->loopOnce($left) && ! $this->transport->isOpen()) {
                // Auto-reconnect has yet to bring it back: back off rather than spin.
                usleep(10_000);
            }
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
            // A connection that ended still owes the application what it delivered — and the
            // client acknowledged — before it ended; only then does the loop stop.
            if ($this->stopRequested || ($this->shouldStop && $this->inbound->isEmpty())) {
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

        // A connection that stayed up proves the broker reachable and willing: the count and
        // the backoff start over. One that died sooner keeps counting. Resetting on every
        // CONNACK let a broker that drops each new connection straight away — a ban applied
        // after the handshake, another client taking the same Client Identifier on MQTT 3.1.1
        // — be retried forever at the base delay.
        if ($this->connectionLasted()) {
            $this->reconnectAttempts = 0;
        }
        $this->connectedAt = null;

        if ($this->reconnectAttempts >= $this->options->reconnectMaxAttempts) {
            $this->logger->warning('Auto-reconnect: max attempts reached; stopping');
            $this->shouldStop = true;

            return false;
        }

        $delay = $this->computeBackoffDelay();
        if ($delay > 0) {
            usleep((int) floor($delay * 1_000_000));
        }

        // Counted before it is made, successful or not; see above for when the count resets.
        $this->reconnectAttempts++;
        $this->reconnecting = true;
        try {
            $reconnected = $this->reconnectOnce();
        } finally {
            $this->reconnecting = false;
        }

        // The resubscribe is over and every flag reset: a listener may now subscribe again
        // and be remembered, and an exception it throws reaches the application (PSR-14)
        // instead of being mistaken for a failed reconnect.
        $this->dispatchDeferredRefusals();

        return $reconnected;
    }

    /**
     * Whether the connection that just ended lasted long enough to count as working: then
     * the reconnect count starts over. A fixed span rather than one derived from the backoff
     * settings — tied to `reconnectMaxDelay`, a daemon with a long backoff cap on a link that
     * drops every minute counted every one of those minutes as a failure and gave up.
     */
    private function connectionLasted(): bool
    {
        return $this->connectedAt !== null && Clock::now() - $this->connectedAt >= self::STABLE_CONNECTION_SECONDS;
    }

    /**
     * Whether auto-reconnect will try again once the current connection has ended — what
     * `ServerDisconnect::$willReconnect` promises, so it must ask exactly what
     * attemptReconnect() will.
     */
    private function willReconnect(): bool
    {
        if (! $this->options->autoReconnect || $this->shouldStop) {
            return false;
        }

        return $this->connectionLasted() || $this->reconnectAttempts < $this->options->reconnectMaxAttempts;
    }

    /**
     * The transport broke under a read. Without auto-reconnect that ends the connection for
     * good, as every other way a connection ends does: run() and messages() return instead of
     * polling a dead socket for as long as the process lives.
     */
    private function noteConnectionLost(): void
    {
        if (! $this->options->autoReconnect) {
            $this->shouldStop = true;
        }
    }

    /**
     * The connection is gone and nothing will reopen it: neither auto-reconnect nor, yet, the
     * application. awaitMessage() returns instead of waiting on it.
     */
    private function connectionEnded(): bool
    {
        return ! $this->transport->isOpen() && (! $this->options->autoReconnect || $this->shouldStop);
    }

    /** One reconnect attempt: connect, then resubscribe. Failure is logged, not thrown. */
    private function reconnectOnce(): bool
    {
        try {
            $this->logger->info('Attempting reconnect', ['attempt' => $this->reconnectAttempts]);
            $this->metrics?->increment('reconnect_attempts');
            $this->establishConnection();
            // Resubscribe
            if ($this->subscriptions !== []) {
                $this->isResubscribing = true;
                try {
                    foreach ($this->subscriptions as $topic => $s) {
                        $this->subscribeWith([
                            // (string): PHP turns the array key of a filter such as "42" into an int.
                            ['filter' => (string) $topic, 'qos' => $s['qos']],
                        ], $s['options']);
                    }
                } finally {
                    // However the resubscribe ends. Left set, every later subscribe() — even
                    // after a manual connect() — would go unrecorded and not survive the
                    // next reconnect.
                    $this->isResubscribing = false;
                }
            }
            // Only now is the connection up: its lifetime is measured from here, so a broker
            // that closes it on the resubscribe — while the SUBACK wait runs on — cannot pass
            // the wait off as a connection that lasted.
            $this->connectedAt = Clock::now();

            return true;
        } catch (Throwable $e) {
            // A connection that came up but could not be fully resubscribed — a SUBACK that
            // never came — is not one to keep: left open, loopOnce() would never reconnect,
            // and the filters not yet sent would stay unsubscribed on a live connection.
            $this->transport->close();
            $this->logger->warning('Reconnect failed', ['error' => $e->getMessage(), 'attempts' => $this->reconnectAttempts]);

            return false;
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
     * Whether a DISCONNECT from the broker is one auto-reconnect should answer.
     *
     * Only an error is (0x80 or above): a normal disconnection is the broker ending the
     * connection on purpose. And not 0x8E Session taken over: another connection with this
     * Client Identifier now owns the session, and reconnecting would take it back, throw that
     * client off, and set the two taking turns for as long as both run.
     */
    private function reconnectsAfter(Disconnect $disconnect): bool
    {
        return $disconnect->isError() && $disconnect->reasonCode !== ReasonCode::SessionTakenOver->value;
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
     * When auto-reconnect will try again this returns null and loopOnce() reports "nothing
     * processed", so the next call reconnects and a pending publish() resends. When nothing
     * will re-establish the connection — auto-reconnect off, or its attempts used up — the
     * violation is thrown.
     *
     * @throws ProtocolError when nothing will reconnect
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

        $willReconnect = $this->willReconnect();
        if (! $willReconnect) {
            // As for an inbound DISCONNECT: nothing will re-establish this connection, so
            // run()/messages() stop instead of spinning on a closed transport. Before the
            // event, so a listener that calls connect() in response is not undone.
            $this->shouldStop = true;
        }
        $this->events?->dispatch(new EvServerDisconnect(
            new Disconnect($reason->value, ['reason_string' => "Closed by the client: $message"]),
            $willReconnect,
        ));
        $this->metrics?->increment('protocol_violations', 1.0, ['reason' => $reason->value]);

        if (! $willReconnect) {
            throw new ProtocolError($message, $reason->value);
        }

        return null;
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
        $inbound     = [];
        foreach ($this->qos2InboundPending as $packetId => $message) {
            $inbound[] = [
                'packetId'   => $packetId,
                'topic'      => $message->topic,
                'payload'    => $message->payload,
                'qos'        => $message->qos->value,
                'retain'     => $message->retain,
                'properties' => $message->properties,
            ];
        }

        // Both halves of what is in flight, so a restarted process can finish it: what this
        // client still owes the broker, and what it received but may not deliver until the
        // broker releases it. Saving only identifiers, as before, meant a replayed PUBREL
        // was answered PUBCOMP for a message the application never saw.
        $state = new SessionState(
            subscriptions: $this->subscriptions,
            pendingQos2: $pendingQos2,
            savedAt: time(),
            outbound: $this->outbound->export(),
            inboundQos2: $inbound,
        );

        try {
            $store->save($clientId, $state);
            $this->logger->debug('Session saved', [
                'clientId'      => $clientId,
                'subscriptions' => count($this->subscriptions),
                'pendingQos2'   => count($pendingQos2),
                'outbound'      => $this->outbound->count(),
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

            $this->subscriptions = $state->subscriptions;

            // What the previous process left in flight. Outbound messages have nobody waiting
            // for them; resendInflight() — next, on this same connection — sends them in order.
            $this->outbound->restore($state->outbound, Clock::now());
            foreach ($state->inboundQos2 as $message) {
                $this->qos2InboundPending[$message['packetId']] ??= new InboundMessage(
                    $message['topic'],
                    $message['payload'],
                    QoS::ExactlyOnce,
                    $message['retain'],
                    false,
                    $message['packetId'],
                    $message['properties'],
                );
            }

            if ($state->outbound !== [] || $state->inboundQos2 !== []) {
                // Taken back, so no longer the store's to replay: a process that dies before its
                // own disconnect() would otherwise leave the next one to resend messages long
                // since completed — a second delivery of a QoS 2 message — or to deliver a
                // stale inbound one under an identifier the broker has since reused.
                $store->save($clientId, new SessionState(subscriptions: $state->subscriptions, savedAt: $state->savedAt));
            }

            $this->logger->debug('Session restored', [
                'clientId'      => $clientId,
                'subscriptions' => count($state->subscriptions),
                'outbound'      => count($state->outbound),
                'inboundQos2'   => count($state->inboundQos2),
                'age'           => $state->getAge(),
            ]);
        } catch (Throwable $e) {
            $this->logger->warning('Failed to restore session', ['error' => $e->getMessage()]);
        }
    }

    /** Drop the messages in flight from the stored session, keeping its subscriptions. */
    private function discardStoredInflight(string $clientId): void
    {
        $store = $this->sessionStore();
        if (! $store instanceof SessionStoreInterface) {
            return;
        }

        try {
            $state = $store->load($clientId);
            if ($state instanceof SessionState && ($state->outbound !== [] || $state->inboundQos2 !== [])) {
                $store->save($clientId, new SessionState(subscriptions: $state->subscriptions, savedAt: $state->savedAt));
                $this->logger->info('Discarded stored messages in flight: the broker kept no session', [
                    'outbound'    => count($state->outbound),
                    'inboundQos2' => count($state->inboundQos2),
                ]);
            }
        } catch (Throwable $e) {
            $this->logger->warning('Failed to update the stored session', ['error' => $e->getMessage()]);
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
            $written = false;
            try {
                $this->sendPublish($msg['topic'], $msg['payload'], $msg['options'], $written);
                $drained++;
            } catch (Throwable $e) {
                // Whether this message reached the wire — not whether any byte did: resends the
                // broker is owed may go out while it waits for a flow-control slot.
                if (($e instanceof TransportError || $e instanceof Timeout) && ! $written) {
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
