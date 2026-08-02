<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

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
use ScienceStories\Mqtt\Util\RandomId;
use ScienceStories\Mqtt\Util\TopicValidator;
use SplQueue;
use Throwable;

use function array_any;
use function array_key_exists;
use function array_key_first;
use function array_keys;
use function array_shift;
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
use function microtime;
use function mt_getrandmax;
use function mt_rand;
use function ord;
use function pack;
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
    private EncoderInterface $encoder;

    private DecoderInterface $decoder;

    /** @var SplQueue<InboundMessage> */
    private readonly SplQueue $inbound;

    /** @var callable|null */
    private $messageHandler;

    private bool $shouldStop = false;

    private ?SubAck $lastSubAck = null;

    private ?UnsubAck $lastUnsubAck = null;

    private float $lastActivity;

    private bool $pingOutstanding = false;

    private ?PubAck $lastPubAck = null;

    private ?PubComp $lastPubComp = null;

    /** @var array<int, InboundMessage> QoS2 inbound pending messages by Packet Identifier */
    private array $qos2InboundPending = [];

    /** @var array<string, array{qos:int, options:?SubscribeOptions}> Map of topic filter => subscription settings */
    private array $subscriptions = [];

    private int $reconnectAttempts = 0;

    private bool $isResubscribing = false;

    /** @var array<int, float> Recently seen QoS1 Packet Identifiers for de-duplication */
    private array $qos1Seen = [];

    private readonly int $qos1SeenMax;

    /** MQTT 5.0 Topic Alias Manager for bandwidth optimization */
    private ?TopicAliasManager $topicAliasManager = null;

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

    }

    private function trackRead(int $length, ?float $timeoutSec = null): string
    {
        $data = $this->transport->readExact($length, $timeoutSec);
        $this->bytesReceived += strlen($data);

        return $data;
    }

    private function touchActivity(): void
    {
        $this->lastActivity = microtime(true);
    }

    private function dispatch(object $event): void
    {
        if ($this->events instanceof EventDispatcherInterface) {
            $this->events->dispatch($event);
        }
    }

    private function sendPingReq(): void
    {
        // Build PINGREQ without awaiting response; loopOnce() or ping() will handle PINGRESP
        $pkt = chr(PacketType::PINGREQ->value << 4) . chr(0);
        $this->logger->debug('>> PINGREQ (auto)');
        $this->trackWrite($pkt);
        $this->pingOutstanding = true;
        $this->touchActivity();
    }

    private function maybeAutoPing(): void
    {
        $keep = $this->options->keepAlive;
        if ($keep <= 0) {
            return;
        }
        $now     = microtime(true);
        $elapsed = $now - $this->lastActivity;
        // Guard threshold: send it slightly before keepAlive expires (e.g., 90% of interval) to be safe
        $threshold = max(1.0, $keep * 0.9);
        if ($elapsed >= $threshold && ! $this->pingOutstanding && $this->transport->isOpen()) {
            $this->sendPingReq();
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
        $this->touchActivity();
    }

    public function connect(): ConnectResult
    {
        $connectStart = microtime(true);
        // Nothing from a previous connection may leak into this one: Packet Identifiers,
        // topic aliases and flow-control quota are all connection-scoped.
        $this->resetConnectionState();
        $this->logger->info('Opening connection', ['host' => $this->options->host, 'port' => $this->options->port]);
        $this->transport->open($this->options->host, $this->options->port);

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
        $this->touchActivity();

        // Read the fixed header: 1 byte type, then varint remaining length (1-4 bytes)
        $this->logger->debug('<< waiting for CONNACK');
        $typeByte = $this->trackRead(1, 5.0);
        $this->touchActivity();
        $packetType = ord($typeByte[0]) >> 4;
        if ($packetType !== PacketType::CONNACK->value) {
            throw new ProtocolError("Expected CONNACK, got type $packetType");
        }

        // Read Remaining Length varint
        $varBytes = '';
        for ($i = 0; $i < 4; $i++) {
            $b = $this->trackRead(1, 5.0);
            $varBytes .= $b;
            $byte = ord($b);
            if (($byte & 0x80) === 0) {
                break;
            }
        }
        $consumed     = 0;
        $remainingLen = Bytes::decodeVarInt($varBytes, $consumed);
        if ($remainingLen < 2) {
            // v3: at least 2 bytes (ack flags + return code), v5: also followed by properties
            throw new ProtocolError("Invalid CONNACK length: $remainingLen");
        }
        // Bound the allocation before it happens: this read is pre-authentication, so an
        // unauthenticated peer must not be able to size it. connect() has no bool contract
        // to honour, so a violation here is fatal rather than a skipped packet.
        if (! $this->packetSizeAllowed($remainingLen, 'CONNACK')) {
            throw new ProtocolError("Declared CONNACK size of $remainingLen bytes exceeds the maximum of {$this->options->maximumPacketSize}");
        }

        $body = $this->trackRead($remainingLen, 5.0);
        $this->touchActivity();
        $this->logger->debug('<< CONNACK', ['bytes' => $remainingLen, 'preview' => $this->hexPreview($typeByte.$varBytes.$body)]);
        $connack = $this->decoder->decodeConnAck($body);

        $versionStr = $this->options->version->value;

        $this->logger->info('CONNACK', ['sessionPresent' => $connack->sessionPresent, 'reasonCode' => $connack->returnCode, 'version' => $versionStr]);

        if ($connack->returnCode !== 0) {
            // MQTT-3.2.2-7: the broker closes the network connection after a refusal.
            // Returning normally here would make the caller replay buffered publishes
            // into a dead socket and surface an auth failure as a later read timeout.
            $this->transport->close();
            $exception = $this->connectRefusedException($connack);
            $this->logger->warning('CONNECT refused', ['reasonCode' => $connack->returnCode, 'error' => $exception->getMessage()]);

            throw $exception;
        }

        // Inbound QoS 2 messages awaiting PUBREL are Session state. Discard them only
        // when the broker has discarded its half of the session too, otherwise a replayed
        // PUBREL would be answered with PUBCOMP for a message never delivered upstream.
        if ($this->options->cleanSession || ! $connack->sessionPresent) {
            $this->qos2InboundPending = [];
        }

        $assignedId = null;
        if ($this->options->version === MqttVersion::V5_0 && is_array($connack->properties)) {
            $assignedId = $connack->properties['assigned_client_identifier'] ?? null;
            if (! is_string($assignedId)) {
                $assignedId = null;
            }

            // Initialize Topic Alias Manager with broker's limit
            $brokerTopicAliasMax = $connack->properties['topic_alias_maximum'] ?? 0;
            if (is_int($brokerTopicAliasMax) && $brokerTopicAliasMax > 0) {
                $this->topicAliasManager = new TopicAliasManager($brokerTopicAliasMax);
                $this->logger->debug('Topic Alias Manager initialized', ['max' => $brokerTopicAliasMax]);
            }

            // Initialize Flow Control with broker's receive_maximum
            $brokerReceiveMax = $connack->properties['receive_maximum'] ?? 65535;
            if (is_int($brokerReceiveMax) && $brokerReceiveMax > 0) {
                $this->flowControl = new FlowControl($brokerReceiveMax);
                $this->logger->debug('Flow Control initialized', ['max' => $brokerReceiveMax]);
            }
        }

        // Restore session state if session store is configured and session was present
        $clientId = $assignedId ?? $this->options->clientId;
        if ($this->options->sessionStore instanceof SessionStoreInterface && $connack->sessionPresent && ! $this->options->cleanSession) {
            $this->restoreSession($clientId);
        }

        if ($this->metrics instanceof MetricsInterface) {
            $this->metrics->increment('connections', 1.0, ['version' => $versionStr]);
            $this->metrics->observe('connect_duration_seconds', microtime(true) - $connectStart);
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
        if ($this->metrics instanceof MetricsInterface) {
            $this->metrics->increment('disconnections', 1.0, ['reason' => $reason !== '' ? 'client' : 'normal']);
        }
        $this->logger->info('Disconnected', ['reason' => $reason]);
    }

    /**
     * Reset every piece of state scoped to a single network connection.
     *
     * Packet Identifiers, topic aliases and flow-control quota are all defined per
     * connection, so anything surviving a reconnect corrupts the next session — a stale
     * `pingOutstanding` silently disables keepalive for the life of the process, and a
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
        if ($this->topicAliasManager instanceof TopicAliasManager) {
            $this->topicAliasManager->reset();
        }
        if ($this->flowControl instanceof FlowControl) {
            $this->flowControl->reset();
        }
        $this->qos1Seen        = [];
        $this->pingOutstanding = false;
        $this->lastSubAck      = null;
        $this->lastUnsubAck    = null;
        $this->lastPubAck      = null;
        $this->lastPubComp     = null;
        $this->touchActivity();
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
        if ($this->metrics instanceof MetricsInterface) {
            $this->metrics->increment('oversized_packets_rejected');
        }

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
                if ($this->metrics instanceof MetricsInterface) {
                    $this->metrics->increment('offline_queue_enqueued');
                }

                return 0;
            }
            $this->logger->warning('Offline queue full, message dropped', ['topic' => $topic]);
            if ($this->metrics instanceof MetricsInterface) {
                $this->metrics->increment('offline_queue_dropped');
            }

            return 0;
        }

        // Rate limiting
        $rateLimiter = $this->options->rateLimiter;
        if ($rateLimiter instanceof RateLimiter) {
            $waited = $rateLimiter->acquire();
            if ($waited > 0 && $this->metrics instanceof MetricsInterface) {
                $this->metrics->observe('rate_limit_wait_seconds', $waited);
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
                $deadline = microtime(true) + 5.0;
                // @phpstan-ignore-next-line booleanNot.alwaysTrue - canSend() state changes via loopOnce()
                while (! $this->flowControl->canSend()) {
                    $timeLeft = $deadline - microtime(true);
                    if ($timeLeft <= 0) {
                        throw new Timeout('Flow control: timed out waiting for slot');
                    }
                    $this->loopOnce(max(0.01, $timeLeft));
                }
            }
        }

        // Handle topic aliases for MQTT 5.0
        $publishTopic = $topic;
        $publishProps = $options->properties;
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
        $this->dispatch(new EvPacketSent($data, PacketType::PUBLISH->value));
        $this->trackWrite($data);

        // Track in-flight for flow control
        // @phpstan-ignore-next-line notIdentical.alwaysTrue - packetId is set when qos > 0
        if ($qos > 0 && $this->flowControl instanceof FlowControl && $packetId !== null) {
            $this->flowControl->trackSend($packetId);
        }

        if ($this->metrics instanceof MetricsInterface) {
            $this->metrics->increment('publish_sent', 1.0, ['qos' => $qos]);
            $this->metrics->size('publish_payload_bytes', strlen($payload), ['qos' => $qos]);
        }
        $this->touchActivity();

        switch ($qos) {
            case 0:
                return 0;
            case 1:
                $pid         = (int) $packetId;
                $ackTimeout  = $this->options->ackTimeout;
                $maxResends  = $this->options->maxResendAttempts;
                $resendCount = 0;
                $deadline    = microtime(true) + $ackTimeout;
                while (true) {
                    $timeLeft = $deadline - microtime(true);
                    if ($timeLeft <= 0) {
                        if ($resendCount >= $maxResends) {
                            throw new Timeout("Timed out waiting for PUBACK after $resendCount resend(s)");
                        }
                        // Resend with DUP flag
                        $resendPkt = new Publish(
                            $publishTopic,
                            $payload,
                            $options->qos,
                            $options->retain,
                            dup: true,
                            packetId: $packetId,
                            properties: $publishProps,
                        );
                        $resendData = $this->encoder->encodePublish($resendPkt);
                        $this->logger->debug('>> PUBLISH (resend)', ['packetId' => $pid, 'attempt' => $resendCount + 1]);
                        $this->trackWrite($resendData);
                        $resendCount++;
                        $deadline = microtime(true) + $ackTimeout;

                        continue;
                    }
                    $ok = $this->loopOnce(max(0.01, $timeLeft));
                    if (! $ok) {
                        continue;
                    }
                    $ack = $this->lastPubAck;
                    if ($ack instanceof PubAck && $ack->packetId === $pid) {
                        $this->logger->info('PUBACK', ['packetId' => $ack->packetId, 'reasonCode' => $ack->reasonCode, 'success' => $ack->isSuccess()]);
                        $this->lastPubAck = null;

                        return $pid;
                    }
                }
                // no break
            case 2:
                $pid               = (int) $packetId;
                $ackTimeout2       = $this->options->ackTimeout;
                $maxResends2       = $this->options->maxResendAttempts;
                $resendCount2      = 0;
                $deadline          = microtime(true) + $ackTimeout2;
                $this->lastPubComp = null;
                while (true) {
                    $timeLeft = $deadline - microtime(true);
                    if ($timeLeft <= 0) {
                        if ($resendCount2 >= $maxResends2) {
                            throw new Timeout("Timed out waiting for QoS2 handshake after $resendCount2 resend(s)");
                        }
                        // Resend with DUP flag
                        $resendPkt2 = new Publish(
                            $publishTopic,
                            $payload,
                            $options->qos,
                            $options->retain,
                            dup: true,
                            packetId: $packetId,
                            properties: $publishProps,
                        );
                        $resendData2 = $this->encoder->encodePublish($resendPkt2);
                        $this->logger->debug('>> PUBLISH (QoS2 resend)', ['packetId' => $pid, 'attempt' => $resendCount2 + 1]);
                        $this->trackWrite($resendData2);
                        $resendCount2++;
                        $deadline = microtime(true) + $ackTimeout2;

                        continue;
                    }
                    $ok = $this->loopOnce(max(0.01, $timeLeft));
                    if (! $ok) {
                        continue;
                    }
                    // Completion when PUBCOMP arrives for our packetId
                    $comp = $this->lastPubComp;
                    // @phpstan-ignore-next-line state is updated by loopOnce()
                    if ($comp instanceof PubComp && $comp->packetId === $pid) {
                        $this->logger->info('PUBCOMP', ['packetId' => $comp->packetId, 'reasonCode' => $comp->reasonCode, 'success' => $comp->isSuccess()]);
                        $this->lastPubComp = null;

                        return $pid;
                    }
                }
        }

        return 0; // @phpstan-ignore-line unreachable: all cases handled or exceptions thrown above
    }

    public function ping(?float $timeoutSec = 5.0): bool
    {
        if (! $this->transport->isOpen()) {
            throw new LogicException('Cannot PING: transport not open');
        }
        $pkt = chr(PacketType::PINGREQ->value << 4) . chr(0);
        $this->logger->debug('>> PINGREQ');
        $this->trackWrite($pkt);
        $this->pingOutstanding = true;
        $this->touchActivity();

        $hdr = $this->trackRead(2, $timeoutSec ?? 5.0);
        $this->touchActivity();
        $type = (ord($hdr[0]) >> 4);
        $rl   = ord($hdr[1]);
        if ($type === PacketType::PINGRESP->value && $rl === 0) {
            $this->pingOutstanding = false;
            $this->logger->info('PINGRESP OK');
            if ($this->metrics instanceof MetricsInterface) {
                $this->metrics->increment('pings');
            }

            return true;
        }
        $this->logger->info('PINGRESP unexpected', ['type' => $type, 'len' => $rl, 'preview' => $this->hexPreview($hdr)]);
        throw new ProtocolError('Unexpected response to PINGREQ');
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
        $this->touchActivity();

        // Await SUBACK; handle interleaved PUBLISH if broker sends retained messages immediately.
        $deadline = microtime(true) + 5.0;
        while (true) {
            $timeLeft = $deadline - microtime(true);
            if ($timeLeft <= 0) {
                throw new Timeout('Timed out waiting for SUBACK');
            }
            $ok = $this->loopOnce(max(0.01, $timeLeft));
            if (! $ok) {
                // timeout in loopOnce - continue loop until overall deadline
                continue;
            }
            // loopOnce handles SUBACK internally and stores last; break when found
            if ($this->lastSubAck instanceof SubAck && $this->lastSubAck->packetId === $pid) {
                $subAck           = $this->lastSubAck;
                $this->lastSubAck = null;
                $this->logger->info('SUBACK', ['packetId' => $pid, 'codes' => $subAck->returnCodes]);

                // Record subscriptions if this is a user-initiated subscription (avoid duplicate records during resubscribe)
                if (! $this->isResubscribing) {
                    $this->recordSubscriptionsFromFilters($filters, $options);
                }

                if ($this->metrics instanceof MetricsInterface) {
                    $this->metrics->increment('subscriptions', (float) count($filters));
                }

                return new SubscribeResult($pid, $subAck->returnCodes, $subAck);
            }
        }
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
        $this->touchActivity();

        $deadline = microtime(true) + 5.0;
        while (true) {
            $timeLeft = $deadline - microtime(true);
            if ($timeLeft <= 0) {
                throw new Timeout('Timed out waiting for UNSUBACK');
            }
            $ok = $this->loopOnce(max(0.01, $timeLeft));
            if (! $ok) {
                continue;
            }
            if ($this->lastUnsubAck instanceof UnsubAck && $this->lastUnsubAck->packetId === $pid) {
                $unsuback           = $this->lastUnsubAck;
                $this->lastUnsubAck = null;
                $codes              = $unsuback->reasonCodes ?? [];
                $this->logger->info('UNSUBACK', ['packetId' => $pid, 'codes' => $codes]);

                // Remove from stored subscriptions
                $this->removeSubscriptions($filters);

                if ($this->metrics instanceof MetricsInterface) {
                    $this->metrics->increment('unsubscriptions', (float) count($filters));
                }

                return;
            }
        }
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

        // Before attempting to read, send an automatic PING if needed.
        $this->maybeAutoPing();
        try {
            $b0 = $this->trackRead(1, $timeoutSec);
        } catch (Timeout) {
            return false; // nothing available
        } catch (TransportError) {
            // Transport broke; attempt to reconnect next iteration
            $this->logger->info('Transport error on readExact; will try to reconnect');
            $this->transport->close();

            return false;
        }
        $type  = (ord($b0[0]) >> 4);
        $flags = (ord($b0[0]) & 0x0F);

        // Remaining Length varint
        $varBytes = '';
        for ($i = 0; $i < 4; $i++) {
            $b = $this->trackRead(1, $timeoutSec);
            $varBytes .= $b;
            $byte = ord($b);
            if (($byte & 0x80) === 0) {
                break;
            }
        }
        $consumed = 0;
        $rl       = Bytes::decodeVarInt($varBytes, $consumed);
        if (! $this->packetSizeAllowed($rl, 'packet')) {
            // The stream is positioned mid-frame and cannot be resynchronised, so the
            // transport is already closed. Report it the way every other transport-level
            // failure in this method does — as "no packet processed" — so the documented
            // bool contract holds and auto-reconnect can take over on the next call.
            return false;
        }
        $body = $this->trackRead($rl, $timeoutSec);
        $this->touchActivity();
        // PSR-14: emit PacketReceived with full frame bytes
        $raw = $b0.$varBytes.$body;
        if ($this->metrics instanceof MetricsInterface) {
            $this->metrics->increment('packets_received', 1.0, ['type' => $type]);
            $this->metrics->size('packet_bytes', strlen($raw), ['dir' => 'in', 'type' => $type]);
        }
        $this->dispatch(new EvPacketReceived($raw, $type, $flags, $rl));

        switch ($type) {
            case PacketType::PUBLISH->value:
                $msg = $this->decoder->decodePublish($flags, $body);
                // QoS1: immediately acknowledge with PUBACK (v3/v5 minimal form)
                if ($msg->qos->value === 1 && $msg->packetId !== null) {
                    $puback = chr(PacketType::PUBACK->value << 4) . chr(2) . pack('n', $msg->packetId);
                    $this->logger->debug('>> PUBACK', ['packetId' => $msg->packetId]);
                    $this->trackWrite($puback);
                    $this->touchActivity();
                    // QoS1 idempotency. MQTT-3.3.1-3: only a re-delivery (DUP=1) can be a
                    // copy of something already handed to the application. The broker is
                    // free to reuse a Packet Identifier the moment we PUBACK it, so
                    // matching on the identifier alone would discard unrelated messages.
                    $pid = $msg->packetId;
                    if ($msg->dup && isset($this->qos1Seen[$pid])) {
                        $this->logger->debug('QoS1 duplicate PUBLISH suppressed', ['packetId' => $pid, 'dup' => $msg->dup]);

                        return true;
                    }
                    $this->qos1Seen[$pid] = microtime(true);
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
                    $this->touchActivity();
                    // Store message only if not already pending
                    if (! isset($this->qos2InboundPending[$pid])) {
                        $this->qos2InboundPending[$pid] = $msg;
                    }

                    return true;
                }
                // QoS0: deliver immediately (subject to client-side filters)
                $this->logger->debug('<< PUBLISH', ['topic' => $msg->topic, 'bytes' => $rl, 'qos' => $msg->qos->value, 'retain' => $msg->retain]);
                $this->deliverIfMatches($msg);

                return true;
            case PacketType::PINGRESP->value:
                $this->logger->debug('<< PINGRESP');
                // Clear outstanding flag to allow future auto PINGs
                $this->pingOutstanding = false;

                return true;
            case PacketType::SUBACK->value:
                // store last SUBACK for subscribeWith waiter
                $dec              = $this->decoder;
                $this->lastSubAck = $dec->decodeSubAck($body);

                return true;
            case PacketType::UNSUBACK->value:
                // store last UNSUBACK for unsubscribe waiter
                $dec                = $this->decoder;
                $this->lastUnsubAck = $dec->decodeUnsubAck($body);

                return true;
            case PacketType::PUBACK->value:
                // QoS 1 acknowledgment: decode full packet with reason code and properties
                $dec              = $this->decoder;
                $this->lastPubAck = $dec->decodePubAck($body);
                $this->logger->debug('<< PUBACK', [
                    'packetId'   => $this->lastPubAck->packetId,
                    'reasonCode' => $this->lastPubAck->reasonCode,
                    'success'    => $this->lastPubAck->isSuccess(),
                ]);
                // Track ACK for flow control
                if ($this->flowControl instanceof FlowControl) {
                    $this->flowControl->trackAck($this->lastPubAck->packetId);
                }

                return true;
            case PacketType::PUBREL->value:
                // QoS 2 step 2 (Rx): decode PUBREL, respond with PUBCOMP, and deliver stored message
                $dec    = $this->decoder;
                $pubrel = $dec->decodePubRel($body);
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
                $this->touchActivity();
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
                $dec    = $this->decoder;
                $pubrec = $dec->decodePubRec($body);
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
                $this->touchActivity();

                return true;
            case PacketType::PUBCOMP->value:
                // QoS 2 step 3 (Tx): decode final PUBCOMP acknowledgment
                $dec               = $this->decoder;
                $this->lastPubComp = $dec->decodePubComp($body);
                $this->logger->debug('<< PUBCOMP', [
                    'packetId'   => $this->lastPubComp->packetId,
                    'reasonCode' => $this->lastPubComp->reasonCode,
                    'success'    => $this->lastPubComp->isSuccess(),
                ]);
                // Track ACK for flow control (QoS 2 complete)
                if ($this->flowControl instanceof FlowControl) {
                    $this->flowControl->trackAck($this->lastPubComp->packetId);
                }

                return true;
            case PacketType::DISCONNECT->value:
                // Inbound DISCONNECT: decode full packet and dispatch event
                $dec        = $this->decoder;
                $disconnect = $dec->decodeDisconnect($body);
                $this->logger->info('<< DISCONNECT', [
                    'reasonCode' => $disconnect->reasonCode,
                    'isError'    => $disconnect->isError(),
                    'reason'     => $disconnect->getReasonDescription(),
                ]);

                // Dispatch ServerDisconnect event before closing transport
                $willReconnect = $this->options->autoReconnect && $disconnect->isError();
                $this->dispatch(new EvServerDisconnect($disconnect, $willReconnect));

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
                $this->logger->debug('<< IGNORED', ['type' => $type, 'bytes' => $rl, 'preview' => $this->hexPreview($b0.$varBytes.$body)]);

                return true;
        }
    }

    public function tick(): bool
    {
        return $this->loopOnce(0.0);
    }

    public function stop(): void
    {
        $this->shouldStop = true;
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

        $deadline = microtime(true) + $timeoutSec;
        for (; ;) {
            // @phpstan-ignore-next-line - queue state may change after loopOnce()
            if (! $this->inbound->isEmpty()) {
                /** @var InboundMessage $m */
                $m = $this->inbound->dequeue();

                return $m;
            }

            $left = $deadline - microtime(true);
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
            if ($this->shouldStop) {
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
            if ($this->shouldStop) {
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
        if ($this->reconnectAttempts >= $this->options->reconnectMaxAttempts) {
            $this->logger->warning('Auto-reconnect: max attempts reached; stopping');
            $this->shouldStop = true;

            return false;
        }

        $delay = $this->computeBackoffDelay();
        if ($delay > 0) {
            usleep((int) floor($delay * 1_000_000));
        }

        try {
            $this->logger->info('Attempting reconnect', ['attempt' => $this->reconnectAttempts + 1]);
            if ($this->metrics instanceof MetricsInterface) {
                $this->metrics->increment('reconnect_attempts');
            }
            $this->connect();
            $this->reconnectAttempts = 0;
            // Resubscribe
            if ($this->subscriptions !== []) {
                $this->isResubscribing = true;
                foreach ($this->subscriptions as $topic => $s) {
                    $this->subscribeWith([
                        ['filter' => $topic, 'qos' => $s['qos']],
                    ], $s['options']);
                }
                $this->isResubscribing = false;
            }

            return true;
        } catch (Throwable $e) {
            $this->reconnectAttempts++;
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

    private function deliverIfMatches(InboundMessage $msg): void
    {
        $filters = $this->options->messageFilters;
        if ($filters !== [] && ! $this->topicMatchesAny($msg->topic, $filters)) {
            $this->logger->debug('Message filtered out by client filters', ['topic' => $msg->topic]);

            return;
        }

        if ($this->metrics instanceof MetricsInterface) {
            $this->metrics->increment('messages_delivered', 1.0, ['qos' => $msg->qos->value, 'retain' => $msg->retain ? 1 : 0]);
            $this->metrics->size('message_payload_bytes', strlen($msg->payload), ['dir' => 'in', 'qos' => $msg->qos->value]);
        }
        // PSR-14: emit MessageReceived when the message is accepted for delivery
        $this->dispatch(new EvMessageReceived($msg));

        $hasHandler = $this->messageHandler !== null;

        // Pull delivery: awaitMessage(), messages() and Client\RequestResponse drain this
        // queue. It is fed even when a handler is registered, because the two channels are
        // documented as independent — but it is bounded, because nothing drains it in the
        // push-only pattern.
        $limit = $this->options->inboundQueueSize;
        if ($limit > 0 && count($this->inbound) >= $limit) {
            $this->inbound->dequeue();
            if ($this->metrics instanceof MetricsInterface) {
                $this->metrics->increment('inbound_queue_dropped');
            }
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
        if (!$store instanceof \ScienceStories\Mqtt\Contract\SessionStoreInterface) {
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
        if (!$store instanceof \ScienceStories\Mqtt\Contract\SessionStoreInterface) {
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
        while (($msg = $this->offlineQueue->dequeue()) !== null) {
            try {
                $this->publish($msg['topic'], $msg['payload'], $msg['options']);
                $drained++;
            } catch (Throwable $e) {
                $this->logger->warning('Failed to drain offline message', ['topic' => $msg['topic'], 'error' => $e->getMessage()]);

                break;
            }
        }

        if ($drained > 0) {
            $this->logger->info('Drained offline queue', ['count' => $drained]);
            if ($this->metrics instanceof MetricsInterface) {
                $this->metrics->increment('offline_queue_drained', (float) $drained);
            }
        }
    }
}
