<?php

declare(strict_types=1);

use Psr\EventDispatcher\EventDispatcherInterface;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Contract\SessionStoreInterface;
use ScienceStories\Mqtt\Events\SubscriptionRefused;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Session\SessionState;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Tests\Support\InterceptingTransport;
use ScienceStories\Mqtt\Util\Bytes;
use ScienceStories\Mqtt\Util\RandomId;

/**
 * A client with auto-reconnect (no backoff) and a recorder for SubscriptionRefused.
 *
 * @param  (Closure(SubscriptionRefused, Client, InMemoryTransport): void)|null  $onRefused
 *                                                                                           a listener, called after the event is recorded
 * @return array{0: Client, 1: InMemoryTransport, 2: ArrayObject<int, SubscriptionRefused>}
 */
function refusalClient(MqttVersion $version = MqttVersion::V3_1_1, ?Closure $onRefused = null): array
{
    RandomId::resetPacketId();
    $refusals  = new ArrayObject();
    $transport = new InMemoryTransport();
    $transport->feedConnAck(properties: $version === MqttVersion::V5_0 ? "\x00" : '');

    // The listener needs the client, which needs the listener: bind it by reference. No
    // refusal can arrive before the constructor below has returned.
    $client   = null;
    $listener = $onRefused instanceof Closure ? static function (SubscriptionRefused $event) use (&$client, $transport, $onRefused): void {
        assert($client instanceof Client);
        $onRefused($event, $client, $transport);
    } : null;
    $client = new Client(
        new Options(host: 'fake.broker', clientId: 'refusal', version: $version)
            ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
        $transport,
        events: new readonly class ($refusals, $listener) implements EventDispatcherInterface {
            /** @param ArrayObject<int, SubscriptionRefused> $refusals */
            public function __construct(private ArrayObject $refusals, private ?Closure $listener)
            {
            }

            public function dispatch(object $event): object
            {
                if ($event instanceof SubscriptionRefused) {
                    $this->refusals->append($event);
                    if ($this->listener instanceof Closure) {
                        ($this->listener)($event);
                    }
                }

                return $event;
            }
        },
    );
    $client->connect();
    $transport->takeWritten();

    return [$client, $transport, $refusals];
}

/** SUBACK in the shape the client's version expects: MQTT 5 adds a property block. */
function subAck(InMemoryTransport $transport, MqttVersion $version, int $packetId, int ...$codes): void
{
    $body = pack('n', $packetId).($version === MqttVersion::V5_0 ? "\x00" : '').pack('C*', ...$codes);
    $transport->feedPacket(PacketType::SUBACK, 0, $body);
}

/**
 * Topic filters and requested QoS of every SUBSCRIBE the client wrote.
 *
 * @return list<array<string, int>> one filter => QoS map per SUBSCRIBE
 */
function sentSubscriptions(InMemoryTransport $transport, MqttVersion $version): array
{
    $out = [];
    foreach ($transport->sentPackets() as $packet) {
        if ($packet['type'] !== PacketType::SUBSCRIBE->value) {
            continue;
        }
        $body   = $packet['body'];
        $offset = 2;                                        // packet identifier
        if ($version === MqttVersion::V5_0) {
            $consumed = 0;
            $offset += Bytes::decodeVarInt($body, $consumed, $offset) + $consumed;
        }
        $filters = [];
        while ($offset < strlen($body)) {
            $filter           = Bytes::decodeString($body, $offset);
            $filters[$filter] = ord($body[$offset++]) & 0x03;
        }
        $out[] = $filters;
    }

    return $out;
}

/**
 * Drop the connection and let the next loopOnce() reconnect and resubscribe.
 *
 * The SUBACKs for the resubscribe are fed after the CONNACK — fed earlier, the reconnect
 * would read one as its first packet and fail on "Expected CONNACK".
 *
 * @param  array<int, list<int>>  $subAcks  return codes by packet identifier, one SUBACK each, in order
 */
function reconnect(Client $client, InMemoryTransport $transport, MqttVersion $version, array $subAcks = [], bool $sessionPresent = false): void
{
    $transport->close();
    $transport->feedConnAck(sessionPresent: $sessionPresent, properties: $version === MqttVersion::V5_0 ? "\x00" : '');
    foreach ($subAcks as $packetId => $codes) {
        subAck($transport, $version, $packetId, ...$codes);
    }
    $client->loopOnce(0.0);
}

describe('a refused subscription', function (): void {
    it('is not asked for again on every reconnect', function (): void {
        // A refused filter was remembered like a granted one, so each reconnect re-sent it.
        [$client, $transport] = refusalClient();
        subAck($transport, MqttVersion::V3_1_1, 1, 0x00, 0x80);
        $client->subscribe(['granted/a', 'refused/b']);
        $transport->takeWritten();

        reconnect($client, $transport, MqttVersion::V3_1_1, [2 => [0x00]]);

        expect(sentSubscriptions($transport, MqttVersion::V3_1_1))->toBe([['granted/a' => 0]]);
    });

    it('is reported with its filter and reason, and in the result of subscribeWith()', function (): void {
        [$client, $transport, $refusals] = refusalClient(MqttVersion::V5_0);
        subAck($transport, MqttVersion::V5_0, 1, 0x01, 0x87);

        $result = $client->subscribeWith([
            ['filter' => 'ok/topic', 'qos' => 1],
            ['filter' => 'forbidden/topic', 'qos' => 1],
        ]);

        expect($result->results)->toBe([0x01, 0x87])
            ->and($refusals)->toHaveCount(1)
            ->and($refusals[0]->filter)->toBe('forbidden/topic')
            ->and($refusals[0]->reasonCode)->toBe(0x87)
            ->and($refusals[0]->resubscribing)->toBeFalse();
    });

    it('is forgotten when the broker starts refusing it on resubscribe', function (): void {
        // An ACL that changed while the client was away: refused on the first reconnect,
        // it must not be retried on every later one.
        [$client, $transport, $refusals] = refusalClient(MqttVersion::V5_0);
        subAck($transport, MqttVersion::V5_0, 1, 0x00);
        $client->subscribe(['sensors/#']);

        reconnect($client, $transport, MqttVersion::V5_0, [2 => [0x87]]);
        $transport->takeWritten();

        reconnect($client, $transport, MqttVersion::V5_0);

        expect($refusals)->toHaveCount(1)
            ->and($refusals[0]->resubscribing)->toBeTrue()
            ->and(sentSubscriptions($transport, MqttVersion::V5_0))->toBe([]);
    });

    it('counts a filter the SUBACK has no code for as refused', function (): void {
        [$client, $transport, $refusals] = refusalClient();
        subAck($transport, MqttVersion::V3_1_1, 1, 0x00);   // two filters, one code

        $client->subscribe(['first', 'second']);

        expect($refusals)->toHaveCount(1)
            ->and($refusals[0]->filter)->toBe('second');
    });

    it('does not undo an earlier grant of the same filter', function (): void {
        // The refusal answers the new request. The subscription granted before it is still
        // the client's to restore; should the broker refuse that too, the resubscribe
        // forgets it then.
        [$client, $transport] = refusalClient();
        subAck($transport, MqttVersion::V3_1_1, 1, 0x01);
        $client->subscribe(['orders/#'], qos: 1);
        subAck($transport, MqttVersion::V3_1_1, 2, 0x80);
        $client->subscribe(['orders/#'], qos: 2);
        $transport->takeWritten();

        reconnect($client, $transport, MqttVersion::V3_1_1, [3 => [0x01]]);

        expect(sentSubscriptions($transport, MqttVersion::V3_1_1))->toBe([['orders/#' => 1]]);
    });
});

describe('a refusal during the automatic resubscribe', function (): void {
    it('is reported once every filter has been sent again, so a listener that throws cannot cut it short', function (): void {
        // Dispatched mid-resubscribe, the exception was taken for a failed reconnect: the
        // filters after the refused one were never sent, on a connection left open.
        [$client, $transport] = refusalClient(onRefused: static function (SubscriptionRefused $event): never {
            throw new RuntimeException('refused: '.$event->filter);
        });
        foreach (['a', 'b', 'c'] as $i => $filter) {
            subAck($transport, MqttVersion::V3_1_1, $i + 1, 0x00);
            $client->subscribe([$filter]);
        }
        $transport->takeWritten();

        expect(fn () => reconnect($client, $transport, MqttVersion::V3_1_1, [4 => [0x80], 5 => [0x00], 6 => [0x00]]))
            ->toThrow(RuntimeException::class, 'refused: a')
            ->and(sentSubscriptions($transport, MqttVersion::V3_1_1))->toBe([['a' => 0], ['b' => 0], ['c' => 0]])
            ->and($transport->isOpen())->toBeTrue();
    });

    it('lets a listener subscribe to a fallback, which the next reconnect restores', function (): void {
        // Subscribed while the client still counted as resubscribing, the fallback was not
        // remembered and silently vanished on the following reconnect.
        [$client, $transport] = refusalClient(onRefused: static function (SubscriptionRefused $event, Client $client, InMemoryTransport $transport): void {
            if ($event->filter === 'private/#') {
                subAck($transport, MqttVersion::V3_1_1, 3, 0x00);
                $client->subscribe(['public/#']);
            }
        });
        subAck($transport, MqttVersion::V3_1_1, 1, 0x00);
        $client->subscribe(['private/#']);

        reconnect($client, $transport, MqttVersion::V3_1_1, [2 => [0x80]]);
        $transport->takeWritten();
        reconnect($client, $transport, MqttVersion::V3_1_1, [4 => [0x00]]);

        expect(sentSubscriptions($transport, MqttVersion::V3_1_1))->toBe([['public/#' => 0]]);
    });

    it('labels a fallback refused inside the listener as the application\'s own request', function (): void {
        [$client, $transport, $refusals] = refusalClient(onRefused: static function (SubscriptionRefused $event, Client $client, InMemoryTransport $transport): void {
            if ($event->filter === 'private/#') {
                subAck($transport, MqttVersion::V3_1_1, 3, 0x80);
                $client->subscribe(['public/#']);
            }
        });
        subAck($transport, MqttVersion::V3_1_1, 1, 0x00);
        $client->subscribe(['private/#']);

        reconnect($client, $transport, MqttVersion::V3_1_1, [2 => [0x80]]);

        expect(array_map(static fn (SubscriptionRefused $e): array => [$e->filter, $e->resubscribing], $refusals->getArrayCopy()))
            ->toBe([['private/#', true], ['public/#', false]]);
    });
});

describe('a resubscribe that fails part-way', function (): void {
    it('drops the new connection, so the next loopOnce() reconnects and sends every filter', function (): void {
        // Left open, loopOnce() never reconnected, and every filter after the failure stayed
        // unsubscribed on a live connection.
        RandomId::resetPacketId();
        $inner     = new InMemoryTransport();
        $transport = new InterceptingTransport($inner);
        $inner->feedConnAck();
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'resub')->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
            $transport,
        );
        $client->connect();
        subAck($inner, MqttVersion::V3_1_1, 1, 0x00);
        $client->subscribe(['a']);
        subAck($inner, MqttVersion::V3_1_1, 2, 0x00);
        $client->subscribe(['b']);

        // CONNECT and the SUBSCRIBE for 'a' go out; the one for 'b' (packet 4) fails.
        $transport->writesBeforeFailure = 2;
        reconnect($client, $inner, MqttVersion::V3_1_1, [3 => [0x00]]);
        expect($inner->isOpen())->toBeFalse();

        $transport->writesBeforeFailure = PHP_INT_MAX;
        $inner->takeWritten();
        reconnect($client, $inner, MqttVersion::V3_1_1, [5 => [0x00], 6 => [0x00]]);

        expect(sentSubscriptions($inner, MqttVersion::V3_1_1))->toBe([['a' => 0], ['b' => 0]])
            ->and($inner->isOpen())->toBeTrue();
    });
});

describe('the session store', function (): void {
    it('is restored from on the first connection only', function (): void {
        // Restored on every reconnect, it brought back a filter the broker had refused since,
        // and lost the ones subscribed after it was saved.
        RandomId::resetPacketId();
        $store = new class () implements SessionStoreInterface {
            /** @var array<string, SessionState> */
            private array $states = [];

            public function save(string $clientId, SessionState $state): void
            {
                $this->states[$clientId] = $state;
            }

            public function load(string $clientId): ?SessionState
            {
                return $this->states[$clientId] ?? null;
            }

            public function delete(string $clientId): void
            {
                unset($this->states[$clientId]);
            }

            public function exists(string $clientId): bool
            {
                return isset($this->states[$clientId]);
            }
        };
        $store->save('stored', new SessionState([
            'acl/revoked' => ['qos' => 0, 'options' => null],
            'kept'        => ['qos' => 0, 'options' => null],
        ]));
        $transport = new InMemoryTransport();
        $transport->feedConnAck(sessionPresent: true);
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'stored')
                ->withSessionStore($store)->withCleanSession(false)
                ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
            $transport,
        );
        $client->connect();
        subAck($transport, MqttVersion::V3_1_1, 1, 0x00);
        $client->subscribe(['added/later']);
        $transport->takeWritten();

        reconnect($client, $transport, MqttVersion::V3_1_1, [2 => [0x80], 3 => [0x00], 4 => [0x00]], sessionPresent: true);
        $transport->takeWritten();
        reconnect($client, $transport, MqttVersion::V3_1_1, [5 => [0x00], 6 => [0x00]], sessionPresent: true);

        expect(sentSubscriptions($transport, MqttVersion::V3_1_1))->toBe([['kept' => 0], ['added/later' => 0]]);
    });
});

describe('matching SUBACK codes to filters', function (): void {
    it('is not thrown off by an empty filter, which is never sent', function (): void {
        // The encoders skip empty filters, so the SUBACK has one code fewer than the list
        // given. Matching by the given list shifted every code after the empty one.
        [$client, $transport, $refusals] = refusalClient();
        subAck($transport, MqttVersion::V3_1_1, 1, 0x01);

        $client->subscribeWith([
            ['filter' => '', 'qos' => 0],
            ['filter' => 'real/topic', 'qos' => 1],
        ]);
        $transport->takeWritten();
        reconnect($client, $transport, MqttVersion::V3_1_1, [2 => [0x01]]);

        expect($refusals)->toHaveCount(0)
            ->and(sentSubscriptions($transport, MqttVersion::V3_1_1))->toBe([['real/topic' => 1]]);
    });

    it('sends nothing when every filter is empty', function (): void {
        // A SUBSCRIBE with no filter at all is a protocol violation (MQTT-3.8.3-2).
        [$client, $transport] = refusalClient();

        $result = $client->subscribeWith([['filter' => '', 'qos' => 0]]);

        expect($result->packetId)->toBe(0)
            ->and($result->results)->toBe([])
            ->and($transport->written())->toBe('');
    });
});

describe('the QoS asked for again after a reconnect', function (): void {
    it('is the one the application requested, not the lower one a broker granted', function (): void {
        // Re-requesting what one broker allowed would lose the application's intent for
        // good, even after failing over to a broker that supports more.
        [$client, $transport] = refusalClient();
        subAck($transport, MqttVersion::V3_1_1, 1, 0x01);   // asked for 2, granted 1
        $client->subscribe(['orders/#'], qos: 2);
        $transport->takeWritten();

        reconnect($client, $transport, MqttVersion::V3_1_1, [2 => [0x02]]);

        expect(sentSubscriptions($transport, MqttVersion::V3_1_1))->toBe([['orders/#' => 2]]);
    });
});
