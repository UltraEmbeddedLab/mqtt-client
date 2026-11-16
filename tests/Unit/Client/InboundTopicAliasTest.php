<?php

declare(strict_types=1);

use Psr\EventDispatcher\EventDispatcherInterface;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\InboundMessage;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Events\ServerDisconnect;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Protocol\ReasonCode;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Tests\Support\InterceptingTransport;
use ScienceStories\Mqtt\Util\Bytes;

/** An MQTT 5 property block holding one Topic Alias (0x23). */
function topicAliasProperty(int $alias): string
{
    return Bytes::encodeVarInt(3)."\x23".pack('n', $alias);
}

/**
 * An MQTT 5 client that accepts up to $inboundMaximum aliases from the broker.
 *
 * @param  int  $brokerMaximum  Topic Alias Maximum the broker advertises in CONNACK — the
 *                              limit on aliases the client may send
 * @return array{0: Client, 1: InMemoryTransport, 2: ArrayObject<int, string>} topics delivered, in order
 */
function aliasClient(int $inboundMaximum = 10, int $brokerMaximum = 0, ?Options $options = null): array
{
    $transport = new InMemoryTransport();
    $transport->feedConnAck(properties: $brokerMaximum > 0 ? Bytes::encodeVarInt(3)."\x22".pack('n', $brokerMaximum) : "\x00");

    $options = ($options ?? new Options(host: 'fake.broker', clientId: 'aliases', version: MqttVersion::V5_0))
        ->withTopicAliasMaximum($inboundMaximum);
    $client = new Client($options, $transport);
    $client->connect();
    $transport->takeWritten();

    // An object, not an array: destructuring the return value copies arrays, so a
    // by-reference array would leave the test reading an empty copy.
    $topics = new ArrayObject();
    $client->onMessage(static function (InboundMessage $m) use ($topics): void {
        $topics[] = $m->topic;
    });

    return [$client, $transport, $topics];
}

/**
 * Topic and Topic Alias of a PUBLISH the client sent (QoS 0, MQTT 5).
 *
 * @param  array{type: int, flags: int, body: string}  $packet
 * @return array{topic: string, alias: int|null}
 */
function sentPublishTopic(array $packet): array
{
    $offset   = 0;
    $topic    = Bytes::decodeString($packet['body'], $offset);
    $consumed = 0;
    $length   = Bytes::decodeVarInt($packet['body'], $consumed, $offset);
    $props    = substr($packet['body'], $offset + $consumed, $length);
    $position = strpos($props, "\x23");

    return [
        'topic' => $topic,
        'alias' => $position === false ? null : unpack('n', $props, $position + 1)[1],
    ];
}

describe('inbound topic aliases', function (): void {
    it('resolves an aliased PUBLISH to the topic the broker established', function (): void {
        // Every PUBLISH after the first used to reach the application with an empty topic.
        [$client, $transport, $topics] = aliasClient();

        $transport->feedPublish('sensors/device-1/temperature', '21.5', properties: topicAliasProperty(1))
            ->feedPublish('', '21.6', properties: topicAliasProperty(1))
            ->feedPublish('', '21.7', properties: topicAliasProperty(1));
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        expect($topics->getArrayCopy())->toBe(array_fill(0, 3, 'sensors/device-1/temperature'));
    });

    it('lets the broker point an alias at a new topic', function (): void {
        [$client, $transport, $topics] = aliasClient();

        $transport->feedPublish('a/first', 'x', properties: topicAliasProperty(2))
            ->feedPublish('a/second', 'y', properties: topicAliasProperty(2))
            ->feedPublish('', 'z', properties: topicAliasProperty(2));
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        expect($topics->getArrayCopy())->toBe(['a/first', 'a/second', 'a/second']);
    });

    it('acknowledges an aliased QoS 1 message after resolving it', function (): void {
        [$client, $transport, $topics] = aliasClient();

        $transport->feedPublish('t/q1', 'one', QoS::AtLeastOnce, 7, properties: topicAliasProperty(1))
            ->feedPublish('', 'two', QoS::AtLeastOnce, 8, properties: topicAliasProperty(1));
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        expect($topics->getArrayCopy())->toBe(['t/q1', 't/q1'])
            ->and($transport->countSent(PacketType::PUBACK))->toBe(2);
    });

    it('forgets the broker\'s aliases when the connection is re-established', function (): void {
        // Aliases are scoped to one network connection (MQTT-3.3.2-7).
        [$client, $transport] = aliasClient();
        $transport->feedPublish('t/old', 'x', properties: topicAliasProperty(1));
        $client->loopOnce(0.0);

        $transport->feedConnAck(properties: "\x00");
        $client->connect();
        $transport->feedPublish('', 'stale', properties: topicAliasProperty(1));

        expect(fn (): bool => $client->loopOnce(0.0))->toThrow(ProtocolError::class, 'never established');
    });
});

describe('invalid inbound topic aliases end the connection', function (): void {
    it('rejects an alias above the maximum the client advertised, with reason 0x94', function (int $alias, int $maximum): void {
        [$client, $transport, $topics] = aliasClient(inboundMaximum: $maximum);
        $transport->feedPublish('t', 'x', QoS::AtLeastOnce, 1, properties: topicAliasProperty($alias));

        expect(fn (): bool => $client->loopOnce(0.0))->toThrow(ProtocolError::class)
            ->and($transport->isOpen())->toBeFalse()
            ->and($topics->getArrayCopy())->toBe([])
            // No PUBACK for a message the client refused — only the DISCONNECT.
            ->and($transport->sentPacketTypes())->toBe([PacketType::DISCONNECT->value])
            ->and($transport->sentPackets()[0]['body'])->toBe(chr(ReasonCode::TopicAliasInvalid->value));
    })->with([
        'above the maximum'          => [11, 10],
        'zero'                       => [0, 10],
        'aliases not offered at all' => [1, 0],
    ]);

    it('rejects an alias the broker never established, with reason 0x82', function (): void {
        [$client, $transport] = aliasClient();
        $transport->feedPublish('', 'x', properties: topicAliasProperty(3));

        expect(fn (): bool => $client->loopOnce(0.0))->toThrow(ProtocolError::class, 'never established')
            ->and($transport->isOpen())->toBeFalse()
            ->and($transport->sentPackets()[0]['body'])->toBe(chr(ReasonCode::ProtocolError->value));
    });

    it('rejects an empty topic with no alias', function (): void {
        [$client, $transport] = aliasClient();
        $transport->feedPublish('', 'x', properties: "\x00");

        expect(fn (): bool => $client->loopOnce(0.0))->toThrow(ProtocolError::class, 'empty topic')
            ->and($transport->isOpen())->toBeFalse();
    });

    it('closes an MQTT 3.1.1 connection on an empty topic without sending a reason', function (): void {
        // 3.1.1 has no DISCONNECT from client to say why; closing is all it can do.
        $transport = new InMemoryTransport();
        $transport->feedConnAck();
        $client = new Client(new Options(host: 'fake.broker', clientId: 'v3'), $transport);
        $client->connect();
        $transport->takeWritten();
        $transport->feedPublish('', 'x');

        expect(fn (): bool => $client->loopOnce(0.0))->toThrow(ProtocolError::class)
            ->and($transport->isOpen())->toBeFalse()
            ->and($transport->written())->toBe('');
    });
});

describe('the two alias directions are independent', function (): void {
    it('keeps the client\'s outbound alias when the broker reuses the same number inbound', function (): void {
        // One shared map let an alias the broker assigned overwrite the client's own.
        [$client, $transport, $topics] = aliasClient(brokerMaximum: 10);

        $client->publish('out/topic', 'a');                                     // outbound alias 1
        $transport->feedPublish('in/topic', 'b', properties: topicAliasProperty(1))
            ->feedPublish('', 'c', properties: topicAliasProperty(1));
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);
        $client->publish('out/topic', 'd');                                     // reuses outbound alias 1

        $publishes = array_values(array_filter(
            $transport->sentPackets(),
            static fn (array $p): bool => $p['type'] === PacketType::PUBLISH->value,
        ));

        expect($topics->getArrayCopy())->toBe(['in/topic', 'in/topic'])
            ->and(sentPublishTopic($publishes[0]))->toBe(['topic' => 'out/topic', 'alias' => 1])
            ->and(sentPublishTopic($publishes[1]))->toBe(['topic' => '', 'alias' => 1]);
    });

    it('stops sending aliases when a reconnect lands on a broker that allows none', function (): void {
        // The outbound alias manager survived from the previous CONNACK, so the client kept
        // sending aliases the new broker had not allowed.
        [$client, $transport] = aliasClient(brokerMaximum: 10);
        $client->publish('t/x', 'before');

        $transport->feedConnAck(properties: "\x00");
        $client->connect();
        $transport->takeWritten();
        $client->publish('t/x', 'after');

        expect(sentPublishTopic($transport->sentPackets()[0]))->toBe(['topic' => 't/x', 'alias' => null]);
    });

    it('publishes with options through the outbound alias without affecting inbound ones', function (): void {
        [$client, $transport, $topics] = aliasClient(inboundMaximum: 5, brokerMaximum: 5);
        $transport->feedPublish('in/a', '1', properties: topicAliasProperty(5));
        $client->loopOnce(0.0);

        $client->publish('out/a', 'x', new PublishOptions(qos: QoS::AtMostOnce));
        $transport->feedPublish('', '2', properties: topicAliasProperty(5));
        $client->loopOnce(0.0);

        expect($topics->getArrayCopy())->toBe(['in/a', 'in/a']);
    });
});

/** A PSR-14 dispatcher that records what it is given. */
function recordingDispatcher(): object
{
    return new class () implements EventDispatcherInterface {
        /** @var list<object> */
        public array $events = [];

        public function dispatch(object $event): object
        {
            $this->events[] = $event;

            return $event;
        }
    };
}

describe('an alias never crosses directions', function (): void {
    it('delivers aliased messages without the broker\'s alias in their properties', function (): void {
        [$client, $transport] = aliasClient();
        $delivered            = [];
        $client->onMessage(static function (InboundMessage $m) use (&$delivered): void {
            $delivered[] = $m;
        });

        $transport->feedPublish('a/b', 'x', properties: topicAliasProperty(4))
            ->feedPublish('', 'y', properties: topicAliasProperty(4));
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        expect($delivered)->toHaveCount(2)
            ->and($delivered[0]->properties)->not->toHaveKey('topic_alias')
            ->and($delivered[1]->properties)->not->toHaveKey('topic_alias');
    });

    it('does not let a forwarded message re-point one of the client\'s own aliases', function (): void {
        // Forwarding with the received properties sent the broker's alias number back out.
        // With the outbound map full it went through untouched, re-pointed alias 1 at the
        // forwarded topic, and the client's next publish to out/a was routed to fwd/in/x.
        [$client, $transport] = aliasClient(brokerMaximum: 2);
        $client->publish('out/a', '1');                       // outbound alias 1
        $client->publish('out/b', '2');                       // outbound alias 2 — map full
        $transport->feedPublish('in/x', 'payload', properties: topicAliasProperty(1));
        $client->onMessage(static function (InboundMessage $m) use ($client): void {
            $client->publish('fwd/'.$m->topic, $m->payload, new PublishOptions(properties: $m->properties));
        });
        $client->loopOnce(0.0);
        $client->publish('out/a', '3');

        $publishes = array_map(sentPublishTopic(...), array_values(array_filter(
            $transport->sentPackets(),
            static fn (array $p): bool => $p['type'] === PacketType::PUBLISH->value,
        )));

        expect($publishes[2])->toBe(['topic' => 'fwd/in/x', 'alias' => null])
            ->and($publishes[3])->toBe(['topic' => '', 'alias' => 1]);
    });

    it('drops a topic_alias the caller sets by hand', function (): void {
        [$client, $transport] = aliasClient();                // broker allows no aliases
        $client->publish('t', 'x', new PublishOptions(properties: ['topic_alias' => 3, 'content_type' => 'text/plain']));

        expect(sentPublishTopic($transport->sentPackets()[0]))->toBe(['topic' => 't', 'alias' => null]);
    });

    it('resends with the full topic, never an alias the connection may no longer have', function (): void {
        // The resend reused the first send's empty topic and alias. After a reconnect inside
        // the wait, the new broker had never seen that alias.
        $brokerAliases = Bytes::encodeVarInt(3)."\x22".pack('n', 5);   // Topic Alias Maximum 5
        $inner         = new InMemoryTransport();
        $transport     = new InterceptingTransport($inner);
        $inner->feedConnAck(properties: $brokerAliases);
        $client = new Client(
            new Options(host: 'fake.broker', clientId: 'resend', version: MqttVersion::V5_0)
                ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
            $transport,
        );
        $client->connect();
        $client->publish('r/t', 'establish');                 // outbound alias 1
        $inner->takeWritten();

        $published          = 0;
        $transport->onWrite = static function (int $type) use ($inner, $brokerAliases, &$published): void {
            if ($type !== PacketType::PUBLISH->value) {
                return;
            }
            if (++$published === 1) {
                // Sent by alias alone — and the connection drops before the PUBACK.
                $inner->close();
                $inner->feedConnAck(sessionPresent: true, properties: $brokerAliases);
            } else {
                $inner->feedPubAck(1);
            }
        };

        expect($client->publish('r/t', 'qos1', new PublishOptions(qos: QoS::AtLeastOnce)))->toBe(1);

        $publishes = array_values(array_filter(
            $inner->sentPackets(),
            static fn (array $p): bool => $p['type'] === PacketType::PUBLISH->value,
        ));
        $resend = $publishes[1];
        // QoS 1 body: topic, packet identifier, properties, payload.
        $offset = 0;
        $topic  = Bytes::decodeString($resend['body'], $offset);

        expect($resend['flags'] & 0x08)->toBe(0x08)
            ->and($topic)->toBe('r/t')
            ->and(substr($resend['body'], $offset + 2, 1))->toBe("\x00");   // empty property block
    });
});

describe('a broker that breaks the alias rules, with auto-reconnect', function (): void {
    it('closes, reports it, and lets the next call reconnect instead of throwing', function (): void {
        // The violation used to escape loopOnce() and end run()/messages() even with
        // auto-reconnect on — unlike an oversized packet, which reconnects.
        $events    = recordingDispatcher();
        $transport = new InMemoryTransport();
        $transport->feedConnAck(properties: "\x00");
        $options = new Options(host: 'fake.broker', clientId: 'auto', version: MqttVersion::V5_0)
            ->withTopicAliasMaximum(2)
            ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0);
        $client = new Client($options, $transport, events: $events);
        $client->connect();

        $transport->feedPublish('t', 'x', properties: topicAliasProperty(9));
        expect($client->loopOnce(0.0))->toBeFalse()
            ->and($transport->isOpen())->toBeFalse();

        $disconnects = array_values(array_filter($events->events, static fn (object $e): bool => $e instanceof ServerDisconnect));
        expect($disconnects)->toHaveCount(1)
            ->and($disconnects[0]->disconnect->reasonCode)->toBe(ReasonCode::TopicAliasInvalid->value)
            ->and($disconnects[0]->willReconnect)->toBeTrue();

        $transport->feedConnAck(properties: "\x00");
        $client->loopOnce(0.0);

        expect($transport->isOpen())->toBeTrue();
    });

    it('reports a violation that surfaces inside a message handler', function (): void {
        // The handler's publish() hit it, deliverIfMatches() caught it as a handler error,
        // and the connection died with nothing dispatched.
        $events    = recordingDispatcher();
        $transport = new InMemoryTransport();
        $transport->feedConnAck(properties: "\x00");
        $options = new Options(host: 'fake.broker', clientId: 'nested', version: MqttVersion::V5_0)
            ->withTopicAliasMaximum(2)->withAckTimeout(0.05)->withMaxResendAttempts(0);
        $client = new Client($options, $transport, events: $events);
        $client->connect();
        $client->onMessage(static function () use ($client): void {
            $client->publish('reply', 'x', new PublishOptions(qos: QoS::AtLeastOnce));
        });

        $transport->feedPublish('trigger', 'go', properties: "\x00")->feedPublish('', 'bad', properties: topicAliasProperty(1));
        $client->loopOnce(0.0);

        $disconnects = array_values(array_filter($events->events, static fn (object $e): bool => $e instanceof ServerDisconnect));
        expect($transport->isOpen())->toBeFalse()
            ->and($disconnects)->toHaveCount(1)
            ->and($disconnects[0]->disconnect->reasonCode)->toBe(ReasonCode::ProtocolError->value);
    });
});

describe('after a connection ends with auto-reconnect off', function (): void {
    it('lets messages() run again once the application reconnects', function (): void {
        // The stop meant for the dead connection outlived it, so the first loop after a
        // manual reconnect returned immediately and processed nothing.
        [$client, $transport] = aliasClient();
        $transport->feedPublish('t', 'x', properties: topicAliasProperty(99));
        expect(fn (): bool => $client->loopOnce(0.0))->toThrow(ProtocolError::class);

        $transport->feedConnAck(properties: "\x00");
        $client->connect();
        $transport->feedPublish('after/reconnect', 'fresh', properties: "\x00");

        $received = null;
        // A real timeout: messages(0.0) only drains what is already queued, never reads.
        foreach ($client->messages(0.05) as $message) {
            $received = $message->payload;
            $client->stop();
        }

        expect($received)->toBe('fresh');
    });
});
