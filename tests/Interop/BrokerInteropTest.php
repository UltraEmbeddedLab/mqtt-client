<?php

/** @noinspection PhpUnhandledExceptionInspection — a throwing test is a failing test; Pest reports it. */

declare(strict_types=1);

namespace ScienceStories\Mqtt\Tests\Interop;

use ReflectionProperty;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\InboundMessage;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Client\WillOptions;
use ScienceStories\Mqtt\Events\PacketReceived;
use ScienceStories\Mqtt\Events\ServerDisconnect;
use ScienceStories\Mqtt\Events\SubscriptionRefused;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Tests\Support\DroppingTransport;
use ScienceStories\Mqtt\Tests\Support\LiveBroker;
use ScienceStories\Mqtt\Tests\Support\RecordingDispatcher;
use ScienceStories\Mqtt\Transport\TcpTransport;
use ScienceStories\Mqtt\Transport\WsTransport;
use ScienceStories\Mqtt\Util\Bytes;
use ScienceStories\Mqtt\Util\Clock;

/*
 * Behaviour that depends on what the broker does, checked against a real one: Mosquitto in
 * the main CI job, EMQX and HiveMQ CE in the interop matrix (see docker-compose.yml). What
 * every conforming broker must do is asserted everywhere; what only some brokers do — EMQX
 * sends topic aliases, and its default ACL refuses `#` — is skipped elsewhere, saying why.
 */

beforeEach(function (): void {
    $broker = LiveBroker::fromEnvironment();
    if (! $broker instanceof LiveBroker) {
        $this->markTestSkipped('MQTT broker not available at '.(getenv('MQTT_HOST') ?: '127.0.0.1').':'.(getenv('MQTT_PORT') ?: 1883));
    }
    $this->broker = $broker;
});

it('delivers QoS 0, 1 and 2', function (MqttVersion $version): void {
    /** @var LiveBroker $broker */
    $broker = $this->broker;
    $topic  = $broker->topic('qos');
    $sub    = new Client($broker->options('sub', $version), new TcpTransport());
    $sub->connect();
    $sub->subscribe([$topic], qos: 2);
    $pub = new Client($broker->options('pub', $version), new TcpTransport());
    $pub->connect();

    foreach ([QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] as $qos) {
        $pub->publish($topic, 'qos'.$qos->value, new PublishOptions(qos: $qos));
    }
    $payloads = array_map(static fn (InboundMessage $m): string => $m->payload, $broker->drain($sub));
    $pub->disconnect();
    $sub->disconnect();

    expect($payloads)->toBe(['qos0', 'qos1', 'qos2']);
})->with(['MQTT 3.1.1' => [MqttVersion::V3_1_1], 'MQTT 5' => [MqttVersion::V5_0]]);

it('delivers a QoS 2 message exactly once when the link drops mid-exchange', function (PacketType $after): void {
    /** @var LiveBroker $broker */
    $broker = $this->broker;
    $topic  = $broker->topic('resend');
    $sub    = new Client($broker->options('sub'), new TcpTransport());
    $sub->connect();
    $sub->subscribe([$topic], qos: 2);
    $pub = new Client(
        $broker->options('pub')->withCleanSession(false)->withSessionExpiry(300)->withAckTimeout(3.0)
            ->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0),
        new DroppingTransport(new TcpTransport(), $after, 1),
    );
    $pub->connect();

    $started  = Clock::now();
    $packetId = $pub->publish($topic, 'once', new PublishOptions(qos: QoS::ExactlyOnce));
    $elapsed  = Clock::now() - $started;
    $copies   = count($broker->drain($sub));
    $pub->disconnect();
    $sub->disconnect();

    // Resent at once on the new connection, not an ackTimeout (3 s) later.
    expect($packetId)->toBe(1)
        ->and($elapsed)->toBeLessThan(2.0)
        ->and($copies)->toBe(1);
})->with(['after the PUBLISH' => [PacketType::PUBLISH], 'after the PUBREL' => [PacketType::PUBREL]]);

it('keeps a client whose session another took over from taking it back', function (): void {
    /** @var LiveBroker $broker */
    $broker  = $this->broker;
    $options = $broker->options('takeover')->withAutoReconnect(baseDelay: 0.1, jitter: 0.0);
    $events  = new RecordingDispatcher();
    $aWire   = new TcpTransport();
    $bWire   = new TcpTransport();
    $a       = new Client($options, $aWire, events: $events);
    $b       = new Client($options, $bWire);
    $a->connect();
    $b->connect();                                          // takes the session from A

    $until = Clock::now() + 1.5;
    while (Clock::now() < $until) {
        $a->loopOnce(0.05);
        $b->loopOnce(0.05);
    }
    $disconnects = $events->of(ServerDisconnect::class);
    $aOpen       = $aWire->isOpen();
    $bOpen       = $bWire->isOpen();
    $b->disconnect();

    expect($disconnects)->toHaveCount(1)
        ->and($disconnects[0]->disconnect->reasonCode)->toBe(0x8E)
        ->and($disconnects[0]->willReconnect)->toBeFalse()
        ->and($aOpen)->toBeFalse()
        ->and($bOpen)->toBeTrue();
});

it('has the broker publish the Will only after its delay', function (): void {
    /** @var LiveBroker $broker */
    $broker  = $this->broker;
    $topic   = $broker->topic('will');
    $watcher = new Client($broker->options('watcher'), new TcpTransport());
    $watcher->connect();
    $watcher->subscribe([$topic], qos: 1);
    $deviceWire = new TcpTransport();
    $device     = new Client(
        // The broker holds a delayed Will only while the session lasts.
        $broker->options('device')->withSessionExpiry(60)->withWill(new WillOptions($topic, 'offline')->withDelayInterval(3)),
        $deviceWire,
    );
    $device->connect();

    $deviceWire->close();                                   // the link dies: no DISCONNECT
    $lost    = Clock::now();
    $will    = $watcher->awaitMessage(8.0);
    $elapsed = Clock::now() - $lost;
    $watcher->disconnect();

    // Brokers check delayed Wills on their own timers, to about a second.
    expect($will?->payload)->toBe('offline')
        ->and($elapsed)->toBeGreaterThan(1.5)
        ->and($elapsed)->toBeLessThan(6.0);
});

it('resolves the topic aliases the broker sends', function (): void {
    /** @var LiveBroker $broker */
    $broker = $this->broker;
    $topic  = $broker->topic('alias');
    $events = new RecordingDispatcher();
    $sub    = new Client($broker->options('sub')->withTopicAliasMaximum(10), new TcpTransport(), events: $events);
    $sub->connect();
    $sub->subscribe([$topic], qos: 1);
    $pub = new Client($broker->options('pub'), new TcpTransport());
    $pub->connect();

    for ($i = 0; $i < 5; $i++) {
        $pub->publish($topic, "m$i", new PublishOptions(qos: QoS::AtLeastOnce));
    }
    $messages  = $broker->drain($sub);
    $aliasOnly = 0;
    foreach ($events->events as $event) {
        if ($event instanceof PacketReceived && $event->packetType === PacketType::PUBLISH->value) {
            $consumed = 0;
            Bytes::decodeVarInt($event->bytes, $consumed, 1);
            $aliasOnly += substr($event->bytes, 1 + $consumed, 2) === "\x00\x00" ? 1 : 0;   // empty topic: alias alone
        }
    }
    $pub->disconnect();
    $sub->disconnect();

    expect(array_map(static fn (InboundMessage $m): string => $m->topic, $messages))->toBe(array_fill(0, 5, $topic));
    if ($broker->name === 'emqx') {
        // The one broker here that uses aliases towards clients: the path really ran.
        expect($aliasOnly)->toBeGreaterThan(0);
    }
});

it('refuses, before sending it, a PUBLISH above the Maximum Packet Size the broker announced', function (): void {
    /** @var LiveBroker $broker */
    $broker = $this->broker;
    $client = new Client($broker->options('big'), new TcpTransport());
    $client->connect();
    $maximum = new ReflectionProperty(Client::class, 'brokerMaximumPacketSize')->getValue($client);
    if ($maximum === null) {
        $client->disconnect();
        $this->markTestSkipped("$broker->name announces no Maximum Packet Size");
    }
    $sentBefore = $client->bytesSent();

    expect(fn (): int => $client->publish($broker->topic('big'), str_repeat('x', $maximum + 10)))->toThrow(ProtocolError::class)
        ->and($client->bytesSent())->toBe($sentBefore);
    $client->disconnect();
});

it('reports a subscription the broker refuses, and does not ask for it again', function (): void {
    /** @var LiveBroker $broker */
    $broker = $this->broker;
    if ($broker->name !== 'emqx') {
        $this->markTestSkipped("$broker->name grants every subscription by default; EMQX's default ACL refuses `#`");
    }
    $events = new RecordingDispatcher();
    $wire   = new TcpTransport();
    $client = new Client($broker->options('refused')->withAutoReconnect(maxAttempts: 3, baseDelay: 0.0, jitter: 0.0), $wire, events: $events);
    $client->connect();
    $allowed = $broker->topic('allowed');
    $result  = $client->subscribeWith([['filter' => '#', 'qos' => 1], ['filter' => $allowed, 'qos' => 1]]);

    $wire->close();                                         // the link drops; the client resubscribes
    $sentBefore = $client->bytesSent();
    for ($i = 0; $i < 5; $i++) {
        $client->loopOnce();
    }
    $resubscribeBytes = $client->bytesSent() - $sentBefore;
    $refusals         = $events->of(SubscriptionRefused::class);
    $client->disconnect();

    // CONNECT plus one SUBSCRIBE for the granted filter alone — not two.
    expect($result->results)->toBe([0x87, 0x01])
        ->and($refusals)->toHaveCount(1)
        ->and($refusals[0]->filter)->toBe('#')
        ->and($resubscribeBytes)->toBeLessThan(2 * (strlen($allowed) + 40));
});

it('works over WebSocket', function (): void {
    /** @var LiveBroker $broker */
    $broker = $this->broker;
    if ($broker->wsPort === null) {
        $this->markTestSkipped('MQTT_WS_PORT is not set for this broker');
    }
    $topic = $broker->topic('ws');
    $sub   = new Client($broker->options('ws-sub', port: $broker->wsPort), new WsTransport($broker->wsPath));
    $sub->connect();
    $sub->subscribe([$topic], qos: 1);
    $pub = new Client($broker->options('ws-pub', port: $broker->wsPort), new WsTransport($broker->wsPath));
    $pub->connect();

    $pub->publish($topic, str_repeat('w', 70_000), new PublishOptions(qos: QoS::AtLeastOnce));   // several frames
    $messages = $broker->drain($sub);
    $pub->disconnect();
    $sub->disconnect();

    expect($messages)->toHaveCount(1)
        ->and(strlen($messages[0]->payload))->toBe(70_000);
});
