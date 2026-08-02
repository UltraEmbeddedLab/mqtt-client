<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Testing\InMemoryTransport;

/**
 * @return array{0: Client, 1: InMemoryTransport}
 */
function keepAliveClient(Options $options, string $connAckProperties = ''): array
{
    $transport = new InMemoryTransport();
    $transport->feedConnAck(0, properties: $connAckProperties);

    $client = new Client($options, $transport);
    $client->connect();
    $transport->takeWritten();

    return [$client, $transport];
}

/** Read a private property without making it public just for tests. */
function readPrivate(object $object, string $property): mixed
{
    return new ReflectionProperty($object::class, $property)->getValue($object);
}

function writePrivate(object $object, string $property, mixed $value): void
{
    new ReflectionProperty($object::class, $property)->setValue($object, $value);
}

describe('keepalive is driven by outbound traffic', function (): void {
    it('sends PINGREQ even while inbound messages keep arriving', function (): void {
        // MQTT-3.1.2-23 obliges the CLIENT to send within the window. Receiving does not
        // count — a busy subscriber that never pings is dropped by the broker.
        $options              = new Options(host: 'fake.broker', keepAlive: 2);
        [$client, $transport] = keepAliveClient($options);

        // The receive must genuinely precede the ping decision, or this test proves
        // nothing: maybeAutoPing() runs at the TOP of loopOnce(), so feeding a packet and
        // asserting in the same iteration would pass even with the old inbound-driven
        // clock. Consume the packet first, then check on the NEXT iteration.
        writePrivate($client, 'lastSent', hrtime(true) / 1e9 - 1.0); // threshold is 1.8s

        $transport->feedPublish('sensors/t', 'busy');
        expect($client->loopOnce(0.0))->toBeTrue()                   // the PUBLISH lands here
            ->and($transport->countSent(PacketType::PINGREQ))->toBe(0);

        usleep(900_000);                                             // now 1.9s since we last sent
        $client->loopOnce(0.0);

        // With the old clock the inbound PUBLISH would have reset the window and no ping
        // would ever be due.
        expect($transport->countSent(PacketType::PINGREQ))->toBe(1);
    });

    it('does not ping when it has recently sent something itself', function (): void {
        $options              = new Options(host: 'fake.broker', keepAlive: 60);
        [$client, $transport] = keepAliveClient($options);

        $client->publish('sensors/t', 'just sent');
        $client->loopOnce(0.0);

        expect($transport->countSent(PacketType::PINGREQ))->toBe(0);
    });
});

describe('a missing PINGRESP is detected', function (): void {
    it('closes the transport when the broker never answers a PINGREQ', function (): void {
        $options = new Options(host: 'fake.broker', keepAlive: 2)
            ->withPingResponseTimeout(0.05);
        [$client, $transport] = keepAliveClient($options);

        // Force a ping to go out.
        writePrivate($client, 'lastSent', hrtime(true) / 1e9 - 10.0);
        $client->loopOnce(0.0);
        expect($transport->countSent(PacketType::PINGREQ))->toBe(1);

        // The broker says nothing. Past the deadline AND after more than one silent poll,
        // the connection is dead even though the socket still looks open.
        usleep(80_000);
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        expect($transport->isOpen())->toBeFalse();
    });

    it('keeps the connection when PINGRESP arrives in time', function (): void {
        $options = new Options(host: 'fake.broker', keepAlive: 2)
            ->withPingResponseTimeout(5.0);
        [$client, $transport] = keepAliveClient($options);

        writePrivate($client, 'lastSent', hrtime(true) / 1e9 - 10.0);
        $client->loopOnce(0.0);

        $transport->feedPingResp();
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        expect($transport->isOpen())->toBeTrue()
            ->and(readPrivate($client, 'pingSentAt'))->toBeNull();
    });

    it('does not condemn a healthy connection just because the caller polls slowly', function (): void {
        // The deadline must measure "we listened and heard nothing", not wall time since
        // the ping. Judging it on elapsed time alone tears down a working connection
        // whenever the caller's loop interval exceeds pingResponseTimeout — ordinary for a
        // worker polling every 30s, or a handler that blocks on a database write.
        $options = new Options(host: 'fake.broker', keepAlive: 2)
            ->withPingResponseTimeout(0.02);
        [$client, $transport] = keepAliveClient($options);

        writePrivate($client, 'lastSent', hrtime(true) / 1e9 - 10.0);
        $client->loopOnce(0.0);
        expect($transport->countSent(PacketType::PINGREQ))->toBe(1);

        // The deadline passes while the answer is already on the wire.
        usleep(60_000);
        $transport->feedPingResp();
        $client->loopOnce(0.0);

        expect($transport->isOpen())->toBeTrue()
            ->and(readPrivate($client, 'pingSentAt'))->toBeNull();
    });

    it('survives a manual ping() that goes unanswered', function (): void {
        // A health check must not manufacture the outage it exists to detect: ping() owns
        // its own round trip, so it must not leave the auto-ping deadline armed behind it.
        $options = new Options(host: 'fake.broker', keepAlive: 2)
            ->withPingResponseTimeout(0.02);
        [$client, $transport] = keepAliveClient($options);

        try {
            $client->ping(0.01);
        } catch (ScienceStories\Mqtt\Exception\Timeout) {
            // expected — nothing was fed
        }

        expect(readPrivate($client, 'pingSentAt'))->toBeNull();

        usleep(40_000);
        $client->loopOnce(0.0);

        expect($transport->isOpen())->toBeTrue();
    });

    it('clears the outstanding ping across a reconnect', function (): void {
        $options = new Options(host: 'fake.broker', keepAlive: 2)
            ->withPingResponseTimeout(5.0);
        [$client, $transport] = keepAliveClient($options);

        writePrivate($client, 'lastSent', hrtime(true) / 1e9 - 10.0);
        $client->loopOnce(0.0);
        expect(readPrivate($client, 'pingSentAt'))->not->toBeNull();

        // Reconnect without ever seeing the PINGRESP.
        $transport->feedConnAck();
        $client->connect();

        expect(readPrivate($client, 'pingSentAt'))->toBeNull();
    });

    it('stops run() instead of spinning when the link dies and reconnect is off', function (): void {
        // autoReconnect defaults to false. Without this, loopOnce() returns false forever
        // on a closed transport, run() never returns, and a supervisor watches a live
        // process that will never deliver another message.
        $options = new Options(host: 'fake.broker', keepAlive: 2, autoReconnect: false)
            ->withPingResponseTimeout(0.02);
        [$client, $transport] = keepAliveClient($options);

        writePrivate($client, 'lastSent', hrtime(true) / 1e9 - 10.0);
        $client->loopOnce(0.0);

        usleep(40_000);
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        expect($transport->isOpen())->toBeFalse()
            ->and(readPrivate($client, 'shouldStop'))->toBeTrue();
    });
});

describe('the broker can override Keep Alive', function (): void {
    it('honours Server Keep Alive from CONNACK', function (): void {
        // MQTT-3.2.2-22: the client MUST use the server's value. Property 0x13, two bytes.
        $properties = chr(0x03).chr(0x13).pack('n', 1);

        $options = new Options(
            host: 'fake.broker',
            keepAlive: 600,
            version: ScienceStories\Mqtt\Protocol\MqttVersion::V5_0,
        );
        [$client, $transport] = keepAliveClient($options, $properties);

        expect(readPrivate($client, 'effectiveKeepAlive'))->toBe(1);

        // With the client's own 600s the ping would be ~540s away; with the broker's 1s
        // it is due almost immediately.
        writePrivate($client, 'lastSent', hrtime(true) / 1e9 - 2.0);
        $client->loopOnce(0.0);

        expect($transport->countSent(PacketType::PINGREQ))->toBe(1);
    });

    it('falls back to the configured Keep Alive when the broker sends none', function (): void {
        $options  = new Options(host: 'fake.broker', keepAlive: 45);
        [$client] = keepAliveClient($options);

        expect(readPrivate($client, 'effectiveKeepAlive'))->toBe(45);
    });
});

describe('deadlines survive a wall-clock jump', function (): void {
    it('measures elapsed time with a monotonic source', function (): void {
        // hrtime() is unaffected by NTP steps and DST; microtime() is not. Every deadline
        // in the client must come from the monotonic one.
        $sources = [];
        foreach (['src/Client/Client.php', 'src/Client/FlowControl.php', 'src/Client/RateLimiter.php'] as $file) {
            $body = file_get_contents(dirname(__DIR__, 3).'/'.$file);
            if (str_contains((string) $body, 'microtime(')) {
                $sources[] = $file;
            }
        }

        expect($sources)->toBe([]);
    });
});

describe('flow control does not leak slots', function (): void {
    it('holds the slot while the broker could still acknowledge, then reclaims it', function (): void {
        // Broker advertises receive_maximum = 1 (property 0x21, two bytes).
        $properties = chr(0x03).chr(0x21).pack('n', 1);

        $options = new Options(
            host: 'fake.broker',
            version: ScienceStories\Mqtt\Protocol\MqttVersion::V5_0,
        )->withAckTimeout(0.1)->withMaxResendAttempts(0);

        [$client] = keepAliveClient($options, $properties);

        $flow = readPrivate($client, 'flowControl');
        expect($flow)->not->toBeNull()
            ->and($flow->maxInFlight)->toBe(1);

        // No PUBACK is ever fed, so this must time out.
        try {
            $client->publish('a/b', 'x', new ScienceStories\Mqtt\Client\PublishOptions(
                qos: ScienceStories\Mqtt\Protocol\QoS::AtLeastOnce,
            ));
        } catch (ScienceStories\Mqtt\Exception\Timeout) {
            // expected
        }

        // Immediately after giving up, the slot is still held: MQTT-4.9.0-1 counts the
        // message against Receive Maximum until the broker acknowledges it, and the socket
        // is still open, so releasing now would let the client exceed the granted window.
        expect($flow->currentInFlight)->toBe(1)
            ->and($flow->canSend())->toBeFalse();

        // But it must not be held forever, or a handful of timeouts against a broker
        // granting receive_maximum 1 would exhaust the quota for the life of the process.
        // Past the full resend budget the sweep reclaims it.
        $grace = max(1.0, 0.1 * 2);
        writePrivate($flow, 'pending', [readPrivate($flow, 'pending') === [] ? 1 : array_key_first(readPrivate($flow, 'pending')) => hrtime(true) / 1e9 - $grace - 1.0]);

        $client->loopOnce(0.0);

        expect($flow->currentInFlight)->toBe(0)
            ->and($flow->canSend())->toBeTrue();
    });
});
