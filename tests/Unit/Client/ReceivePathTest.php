<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\InboundMessage;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Tests\Support\Loopback;
use ScienceStories\Mqtt\Transport\TcpTransport;
use ScienceStories\Mqtt\Util\Bytes;
use ScienceStories\Mqtt\Util\RandomId;

/** A QoS 0 MQTT 3.1.1 PUBLISH frame, built by hand so the test does not trust the encoder. */
function publishFrame(string $topic, string $payload): string
{
    $body = Bytes::encodeString($topic).$payload;

    return "\x30".Bytes::encodeVarInt(strlen($body)).$body;
}

/**
 * A client on a real loopback socket, never connect()ed — loopOnce() only needs an open
 * transport, and skipping the handshake keeps a broker out of a receive-path test.
 *
 * @return array{0: Client, 1: resource}
 */
function socketClient(int $keepAlive = 0): array
{
    $loopback  = Loopback::listener();
    $transport = new TcpTransport();
    $transport->open($loopback->host, $loopback->port);
    $peer = $loopback->accept();

    return [new Client(new Options(host: $loopback->host, port: $loopback->port, keepAlive: $keepAlive), $transport), $peer];
}

/**
 * @return array{0: Client, 1: InMemoryTransport}
 */
function memoryClient(?Options $options = null): array
{
    $transport = new InMemoryTransport();
    $transport->feedConnAck();
    $client = new Client($options ?? new Options(host: 'fake.broker', clientId: 'receive-path'), $transport);
    $client->connect();
    $transport->takeWritten();

    return [$client, $transport];
}

describe('a frame split by a timeout', function (): void {
    it('resumes mid-frame instead of misreading the next byte as a packet type', function (): void {
        [$client, $transport] = memoryClient();
        $frame                = publishFrame('a/b', 'payload-one');

        // Only the fixed-header byte has arrived.
        $transport->feed($frame[0]);
        expect($client->loopOnce(0.0))->toBeFalse();

        // Then the Remaining Length, still no body.
        $transport->feed($frame[1]);
        expect($client->loopOnce(0.0))->toBeFalse();

        // Then the rest, followed by a second, complete packet.
        $transport->feed(substr($frame, 2).publishFrame('a/c', 'payload-two'));

        expect($client->loopOnce(0.0))->toBeTrue()
            ->and($client->loopOnce(0.0))->toBeTrue()
            ->and($client->awaitMessage(0.0)?->payload)->toBe('payload-one')
            ->and($client->awaitMessage(0.0)?->payload)->toBe('payload-two');
    });

    it('survives a body that times out half-delivered on a real socket', function (): void {
        [$client, $peer] = socketClient();
        $first           = publishFrame('sensors/1', str_repeat('A', 3_000));

        fwrite($peer, substr($first, 0, 1_500));
        usleep(20_000);
        expect($client->loopOnce(0.05))->toBeFalse();

        fwrite($peer, substr($first, 1_500).publishFrame('sensors/2', 'second'));

        $first  = $client->awaitMessage(1.0);
        $second = $client->awaitMessage(1.0);
        expect($first?->topic)->toBe('sensors/1')
            ->and($first?->payload)->toBe(str_repeat('A', 3_000))
            ->and($second?->topic)->toBe('sensors/2')
            ->and($second?->payload)->toBe('second');
    });
});

describe('tick()', function (): void {
    it('delivers a message that is already waiting on a real socket', function (): void {
        // The documented non-blocking integration path. It used to fail before reaching
        // select() and so silently delivered nothing, ever.
        [$client, $peer] = socketClient();
        fwrite($peer, publishFrame('t', 'via-tick'));
        usleep(20_000);

        expect($client->tick())->toBeTrue()
            ->and($client->awaitMessage(0.0)?->payload)->toBe('via-tick');
    });
});

describe('ping()', function (): void {
    it('delivers a PUBLISH that arrives before the PINGRESP instead of eating its header', function (): void {
        [$client, $transport] = memoryClient();
        $received             = [];
        $client->onMessage(function (InboundMessage $m) use (&$received): void {
            $received[] = $m->payload;
        });

        $transport->feedPublish('a/b', 'arrived-first')->feedPingResp();

        expect($client->ping(1.0))->toBeTrue()
            ->and($received)->toBe(['arrived-first'])
            ->and($transport->pendingBytes())->toBe(0);
    });

    it('times out when no PINGRESP comes', function (): void {
        [$client] = memoryClient();

        expect(fn (): bool => $client->ping(0.02))->toThrow(Timeout::class);
    });
});

describe('acknowledgements are matched by packet identifier', function (): void {
    it('lets a publish from inside a message handler complete without stealing the outer PUBACK', function (): void {
        // Both PUBACKs arrive while the inner publish() is pumping. With a single "last
        // PUBACK" slot the second overwrote the first, and the outer publish() resent with
        // DUP until it timed out for a message the broker had acknowledged.
        RandomId::resetPacketId();
        [$client, $transport] = memoryClient(
            new Options(host: 'fake.broker', clientId: 'reentrant')->withAckTimeout(0.2)->withMaxResendAttempts(0),
        );

        $innerId = null;
        $client->onMessage(function () use ($client, &$innerId): void {
            $innerId = $client->publish('inner/topic', 'from-handler', new PublishOptions(qos: QoS::AtLeastOnce));
        });

        $transport->feedPublish('trigger', 'go')->feedPubAck(1)->feedPubAck(2);

        $outerId = $client->publish('outer/topic', 'outer', new PublishOptions(qos: QoS::AtLeastOnce));

        expect($outerId)->toBe(1)
            ->and($innerId)->toBe(2)
            ->and($transport->countSent(PacketType::PUBLISH))->toBe(2);
    });

    it('drops an acknowledgement that nobody is waiting for', function (): void {
        RandomId::resetPacketId();
        [$client, $transport] = memoryClient(
            new Options(host: 'fake.broker', clientId: 'late-ack')->withAckTimeout(0.1)->withMaxResendAttempts(0),
        );

        expect(fn (): int => $client->publish('t', 'x', new PublishOptions(qos: QoS::AtLeastOnce)))
            ->toThrow(Timeout::class);

        // The PUBACK turns up after publish() gave up, along with a SUBACK nobody asked for.
        $transport->feedPubAck(1)->feedSubAck(99);
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        expect(new ReflectionProperty(Client::class, 'acks')->getValue($client))->toBe([]);
    });

    it('resends with DUP and the same packet identifier until the budget runs out', function (): void {
        RandomId::resetPacketId();
        [$client, $transport] = memoryClient(
            new Options(host: 'fake.broker', clientId: 'resend')->withAckTimeout(0.05)->withMaxResendAttempts(2),
        );

        expect(fn (): int => $client->publish('t', 'x', new PublishOptions(qos: QoS::AtLeastOnce)))
            ->toThrow(Timeout::class, 'after 2 resend(s)');

        $publishes = array_values(array_filter(
            $transport->sentPackets(),
            static fn (array $p): bool => $p['type'] === PacketType::PUBLISH->value,
        ));
        // Body is topic, then the two-byte packet identifier, then the payload "x".
        $packetId = static fn (array $p): string => substr($p['body'], -3, 2);

        expect($publishes)->toHaveCount(3)
            ->and($publishes[0]['flags'] & 0x08)->toBe(0)
            ->and($publishes[1]['flags'] & 0x08)->toBe(0x08)
            ->and($publishes[2]['flags'] & 0x08)->toBe(0x08)
            ->and($packetId($publishes[1]))->toBe($packetId($publishes[0]))
            ->and($packetId($publishes[2]))->toBe($packetId($publishes[0]));
    });
});

describe('Keep Alive while blocked in a read', function (): void {
    it('wakes from loopOnce(null) in time to send the PINGREQ', function (): void {
        // awaitMessage(null) runs loopOnce(null). That used to block in the read for as
        // long as the connection stayed quiet, so no PINGREQ was ever sent and the broker
        // dropped the client after 1.5x Keep Alive.
        [$client, $peer] = socketClient(keepAlive: 2);
        // Pretend the last packet went out 1.7s ago: the ping is due in ~0.1s.
        new ReflectionProperty(Client::class, 'lastSent')->setValue($client, hrtime(true) / 1e9 - 1.7);

        $start = hrtime(true);
        expect($client->loopOnce(null))->toBeFalse();
        expect((hrtime(true) - $start) / 1e9)->toBeLessThan(1.0);

        $client->loopOnce(0.0);
        usleep(20_000);

        expect(fread($peer, 16))->toBe("\xC0\x00");
    });
});

describe('Keep Alive while a large packet is still arriving', function (): void {
    /**
     * A client whose PINGREQ is already overdue, so the next loopOnce() sends it while a
     * PUBLISH body is in flight — the PINGRESP is then queued behind that body.
     *
     * @return array{0: Client, 1: InMemoryTransport}
     */
    function clientWithPingDue(int $keepAlive = 60): array
    {
        [$client, $transport] = memoryClient(
            new Options(host: 'fake.broker', clientId: 'slow-frame', keepAlive: $keepAlive)->withPingResponseTimeout(0.2),
        );
        new ReflectionProperty(Client::class, 'lastSent')->setValue($client, hrtime(true) / 1e9 - 120.0);

        return [$client, $transport];
    }

    it('does not condemn a link whose body is trickling in behind the PINGRESP', function (): void {
        // Bytes kept by the transport used to be invisible to liveness until the whole body
        // had arrived, so a slow transfer that straddled a PINGREQ was torn down as silent.
        [$client, $transport] = clientWithPingDue();
        $frame                = publishFrame('fw/image', str_repeat('P', 2_000));
        $received             = null;
        $client->onMessage(function (InboundMessage $m) use (&$received): void {
            $received = $m;
        });

        // ~3 KB/s: far slower than the body, far faster than 256 bytes per 0.2 s timeout.
        foreach (str_split($frame, 64) as $chunk) {
            $transport->feed($chunk);
            $client->loopOnce(0.0);
            expect($transport->isOpen())->toBeTrue();
            usleep(20_000);
        }
        $transport->feedPingResp();
        $client->loopOnce(0.0);
        $client->loopOnce(0.0);

        expect($received?->payload)->toBe(str_repeat('P', 2_000))
            ->and($transport->countSent(PacketType::PINGREQ))->toBeGreaterThanOrEqual(1)
            ->and($transport->isOpen())->toBeTrue();
    });

    it('still condemns a link that goes silent in the middle of a body', function (): void {
        // The bytes that did arrive prove the link was alive, so the first PINGREQ is
        // cleared; the stall is caught on the next Keep Alive cycle, exactly as an idle
        // link would be. Keep Alive 1 s keeps that cycle short enough for a unit test.
        [$client, $transport] = clientWithPingDue(keepAlive: 1);
        $frame                = publishFrame('fw/image', str_repeat('P', 2_000));
        $transport->feed(substr($frame, 0, 600));

        $deadline = hrtime(true) + 4_000_000_000;
        while ($transport->isOpen() && hrtime(true) < $deadline) {
            $client->loopOnce(0.0);
            usleep(10_000);
        }

        expect($transport->isOpen())->toBeFalse();
    });
});

describe('ping() owns its own deadline', function (): void {
    it('times out with Timeout and keeps the link when it outlasts the Ping Response Timeout', function (): void {
        // Housekeeping used to apply its own, shorter deadline to the PINGREQ ping() had
        // sent: it closed the transport and ping() threw TransportError — a health check
        // that caused the outage it was checking for.
        [$client, $transport] = memoryClient(
            new Options(host: 'fake.broker', clientId: 'ping-owner', keepAlive: 60)->withPingResponseTimeout(0.05),
        );

        $start = hrtime(true);
        expect(fn (): bool => $client->ping(0.15))->toThrow(Timeout::class);

        expect((hrtime(true) - $start) / 1e9)->toBeGreaterThanOrEqual(0.15)
            ->and($transport->isOpen())->toBeTrue()
            ->and(new ReflectionProperty(Client::class, 'shouldStop')->getValue($client))->toBeFalse();
    });
});

describe('a failed connect()', function (): void {
    it('closes the transport when no CONNACK arrives', function (): void {
        // A half-open transport kept isOpen() true, so auto-reconnect never retried and the
        // next loopOnce() read whatever came next as traffic on a live session.
        $transport = new InMemoryTransport();
        $client    = new Client(new Options(host: 'fake.broker', clientId: 'no-connack'), $transport);

        expect(fn (): ScienceStories\Mqtt\Client\ConnectResult => $client->connect())->toThrow(Timeout::class)
            ->and($transport->isOpen())->toBeFalse();
    });

    it('closes the transport when the first packet is not a CONNACK', function (): void {
        $transport = new InMemoryTransport();
        $transport->feedPublish('a', 'b');
        $client = new Client(new Options(host: 'fake.broker', clientId: 'wrong-first'), $transport);

        expect(fn (): ScienceStories\Mqtt\Client\ConnectResult => $client->connect())
            ->toThrow(ScienceStories\Mqtt\Exception\ProtocolError::class, 'Expected CONNACK')
            ->and($transport->isOpen())->toBeFalse();
    });
});
