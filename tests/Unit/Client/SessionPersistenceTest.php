<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Contract\SessionStoreInterface;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Session\FileSessionStore;
use ScienceStories\Mqtt\Session\SessionState;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Util\Bytes;

/** One process's client: a persistent session in $store, over its own transport. */
function persistentClient(SessionStoreInterface $store, InMemoryTransport $transport): Client
{
    return new Client(
        new Options(host: 'fake.broker', clientId: 'restarted')
            ->withCleanSession(false)->withSessionStore($store)
            ->withAckTimeout(0.1)->withMaxResendAttempts(0),
        $transport,
    );
}

describe('a process restarted with a persistent session', function (): void {
    beforeEach(function (): void {
        $this->directory = sys_get_temp_dir().'/mqtt-persist-'.bin2hex(random_bytes(4));
    });

    afterEach(function (): void {
        array_map(unlink(...), glob($this->directory.'/*') ?: []);
        @rmdir($this->directory);
    });

    it('finishes what the previous one left in flight, in both directions', function (): void {
        // Only subscriptions came back. A message the previous process had received but not
        // yet been allowed to deliver was acknowledged unseen when the broker released it, and
        // one it had published was never resent — the broker held it unreleased.
        $store = new FileSessionStore($this->directory);

        $before = new InMemoryTransport();
        $before->feedConnAck();
        $first = persistentClient($store, $before);
        $first->connect();
        $before->feedPubRec(1);                                             // ours: received, not completed
        expect(fn (): int => $first->publish('out', 'sent before the restart', new PublishOptions(qos: QoS::ExactlyOnce)))
            ->toThrow(Timeout::class);
        $before->feedPublish('in', 'received before the restart', QoS::ExactlyOnce, 9);   // theirs: PUBREC sent
        $first->loopOnce(0.0);
        $first->disconnect();

        $after = new InMemoryTransport();
        $after->feedConnAck(sessionPresent: true)->feedPubRel(9);
        $second = persistentClient($store, $after);
        $second->connect();                                                 // resends the PUBREL for 1 at once
        $after->feedPubAck(2);

        // Packet 1 is still in flight, so the next message gets 2; the wait for its PUBACK
        // reads the broker's PUBREL for 9 on the way, which releases the stored message.
        expect($second->publish('next', 'x', new PublishOptions(qos: QoS::AtLeastOnce)))->toBe(2)
            ->and($after->sentPackets()[1] ?? null)->toBe(['type' => PacketType::PUBREL->value, 'flags' => 0x02, 'body' => pack('n', 1)])
            ->and($second->awaitMessage(0.0)?->payload)->toBe('received before the restart')
            ->and($after->countSent(PacketType::PUBCOMP))->toBe(1);
    });

    it('drops what was in flight when the broker did not keep the session', function (): void {
        $store = new FileSessionStore($this->directory);
        $store->save('restarted', new SessionState(outbound: [
            ['packetId' => 4, 'topic' => 'a', 'payload' => 'x', 'qos' => 1, 'retain' => false, 'properties' => null, 'released' => false],
        ]));

        $transport = new InMemoryTransport();
        $transport->feedConnAck();                                          // session not present
        persistentClient($store, $transport)->connect();

        // Nor left in the store, for a later session to replay.
        expect($transport->sentPacketTypes())->toBe([PacketType::CONNECT->value])
            ->and($store->load('restarted')?->outbound)->toBe([]);
    });

    it('takes what was in flight out of the store once restored, so a crash cannot replay it', function (): void {
        // The next process restored and finished it, then died before its own disconnect():
        // the one after that resent it again, and the subscriber got a QoS 2 message twice.
        $store = new FileSessionStore($this->directory);
        $store->save('restarted', new SessionState(
            subscriptions: ['keep/#' => ['qos' => 1, 'options' => null]],
            outbound: [['packetId' => 4, 'topic' => 'a', 'payload' => 'x', 'qos' => 2, 'retain' => false, 'properties' => null, 'released' => true]],
            inboundQos2: [['packetId' => 7, 'topic' => 'b', 'payload' => 'y', 'qos' => 2, 'retain' => false, 'properties' => null]],
        ));

        $transport = new InMemoryTransport();
        $transport->feedConnAck(sessionPresent: true);
        persistentClient($store, $transport)->connect();

        $stored = $store->load('restarted');
        expect($transport->countSent(PacketType::PUBREL))->toBe(1)
            ->and($stored?->outbound)->toBe([])
            ->and($stored?->inboundQos2)->toBe([])
            ->and(array_keys($stored->subscriptions ?? []))->toBe(['keep/#']);
    });

    it('is saved under the Client Identifier the broker assigned', function (): void {
        // Saved under the empty identifier the application configured, it was never found by
        // a process reconnecting with the assigned one.
        $store     = new FileSessionStore($this->directory);
        $transport = new InMemoryTransport();
        $assigned  = "\x12".Bytes::encodeString('auto-1');
        $transport->feedConnAck(properties: Bytes::encodeVarInt(strlen($assigned)).$assigned);
        $client = new Client(
            new Options(host: 'fake.broker', clientId: '', cleanSession: false, version: MqttVersion::V5_0)
                ->withSessionExpiry(300)->withSessionStore($store)->withAckTimeout(0.1)->withMaxResendAttempts(0),
            $transport,
        );
        $client->connect();
        expect(fn (): int => $client->publish('t', 'in flight at shutdown', new PublishOptions(qos: QoS::AtLeastOnce)))->toThrow(Timeout::class);
        $client->disconnect();

        expect($store->load('auto-1')?->outbound)->toHaveCount(1)
            ->and($store->exists(''))->toBeFalse();
    });
});
