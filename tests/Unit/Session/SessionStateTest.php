<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\SubscribeOptions;
use ScienceStories\Mqtt\Session\SessionState;

test('default constructor sets savedAt to current time', function (): void {
    $before = time();
    $state  = new SessionState();
    $after  = time();

    expect($state->savedAt)->toBeGreaterThanOrEqual($before)
        ->and($state->savedAt)->toBeLessThanOrEqual($after);
});

test('explicit savedAt is preserved', function (): void {
    $state = new SessionState(savedAt: 1000000);
    expect($state->savedAt)->toBe(1000000);
});

test('isExpired returns false when expirySeconds is 0', function (): void {
    $state = new SessionState(savedAt: 1);
    expect($state->isExpired(0))->toBeFalse();
});

test('isExpired returns false for fresh session', function (): void {
    $state = new SessionState();
    expect($state->isExpired(3600))->toBeFalse();
});

test('isExpired returns true for old session', function (): void {
    $state = new SessionState(savedAt: time() - 7200);
    expect($state->isExpired(3600))->toBeTrue();
});

test('hasSubscriptions true/false', function (): void {
    $empty = new SessionState();
    expect($empty->hasSubscriptions())->toBeFalse();

    $withSubs = new SessionState(subscriptions: [
        'sensors/temp' => ['qos' => 1, 'options' => null],
    ]);
    expect($withSubs->hasSubscriptions())->toBeTrue();
});

test('hasPendingQos2 true/false', function (): void {
    $empty = new SessionState();
    expect($empty->hasPendingQos2())->toBeFalse();

    $withPending = new SessionState(pendingQos2: [1234, 5678]);
    expect($withPending->hasPendingQos2())->toBeTrue();
});

test('getSubscriptionCount returns correct count', function (): void {
    $state = new SessionState(subscriptions: [
        'topic/a' => ['qos' => 0, 'options' => null],
        'topic/b' => ['qos' => 1, 'options' => null],
    ]);
    expect($state->getSubscriptionCount())->toBe(2);
});

test('toArray/fromArray roundtrip preserves data', function (): void {
    $original = new SessionState(
        subscriptions: ['sensors/#' => ['qos' => 2, 'options' => null]],
        pendingQos2: [100, 200],
        savedAt: 1699999999,
    );

    $array    = $original->toArray();
    $restored = SessionState::fromArray($array);

    expect($restored->subscriptions)->toBe($original->subscriptions)
        ->and($restored->pendingQos2)->toBe($original->pendingQos2)
        ->and($restored->savedAt)->toBe($original->savedAt);
});

test('fromArray handles missing keys gracefully', function (): void {
    $state = SessionState::fromArray([]);

    expect($state->subscriptions)->toBe([])
        ->and($state->pendingQos2)->toBe([]);
    // savedAt defaults to current time when 0 is passed
    expect($state->savedAt)->toBeGreaterThan(0);
});

describe('what survives a store that keeps JSON', function (): void {
    /** Through JSON and back, as FileSessionStore does. */
    function throughJson(SessionState $state): SessionState
    {
        $decoded = json_decode(json_encode($state->toArray(), JSON_THROW_ON_ERROR), true, 512, JSON_THROW_ON_ERROR);

        return SessionState::fromArray($decoded);
    }

    it('keeps SubscribeOptions as SubscribeOptions', function (): void {
        // They came back as arrays, and the first resubscribe after a restart died on a
        // TypeError — every reconnect after it too.
        $options = new SubscribeOptions(noLocal: true, retainHandling: 2, properties: ['subscription_identifier' => 7]);
        $state   = throughJson(new SessionState(['a/#' => ['qos' => 1, 'options' => $options]]));

        expect($state->subscriptions['a/#']['options'])->toEqual($options);
    });

    it('reads subscription options written by earlier versions', function (): void {
        $state = SessionState::fromArray(['subscriptions' => [
            'a/#' => ['qos' => 1, 'options' => ['noLocal' => true, 'retainAsPublished' => false, 'retainHandling' => 1, 'properties' => null]],
        ]]);

        expect($state->subscriptions['a/#']['options'])->toEqual(new SubscribeOptions(noLocal: true, retainHandling: 1));
    });

    it('keeps messages in flight, binary payloads and Correlation Data included', function (): void {
        $outbound = [
            ['packetId' => 7, 'topic' => 'a', 'payload' => "\x00\xFF bytes", 'qos' => 2, 'retain' => true, 'properties' => ['correlation_data' => "\x01\x80"], 'released' => true],
            ['packetId' => 9, 'topic' => 'b', 'payload' => '', 'qos' => 1, 'retain' => false, 'properties' => null, 'released' => false],
        ];
        $inbound = [
            ['packetId' => 3, 'topic' => 'c', 'payload' => "\xC3\x28", 'qos' => 2, 'retain' => false, 'properties' => ['user_properties' => ['k' => 'v']]],
        ];

        $state = throughJson(new SessionState(outbound: $outbound, inboundQos2: $inbound));

        expect($state->outbound)->toBe($outbound)
            ->and($state->inboundQos2)->toBe($inbound);
    });

    it('skips entries it cannot trust rather than resending garbage', function (): void {
        $state = SessionState::fromArray([
            'outbound' => [
                ['packet_id' => 0, 'topic' => 'a', 'payload' => '', 'qos' => 1, 'retain' => false, 'properties' => null, 'released' => false],
                ['packet_id' => 5, 'topic' => 'a', 'payload' => '!!not base64!!', 'qos' => 1, 'retain' => false, 'properties' => null, 'released' => false],
                ['packet_id' => 6, 'topic' => 'a', 'payload' => '', 'qos' => 0, 'retain' => false, 'properties' => null, 'released' => false],
                ['packet_id' => 8, 'topic' => 'ok', 'payload' => base64_encode('x'), 'qos' => 1, 'retain' => false, 'properties' => 'garbage', 'released' => false],
                'not an entry',
            ],
        ]);

        expect($state->outbound)->toBe([
            ['packetId' => 8, 'topic' => 'ok', 'payload' => 'x', 'qos' => 1, 'retain' => false, 'properties' => null, 'released' => false],
        ]);
    });

    it('never revives objects from stored properties', function (): void {
        $state = SessionState::fromArray(['inbound_qos2' => [
            ['packet_id' => 4, 'topic' => 't', 'payload' => '', 'qos' => 2, 'retain' => false, 'properties' => base64_encode(serialize(['x' => new ArrayObject()]))],
        ]]);

        expect($state->inboundQos2[0]['properties']['x'] ?? null)->toBeInstanceOf(__PHP_Incomplete_Class::class);
    });
});

it('keeps a topic filter made only of digits', function (): void {
    // PHP and JSON turn the filter "42" into an int key, which fromArray() skipped.
    $state   = new SessionState(['42' => ['qos' => 1, 'options' => null], 'a/b' => ['qos' => 0, 'options' => null]]);
    $decoded = json_decode(json_encode($state->toArray(), JSON_THROW_ON_ERROR), true, 512, JSON_THROW_ON_ERROR);

    expect(array_map(strval(...), array_keys(SessionState::fromArray($decoded)->subscriptions)))->toBe(['42', 'a/b']);
});
