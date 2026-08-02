<?php

declare(strict_types=1);

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
