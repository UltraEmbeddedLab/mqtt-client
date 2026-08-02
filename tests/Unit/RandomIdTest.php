<?php

/** @noinspection PhpUnhandledExceptionInspection — a throwing test is a failing test; Pest reports it. */

declare(strict_types=1);

use ScienceStories\Mqtt\Util\RandomId;

beforeEach(function (): void {
    RandomId::resetPacketId();
});

test('clientId generates string of requested length', function (): void {
    $id = RandomId::clientId(16);
    expect(strlen($id))->toBe(16);
});

test('clientId with length 1', function (): void {
    $id = RandomId::clientId(1);
    expect(strlen($id))->toBe(1);
});

test('clientId throws for length < 1', function (): void {
    RandomId::clientId(0);
})->throws(InvalidArgumentException::class);

test('clientId contains only alphanumeric chars', function (): void {
    $id = RandomId::clientId(32);
    expect($id)->toMatch('/^[a-zA-Z0-9]+$/');
});

test('packetId starts at 1', function (): void {
    expect(RandomId::packetId())->toBe(1);
});

test('packetId increments sequentially', function (): void {
    expect(RandomId::packetId())->toBe(1)
        ->and(RandomId::packetId())->toBe(2)
        ->and(RandomId::packetId())->toBe(3);
});

test('packetId wraps at 65535 back to 1', function (): void {
    for ($i = 1; $i < 65535; $i++) {
        RandomId::packetId();
    }
    // Counter is now at 65535, next call returns 65535 and increments past
    expect(RandomId::packetId())->toBe(65535);
    // Should wrap back to 1
    expect(RandomId::packetId())->toBe(1);
});

test('resetPacketId resets counter', function (): void {
    RandomId::packetId();
    RandomId::packetId();
    RandomId::resetPacketId();
    expect(RandomId::packetId())->toBe(1);
});
