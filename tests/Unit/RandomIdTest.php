<?php

/** @noinspection PhpUnhandledExceptionInspection — a throwing test is a failing test; Pest reports it. */

declare(strict_types=1);

use ScienceStories\Mqtt\Util\RandomId;

beforeEach(function (): void {
    RandomId::resetPacketId();
});

test('clientId generates string of requested length', function (): void {
    // Not the default of 16, or a length argument that was ignored would still pass.
    $id = RandomId::clientId(24);
    expect(strlen($id))->toBe(24);
});

test('clientId never returns fewer characters than requested', function (): void {
    // Stripping base64's +, / and = used to shorten about 0.15% of 6-character IDs.
    foreach ([1, 2, 3, 6, 16] as $length) {
        for ($i = 0; $i < 2_000; $i++) {
            $id = RandomId::clientId($length);
            expect(strlen($id))->toBe($length)
                ->and(ctype_alnum($id))->toBeTrue();
        }
    }
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
