<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\SharedSubscription;

test('filter creates correct shared subscription string', function (): void {
    $result = SharedSubscription::filter('mygroup', 'sensors/+/temp');
    expect($result)->toBe('$share/mygroup/sensors/+/temp');
});

test('filter with simple topic', function (): void {
    $result = SharedSubscription::filter('workers', 'jobs/queue');
    expect($result)->toBe('$share/workers/jobs/queue');
});

test('filter throws for empty share name', function (): void {
    SharedSubscription::filter('', 'sensors/temp');
})->throws(InvalidArgumentException::class, 'Share name must not be empty');

test('filter throws for share name containing slash', function (): void {
    SharedSubscription::filter('my/group', 'sensors/temp');
})->throws(InvalidArgumentException::class, 'Share name must not contain "/"');

test('filter throws for empty topic filter', function (): void {
    SharedSubscription::filter('group', '');
})->throws(InvalidArgumentException::class, 'Topic filter must not be empty');

test('isShared returns true for shared subscription', function (): void {
    expect(SharedSubscription::isShared('$share/group/sensors/temp'))->toBeTrue();
});

test('isShared returns false for regular topic', function (): void {
    expect(SharedSubscription::isShared('sensors/temp'))->toBeFalse();
});

test('isShared returns false for empty string', function (): void {
    expect(SharedSubscription::isShared(''))->toBeFalse();
});

test('parse returns components for valid shared subscription', function (): void {
    $result = SharedSubscription::parse('$share/mygroup/sensors/+/temp');
    expect($result)->toBe(['shareName' => 'mygroup', 'filter' => 'sensors/+/temp']);
});

test('parse returns null for non-shared topic', function (): void {
    expect(SharedSubscription::parse('sensors/temp'))->toBeNull();
});

test('parse returns null for malformed shared subscription without filter', function (): void {
    expect(SharedSubscription::parse('$share/mygroup/'))->toBeNull();
});

test('parse returns null for shared subscription without slash after name', function (): void {
    expect(SharedSubscription::parse('$share/'))->toBeNull();
});

test('roundtrip filter and parse', function (): void {
    $filter = SharedSubscription::filter('workers', 'tasks/#');
    $parsed = SharedSubscription::parse($filter);
    expect($parsed)->toBe(['shareName' => 'workers', 'filter' => 'tasks/#']);
});
