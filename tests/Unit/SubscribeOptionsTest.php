<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\SubscribeOptions;

test('default values', function (): void {
    $opts = new SubscribeOptions();

    expect($opts->noLocal)->toBeFalse()
        ->and($opts->retainAsPublished)->toBeFalse()
        ->and($opts->retainHandling)->toBe(0)
        ->and($opts->properties)->toBeNull();
});

test('noLocal flag', function (): void {
    $opts = new SubscribeOptions(noLocal: true);
    expect($opts->noLocal)->toBeTrue();
});

test('retainAsPublished flag', function (): void {
    $opts = new SubscribeOptions(retainAsPublished: true);
    expect($opts->retainAsPublished)->toBeTrue();
});

test('retainHandling values (0, 1, 2)', function (): void {
    expect((new SubscribeOptions(retainHandling: 0))->retainHandling)->toBe(0)
        ->and((new SubscribeOptions(retainHandling: 1))->retainHandling)->toBe(1)
        ->and((new SubscribeOptions(retainHandling: 2))->retainHandling)->toBe(2);
});

test('properties stored', function (): void {
    $props = ['user_properties' => ['key' => 'value']];
    $opts  = new SubscribeOptions(properties: $props);

    expect($opts->properties)->toBe($props);
});
