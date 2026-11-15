<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\SubscribeOptions;
use ScienceStories\Mqtt\Client\TlsOptions;
use ScienceStories\Mqtt\Client\WillOptions;
use ScienceStories\Mqtt\Protocol\MqttVersion;

test('Options can be created with default values', function (): void {
    $options = new Options('localhost');

    expect($options->clientId)->toBeString()
        ->and($options->version)->toBe(MqttVersion::V3_1_1)
        ->and($options->host)->toBe('localhost')
        ->and($options->port)->toBe(1883)
        ->and($options->keepAlive)->toBe(60)
        ->and($options->cleanSession)->toBeTrue();
});

test('Options can be created with custom client ID', function (): void {
    $options = new Options('localhost', clientId: 'test-client-123');

    expect($options->clientId)->toBe('test-client-123');
});

test('Options can be configured for MQTT 3.1.1', function (): void {
    $options = new Options('localhost', version: MqttVersion::V3_1_1);

    expect($options->version)->toBe(MqttVersion::V3_1_1);
});

test('Options withHost returns new instance with updated values', function (): void {
    $original = new Options('localhost');
    $updated  = $original->withHost('broker.example.com', 8883);

    expect($original->host)->toBe('localhost')
        ->and($original->port)->toBe(1883)
        ->and($updated->host)->toBe('broker.example.com')
        ->and($updated->port)->toBe(8883)
        ->and($updated)->not->toBe($original);
});

test('Options withUser returns new instance with credentials', function (): void {
    $original = new Options('localhost');
    $updated  = $original->withUser('user123', 'pass456');

    expect($original->username)->toBeNull()
        ->and($original->password)->toBeNull()
        ->and($updated->username)->toBe('user123')
        ->and($updated->password)->toBe('pass456')
        ->and($updated)->not->toBe($original);
});

test('Options withKeepAlive returns new instance', function (): void {
    $original = new Options('localhost');
    $updated  = $original->withKeepAlive(120);

    expect($original->keepAlive)->toBe(60)
        ->and($updated->keepAlive)->toBe(120)
        ->and($updated)->not->toBe($original);
});

test('Options withCleanSession returns new instance', function (): void {
    $original = new Options('localhost', cleanSession: true);
    $updated  = $original->withCleanSession(false);

    expect($original->cleanSession)->toBeTrue()
        ->and($updated->cleanSession)->toBeFalse()
        ->and($updated)->not->toBe($original);
});

test('every with*() on the immutable options classes is #[NoDiscard]', function (string $class): void {
    // `$options->withKeepAlive(30);` without using the result compiles, runs and changes
    // nothing. PHP 8.5 turns that into a warning, but only where the attribute is present.
    $withers = array_filter(
        new ReflectionClass($class)->getMethods(ReflectionMethod::IS_PUBLIC),
        static fn (ReflectionMethod $m): bool => str_starts_with($m->getName(), 'with'),
    );

    expect($withers)->not->toBeEmpty();
    foreach ($withers as $method) {
        expect($method->getAttributes(NoDiscard::class))
            ->toHaveCount(1, "$class::{$method->getName()}() is missing #[\NoDiscard]");
    }
})->with([
    Options::class,
    TlsOptions::class,
    SubscribeOptions::class,
    WillOptions::class,
]);

test('clone-with withers leave the original untouched and copy every other field', function (): void {
    $original = new Options(host: 'a', clientId: 'id', keepAlive: 10)->withMessageFilter('x/#');
    $changed  = $original->withMessageFilter('y/+')->withTopicAliasMaximum(70_000)->withSessionExpiry(-5);

    expect($original->messageFilters)->toBe(['x/#'])
        ->and($original->topicAliasMaximum)->toBe(0)
        ->and($changed->messageFilters)->toBe(['x/#', 'y/+'])
        ->and($changed->topicAliasMaximum)->toBe(65535)
        ->and($changed->sessionExpiry)->toBe(0)
        ->and($changed->clientId)->toBe('id')
        ->and($changed->keepAlive)->toBe(10);
});
