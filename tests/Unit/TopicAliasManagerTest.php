<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\TopicAliasManager;

test('disabled mode (maxAliases=0) returns null alias', function (): void {
    $manager = new TopicAliasManager(0);
    $result  = $manager->getOrCreateAlias('sensors/temp');

    expect($result['alias'])->toBeNull()
        ->and($result['isNew'])->toBeFalse();
});

test('getOrCreateAlias creates new alias', function (): void {
    $manager = new TopicAliasManager(10);
    $result  = $manager->getOrCreateAlias('sensors/temp');

    expect($result['alias'])->toBe(1)
        ->and($result['isNew'])->toBeTrue();
});

test('getOrCreateAlias returns existing alias (isNew=false)', function (): void {
    $manager = new TopicAliasManager(10);
    $manager->getOrCreateAlias('sensors/temp');
    $result = $manager->getOrCreateAlias('sensors/temp');

    expect($result['alias'])->toBe(1)
        ->and($result['isNew'])->toBeFalse();
});

test('aliases are sequential starting from 1', function (): void {
    $manager = new TopicAliasManager(10);

    $r1 = $manager->getOrCreateAlias('topic/a');
    $r2 = $manager->getOrCreateAlias('topic/b');
    $r3 = $manager->getOrCreateAlias('topic/c');

    expect($r1['alias'])->toBe(1)
        ->and($r2['alias'])->toBe(2)
        ->and($r3['alias'])->toBe(3);
});

test('no slots available returns null alias', function (): void {
    $manager = new TopicAliasManager(1);
    $manager->getOrCreateAlias('topic/a');
    $result = $manager->getOrCreateAlias('topic/b');

    expect($result['alias'])->toBeNull()
        ->and($result['isNew'])->toBeFalse();
});

test('resolveAlias returns topic', function (): void {
    $manager = new TopicAliasManager(10);
    $manager->getOrCreateAlias('sensors/temp');

    expect($manager->resolveAlias(1))->toBe('sensors/temp');
});

test('resolveAlias unknown returns null', function (): void {
    $manager = new TopicAliasManager(10);
    expect($manager->resolveAlias(99))->toBeNull();
});

test('registerAlias maps correctly', function (): void {
    $manager = new TopicAliasManager(10);
    $manager->registerAlias(5, 'sensors/humidity');

    expect($manager->resolveAlias(5))->toBe('sensors/humidity')
        ->and($manager->hasAlias('sensors/humidity'))->toBeTrue()
        ->and($manager->getAlias('sensors/humidity'))->toBe(5);
});

test('registerAlias cleans up old mapping when reassigning alias', function (): void {
    $manager = new TopicAliasManager(10);
    $manager->registerAlias(1, 'topic/old');
    $manager->registerAlias(1, 'topic/new');

    expect($manager->resolveAlias(1))->toBe('topic/new')
        ->and($manager->hasAlias('topic/old'))->toBeFalse()
        ->and($manager->hasAlias('topic/new'))->toBeTrue();
});

test('registerAlias cleans up old alias when topic gets new alias', function (): void {
    $manager = new TopicAliasManager(10);
    $manager->registerAlias(1, 'sensors/temp');
    $manager->registerAlias(2, 'sensors/temp');

    expect($manager->getAlias('sensors/temp'))->toBe(2)
        ->and($manager->resolveAlias(1))->toBeNull()
        ->and($manager->resolveAlias(2))->toBe('sensors/temp');
});

test('registerAlias rejects alias < 1', function (): void {
    $manager = new TopicAliasManager(10);
    $manager->registerAlias(0, 'sensors/temp');

    expect($manager->resolveAlias(0))->toBeNull()
        ->and($manager->hasAlias('sensors/temp'))->toBeFalse();
});

test('registerAlias rejects empty topic', function (): void {
    $manager = new TopicAliasManager(10);
    $manager->registerAlias(1, '');

    expect($manager->resolveAlias(1))->toBeNull();
});

test('hasAlias true/false', function (): void {
    $manager = new TopicAliasManager(10);

    expect($manager->hasAlias('sensors/temp'))->toBeFalse();
    $manager->getOrCreateAlias('sensors/temp');
    expect($manager->hasAlias('sensors/temp'))->toBeTrue();
});

test('reset clears everything', function (): void {
    $manager = new TopicAliasManager(10);
    $manager->getOrCreateAlias('topic/a');
    $manager->getOrCreateAlias('topic/b');
    $manager->reset();

    expect($manager->getAliasCount())->toBe(0)
        ->and($manager->hasAlias('topic/a'))->toBeFalse()
        ->and($manager->resolveAlias(1))->toBeNull();
});

test('getAliasCount returns count', function (): void {
    $manager = new TopicAliasManager(10);
    expect($manager->getAliasCount())->toBe(0);

    $manager->getOrCreateAlias('topic/a');
    expect($manager->getAliasCount())->toBe(1);

    $manager->getOrCreateAlias('topic/b');
    expect($manager->getAliasCount())->toBe(2);
});

test('hasAvailableSlots returns correct state', function (): void {
    $manager = new TopicAliasManager(2);
    expect($manager->hasAvailableSlots())->toBeTrue();

    $manager->getOrCreateAlias('topic/a');
    expect($manager->hasAvailableSlots())->toBeTrue();

    $manager->getOrCreateAlias('topic/b');
    expect($manager->hasAvailableSlots())->toBeFalse();
});
