<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\WillOptions;
use ScienceStories\Mqtt\Protocol\QoS;

test('default constructor values', function (): void {
    $will = new WillOptions(topic: '', payload: '');

    expect($will->topic)->toBe('')
        ->and($will->payload)->toBe('')
        ->and($will->qos)->toBe(QoS::AtMostOnce)
        ->and($will->retain)->toBeFalse()
        ->and($will->properties)->toBeNull();
});

test('withTopic returns new instance', function (): void {
    $will = new WillOptions(topic: 'old', payload: 'data');
    $new  = $will->withTopic('new');

    expect($new->topic)->toBe('new')
        ->and($new)->not->toBe($will);
});

test('withPayload returns new instance', function (): void {
    $will = new WillOptions(topic: 'test', payload: 'old');
    $new  = $will->withPayload('new');

    expect($new->payload)->toBe('new')
        ->and($new)->not->toBe($will);
});

test('withQos returns new instance', function (): void {
    $will = new WillOptions(topic: 'test', payload: 'data');
    $new  = $will->withQos(QoS::ExactlyOnce);

    expect($new->qos)->toBe(QoS::ExactlyOnce)
        ->and($new)->not->toBe($will);
});

test('withRetain returns new instance', function (): void {
    $will = new WillOptions(topic: 'test', payload: 'data');
    $new  = $will->withRetain();

    expect($new->retain)->toBeTrue()
        ->and($new)->not->toBe($will);
});

test('immutability - original not modified after with*()', function (): void {
    $original = new WillOptions(topic: 'original', payload: 'data', qos: QoS::AtMostOnce, retain: false);

    // Assert on the returned copies as well as the original: discarding the return value
    // would prove the original is untouched, but not that the copy actually changed.
    expect($original->withTopic('changed')->topic)->toBe('changed')
        ->and($original->withPayload('changed')->payload)->toBe('changed')
        ->and($original->withQos(QoS::ExactlyOnce)->qos)->toBe(QoS::ExactlyOnce)
        ->and($original->withRetain()->retain)->toBeTrue()
        ->and($original->topic)->toBe('original')
        ->and($original->payload)->toBe('data')
        ->and($original->qos)->toBe(QoS::AtMostOnce)
        ->and($original->retain)->toBeFalse();
});
