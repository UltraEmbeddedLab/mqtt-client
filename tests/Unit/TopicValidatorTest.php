<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Util\TopicValidator;

test('valid publish topic accepted', function (): void {
    TopicValidator::validatePublishTopic('sensors/temperature');
    expect(true)->toBeTrue();
});

test('empty publish topic throws ProtocolError', function (): void {
    TopicValidator::validatePublishTopic('');
})->throws(ProtocolError::class);

test('publish topic with null byte throws ProtocolError', function (): void {
    TopicValidator::validatePublishTopic("sensors/\0temp");
})->throws(ProtocolError::class);

test('publish topic with plus wildcard throws ProtocolError', function (): void {
    TopicValidator::validatePublishTopic('sensors/+/temp');
})->throws(ProtocolError::class);

test('publish topic with hash wildcard throws ProtocolError', function (): void {
    TopicValidator::validatePublishTopic('sensors/#');
})->throws(ProtocolError::class);

test('publish topic exceeding max length throws ProtocolError', function (): void {
    $topic = str_repeat('a', 65536);
    TopicValidator::validatePublishTopic($topic);
})->throws(ProtocolError::class);

test('valid subscribe filter accepted (simple topic)', function (): void {
    TopicValidator::validateSubscribeFilter('sensors/temperature');
    expect(true)->toBeTrue();
});

test('subscribe filter with + wildcard at entire level accepted', function (): void {
    TopicValidator::validateSubscribeFilter('sensors/+/temperature');
    expect(true)->toBeTrue();
});

test('subscribe filter with # at end accepted', function (): void {
    TopicValidator::validateSubscribeFilter('sensors/#');
    expect(true)->toBeTrue();
});

test('subscribe filter with # only accepted', function (): void {
    TopicValidator::validateSubscribeFilter('#');
    expect(true)->toBeTrue();
});

test('subscribe filter +/+/# accepted', function (): void {
    TopicValidator::validateSubscribeFilter('+/+/#');
    expect(true)->toBeTrue();
});

test('empty subscribe filter throws ProtocolError', function (): void {
    TopicValidator::validateSubscribeFilter('');
})->throws(ProtocolError::class);

test('subscribe filter with + not at entire level throws', function (): void {
    TopicValidator::validateSubscribeFilter('sen+ors');
})->throws(ProtocolError::class);

test('subscribe filter with # not at end throws', function (): void {
    TopicValidator::validateSubscribeFilter('#/foo');
})->throws(ProtocolError::class);

test('subscribe filter with # not alone in level throws', function (): void {
    TopicValidator::validateSubscribeFilter('foo#');
})->throws(ProtocolError::class);

test('isValidPublishTopic returns true for valid', function (): void {
    expect(TopicValidator::isValidPublishTopic('sensors/temperature'))->toBeTrue();
});

test('isValidPublishTopic returns false for invalid', function (): void {
    expect(TopicValidator::isValidPublishTopic(''))->toBeFalse()
        ->and(TopicValidator::isValidPublishTopic('sensors/+'))->toBeFalse();
});

test('isValidSubscribeFilter returns true for valid', function (): void {
    expect(TopicValidator::isValidSubscribeFilter('sensors/#'))->toBeTrue();
});

test('isValidSubscribeFilter returns false for invalid', function (): void {
    expect(TopicValidator::isValidSubscribeFilter(''))->toBeFalse()
        ->and(TopicValidator::isValidSubscribeFilter('sen+ors'))->toBeFalse();
});
