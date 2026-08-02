<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\FlowControl;

test('constructor clamps to minimum 1', function (): void {
    $flow = new FlowControl(0);
    expect($flow->maxInFlight)->toBe(1);
});

test('constructor clamps to maximum 65535', function (): void {
    $flow = new FlowControl(99999);
    expect($flow->maxInFlight)->toBe(65535);
});

test('canSend initially true', function (): void {
    $flow = new FlowControl();
    expect($flow->canSend())->toBeTrue();
});

test('trackSend increments in-flight count', function (): void {
    $flow = new FlowControl();
    $flow->trackSend(1);
    expect($flow->currentInFlight)->toBe(1);
    $flow->trackSend(2);
    expect($flow->currentInFlight)->toBe(2);
});

test('canSend false when at max', function (): void {
    $flow = new FlowControl(2);
    $flow->trackSend(1);
    $flow->trackSend(2);
    expect($flow->canSend())->toBeFalse();
});

test('trackAck decrements count and reopens slot', function (): void {
    $flow = new FlowControl(2);
    $flow->trackSend(1);
    $flow->trackSend(2);
    expect($flow->canSend())->toBeFalse();

    $flow->trackAck(1);
    expect($flow->currentInFlight)->toBe(1)
        ->and($flow->canSend())->toBeTrue();
});

test('trackAck for unknown packetId is no-op', function (): void {
    $flow = new FlowControl();
    $flow->trackSend(1);
    $flow->trackAck(999);
    expect($flow->currentInFlight)->toBe(1);
});

test('duplicate trackSend same packetId does not increment twice', function (): void {
    $flow = new FlowControl();
    $flow->trackSend(1);
    $flow->trackSend(1);
    expect($flow->currentInFlight)->toBe(1);
});

test('getPendingPacketIds returns tracked ids', function (): void {
    $flow = new FlowControl();
    $flow->trackSend(10);
    $flow->trackSend(20);
    expect($flow->getPendingPacketIds())->toBe([10, 20]);
});

test('isPending returns correct state', function (): void {
    $flow = new FlowControl();
    $flow->trackSend(5);
    expect($flow->isPending(5))->toBeTrue()
        ->and($flow->isPending(6))->toBeFalse();
});

test('getTimedOutPackets identifies stale packets', function (): void {
    $flow = new FlowControl();
    $flow->trackSend(1);
    usleep(50000); // 50ms
    $flow->trackSend(2);

    $timedOut = $flow->getTimedOutPackets(0.01); // 10ms timeout
    expect($timedOut)->toContain(1);
});

test('reset clears all state', function (): void {
    $flow = new FlowControl();
    $flow->trackSend(1);
    $flow->trackSend(2);
    $flow->reset();

    expect($flow->currentInFlight)->toBe(0)
        ->and($flow->getPendingPacketIds())->toBe([])
        ->and($flow->canSend())->toBeTrue();
});

test('setMaxInFlight updates limit', function (): void {
    $flow = new FlowControl(10);
    expect($flow->maxInFlight)->toBe(10);

    $flow->setMaxInFlight(5);
    expect($flow->maxInFlight)->toBe(5);
});
