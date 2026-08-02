<?php

/** @noinspection PhpUnhandledExceptionInspection — a throwing test is a failing test; Pest reports it. */

declare(strict_types=1);

use ScienceStories\Mqtt\Session\FileSessionStore;
use ScienceStories\Mqtt\Session\SessionState;

beforeEach(function (): void {
    $this->tempDir = sys_get_temp_dir().'/mqtt_test_'.uniqid();
});

afterEach(function (): void {
    if (is_dir($this->tempDir)) {
        $files = glob($this->tempDir.'/*');
        if ($files !== false) {
            foreach ($files as $file) {
                unlink($file);
            }
        }
        rmdir($this->tempDir);
    }
});

test('save and load roundtrip', function (): void {
    $store = new FileSessionStore($this->tempDir);
    $state = new SessionState(
        subscriptions: ['sensors/#' => ['qos' => 1, 'options' => null]],
        pendingQos2: [42],
    );

    $store->save('client-1', $state);
    $loaded = $store->load('client-1');

    expect($loaded)->not->toBeNull()
        ->and($loaded->subscriptions)->toBe($state->subscriptions)
        ->and($loaded->pendingQos2)->toBe($state->pendingQos2);
});

test('load non-existent returns null', function (): void {
    $store = new FileSessionStore($this->tempDir);
    expect($store->load('nonexistent'))->toBeNull();
});

test('delete removes file', function (): void {
    $store = new FileSessionStore($this->tempDir);
    $store->save('client-1', new SessionState());
    $store->delete('client-1');

    expect($store->load('client-1'))->toBeNull();
});

test('exists returns true after save', function (): void {
    $store = new FileSessionStore($this->tempDir);
    $store->save('client-1', new SessionState());

    expect($store->exists('client-1'))->toBeTrue();
});

test('exists returns false after delete', function (): void {
    $store = new FileSessionStore($this->tempDir);
    $store->save('client-1', new SessionState());
    $store->delete('client-1');

    expect($store->exists('client-1'))->toBeFalse();
});

test('expired session returns null on load', function (): void {
    $store = new FileSessionStore($this->tempDir, 1); // 1 second expiry
    $state = new SessionState(savedAt: time() - 10);

    $store->save('client-1', $state);
    expect($store->load('client-1'))->toBeNull();
});

test('cleanupExpired removes stale files', function (): void {
    $store = new FileSessionStore($this->tempDir, 1); // 1 second expiry

    $oldState = new SessionState(savedAt: time() - 10);
    $store->save('old-client', $oldState);

    $freshState = new SessionState();
    $store->save('fresh-client', $freshState);

    $removed = $store->cleanupExpired();

    expect($removed)->toBe(1)
        ->and($store->exists('fresh-client'))->toBeTrue();
});

test('sanitize safe client ID used as filename directly', function (): void {
    $store = new FileSessionStore($this->tempDir);
    $store->save('simple-client_123', new SessionState());

    $expectedFile = $this->tempDir.'/simple-client_123.json';
    expect(file_exists($expectedFile))->toBeTrue();
});

test('unsafe client ID gets hashed', function (): void {
    $store    = new FileSessionStore($this->tempDir);
    $unsafeId = '../../../etc/passwd';
    $store->save($unsafeId, new SessionState());

    $expectedFile = $this->tempDir.'/mqtt_'.sha1($unsafeId).'.json';
    expect(file_exists($expectedFile))->toBeTrue();
});
