<?php

/** @noinspection PhpUnhandledExceptionInspection — a throwing test is a failing test; Pest reports it. */

declare(strict_types=1);

use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Transport\WsTransport;

/**
 * RFC 6455 §1.3 fixes the GUID a server appends to Sec-WebSocket-Key before hashing.
 * If the client's copy differs by a single character, every handshake fails after the
 * server has already returned a valid 101 — so pin it to the RFC's own test vector.
 */
describe('RFC 6455 handshake', function (): void {
    it('uses the GUID mandated by RFC 6455', function (): void {
        $guid = new ReflectionClass(WsTransport::class)->getConstant('WS_GUID');

        expect($guid)->toBe('258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
            ->and($guid)->toHaveLength(36);
    });

    it('derives the Sec-WebSocket-Accept value from RFC 6455 §1.3', function (): void {
        $guid = new ReflectionClass(WsTransport::class)->getConstant('WS_GUID');

        // The RFC's worked example: key "dGhlIHNhbXBsZSBub25jZQ==" must yield
        // "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".
        $accept = base64_encode(sha1('dGhlIHNhbXBsZSBub25jZQ==' . $guid, true));

        expect($accept)->toBe('s3pPLMBiTxaQ9kYGzzhZRbK+xOo=');
    });
});

/**
 * Run the client's check of a handshake response, for the RFC 6455 example key.
 *
 * @throws ReflectionException
 */
function checkAnswer(string $response): void
{
    new ReflectionMethod(WsTransport::class, 'checkHandshakeResponse')
        ->invoke(new WsTransport(), $response, 'dGhlIHNhbXBsZSBub25jZQ==');
}

describe('the server\'s answer to the upgrade', function (): void {
    it('is accepted whatever the case of its header names', function (string $name): void {
        // Header names are case-insensitive (RFC 9110 §5.1). EMQX sends them in lower case,
        // and a case-sensitive match refused every WebSocket connection to it.
        checkAnswer("HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n$name: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n");

        expect(true)->toBeTrue();
    })->with(['as in the RFC' => ['Sec-WebSocket-Accept'], 'as EMQX sends it' => ['sec-websocket-accept'], 'upper case' => ['SEC-WEBSOCKET-ACCEPT']]);

    it('is refused with the wrong Sec-WebSocket-Accept, or none', function (string $headers): void {
        expect(fn () => checkAnswer("HTTP/1.1 101 Switching Protocols\r\n$headers\r\n"))
            ->toThrow(TransportError::class, 'Sec-WebSocket-Accept');
    })->with(['wrong' => ["Sec-WebSocket-Accept: AAAA\r\n"], 'missing' => ["Upgrade: websocket\r\n"]]);

    it('is refused unless it is a 101', function (): void {
        expect(fn () => checkAnswer("HTTP/1.1 404 Not Found\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n"))
            ->toThrow(TransportError::class, '404');
    });
});
