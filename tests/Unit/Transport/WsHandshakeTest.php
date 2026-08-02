<?php

declare(strict_types=1);

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
