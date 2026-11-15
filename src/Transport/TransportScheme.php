<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Transport;

/**
 * @deprecated since 2.1, to be removed in 3.0. Nothing in the library reads it — there is
 *             no URI parsing that selects a transport — so it describes a feature that does
 *             not exist. Construct `TcpTransport` or `WsTransport` directly.
 */
enum TransportScheme: string
{
    case TCP = 'tcp';
    case TLS = 'tls';
    case WS  = 'ws';
    case WSS = 'wss';
}
