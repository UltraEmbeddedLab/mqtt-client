<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Tests\Support;

use RuntimeException;

use function explode;
use function stream_set_blocking;
use function stream_socket_accept;
use function stream_socket_get_name;
use function stream_socket_server;

/**
 * A real TCP connection over 127.0.0.1, for tests that need the kernel in the loop.
 *
 * The in-memory transport cannot reproduce what these tests are about: bytes arriving in
 * pieces, a read that times out with half a packet delivered, select() on a socket. Both
 * ends live in the test process, so the "broker" end must only ever be written to in
 * amounts the kernel will buffer, or the test deadlocks.
 *
 * One listener serves the whole run. Closing a listening socket costs 500 ms on Windows —
 * PHP waits for it to become writable, which it never does — so creating one per test
 * added half a second to each.
 */
final class Loopback
{
    private static ?self $shared = null;

    /** @var resource */
    private $server;

    public readonly string $host;

    public readonly int $port;

    private function __construct()
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            throw new RuntimeException("Cannot listen on loopback: $errstr");
        }
        $this->server = $server;

        [$host, $port] = explode(':', (string) stream_socket_get_name($server, false));
        $this->host    = $host;
        $this->port    = (int) $port;
    }

    public static function listener(): self
    {
        return self::$shared ??= new self();
    }

    /**
     * Accept the connection a transport has just opened to this listener.
     *
     * @return resource the broker end, non-blocking
     */
    public function accept()
    {
        $peer = stream_socket_accept($this->server, 5);
        if ($peer === false) {
            throw new RuntimeException('No connection to accept');
        }
        stream_set_blocking($peer, false);

        return $peer;
    }
}
