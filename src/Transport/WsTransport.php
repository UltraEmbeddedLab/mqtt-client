<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Transport;

use Random\RandomException;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;
use Throwable;

use function base64_encode;
use function chr;
use function explode;
use function fclose;
use function feof;
use function floor;
use function fread;
use function fwrite;
use function is_resource;
use function max;
use function microtime;
use function ord;
use function pack;
use function random_bytes;
use function sha1;
use function sprintf;
use function str_contains;
use function stream_context_create;
use function stream_select;
use function stream_set_blocking;
use function stream_set_timeout;
use function stream_socket_client;
use function strlen;
use function substr;
use function trim;
use function usleep;

/**
 * WebSocket transport for MQTT over WebSockets (ws:// and wss://).
 *
 * Implements RFC 6455 WebSocket framing over a TCP/TLS stream,
 * using the 'mqtt' subprotocol as required by the MQTT specification.
 *
 * Supports:
 * - ws:// (plain WebSocket)
 * - Wss:// (WebSocket over TLS)
 * - Custom URI paths (default: /mqtt)
 * - Fragmented message reassembly
 * - Ping/pong keepalive frames
 */
final class WsTransport implements TransportInterface
{
    /** Owns $context and $tlsEnabled, and the whole TLS upgrade. */
    use NegotiatesTls;
    /**
     * The GUID RFC 6455 §1.3 requires servers to append to Sec-WebSocket-Key before
     * hashing. Exactly 36 characters — a single wrong character makes every handshake
     * fail after the server has already returned a valid 101.
     */
    private const string WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

    private const int OPCODE_BINARY = 0x02;

    private const int OPCODE_CLOSE = 0x08;

    private const int OPCODE_PING = 0x09;

    private const int OPCODE_PONG = 0x0A;

    /** @var resource|null */
    private $stream;

    /** Buffer for reassembling fragmented MQTT data */
    private string $mqttBuffer = '';

    /**
     * @param  string  $path  HTTP path used for the Upgrade request
     * @param  int  $maxFrameSize  Largest accepted WebSocket payload, in bytes. Checked
     *                             against the declared length before allocating, so a
     *                             peer cannot force a large allocation with a 10-byte
     *                             header. Defaults to the same 16 MiB the client applies
     *                             to MQTT packets.
     */
    public function __construct(
        private readonly string $path = '/mqtt',
        private readonly int $maxFrameSize = Options::DEFAULT_MAXIMUM_PACKET_SIZE,
    ) {
    }

    /**
     * @throws RandomException
     */
    public function open(string $host, int $port, float $timeoutSec = 5.0): void
    {
        $this->close();

        $remote        = sprintf('tcp://%s:%d', $host, $port);
        $this->context = stream_context_create([]);

        $errno  = 0;
        $errstr = '';
        $stream = @stream_socket_client(
            $remote,
            $errno,
            $errstr,
            $timeoutSec,
            STREAM_CLIENT_CONNECT,
            $this->context,
        );

        if ($stream === false) {
            throw new TransportError(sprintf('WebSocket: Failed to connect to %s:%d: [%d] %s', $host, $port, $errno, $errstr));
        }

        stream_set_blocking($stream, true);
        $sec  = (int) floor($timeoutSec);
        $usec = (int) floor(($timeoutSec - $sec) * 1_000_000);
        @stream_set_timeout($stream, $sec, $usec);

        $this->stream     = $stream;
        $this->tlsEnabled = false;
        $this->mqttBuffer = '';

        // Perform WebSocket handshake
        $this->performHandshake($host, $port);
    }

    /**
     * @throws RandomException
     */
    public function write(string $bytes): int
    {
        if (!$this->isOpen()) {
            throw new TransportError('WebSocket: Cannot write: transport is not open');
        }

        $frame = $this->encodeFrame($bytes, self::OPCODE_BINARY);

        return $this->rawWrite($frame);
    }

    /**
     * @throws RandomException
     */
    public function readExact(int $length, ?float $timeoutSec = null): string
    {
        if ($length < 0) {
            throw new TransportError('readExact length cannot be negative');
        }
        if ($length === 0) {
            return '';
        }
        if (!$this->isOpen()) {
            throw new TransportError('WebSocket: Cannot read: transport is not open');
        }

        $deadline = $timeoutSec !== null ? (microtime(true) + $timeoutSec) : null;

        // Drain from mqttBuffer first
        while (strlen($this->mqttBuffer) < $length) {
            if ($deadline !== null) {
                $timeLeft = $deadline - microtime(true);
                if ($timeLeft <= 0) {
                    throw new Timeout('WebSocket: Read timed out');
                }
            }

            $payload = $this->readFrame($deadline);
            if ($payload !== null) {
                $this->mqttBuffer .= $payload;
            }
        }

        $result           = substr($this->mqttBuffer, 0, $length);
        $this->mqttBuffer = substr($this->mqttBuffer, $length);

        return $result;
    }

    public function close(): void
    {
        if (is_resource($this->stream)) {
            // Send WebSocket close frame (best-effort)
            try {
                $frame = $this->encodeFrame(pack('n', 1000), self::OPCODE_CLOSE);
                @fwrite($this->stream, $frame);
            } catch (Throwable) {
                // Ignore errors during close
            }
            @fclose($this->stream);
        }
        $this->stream     = null;
        $this->context    = null;
        $this->tlsEnabled = false;
        $this->mqttBuffer = '';
    }

    public function isOpen(): bool
    {
        return is_resource($this->stream);
    }

    /**
     * @param array<string, mixed>|null $tlsOptions
     */
    public function enableTls(?array $tlsOptions = null): void
    {
        $this->assertTlsAvailable('WebSocket: ');

        if (!$this->isOpen()) {
            throw new TransportError('WebSocket: Cannot enable TLS: transport is not open');
        }
        if ($this->tlsEnabled) {
            return;
        }

        $stream = $this->stream;
        if (!is_resource($stream)) {
            throw new TransportError('Invalid stream resource');
        }

        $this->negotiateTls($stream, $tlsOptions, 'WebSocket: ');
    }

    /**
     * @throws RandomException
     */
    private function performHandshake(string $host, int $port): void
    {
        $key = base64_encode(random_bytes(16));

        $request = "GET $this->path HTTP/1.1\r\n"
            . "Host: $host:$port\r\n"
            . "Upgrade: websocket\r\n"
            . "Connection: Upgrade\r\n"
            . "Sec-WebSocket-Key: $key\r\n"
            . "Sec-WebSocket-Version: 13\r\n"
            . "Sec-WebSocket-Protocol: mqtt\r\n"
            . "\r\n";

        $this->rawWrite($request);

        // Read HTTP response
        $response = '';
        $deadline = microtime(true) + 5.0;
        while (!str_contains($response, "\r\n\r\n")) {
            if (microtime(true) > $deadline) {
                throw new TransportError('WebSocket: Handshake timed out');
            }
            $chunk = $this->rawRead(1, $deadline);
            $response .= $chunk;
        }

        // Validate response
        $lines      = explode("\r\n", $response);
        $statusLine = $lines[0];
        if (!str_contains($statusLine, '101')) {
            throw new TransportError("WebSocket: Handshake failed: $statusLine");
        }

        // Verify Sec-WebSocket-Accept
        $expectedAccept = base64_encode(sha1($key . self::WS_GUID, true));
        $found          = false;
        foreach ($lines as $line) {
            if (str_contains($line, 'Sec-WebSocket-Accept')) {
                $parts  = explode(':', $line, 2);
                $accept = trim($parts[1] ?? '');
                if ($accept === $expectedAccept) {
                    $found = true;
                }

                break;
            }
        }

        if (!$found) {
            throw new TransportError('WebSocket: Invalid Sec-WebSocket-Accept in handshake response');
        }
    }

    /**
     * Encode data into a WebSocket frame (client-to-server, always masked).
     * @throws RandomException
     */
    private function encodeFrame(string $payload, int $opcode): string
    {
        $length = strlen($payload);
        $frame  = chr(0x80 | $opcode); // FIN + opcode

        // Mask bit is always set for client-to-server
        if ($length < 126) {
            $frame .= chr(0x80 | $length);
        } elseif ($length < 65536) {
            $frame .= chr(0x80 | 126) . pack('n', $length);
        } else {
            $frame .= chr(0x80 | 127) . pack('J', $length);
        }

        // Masking key
        $mask = random_bytes(4);
        $frame .= $mask;

        // Mask payload
        for ($i = 0; $i < $length; $i++) {
            $frame .= chr(ord($payload[$i]) ^ ord($mask[$i % 4]));
        }

        return $frame;
    }

    /**
     * Read and decode a single WebSocket frame, returning the payload for binary frames.
     * Handles ping/pong/close control frames transparently.
     * @throws RandomException
     */
    private function readFrame(?float $deadline): ?string
    {
        $stream = $this->stream;
        if (!is_resource($stream)) {
            throw new TransportError('Invalid stream resource');
        }

        // Wait for data
        if ($deadline !== null) {
            $timeLeft = $deadline - microtime(true);
            if ($timeLeft <= 0) {
                throw new Timeout('WebSocket: Read timed out');
            }

            $r    = [$stream];
            $w    = null;
            $e    = null;
            $sec  = (int) floor($timeLeft);
            $usec = (int) floor(($timeLeft - $sec) * 1_000_000);
            $n    = @stream_select($r, $w, $e, $sec, $usec);
            if ($n === false) {
                throw new TransportError('stream_select failed');
            }
            if ($n === 0) {
                throw new Timeout('WebSocket: Read timed out');
            }
        }

        // Read frame header (2 bytes minimum)
        $header = $this->rawRead(2, $deadline);
        $b0     = ord($header[0]);
        $b1     = ord($header[1]);

        $opcode     = $b0 & 0x0F;
        $masked     = ($b1 & 0x80) !== 0;
        $payloadLen = $b1 & 0x7F;

        if ($payloadLen === 126) {
            $ext = $this->rawRead(2, $deadline);
            /** @var array{1: int} $unpacked */
            $unpacked   = unpack('n', $ext);
            $payloadLen = $unpacked[1];
        } elseif ($payloadLen === 127) {
            $ext = $this->rawRead(8, $deadline);
            /** @var array{1: int} $unpacked */
            $unpacked   = unpack('J', $ext);
            $payloadLen = $unpacked[1];
        }

        // RFC 6455 permits a 63-bit length, and `unpack('J', ...)` turns anything at or
        // above 2^63 into a negative int. Bound it before a single header byte can size
        // an allocation. This runs before the MQTT layer sees anything, so it is the only
        // place the WebSocket path can be protected.
        if ($payloadLen < 0 || $payloadLen > $this->maxFrameSize) {
            $this->close();

            throw new ProtocolError("WebSocket frame of $payloadLen bytes exceeds the maximum of $this->maxFrameSize");
        }

        $mask = '';
        if ($masked) {
            $mask = $this->rawRead(4, $deadline);
        }

        $payload = '';
        if ($payloadLen > 0) {
            $payload = $this->rawRead((int) $payloadLen, $deadline);
            if ($masked) {
                for ($i = 0; $i < $payloadLen; $i++) {
                    $payload[$i] = chr(ord($payload[$i]) ^ ord($mask[$i % 4]));
                }
            }
        }

        return match ($opcode) {
            self::OPCODE_BINARY => $payload,
            self::OPCODE_PING   => $this->handlePing($payload),
            self::OPCODE_CLOSE  => throw new TransportError('WebSocket: Connection closed by server'),
            default             => null, // text frames, continuation, etc.
        };
    }

    /**
     * @throws RandomException
     */
    private function handlePing(string $payload): null
    {
        // Respond with pong
        $frame = $this->encodeFrame($payload, self::OPCODE_PONG);
        $this->rawWrite($frame);

        return null;
    }

    private function rawWrite(string $bytes): int
    {
        $stream = $this->stream;
        if (!is_resource($stream)) {
            throw new TransportError('Invalid stream resource');
        }

        $total = 0;
        $len   = strlen($bytes);
        while ($total < $len) {
            $written = @fwrite($stream, substr($bytes, $total));
            if ($written === false) {
                throw new TransportError('WebSocket: Write failed');
            }
            if ($written === 0) {
                if (feof($stream)) {
                    throw new TransportError('WebSocket: Connection closed by peer during write');
                }
                usleep(1000);

                continue;
            }
            $total += $written;
        }

        return $total;
    }

    private function rawRead(int $length, ?float $deadline): string
    {
        $stream = $this->stream;
        if (!is_resource($stream)) {
            throw new TransportError('Invalid stream resource');
        }

        $data = '';
        while (strlen($data) < $length) {
            if ($deadline !== null && microtime(true) > $deadline) {
                throw new Timeout('WebSocket: Read timed out');
            }

            $remaining = max(1, $length - strlen($data));
            $chunk     = @fread($stream, $remaining);
            if ($chunk === false) {
                throw new TransportError('WebSocket: Read failed');
            }
            if ($chunk === '') {
                if (feof($stream)) {
                    throw new TransportError('WebSocket: Connection closed by peer during read');
                }
                usleep(1000);

                continue;
            }
            $data .= $chunk;
        }

        return $data;
    }
}
