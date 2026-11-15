<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Transport;

use Random\RandomException;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Util\Clock;
use Throwable;

use function base64_encode;
use function chr;
use function explode;
use function fclose;
use function feof;
use function floor;
use function fread;
use function fwrite;
use function intdiv;
use function is_resource;
use function max;
use function ord;
use function pack;
use function random_bytes;
use function sha1;
use function sprintf;
use function str_contains;
use function str_repeat;
use function stream_context_create;
use function stream_select;
use function stream_set_blocking;
use function stream_set_timeout;
use function stream_socket_client;
use function strlen;
use function strpos;
use function substr;
use function trim;
use function unpack;
use function usleep;

/**
 * WebSocket transport for MQTT over WebSockets (ws://).
 *
 * Implements RFC 6455 WebSocket framing over a TCP stream, using the 'mqtt' subprotocol
 * as required by the MQTT specification.
 *
 * Supports:
 * - ws:// (plain WebSocket), custom URI paths (default: /mqtt)
 * - binary messages split across continuation frames
 * - ping/pong control frames, including between fragments
 *
 * Not supported: wss://. enableTls() runs after the HTTP upgrade, which is too late for
 * a TLS-terminated WebSocket endpoint.
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

    private const int OPCODE_CONTINUATION = 0x00;

    private const int OPCODE_TEXT = 0x01;

    private const int OPCODE_BINARY = 0x02;

    private const int OPCODE_CLOSE = 0x08;

    private const int OPCODE_PING = 0x09;

    private const int OPCODE_PONG = 0x0A;

    /** @var resource|null */
    private $stream;

    /** Buffer for reassembling fragmented MQTT data */
    private string $mqttBuffer = '';

    /** Position of the first unread byte in $mqttBuffer. */
    private int $mqttOffset = 0;

    /**
     * Opcode of the data message whose fragments are still arriving, or null.
     *
     * Continuation frames carry opcode 0; what they continue is only known from the frame
     * that started the message. MQTT is a byte stream, so a binary message's fragments are
     * simply appended in order — but a fragmented text message must not leak into it.
     */
    private ?int $fragmentOpcode = null;

    /**
     * Bytes read from the socket that do not yet form a complete WebSocket frame.
     *
     * A frame is parsed only once all of it is here, so a timeout part-way through one
     * leaves these bytes for the next call. Reading header, length, mask and payload with
     * separate blocking reads — as before — lost the header whenever the payload read timed
     * out, and every later frame boundary was wrong.
     */
    private string $rawBuffer = '';

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

        $this->stream         = $stream;
        $this->tlsEnabled     = false;
        $this->mqttBuffer     = '';
        $this->mqttOffset     = 0;
        $this->rawBuffer      = '';
        $this->fragmentOpcode = null;

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

        $deadline = $timeoutSec !== null ? (Clock::now() + $timeoutSec) : null;

        // Whole frames only: readFrame() either appends a complete payload or throws with
        // the partial frame still in $rawBuffer, so this is atomic like TcpTransport.
        if (strlen($this->mqttBuffer) - $this->mqttOffset < $length && $this->mqttOffset > 0) {
            // Drop the consumed prefix once, before growing the buffer — cutting it off on
            // every read copied the remainder each time, quadratic for a large frame that
            // carries many small MQTT packets.
            $this->mqttBuffer = substr($this->mqttBuffer, $this->mqttOffset);
            $this->mqttOffset = 0;
        }
        while (strlen($this->mqttBuffer) - $this->mqttOffset < $length) {
            $payload = $this->readFrame($deadline);
            if ($payload !== null) {
                $this->mqttBuffer .= $payload;
            }
        }

        $result = substr($this->mqttBuffer, $this->mqttOffset, $length);
        $this->mqttOffset += $length;
        if ($this->mqttOffset === strlen($this->mqttBuffer)) {
            $this->mqttBuffer = '';
            $this->mqttOffset = 0;
        }

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
        $this->stream         = null;
        $this->context        = null;
        $this->tlsEnabled     = false;
        $this->mqttBuffer     = '';
        $this->mqttOffset     = 0;
        $this->rawBuffer      = '';
        $this->fragmentOpcode = null;
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

        // Read the HTTP response. Anything after the blank line is already WebSocket data
        // and stays in $rawBuffer for readFrame().
        $deadline = Clock::now() + 5.0;
        while (($end = strpos($this->rawBuffer, "\r\n\r\n")) === false) {
            try {
                $this->fillRaw(strlen($this->rawBuffer) + 1, $deadline);
            } catch (Timeout) {
                throw new TransportError('WebSocket: Handshake timed out');
            }
        }
        $response        = substr($this->rawBuffer, 0, $end + 4);
        $this->rawBuffer = substr($this->rawBuffer, $end + 4);

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
     *
     * @param  self::OPCODE_*  $opcode
     *
     * @throws RandomException
     */
    private function encodeFrame(string $payload, int $opcode): string
    {
        $length = strlen($payload);
        $frame  = chr(0x80 + $opcode); // FIN + opcode

        // Mask bit is always set for client-to-server
        if ($length < 126) {
            $frame .= chr(0x80 + $length);
        } elseif ($length < 65536) {
            $frame .= chr(0x80 | 126).pack('n', $length);
        } else {
            $frame .= chr(0x80 | 127).pack('J', $length);
        }

        $mask = random_bytes(4);

        return $frame.$mask.$this->applyMask($payload, $mask);
    }

    /**
     * XOR $payload with the repeating 4-byte $mask (RFC 6455 §5.3).
     *
     * One string-wide XOR in C rather than a chr()/ord() round trip per byte in PHP: the
     * per-byte loop this replaces cost about a millisecond per 64 KiB, which made the
     * WebSocket transport unusable for anything but small messages.
     */
    private function applyMask(string $payload, string $mask): string
    {
        $length = strlen($payload);
        if ($length === 0) {
            return '';
        }

        // XOR of two strings yields the length of the shorter one, so the key only has to
        // cover the payload.
        return $payload ^ str_repeat($mask, intdiv($length + 3, 4));
    }

    /**
     * Read and decode a single WebSocket frame, returning the payload for binary frames.
     * Handles ping/pong/close control frames transparently.
     *
     * Nothing is consumed from $rawBuffer until the whole frame is there, so a Timeout
     * leaves the stream exactly where it was.
     *
     * @throws RandomException
     * @throws Timeout
     */
    private function readFrame(?float $deadline): ?string
    {
        // Header: 2 bytes, then a 0/2/8-byte extended length, then a 0/4-byte mask.
        $this->fillRaw(2, $deadline);
        $b0 = ord($this->rawBuffer[0]);
        $b1 = ord($this->rawBuffer[1]);

        $final      = ($b0 & 0x80) !== 0;
        $opcode     = $b0 & 0x0F;
        $masked     = ($b1 & 0x80) !== 0;
        $payloadLen = $b1 & 0x7F;
        $headerLen  = 2 + match ($payloadLen) {
            126     => 2,
            127     => 8,
            default => 0,
        } + ($masked ? 4 : 0);

        $this->fillRaw($headerLen, $deadline);
        if ($payloadLen === 126) {
            /** @var array{1: int} $unpacked */
            $unpacked   = unpack('n', $this->rawBuffer, 2);
            $payloadLen = $unpacked[1];
        } elseif ($payloadLen === 127) {
            /** @var array{1: int} $unpacked */
            $unpacked   = unpack('J', $this->rawBuffer, 2);
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

        $this->fillRaw($headerLen + $payloadLen, $deadline);
        $payload = substr($this->rawBuffer, $headerLen, $payloadLen);
        if ($masked) {
            $payload = $this->applyMask($payload, substr($this->rawBuffer, $headerLen - 4, 4));
        }
        $this->rawBuffer = substr($this->rawBuffer, $headerLen + $payloadLen);

        // A continuation frame belongs to whichever data message is in progress. Control
        // frames (ping, pong, close) may arrive between fragments and change nothing here.
        $messageOpcode = $opcode;
        if ($opcode === self::OPCODE_CONTINUATION) {
            $messageOpcode = $this->fragmentOpcode;
            if ($final) {
                $this->fragmentOpcode = null;
            }
        } elseif ($opcode === self::OPCODE_BINARY || $opcode === self::OPCODE_TEXT) {
            $this->fragmentOpcode = $final ? null : $opcode;
        }

        return match ($messageOpcode) {
            self::OPCODE_BINARY => $payload,
            self::OPCODE_PING   => $this->handlePing($payload),
            self::OPCODE_CLOSE  => throw new TransportError('WebSocket: Connection closed by server'),
            default             => null, // text messages, pongs, unknown opcodes
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

    /**
     * Grow $rawBuffer until it holds at least $length bytes, keeping them all on Timeout.
     *
     * @throws Timeout
     */
    private function fillRaw(int $length, ?float $deadline): void
    {
        $stream = $this->stream;
        if (!is_resource($stream)) {
            throw new TransportError('Invalid stream resource');
        }

        while (strlen($this->rawBuffer) < $length) {
            // Poll at least once even past the deadline, so a zero timeout returns data
            // that is already waiting instead of failing outright.
            $sec  = null;
            $usec = 0;
            if ($deadline !== null) {
                $timeLeft = max(0.0, $deadline - Clock::now());
                $sec      = (int) floor($timeLeft);
                $usec     = (int) floor(($timeLeft - $sec) * 1_000_000);
            }
            $r = [$stream];
            $w = null;
            $e = null;
            $n = @stream_select($r, $w, $e, $sec, $usec);
            if ($n === false) {
                throw new TransportError('stream_select failed');
            }
            if ($n === 0) {
                throw new Timeout('WebSocket: Read timed out');
            }

            $chunk = @fread($stream, max($length - strlen($this->rawBuffer), 65_536));
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
            $this->rawBuffer .= $chunk;
        }
    }
}
