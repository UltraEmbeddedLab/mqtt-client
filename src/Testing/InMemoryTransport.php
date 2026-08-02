<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Testing;

use ScienceStories\Mqtt\Contract\TransportInterface;
use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Exception\TransportError;
use ScienceStories\Mqtt\Protocol\Packet\PacketType;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Util\Bytes;

use function array_filter;
use function chr;
use function count;
use function ord;
use function pack;
use function strlen;
use function substr;

/**
 * A scriptable, in-memory TransportInterface for tests.
 *
 * Shipped in `src/` on purpose: it lets both this library and its consumers unit-test
 * client behaviour without a broker, a socket, or Docker.
 *
 * ```php
 * $transport = new InMemoryTransport();
 * $transport->feedConnAck();
 *
 * $client = new Client(new Options('fake'), $transport);
 * $client->connect();
 *
 * $transport->feedPublish('sensors/temp', '21.5');
 * $client->loopOnce(0.0);
 *
 * expect($transport->sentPackets()[0]['type'])->toBe(PacketType::CONNECT->value);
 * ```
 *
 * Reads are served from a byte buffer that `feed*()` appends to. When the buffer holds
 * fewer bytes than requested, `readExact()` throws {@see Timeout} — the same contract
 * {@see \ScienceStories\Mqtt\Transport\TcpTransport} has when no data arrives in time.
 */
final class InMemoryTransport implements TransportInterface
{
    /** Bytes waiting to be read by the client (i.e. "sent by the broker"). */
    private string $readBuffer = '';

    /** Every byte the client has written, in order. */
    private string $writeBuffer = '';

    private bool $open = false;

    private bool $tlsEnabled = false;

    /** @var array<string, mixed>|null */
    private ?array $tlsOptions = null;

    private ?string $host = null;

    private ?int $port = null;

    /** When true, reads fail with TransportError instead of Timeout. */
    private bool $peerClosed = false;

    public function open(string $host, int $port, float $timeoutSec = 5.0): void
    {
        $this->host       = $host;
        $this->port       = $port;
        $this->open       = true;
        $this->peerClosed = false;
        $this->tlsEnabled = false;
    }

    public function write(string $bytes): int
    {
        if (! $this->open) {
            throw new TransportError('Cannot write: transport is not open');
        }
        $this->writeBuffer .= $bytes;

        return strlen($bytes);
    }

    public function readExact(int $length, ?float $timeoutSec = null): string
    {
        if ($length < 0) {
            throw new TransportError('readExact length cannot be negative');
        }
        if ($length === 0) {
            return '';
        }
        if (! $this->open) {
            throw new TransportError('Cannot read: transport is not open');
        }
        if (strlen($this->readBuffer) < $length) {
            if ($this->peerClosed) {
                throw new TransportError('Connection closed by peer during read');
            }

            throw new Timeout('Read timed out');
        }

        $out              = substr($this->readBuffer, 0, $length);
        $this->readBuffer = substr($this->readBuffer, $length);

        return $out;
    }

    public function close(): void
    {
        $this->open       = false;
        $this->tlsEnabled = false;
    }

    public function isOpen(): bool
    {
        return $this->open;
    }

    /**
     * @param  array<string, mixed>|null  $tlsOptions
     */
    public function enableTls(?array $tlsOptions = null): void
    {
        if (! $this->open) {
            throw new TransportError('Cannot enable TLS: transport is not open');
        }
        $this->tlsEnabled = true;
        $this->tlsOptions = $tlsOptions;
    }

    // ---------------------------------------------------------------- scripting

    /** Append raw bytes to be read by the client. */
    public function feed(string $bytes): self
    {
        $this->readBuffer .= $bytes;

        return $this;
    }

    /** Append a full MQTT packet, computing the fixed header for you. */
    public function feedPacket(PacketType $type, int $flags, string $body): self
    {
        return $this->feed(
            chr(($type->value << 4) | ($flags & 0x0F)) . Bytes::encodeVarInt(strlen($body)) . $body,
        );
    }

    /**
     * Append a CONNACK.
     *
     * @param  string  $properties  Raw MQTT 5 property block (already length-prefixed), or '' for 3.1.1
     */
    public function feedConnAck(int $reasonCode = 0, bool $sessionPresent = false, string $properties = ''): self
    {
        return $this->feedPacket(
            PacketType::CONNACK,
            0,
            chr($sessionPresent ? 0x01 : 0x00) . chr($reasonCode) . $properties,
        );
    }

    /**
     * Append a PUBLISH.
     *
     * @param  string  $properties  Raw MQTT 5 property block (already length-prefixed), or '' for 3.1.1
     */
    public function feedPublish(
        string $topic,
        string $payload,
        QoS $qos = QoS::AtMostOnce,
        ?int $packetId = null,
        bool $dup = false,
        bool $retain = false,
        string $properties = '',
    ): self {
        $body = Bytes::encodeString($topic);
        if ($qos !== QoS::AtMostOnce) {
            $body .= pack('n', $packetId ?? 1);
        }
        $body .= $properties . $payload;

        $flags = ($dup ? 0x08 : 0x00) | ($qos->value << 1) | ($retain ? 0x01 : 0x00);

        return $this->feedPacket(PacketType::PUBLISH, $flags, $body);
    }

    /** @param  list<int>  $returnCodes */
    public function feedSubAck(int $packetId, array $returnCodes = [0]): self
    {
        $body = pack('n', $packetId);
        foreach ($returnCodes as $code) {
            $body .= chr($code);
        }

        return $this->feedPacket(PacketType::SUBACK, 0, $body);
    }

    /** @param  list<int>  $reasonCodes */
    public function feedUnsubAck(int $packetId, array $reasonCodes = []): self
    {
        $body = pack('n', $packetId);
        foreach ($reasonCodes as $code) {
            $body .= chr($code);
        }

        return $this->feedPacket(PacketType::UNSUBACK, 0, $body);
    }

    public function feedPubAck(int $packetId): self
    {
        return $this->feedPacket(PacketType::PUBACK, 0, pack('n', $packetId));
    }

    public function feedPubRec(int $packetId): self
    {
        return $this->feedPacket(PacketType::PUBREC, 0, pack('n', $packetId));
    }

    public function feedPubRel(int $packetId): self
    {
        return $this->feedPacket(PacketType::PUBREL, 0x02, pack('n', $packetId));
    }

    public function feedPubComp(int $packetId): self
    {
        return $this->feedPacket(PacketType::PUBCOMP, 0, pack('n', $packetId));
    }

    public function feedPingResp(): self
    {
        return $this->feedPacket(PacketType::PINGRESP, 0, '');
    }

    public function feedDisconnect(int $reasonCode = 0): self
    {
        return $this->feedPacket(PacketType::DISCONNECT, 0, $reasonCode === 0 ? '' : chr($reasonCode));
    }

    /** Make subsequent short reads fail with TransportError instead of Timeout. */
    public function closeByPeer(): self
    {
        $this->peerClosed = true;

        return $this;
    }

    // ------------------------------------------------------------- assertions

    /** Everything the client has written so far. */
    public function written(): string
    {
        return $this->writeBuffer;
    }

    /** Everything the client has written so far, clearing the buffer. */
    public function takeWritten(): string
    {
        $out               = $this->writeBuffer;
        $this->writeBuffer = '';

        return $out;
    }

    /**
     * Parse everything written by the client into discrete packets.
     *
     * A trailing partial packet is ignored, so this is safe to call mid-exchange.
     *
     * @return list<array{type: int, flags: int, body: string}>
     */
    public function sentPackets(): array
    {
        $out    = [];
        $data   = $this->writeBuffer;
        $offset = 0;
        $len    = strlen($data);

        while ($offset < $len) {
            $header = ord($data[$offset]);
            // The Remaining Length varint may itself be incomplete at the tail. Decoding
            // it would throw, so treat a short or malformed tail as a partial packet.
            $consumed = 0;
            try {
                $remaining = Bytes::decodeVarInt(substr($data, $offset + 1, 4), $consumed);
            } catch (ProtocolError) {
                break;
            }
            $bodyStart = $offset + 1 + $consumed;
            if ($bodyStart + $remaining > $len) {
                break; // partial packet still in flight
            }
            $out[] = [
                'type'  => $header >> 4,
                'flags' => $header & 0x0F,
                'body'  => substr($data, $bodyStart, $remaining),
            ];
            $offset = $bodyStart + $remaining;
        }

        return $out;
    }

    /**
     * Packet type values written by the client, in order.
     *
     * @return list<int>
     */
    public function sentPacketTypes(): array
    {
        $types = [];
        foreach ($this->sentPackets() as $packet) {
            $types[] = $packet['type'];
        }

        return $types;
    }

    public function countSent(PacketType $type): int
    {
        return count(array_filter($this->sentPackets(), static fn (array $p): bool => $p['type'] === $type->value));
    }

    /** Bytes fed but not yet consumed by the client. */
    public function pendingBytes(): int
    {
        return strlen($this->readBuffer);
    }

    public function isTlsEnabled(): bool
    {
        return $this->tlsEnabled;
    }

    /** @return array<string, mixed>|null */
    public function tlsOptions(): ?array
    {
        return $this->tlsOptions;
    }

    public function connectedHost(): ?string
    {
        return $this->host;
    }

    public function connectedPort(): ?int
    {
        return $this->port;
    }
}
