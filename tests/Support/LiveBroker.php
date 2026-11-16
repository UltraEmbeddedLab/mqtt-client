<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Tests\Support;

use Random\RandomException;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\InboundMessage;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Util\Clock;

use function bin2hex;
use function fclose;
use function getenv;
use function random_bytes;
use function stream_socket_client;

/**
 * The broker the Interop suite runs against, from the environment:
 *
 * - MQTT_BROKER  mosquitto (default), emqx or hivemq — what to expect where brokers differ
 * - MQTT_HOST, MQTT_PORT  where it listens (127.0.0.1:1883 by default)
 * - MQTT_WS_PORT, MQTT_WS_PATH  its WebSocket listener, when the test should use one
 *
 * docker-compose.yml starts each of them; see its comment for the ports.
 */
final readonly class LiveBroker
{
    private function __construct(
        public string $name,
        public string $host,
        public int $port,
        public ?int $wsPort,
        public string $wsPath,
    ) {
    }

    /** The configured broker, or null when nothing listens there. */
    public static function fromEnvironment(): ?self
    {
        $host   = getenv('MQTT_HOST') ?: '127.0.0.1';
        $port   = (int) (getenv('MQTT_PORT') ?: 1883);
        $wsPort = getenv('MQTT_WS_PORT');

        $socket = @stream_socket_client("tcp://$host:$port", $errno, $errstr, 1.0);
        if ($socket === false) {
            return null;
        }
        fclose($socket);

        return new self(
            getenv('MQTT_BROKER') ?: 'mosquitto',
            $host,
            $port,
            $wsPort !== false && $wsPort !== '' ? (int) $wsPort : null,
            getenv('MQTT_WS_PATH') ?: '/mqtt',
        );
    }

    /**
     * Options for a client of its own, under a Client Identifier no other test uses.
     *
     * @param  int|null  $port  another listener of the same broker, such as its WebSocket one
     *
     * @throws RandomException
     */
    public function options(string $role, MqttVersion $version = MqttVersion::V5_0, ?int $port = null): Options
    {
        return new Options(host: $this->host, port: $port ?? $this->port, clientId: $role.'-'.bin2hex(random_bytes(4)), version: $version);
    }

    /**
     * A topic no other test publishes to.
     *
     * @throws RandomException
     */
    public function topic(string $prefix): string
    {
        return 'interop/'.$prefix.'/'.bin2hex(random_bytes(4));
    }

    /**
     * Everything $client receives for $seconds.
     *
     * @return list<InboundMessage>
     */
    public function drain(Client $client, float $seconds = 1.0): array
    {
        $messages = [];
        $until    = Clock::now() + $seconds;
        while (Clock::now() < $until) {
            $message = $client->awaitMessage(0.1);
            if ($message instanceof InboundMessage) {
                $messages[] = $message;
            }
        }

        return $messages;
    }
}
