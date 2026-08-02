<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Easy;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Random\RandomException;
use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options as ClientOptions;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Client\TlsOptions;
use ScienceStories\Mqtt\Contract\ClientInterface;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Transport\TcpTransport;
use ScienceStories\Mqtt\Util\RandomId;

final class Mqtt
{
    /**
     * One-shot publish: connect, publish, disconnect.
     * Simplified API requiring only essential parameters.
     *
     * @param int|null $port MQTT port (defaults: 1883 for TCP, 8883 for TLS)
     * @param TlsOptions|array<string,mixed>|null $tlsOptions
     * @param array<string,mixed>|null $properties MQTT v5 publish properties
     * @throws RandomException
     */
    public static function publish(
        string $host,
        string $topic,
        string $payload,
        ?int $port = null,
        string $version = 'v3',
        bool $tls = false,
        ?string $username = null,
        ?string $password = null,
        ?QoS $qos = null,
        bool $retain = false,
        TlsOptions|array|null $tlsOptions = null,
        ?array $properties = null,
        ?string $clientId = null,
        int $keepAlive = 60,
        bool $cleanStart = true,
        ?LoggerInterface $logger = null,
    ): void {
        // Auto-detect port based on TLS setting
        if ($port === null) {
            $port = $tls ? 8883 : 1883;
        }

        $client = self::connect(
            host: $host,
            port: $port,
            version: $version,
            tls: $tls,
            username: $username,
            password: $password,
            tlsOptions: $tlsOptions,
            clientId: $clientId,
            keepAlive: $keepAlive,
            cleanStart: $cleanStart,
            logger: $logger,
        );

        try {
            $client->publish($topic, $payload, new PublishOptions(
                qos: $qos ?? QoS::AtMostOnce,
                retain: $retain,
                dup: false,
                properties: $properties,
            ));

            // For QoS 0, give the transport a moment to flush the write buffer
            // before disconnecting, ensuring the PUBLISH packet is transmitted
            if (($qos ?? QoS::AtMostOnce) === QoS::AtMostOnce) {
                usleep(50000); // 50ms
            }
        } finally {
            $client->disconnect();
        }
    }

    /**
     * One-shot send: connect, publish, disconnect.
     * Alias for publish() - kept for backward compatibility.
     *
     * @param TlsOptions|array<string,mixed>|null $tlsOptions
     * @param array<string,mixed>|null $properties MQTT v5 publish properties
     * @throws RandomException
     */
    public static function send(
        string $host,
        int $port,
        string $topic,
        string $payload,
        string $version = 'v3',
        bool $tls = false,
        ?string $username = null,
        ?string $password = null,
        ?QoS $qos = null,
        bool $retain = false,
        TlsOptions|array|null $tlsOptions = null,
        ?array $properties = null,
        ?string $clientId = null,
        int $keepAlive = 60,
        bool $cleanStart = true,
        ?LoggerInterface $logger = null,
    ): void {
        self::publish(
            host: $host,
            topic: $topic,
            payload: $payload,
            port: $port,
            version: $version,
            tls: $tls,
            username: $username,
            password: $password,
            qos: $qos,
            retain: $retain,
            tlsOptions: $tlsOptions,
            properties: $properties,
            clientId: $clientId,
            keepAlive: $keepAlive,
            cleanStart: $cleanStart,
            logger: $logger,
        );
    }

    /**
     * Connect and return a fully configured ClientInterface for longer sessions.
     *
     * @param TlsOptions|array<string,mixed>|null $tlsOptions
     * @throws RandomException
     */
    public static function connect(
        string $host,
        int $port,
        string $version = 'v3',
        bool $tls = false,
        ?string $username = null,
        ?string $password = null,
        TlsOptions|array|null $tlsOptions = null,
        ?string $clientId = null,
        int $keepAlive = 60,
        bool $cleanStart = true,
        ?LoggerInterface $logger = null,
    ): ClientInterface {
        $ver = $version === 'v5' ? MqttVersion::V5_0 : MqttVersion::V3_1_1;
        $id  = $clientId !== null && $clientId !== '' ? $clientId : ('php-iot-'.RandomId::clientId(8));

        $opts = new ClientOptions(
            host: $host,
            port: $port,
            version: $ver,
        );
        $opts = $opts
            ->withClientId($id)
            ->withKeepAlive($keepAlive)
            ->withCleanSession($cleanStart);

        if ($username !== null) {
            $opts = $opts->withUser($username, $password);
        }
        if ($tls) {
            $opts = $opts->withTls($tlsOptions ?? new TlsOptions());
        }

        $client = new Client($opts, new TcpTransport(), logger: $logger ?? new NullLogger());
        // Client::connect() throws a typed MqttException (AuthenticationError,
        // ServerError, ProtocolError) when the broker refuses. Those extend
        // RuntimeException, so existing catch blocks keep working.
        $client->connect();

        return $client;
    }
}
