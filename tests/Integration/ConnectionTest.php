<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Tests\Integration;

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\PublishOptions;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Transport\TcpTransport;

beforeEach(function (): void {
    $host = getenv('MQTT_HOST') ?: '127.0.0.1';
    $port = (int) (getenv('MQTT_PORT') ?: 1883);

    // Skip if broker not available
    $socket = @fsockopen($host, $port, $errno, $errstr, 1);
    if ($socket === false) {
        $this->markTestSkipped("MQTT broker not available at {$host}:{$port}");
    }
    fclose($socket);

    $this->host = $host;
    $this->port = $port;
});

test('connects and disconnects with MQTT 3.1.1', function (): void {
    $options = new Options(
        host: $this->host,
        port: $this->port,
        clientId: 'php-iot-test-v3-' . bin2hex(random_bytes(4)),
        version: MqttVersion::V3_1_1,
    );

    $client = new Client($options, new TcpTransport());
    $result = $client->connect();

    expect($result->reasonCode)->toBe(0);
    expect($result->protocol)->toBe('MQTT');

    $client->disconnect();
});

test('connects and disconnects with MQTT 5.0', function (): void {
    $options = new Options(
        host: $this->host,
        port: $this->port,
        clientId: 'php-iot-test-v5-' . bin2hex(random_bytes(4)),
        version: MqttVersion::V5_0,
    );

    $client = new Client($options, new TcpTransport());
    $result = $client->connect();

    expect($result->reasonCode)->toBe(0);

    $client->disconnect();
});

test('ping succeeds after connect', function (): void {
    $options = new Options(
        host: $this->host,
        port: $this->port,
        clientId: 'php-iot-test-ping-' . bin2hex(random_bytes(4)),
    );

    $client = new Client($options, new TcpTransport());
    $client->connect();

    $result = $client->ping(5.0);
    expect($result)->toBeTrue();

    $client->disconnect();
});

test('publish QoS 0 succeeds', function (): void {
    $options = new Options(
        host: $this->host,
        port: $this->port,
        clientId: 'php-iot-test-pub0-' . bin2hex(random_bytes(4)),
    );

    $client = new Client($options, new TcpTransport());
    $client->connect();

    $result = $client->publish('test/qos0', 'hello qos0', new PublishOptions(qos: QoS::AtMostOnce));
    expect($result)->toBe(0);

    $client->disconnect();
});

test('publish QoS 1 receives PUBACK', function (): void {
    $options = new Options(
        host: $this->host,
        port: $this->port,
        clientId: 'php-iot-test-pub1-' . bin2hex(random_bytes(4)),
    );

    $client = new Client($options, new TcpTransport());
    $client->connect();

    $packetId = $client->publish('test/qos1', 'hello qos1', new PublishOptions(qos: QoS::AtLeastOnce));
    expect($packetId)->toBeGreaterThan(0);

    $client->disconnect();
});

test('publish QoS 2 completes full handshake', function (): void {
    $options = new Options(
        host: $this->host,
        port: $this->port,
        clientId: 'php-iot-test-pub2-' . bin2hex(random_bytes(4)),
    );

    $client = new Client($options, new TcpTransport());
    $client->connect();

    $packetId = $client->publish('test/qos2', 'hello qos2', new PublishOptions(qos: QoS::ExactlyOnce));
    expect($packetId)->toBeGreaterThan(0);

    $client->disconnect();
});

test('subscribe and receive message', function (): void {
    $topic = 'test/integration/' . bin2hex(random_bytes(4));

    // Publisher
    $pubOptions = new Options(
        host: $this->host,
        port: $this->port,
        clientId: 'php-iot-test-pub-' . bin2hex(random_bytes(4)),
    );
    $publisher = new Client($pubOptions, new TcpTransport());
    $publisher->connect();

    // Subscriber
    $subOptions = new Options(
        host: $this->host,
        port: $this->port,
        clientId: 'php-iot-test-sub-' . bin2hex(random_bytes(4)),
    );
    $subscriber = new Client($subOptions, new TcpTransport());
    $subscriber->connect();
    $subscriber->subscribe([$topic], 0);

    // Give broker time to process subscription
    usleep(100_000);

    // Publish
    $publisher->publish($topic, 'integration test payload');

    // Receive
    $msg = $subscriber->awaitMessage(5.0);
    expect($msg)->not->toBeNull();
    expect($msg->topic)->toBe($topic);
    expect($msg->payload)->toBe('integration test payload');

    $subscriber->disconnect();
    $publisher->disconnect();
});

test('unsubscribe stops message delivery', function (): void {
    $topic = 'test/unsub/' . bin2hex(random_bytes(4));

    $publisher = new Client(
        new Options(
            host: $this->host,
            port: $this->port,
            clientId: 'php-iot-test-unsub-pub-' . bin2hex(random_bytes(4)),
        ),
        new TcpTransport(),
    );
    $publisher->connect();

    $subscriber = new Client(
        new Options(
            host: $this->host,
            port: $this->port,
            clientId: 'php-iot-test-unsub-sub-' . bin2hex(random_bytes(4)),
        ),
        new TcpTransport(),
    );
    $subscriber->connect();
    $subscriber->subscribe([$topic], 0);
    usleep(100_000);

    // While subscribed, the message arrives.
    $publisher->publish($topic, 'before-unsubscribe');
    $before = $subscriber->awaitMessage(5.0);
    expect($before)->not->toBeNull();
    expect($before->payload)->toBe('before-unsubscribe');

    // After UNSUBACK, the same topic must go quiet.
    $subscriber->unsubscribe([$topic]);
    usleep(100_000);
    $publisher->publish($topic, 'after-unsubscribe');

    expect($subscriber->awaitMessage(1.0))->toBeNull();

    $subscriber->disconnect();
    $publisher->disconnect();
});

test('MQTT 5.0 publish with properties', function (): void {
    $options = new Options(
        host: $this->host,
        port: $this->port,
        clientId: 'php-iot-test-v5props-' . bin2hex(random_bytes(4)),
        version: MqttVersion::V5_0,
    );

    $client = new Client($options, new TcpTransport());
    $client->connect();

    $packetId = $client->publish('test/v5/props', '{"temp":22.5}', new PublishOptions(
        qos: QoS::AtLeastOnce,
        properties: [
            'payload_format_indicator' => 1,
            'content_type'             => 'application/json',
            'message_expiry_interval'  => 3600,
        ],
    ));

    expect($packetId)->toBeGreaterThan(0);

    $client->disconnect();
});

test('multiple sequential publishes are all delivered', function (): void {
    $prefix = 'test/multi/' . bin2hex(random_bytes(4));

    $publisher = new Client(
        new Options(
            host: $this->host,
            port: $this->port,
            clientId: 'php-iot-test-multi-pub-' . bin2hex(random_bytes(4)),
        ),
        new TcpTransport(),
    );
    $publisher->connect();

    $subscriber = new Client(
        new Options(
            host: $this->host,
            port: $this->port,
            clientId: 'php-iot-test-multi-sub-' . bin2hex(random_bytes(4)),
        ),
        new TcpTransport(),
    );
    $subscriber->connect();
    $subscriber->subscribe(["{$prefix}/#"], 1);
    usleep(100_000);

    // QoS 1 in both directions so the assertion is about delivery, not timing.
    for ($i = 0; $i < 10; $i++) {
        $publisher->publish("{$prefix}/{$i}", "message {$i}", new PublishOptions(qos: QoS::AtLeastOnce));
    }

    $received = [];
    $deadline = microtime(true) + 10.0;
    while (count($received) < 10 && microtime(true) < $deadline) {
        $msg = $subscriber->awaitMessage(0.5);
        if ($msg instanceof \ScienceStories\Mqtt\Client\InboundMessage) {
            $received[] = $msg->payload;
        }
    }

    expect($received)->toHaveCount(10);
    sort($received);
    expect($received)->toContain('message 0')->toContain('message 9');

    $subscriber->disconnect();
    $publisher->disconnect();
});
