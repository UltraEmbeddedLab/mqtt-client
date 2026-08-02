<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Easy\Mqtt;

require __DIR__.'/../vendor/autoload.php';

// Simple example using config.php for broker settings
// Automatically uses TLS, authentication, and correct port from .env
//
// To verify the message is actually sent:
// 1. Run: php examples/simple_subscribe.php (in another terminal)
// 2. Run: php examples/simple_publish.php (in this terminal)
// 3. You should see the message appear in the subscriber terminal
// Use examples/config.php when present (gitignored, for your own broker),
// otherwise fall back to the committed defaults so a fresh clone just runs.
$configFile = is_file(__DIR__.'/config.php') ? __DIR__.'/config.php' : __DIR__.'/config.php.dist';
$config     = require $configFile;

$host    = $config['host'];
$port    = $config['port'];
$tls     = ($config['scheme'] ?? 'tcp') === 'tls';
$topic   = 'php-iot/test';
$payload = 'Hello World!';

Mqtt::publish(
    host: $host,
    topic: $topic,
    payload: $payload,
    port: $port,
    tls: $tls,
    username: $config['username'] ?? null,
    password: $config['password'] ?? null,
    tlsOptions: $config['tls']    ?? null,
);

echo "✅ Message published to {$topic}\n";
