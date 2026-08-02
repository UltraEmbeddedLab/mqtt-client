<?php

declare(strict_types=1);

namespace ScienceStories\Mqtt\Client;

use Random\RandomException;
use ScienceStories\Mqtt\Contract\ClientInterface;
use ScienceStories\Mqtt\Exception\Timeout;
use ScienceStories\Mqtt\Protocol\QoS;
use ScienceStories\Mqtt\Util\Clock;

use function bin2hex;
use function random_bytes;

/**
 * MQTT 5.0 Request/Response pattern helper.
 *
 * Simplifies the request/response pattern by managing response topics,
 * correlation data, and response matching automatically.
 *
 * Usage:
 * ```php
 * $rr = new RequestResponse($client, 'my/response/topic');
 * $response = $rr->request('service/endpoint', '{"action":"status"}', timeoutSec: 5.0);
 * echo $response->payload;
 * ```
 */
final class RequestResponse
{
    private bool $subscribed = false;

    public function __construct(
        private readonly ClientInterface $client,
        private readonly string $responseTopic,
        private readonly QoS $qos = QoS::AtLeastOnce,
    ) {
    }

    /**
     * Send a request and wait for a correlated response.
     *
     * @param string $topic The request topic
     * @param string $payload The request payload
     * @param float $timeoutSec Maximum time to wait for response
     * @param array<string, mixed>|null $properties Additional MQTT 5.0 properties
     *
     * @throws Timeout if no response received within timeout
     * @throws RandomException
     */
    public function request(
        string $topic,
        string $payload,
        float $timeoutSec = 5.0,
        ?array $properties = null,
    ): InboundMessage {
        // Ensure we're subscribed to the response topic
        if (!$this->subscribed) {
            $this->client->subscribe([$this->responseTopic], $this->qos->value);
            $this->subscribed = true;
        }

        // Generate unique correlation data
        $correlationData = bin2hex(random_bytes(8));

        // Build publish properties with response topic and correlation data
        $publishProps                     = $properties ?? [];
        $publishProps['response_topic']   = $this->responseTopic;
        $publishProps['correlation_data'] = $correlationData;

        // Publish the request
        $this->client->publish($topic, $payload, new PublishOptions(
            qos: $this->qos,
            properties: $publishProps,
        ));

        // Wait for correlated response
        $deadline = Clock::now() + $timeoutSec;
        while (true) {
            $remaining = $deadline - Clock::now();
            if ($remaining <= 0) {
                throw new Timeout("Request/Response timed out waiting for response on '$this->responseTopic' with correlation '$correlationData'");
            }

            $msg = $this->client->awaitMessage($remaining);
            if (!$msg instanceof InboundMessage) {
                continue;
            }

            // Check if this message matches our correlation data
            $msgCorrelation = $msg->properties['correlation_data'] ?? null;
            if ($msg->topic === $this->responseTopic && $msgCorrelation === $correlationData) {
                return $msg;
            }
        }
    }

    /**
     * Clean up by unsubscribing from the response topic.
     */
    public function close(): void
    {
        if ($this->subscribed) {
            $this->client->unsubscribe([$this->responseTopic]);
            $this->subscribed = false;
        }
    }
}
