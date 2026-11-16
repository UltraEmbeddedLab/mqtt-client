<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Client\Client;
use ScienceStories\Mqtt\Client\Options;
use ScienceStories\Mqtt\Client\WillOptions;
use ScienceStories\Mqtt\Protocol\MqttVersion;
use ScienceStories\Mqtt\Protocol\Packet\Connect;
use ScienceStories\Mqtt\Protocol\V5\Encoder;
use ScienceStories\Mqtt\Protocol\V5\PropertyDecoder;
use ScienceStories\Mqtt\Testing\InMemoryTransport;
use ScienceStories\Mqtt\Util\Bytes;

/**
 * The CONNECT property block, length prefix included: it starts right after the ten fixed
 * bytes of the variable header (protocol name, level, flags, Keep Alive).
 */
function connectPropertyBlock(string $packet): string
{
    $consumed = 0;
    Bytes::decodeVarInt($packet, $consumed, 1);
    $start   = 1 + $consumed + 10;
    $lenSize = 0;
    $length  = Bytes::decodeVarInt($packet, $lenSize, $start);

    return substr($packet, $start, $lenSize + $length);
}

describe('MQTT 5 CONNECT properties', function (): void {
    it('encodes every one the CONNECT packet documents', function (): void {
        // Five of these were dropped without a word.
        $packet = new Encoder()->encodeConnect(new Connect('c', properties: [
            'session_expiry_interval'      => 60,
            'receive_maximum'              => 10,
            'topic_alias_maximum'          => 5,
            'maximum_packet_size'          => 1024,
            'request_response_information' => true,
            'request_problem_information'  => false,
            'user_properties'              => ['a' => 'b'],
            'authentication_method'        => 'SCRAM',
            'authentication_data'          => "\x01",
        ]));

        $properties = "\x11\x00\x00\x00\x3C"     // Session Expiry Interval 60
            ."\x21\x00\x0A"                       // Receive Maximum 10
            ."\x22\x00\x05"                       // Topic Alias Maximum 5
            ."\x27\x00\x00\x04\x00"               // Maximum Packet Size 1024
            ."\x19\x01"                           // Request Response Information
            ."\x17\x00"                           // Request Problem Information
            ."\x26\x00\x01a\x00\x01b"             // User Property a=b
            ."\x15\x00\x05SCRAM"                  // Authentication Method
            ."\x16\x00\x01\x01";                  // Authentication Data

        expect(connectPropertyBlock($packet))->toBe(Bytes::encodeVarInt(strlen($properties)).$properties);
    });

    it('sends the request flags as 0 or 1, the only values allowed', function (): void {
        $packet = new Encoder()->encodeConnect(new Connect('c', properties: ['request_problem_information' => 5]));

        expect(connectPropertyBlock($packet))->toBe("\x02\x17\x01");
    });

    it('leaves out Authentication Data without an Authentication Method', function (): void {
        // A Protocol Error the broker would answer by refusing the connection.
        $packet = new Encoder()->encodeConnect(new Connect('c', properties: ['authentication_data' => 'x']));

        expect(connectPropertyBlock($packet))->toBe("\x00");
    });
});

describe('an MQTT 5 Will', function (): void {
    it('carries its properties, the Will Delay Interval first', function (): void {
        // The block was always empty, so the Will went out the moment a connection dropped
        // whatever delay was set.
        $will = new WillOptions('w/t', 'gone')
            ->withProperties(['content_type' => 'text/plain', 'user_properties' => ['k' => 'v'], 'topic_alias' => 3])
            ->withDelayInterval(30);

        $packet = new Encoder()->encodeConnect(new Connect('c', will: $will));

        $willProperties = "\x18\x00\x00\x00\x1E"          // Will Delay Interval 30
            ."\x03\x00\x0Atext/plain"                      // Content Type
            ."\x26\x00\x01k\x00\x01v";                     // User Property; no Topic Alias
        expect($packet)->toEndWith(
            Bytes::encodeString('c')
            .Bytes::encodeVarInt(strlen($willProperties)).$willProperties
            .Bytes::encodeString('w/t').Bytes::encodeString('gone'),
        );
    });

    it('carries an empty property block when it has none', function (): void {
        $packet = new Encoder()->encodeConnect(new Connect('c', will: new WillOptions('w/t', 'gone')));

        expect($packet)->toEndWith(Bytes::encodeString('c')."\x00".Bytes::encodeString('w/t').Bytes::encodeString('gone'));
    });
});

it('WillOptions::withDelayInterval() sets the delay and keeps the other properties', function (): void {
    $will = new WillOptions('w/t', 'gone')->withProperties(['content_type' => 'x'])->withDelayInterval(-5);

    expect($will->properties)->toBe(['will_delay_interval' => 0, 'content_type' => 'x']);
});

it('Options carry User Properties and the request flags to the CONNECT the client sends', function (): void {
    $transport = new InMemoryTransport();
    $transport->feedConnAck(properties: "\x00");
    $options = new Options(host: 'fake.broker', clientId: 'c', version: MqttVersion::V5_0)
        ->withUserProperties(['fw' => '1.2'])
        ->withRequestResponseInformation()
        ->withRequestProblemInformation(false);
    new Client($options, $transport)->connect();

    // The bytes are pinned by the encoder tests above; this is about the options reaching it.
    $block    = connectPropertyBlock($transport->written());
    $consumed = 0;
    Bytes::decodeVarInt($block, $consumed);

    expect(PropertyDecoder::decode(substr($block, $consumed)))->toMatchArray([
        'request_response_information' => 1,
        'request_problem_information'  => 0,
        'user_properties'              => ['fw' => '1.2'],
    ]);
});
