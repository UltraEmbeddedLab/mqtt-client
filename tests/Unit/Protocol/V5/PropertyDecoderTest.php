<?php

declare(strict_types=1);

use ScienceStories\Mqtt\Exception\ProtocolError;
use ScienceStories\Mqtt\Protocol\V5\Decoder;
use ScienceStories\Mqtt\Protocol\V5\PropertyDecoder;
use ScienceStories\Mqtt\Util\Bytes;

/** A length-prefixed property block, as it appears in a packet. */
function propertyBlock(string $properties): string
{
    return Bytes::encodeVarInt(strlen($properties)).$properties;
}

describe('the MQTT 5 property decoder', function (): void {
    it('decodes every property the specification defines', function (): void {
        $s     = static fn (string $v): string => Bytes::encodeString($v);
        $block = "\x01\x01"                         // payload_format_indicator
            ."\x02\x00\x00\x0E\x10"                 // message_expiry_interval 3600
            ."\x03".$s('text/plain')
            ."\x08".$s('reply/here')
            ."\x09".$s("\x00\xFF")                  // correlation_data, binary
            ."\x0B\xAC\x02"                         // subscription identifier 300
            ."\x11\x00\x00\x00\x3C"                 // session_expiry_interval 60
            ."\x12".$s('assigned')
            ."\x13\x00\x1E"                         // server_keep_alive 30
            ."\x15".$s('SCRAM-SHA-1')
            ."\x16".$s("\x01\x02")
            ."\x17\x01"
            ."\x18\x00\x00\x00\x05"                 // will_delay_interval 5
            ."\x19\x01"
            ."\x1A".$s('info')
            ."\x1C".$s('other.broker')
            ."\x1F".$s('why')
            ."\x21\x00\x0A"                         // receive_maximum 10
            ."\x22\x00\x05"                         // topic_alias_maximum 5
            ."\x23\x00\x02"                         // topic_alias 2
            ."\x24\x01"
            ."\x25\x00"
            ."\x26".$s('k').$s('v')
            ."\x27\x00\x01\x00\x00"                 // maximum_packet_size 65536
            ."\x28\x01"
            ."\x29\x00"
            ."\x2A\x01";

        expect(PropertyDecoder::decode($block))->toBe([
            'payload_format_indicator'          => 1,
            'message_expiry_interval'           => 3600,
            'content_type'                      => 'text/plain',
            'response_topic'                    => 'reply/here',
            'correlation_data'                  => "\x00\xFF",
            'session_expiry_interval'           => 60,
            'assigned_client_identifier'        => 'assigned',
            'server_keep_alive'                 => 30,
            'authentication_method'             => 'SCRAM-SHA-1',
            'authentication_data'               => "\x01\x02",
            'request_problem_information'       => 1,
            'will_delay_interval'               => 5,
            'request_response_information'      => 1,
            'response_information'              => 'info',
            'server_reference'                  => 'other.broker',
            'reason_string'                     => 'why',
            'receive_maximum'                   => 10,
            'topic_alias_maximum'               => 5,
            'topic_alias'                       => 2,
            'maximum_qos'                       => 1,
            'retain_available'                  => 0,
            'maximum_packet_size'               => 65536,
            'wildcard_subscription_available'   => 1,
            'subscription_identifier_available' => 0,
            'shared_subscription_available'     => 1,
            'user_properties'                   => ['k' => 'v'],
            'subscription_identifiers'          => [300],
        ]);
    });

    it('collects every Subscription Identifier a PUBLISH carries (MQTT-3.3.4-3)', function (): void {
        expect(PropertyDecoder::decode("\x0B\x01\x0B\xAC\x02"))->toBe(['subscription_identifiers' => [1, 300]]);
    });

    it('keeps what came before an identifier the specification does not define', function (): void {
        // Its length is unknown, so nothing after it can be read.
        expect(PropertyDecoder::decode("\x1F".Bytes::encodeString('why')."\x7F\x00\x01"))->toBe(['reason_string' => 'why']);
    });

    it('stops at a fixed-size value the block cuts short', function (): void {
        expect(PropertyDecoder::decode("\x1F".Bytes::encodeString('a')."\x27\x00\x01"))->toBe(['reason_string' => 'a']);
    });

    it('rejects a string the block cuts short', function (): void {
        expect(fn (): array => PropertyDecoder::decode("\x1F\x00\x05ab"))->toThrow(ProtocolError::class);
    });
});

describe('properties a packet type did not expect', function (): void {
    it('no longer cost a PUBLISH the User Properties after a Subscription Identifier', function (): void {
        $properties = "\x0B\x05\x26".Bytes::encodeString('trace').Bytes::encodeString('abc');
        $message    = new Decoder()->decodePublish(0x00, Bytes::encodeString('t').propertyBlock($properties).'payload');

        expect($message->properties)->toBe(['user_properties' => ['trace' => 'abc'], 'subscription_identifiers' => [5]])
            ->and($message->payload)->toBe('payload');
    });

    it('no longer cost a CONNACK the limits after an Authentication Method', function (): void {
        // Receive Maximum was dropped, so the client assumed 65,535 and could overrun the
        // quota the broker had set.
        $properties = "\x15".Bytes::encodeString('SCRAM-SHA-1')."\x21\x00\x0A\x24\x01";
        $connAck    = new Decoder()->decodeConnAck("\x00\x00".propertyBlock($properties));

        expect($connAck->properties)->toBe([
            'authentication_method' => 'SCRAM-SHA-1',
            'receive_maximum'       => 10,
            'maximum_qos'           => 1,
        ]);
    });
});
