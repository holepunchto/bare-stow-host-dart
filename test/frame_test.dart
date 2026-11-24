import 'dart:convert';

import 'package:bare_stow_host/bare_stow_host.dart';
import 'package:test/test.dart';

List<int> hex(String input) => [
  for (var i = 0; i < input.length; i += 2)
    int.parse(input.substring(i, i + 2), radix: 16),
];

List<int> user(String data) => encodeFrame(userTag, utf8.encode(data));

// Records compare their fields by identity, so the frames are compared as
// strings.
List<String> decode(FrameDecoder decoder, List<List<int>> chunks) => [
  for (final chunk in chunks)
    for (final (tag, payload) in decoder.feed(chunk))
      '$tag:${utf8.decode(payload)}',
];

void main() {
  // Written by the JavaScript bare-stow-protocol for the same frames.
  test('encodes frames like the JavaScript implementation', () {
    expect(
      encodeControl('configure', {'grace': 5000}),
      hex(
        '22000000007b2274797065223a22636f6e666967757265222c226772616365223a353030307d',
      ),
    );
    expect(
      encodeControl('ready'),
      hex('11000000007b2274797065223a227265616479227d'),
    );
    expect(user('hello'), hex('060000000168656c6c6f'));
  });

  test('decodes frames split across chunks', () {
    final data = [...user('hello'), ...encodeControl('ready')];

    expect(
      decode(FrameDecoder(), [
        for (final byte in data) [byte],
      ]),
      ['1:hello', '0:{"type":"ready"}'],
    );
  });

  test('decodes several frames from one chunk', () {
    final data = [...user('a'), ...user(''), ...user('b')];

    expect(decode(FrameDecoder(), [data]), ['1:a', '1:', '1:b']);
  });

  test('skips empty frames', () {
    expect(
      decode(FrameDecoder(), [
        [0, 0, 0, 0, ...user('a')],
      ]),
      ['1:a'],
    );
  });

  test('ignores invalid control payloads', () {
    expect(decodeControl(utf8.encode('not json')), null);
    expect(decodeControl(utf8.encode('[1, 2]')), null);
    expect(decodeControl(utf8.encode('{"type":"ready"}')), {'type': 'ready'});
  });
}
