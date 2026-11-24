import 'dart:convert';
import 'dart:typed_data';

/// The tag of a control frame.
const controlTag = 0x00;

/// The tag of a user data frame.
const userTag = 0x01;

/// Encode a frame of [tag] carrying [payload].
Uint8List encodeFrame(int tag, List<int> payload) {
  final frame = Uint8List(5 + payload.length);

  ByteData.sublistView(frame).setUint32(0, payload.length + 1, Endian.little);

  frame[4] = tag;
  frame.setAll(5, payload);

  return frame;
}

/// Encode a control frame of [type] with integer [fields], in order.
Uint8List encodeControl(String type, [Map<String, int> fields = const {}]) {
  return encodeFrame(
    controlTag,
    utf8.encode(jsonEncode({'type': type, ...fields})),
  );
}

/// Decode the JSON object of a control frame, or return `null` if it isn't
/// one.
Map<String, Object?>? decodeControl(List<int> payload) {
  try {
    final message = jsonDecode(utf8.decode(payload));

    return message is Map<String, Object?> ? message : null;
  } on FormatException {
    return null;
  }
}

/// Decodes frames from a stream of bytes.
final class FrameDecoder {
  var _buffer = Uint8List(0);

  /// Feed [data] to the decoder and return the frames it completes, as their
  /// tag and payload.
  List<(int, Uint8List)> feed(List<int> data) {
    final buffer = Uint8List(_buffer.length + data.length)
      ..setAll(0, _buffer)
      ..setAll(_buffer.length, data);

    final view = ByteData.sublistView(buffer);
    final frames = <(int, Uint8List)>[];

    var offset = 0;

    while (buffer.length - offset >= 4) {
      final length = view.getUint32(offset, Endian.little);

      final start = offset + 4;
      final end = start + length;

      if (buffer.length < end) break;

      if (length > 0) {
        frames.add((buffer[start], buffer.sublist(start + 1, end)));
      }

      offset = end;
    }

    _buffer = buffer.sublist(offset);

    return frames;
  }
}
