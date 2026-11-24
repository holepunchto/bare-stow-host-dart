import 'dart:async';
import 'dart:typed_data';

import 'package:async/async.dart';
import 'package:bare_stow_host/bare_stow_host.dart';

class End {
  final _incoming = StreamController<List<int>>();

  late final End peer;

  var closed = false;

  Stream<List<int>> get stream => _incoming.stream;

  late final StreamSink<List<int>> sink = _Sink(this);

  void close() {
    for (final end in [this, peer]) {
      if (!end.closed) {
        end.closed = true;
        end._incoming.close();
      }
    }
  }

  void fail(Object error) {
    _incoming.addError(error);

    close();
  }
}

class _Sink implements StreamSink<List<int>> {
  _Sink(this.end);

  final End end;

  @override
  void add(List<int> data) {
    if (end.closed) throw StateError('End is closed');

    end.peer._incoming.add(data);
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<void> addStream(Stream<List<int>> stream) => stream.forEach(add);

  @override
  Future<void> close() async => end.close();

  @override
  Future<void> get done => end._incoming.done;
}

class FailingSink implements StreamSink<List<int>> {
  FailingSink(this.sink);

  final StreamSink<List<int>> sink;

  Object? error;

  final _done = Completer<void>();

  void failDone(Object error) => _done.completeError(error);

  @override
  void add(List<int> data) {
    if (error case final error?) throw error;

    sink.add(data);
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<void> addStream(Stream<List<int>> stream) => stream.forEach(add);

  @override
  Future<void> close() => sink.close();

  @override
  Future<void> get done => _done.future;
}

(End, End) pair() {
  final a = End();
  final b = End();

  a.peer = b;
  b.peer = a;

  return (a, b);
}

Ipc wrap(End end, {Duration? timeout, Duration grace = defaultGrace}) {
  return Ipc(
    end.stream,
    end.sink,
    kill: end.close,
    timeout: timeout,
    grace: grace,
  );
}

class Worker {
  Worker(this.end)
    : _frames = StreamQueue(end.stream.expand(FrameDecoder().feed));

  final End end;

  final StreamQueue<(int, Uint8List)> _frames;

  void send(String type, [Map<String, int> fields = const {}]) {
    end.sink.add(encodeControl(type, fields));
  }

  void sendJson(String json) {
    end.sink.add(encodeFrame(controlTag, json.codeUnits));
  }

  void write(List<int> data) {
    end.sink.add(encodeFrame(userTag, data));
  }

  Future<Map<String, Object?>> receive(String type) async {
    while (true) {
      final (tag, payload) = await _frames.next;

      if (tag == controlTag) {
        final message = decodeControl(payload)!;

        if (message['type'] == type) return message;
      }
    }
  }

  Future<Uint8List> data() async {
    while (true) {
      final (tag, payload) = await _frames.next;

      if (tag == userTag) return payload;
    }
  }

  void close() => end.close();

  // Closes the transport once asked to terminate.
  Future<void> closeOnTerminate() async {
    await receive('terminate');

    close();
  }
}
