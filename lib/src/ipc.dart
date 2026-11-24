import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'frame.dart';

/// The default time a terminating worker gets to stop and exit.
const defaultGrace = Duration(seconds: 5);

/// A fault reported by or observed in the worker.
final class WorkerException implements Exception {
  const WorkerException(
    this.message, {
    required this.code,
    this.stack,
    this.exitCode,
    this.signal,
    this.cause,
  });

  /// The message of the error.
  final String message;

  /// The kind of the error: `WORKER_ERROR` for a fault the worker reported,
  /// `WORKER_EXITED` with [exitCode] for a worker that exited, `WORKER_KILLED`
  /// with [signal] for a worker killed by a signal, `WORKER_CLOSED` for a
  /// transport that closed before ready or a write to a closed transport,
  /// `WORKER_TIMEOUT` for a worker that timed out before ready, and
  /// `TRANSPORT_ERROR` for a transport that failed while reading or writing.
  /// A transport that is reset or broken by the worker going away counts as
  /// closing rather than failing.
  final String code;

  /// The stack of the error reported by the worker, if any.
  final String? stack;

  /// The exit code of a worker that exited.
  final int? exitCode;

  /// The signal that killed a worker that exited without saying why.
  final int? signal;

  /// The error that caused this one. For a `TRANSPORT_ERROR`, it is the error
  /// of the transport. For the exit of a worker whose transport failed first,
  /// it is that `TRANSPORT_ERROR`.
  final Object? cause;

  @override
  String toString() => 'WorkerException: $message';
}

/// The host side of a stowed bundle, layering readiness, termination, and exit
/// reporting over a transport.
///
/// Listening yields the user data written by the worker until the transport
/// ends, followed by a [WorkerException] if the worker has reported a fault or
/// exited without saying why. Adding data sends it to the worker as user data,
/// and closing terminates the worker.
///
/// Whoever creates it must call [exited] once the worker has exited. The end
/// of a transport that ends without the worker saying why is held back until
/// then, so that the handle fails with the reason. A transport that fails
/// instead waits for the exit for the grace period, failing with the exit and
/// the failure as its `cause`, or calling `kill` and failing with
/// `TRANSPORT_ERROR` if the worker hasn't exited by then.
final class Ipc extends Stream<Uint8List> implements StreamSink<List<int>> {
  /// Wrap the host side of a stowed bundle's transport, reading from [stream]
  /// and writing to [sink], and send the [grace] to the worker.
  ///
  /// [kill] stops a worker that ignores `terminate` for [grace]. [timeout] is
  /// how long to wait for the worker to signal ready before failing [ready]
  /// and terminating it, or `null` to wait indefinitely.
  Ipc(
    Stream<List<int>> stream,
    StreamSink<List<int>> sink, {
    required void Function() kill,
    Duration? timeout,
    Duration grace = defaultGrace,
  }) : _sink = sink,
       _kill = kill,
       _grace = grace {
    _ready.future.ignore();

    _send(encodeControl('configure', {'grace': grace.inMilliseconds}));

    stream.listen(_onData, onError: _onTransportError, onDone: _onDone);

    sink.done.then<void>((_) {}, onError: _onTransportError);

    if (timeout != null) {
      _timer = Timer(timeout, () {
        _settleReady(
          const WorkerException(
            'Worker timed out before ready',
            code: 'WORKER_TIMEOUT',
          ),
        );
      });
    }
  }

  final StreamSink<List<int>> _sink;
  final void Function() _kill;
  final Duration _grace;

  final _controller = StreamController<Uint8List>();
  final _decoder = FrameDecoder();
  final _ready = Completer<void>();
  final _closed = Completer<void>();

  Timer? _timer;
  int? _exitCode;
  var _exitReported = false;
  var _ended = false;
  var _terminateSent = false;
  WorkerException? _failure;
  WorkerException? _transportFailure;
  Timer? _failing;
  Future<void>? _terminating;

  /// Completes once the worker has signaled ready.
  ///
  /// It fails with a [WorkerException] if the worker errors, exits, closes, or
  /// times out before then, in which case the worker is terminated.
  Future<void> get ready => _ready.future;

  /// The exit code the worker reported, if any.
  int? get exitCode => _exitCode;

  /// Wait for the transport to end and return the exit code the worker
  /// reported, if any.
  Future<int?> wait() async {
    await _closed.future;

    return _exitCode;
  }

  /// Ask the worker to terminate and return the exit code it reported, if any.
  ///
  /// The transport is waited for to end, calling `kill` if the worker hasn't
  /// exited within the grace period.
  Future<int?> terminate() async {
    await close();

    return _exitCode;
  }

  /// Like [terminate], without the exit code.
  @override
  Future<void> close() => _terminating ??= _terminate();

  /// Completes once the transport has ended.
  @override
  Future<void> get done => _closed.future;

  /// Report that the worker has exited, as observed by the transport, such as
  /// the process of a sidecar.
  ///
  /// If the worker exited without saying why and was not asked to, the handle
  /// fails with a `WORKER_EXITED` or `WORKER_KILLED` [WorkerException]
  /// carrying `exitCode` and `signal`.
  void exited(int? code, {int? signal}) {
    if (_exitReported || _terminateSent || _failure != null) return;

    _failing?.cancel();

    _fail(
      signal != null
          ? WorkerException(
              'Worker was killed by signal $signal',
              code: 'WORKER_KILLED',
              exitCode: code,
              signal: signal,
              cause: _transportFailure,
            )
          : WorkerException(
              'Worker exited with code ${code ?? -1}',
              code: 'WORKER_EXITED',
              exitCode: code,
              cause: _transportFailure,
            ),
    );
  }

  @override
  StreamSubscription<Uint8List> listen(
    void Function(Uint8List data)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    return _controller.stream.listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }

  /// Send [data] to the worker as user data.
  ///
  /// It throws a `WORKER_CLOSED` [WorkerException] if the transport has closed,
  /// or a `TRANSPORT_ERROR` one if it failed.
  @override
  void add(List<int> data) {
    final error = _send(encodeFrame(userTag, data));

    if (error != null) throw error;
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {
    throw UnsupportedError('Cannot send errors to a worker');
  }

  @override
  Future<void> addStream(Stream<List<int>> stream) async {
    await for (final data in stream) {
      add(data);
    }
  }

  WorkerException? _send(List<int> frame) {
    try {
      _sink.add(frame);
    } on StateError catch (error) {
      // A sink that has closed is the transport ending rather than failing, so
      // control frames written to it are dropped.
      return _closedError(error);
    } catch (error) {
      return _onTransportError(error);
    }

    return null;
  }

  WorkerException _onTransportError(Object error) {
    if (_isPeerGone(error)) _onDone();

    if (_ended || _exitReported || _terminateSent) return _closedError(error);

    final failure = WorkerException(
      '$error',
      code: 'TRANSPORT_ERROR',
      cause: error,
    );

    if (_mayHaveCrashed && _transportFailure == null) {
      _timer?.cancel();
      _transportFailure = failure;
      _failing = Timer(_grace, () {
        _kill();
        _fail(failure);
      });
    }

    return failure;
  }

  WorkerException _closedError(Object cause) {
    return WorkerException(
      'Worker closed',
      code: 'WORKER_CLOSED',
      cause: cause,
    );
  }

  bool get _mayHaveCrashed =>
      !_exitReported && !_terminateSent && _failure == null;

  void _onData(List<int> chunk) {
    for (final (tag, payload) in _decoder.feed(chunk)) {
      _onFrame(tag, payload);
    }
  }

  void _onFrame(int tag, Uint8List payload) {
    if (tag == userTag) {
      if (_failure == null && !_controller.isClosed && payload.isNotEmpty) {
        _controller.add(payload);
      }
      return;
    }

    if (tag != controlTag) return;

    final message = decodeControl(payload);

    if (message == null) return;

    switch (message['type']) {
      case 'ready':
        _settleReady();
      case 'exit':
        final code = message['code'];

        _exitReported = true;
        _exitCode = code is int ? code : null;

        // An error frame precedes the exit frame of a worker that faulted.
        if (_failure == null) {
          _settleReady(
            WorkerException(
              'Worker exited with code ${_exitCode ?? -1} before ready',
              code: 'WORKER_EXITED',
              exitCode: _exitCode,
            ),
          );
        }
      case 'error':
        final text = message['message'];
        final stack = message['stack'];

        _fail(
          WorkerException(
            text is String ? text : '',
            code: 'WORKER_ERROR',
            stack: stack is String ? stack : null,
          ),
        );
    }
  }

  void _onDone() {
    if (_ended) return;

    _ended = true;

    // A worker whose transport ends without it saying why has crashed, so the
    // end is held back until its exit is reported for readers to fail with.
    if (_mayHaveCrashed) {
      _timer?.cancel();
      return;
    }

    _end();
  }

  void _end() {
    if (!_closed.isCompleted) _closed.complete();

    _settleReady(
      const WorkerException(
        'Worker closed before ready',
        code: 'WORKER_CLOSED',
      ),
    );

    if (!_controller.isClosed) _controller.close();
  }

  void _fail(WorkerException error) {
    if (_failure != null) return;

    _failure = error;

    _settleReady(error);

    if (!_controller.isClosed) {
      _controller.addError(error);
      _controller.close();
    }

    unawaited(close());

    if (_ended) _end();
  }

  void _settleReady([WorkerException? error]) {
    if (_ready.isCompleted) return;

    _timer?.cancel();

    if (error == null) {
      _ready.complete();
    } else {
      _ready.completeError(error);

      unawaited(close());
    }
  }

  Future<void> _terminate() async {
    if (_ended) return _closed.future;

    _terminateSent = true;

    final timer = Timer(_grace, _kill);

    _send(encodeControl('terminate'));

    await _closed.future;

    timer.cancel();
  }
}

// A peer that goes away resets or breaks the connection rather than ending it,
// which is no different from it closing.
bool _isPeerGone(Object error) {
  final osError = switch (error) {
    SocketException(:final osError) => osError,
    FileSystemException(:final osError) => osError,
    OSError() => error,
    _ => null,
  };

  return osError != null && _peerGoneCodes.contains(osError.errorCode);
}

// EPIPE and ECONNRESET, or ERROR_BROKEN_PIPE, ERROR_NO_DATA and WSAECONNRESET
// on Windows.
final _peerGoneCodes = Platform.isWindows
    ? const {109, 232, 10054}
    : Platform.isMacOS || Platform.isIOS
    ? const {32, 54}
    : const {32, 104};
