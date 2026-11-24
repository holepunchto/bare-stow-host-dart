import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:async/async.dart';
import 'package:bare_stow_host/bare_stow_host.dart';
import 'package:test/test.dart';

import 'pipe.dart';

final _brokenPipe = SocketException(
  'Broken pipe',
  osError: OSError('Broken pipe', Platform.isWindows ? 109 : 32),
);

final _reset = SocketException(
  'Connection reset',
  osError: OSError(
    'Connection reset',
    Platform.isWindows
        ? 10054
        : Platform.isMacOS
        ? 54
        : 104,
  ),
);

Matcher throwsWorkerException(
  String code, {
  int? exitCode,
  int? signal,
  Object? cause,
}) => throwsA(
  isA<WorkerException>()
      .having((err) => err.code, 'code', code)
      .having((err) => err.exitCode, 'exitCode', exitCode)
      .having((err) => err.signal, 'signal', signal)
      .having((err) => err.cause, 'cause', cause),
);

void main() {
  test('resolves ready when the worker signals', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);
    final closing = worker.closeOnTerminate();

    worker.send('ready');

    await ipc.ready;
    await ipc.close();
    await closing;
  });

  test('sends the grace to the worker', () async {
    final (left, right) = pair();
    final ipc = wrap(left, grace: const Duration(milliseconds: 2500));
    final worker = Worker(right);

    expect(await worker.receive('configure'), {
      'type': 'configure',
      'grace': 2500,
    });

    final closing = worker.closeOnTerminate();

    await ipc.close();
    await closing;
  });

  test('rejects ready when the worker errors before ready', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);
    final closing = worker.closeOnTerminate();

    worker.sendJson('{"type":"error","message":"boom","stack":"at core.js"}');

    await expectLater(
      ipc.ready,
      throwsA(
        isA<WorkerException>()
            .having((err) => err.message, 'message', 'boom')
            .having((err) => err.code, 'code', 'WORKER_ERROR')
            .having((err) => err.stack, 'stack', 'at core.js'),
      ),
    );

    await closing;
  });

  test('rejects ready when the worker exits before ready', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);
    final closing = worker.closeOnTerminate();

    worker.send('exit', {'code': 1});

    await expectLater(
      ipc.ready,
      throwsWorkerException('WORKER_EXITED', exitCode: 1),
    );

    await closing;
  });

  test('rejects ready when the transport closes before ready', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final closing = Worker(right).closeOnTerminate();
    final closed = ipc.close();

    await expectLater(ipc.ready, throwsWorkerException('WORKER_CLOSED'));

    await closed;
    await closing;
  });

  test('ignores exit after ready', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);

    worker.send('ready');

    await ipc.ready;

    worker
      ..send('exit', {'code': 1})
      ..close();

    await ipc.wait();
    await ipc.ready;
  });

  test('reports the exit code', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);

    worker
      ..send('ready')
      ..send('exit', {'code': 7})
      ..close();

    expect(await ipc.wait(), 7);
    expect(ipc.exitCode, 7);
  });

  test('carries user data', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);
    final data = StreamQueue(ipc);

    worker.write(utf8.encode('payload'));

    expect(utf8.decode(await data.next), 'payload');

    ipc.add(utf8.encode('reply'));

    expect(utf8.decode(await worker.data()), 'reply');

    worker
      ..write([])
      ..send('exit', {'code': 0})
      ..close();

    expect(await data.hasNext, false);
  });

  test('terminate resolves to the exit code', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);

    worker.send('ready');

    await ipc.ready;

    unawaited(
      worker.receive('terminate').then((_) {
        worker
          ..send('exit', {'code': 0})
          ..close();
      }),
    );

    expect(await ipc.terminate(), 0);
  });

  test('close sends terminate to the worker', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final closing = Worker(right).closeOnTerminate();

    await ipc.close();
    await closing;
  });

  test('rejects ready and terminates the worker on timeout', () async {
    final (left, right) = pair();
    final ipc = wrap(left, timeout: const Duration(milliseconds: 10));
    final closing = Worker(right).closeOnTerminate();

    await expectLater(ipc.ready, throwsWorkerException('WORKER_TIMEOUT'));

    await closing;
  });

  test('terminate calls kill after the grace period', () async {
    final (left, right) = pair();
    var killed = false;

    final ipc = Ipc(
      left.stream,
      left.sink,
      kill: () {
        killed = true;
        left.close();
      },
      grace: const Duration(milliseconds: 10),
    );

    Worker(right).send('ready');

    await ipc.ready;

    expect(await ipc.terminate(), null);
    expect(killed, true);
  });

  test('terminate resolves once the transport has closed', () async {
    final (left, _) = pair();
    final ipc = wrap(left);

    left.close();

    ipc.exited(1);

    await expectLater(ipc.ready, throwsA(isA<WorkerException>()));

    expect(await ipc.terminate(), null);
  });

  test('exited before ready rejects ready with the exit code', () async {
    final (left, _) = pair();
    final ipc = wrap(left, grace: const Duration(milliseconds: 10));

    ipc.exited(3);

    await expectLater(
      ipc.ready,
      throwsWorkerException('WORKER_EXITED', exitCode: 3),
    );

    await ipc.close();
  });

  test('exited after ready fails the data with the signal', () async {
    final (left, right) = pair();
    final ipc = wrap(left, grace: const Duration(milliseconds: 10));

    Worker(right).send('ready');

    await ipc.ready;

    ipc.exited(137, signal: 9);

    await expectLater(
      ipc.toList(),
      throwsWorkerException('WORKER_KILLED', exitCode: 137, signal: 9),
    );

    await ipc.close();
  });

  test('fails with the error of a transport write', () async {
    final (left, right) = pair();
    final sink = FailingSink(left.sink);
    final error = Exception('broken pipe');
    final ipc = Ipc(
      left.stream,
      sink,
      kill: left.close,
      grace: const Duration(milliseconds: 10),
    );

    Worker(right).send('ready');

    await ipc.ready;

    sink.error = error;

    expect(
      () => ipc.add(utf8.encode('payload')),
      throwsA(
        isA<WorkerException>()
            .having((err) => err.code, 'code', 'TRANSPORT_ERROR')
            .having((err) => err.cause, 'cause', same(error)),
      ),
    );

    await expectLater(
      ipc.toList(),
      throwsA(
        isA<WorkerException>()
            .having((err) => err.code, 'code', 'TRANSPORT_ERROR')
            .having((err) => err.cause, 'cause', same(error)),
      ),
    );

    await ipc.close();
  });

  test('fails ready with the error of a control frame write', () async {
    final (left, _) = pair();
    final sink = FailingSink(left.sink)..error = Exception('broken pipe');
    final ipc = Ipc(
      left.stream,
      sink,
      kill: left.close,
      grace: const Duration(milliseconds: 10),
    );

    await expectLater(
      ipc.ready,
      throwsA(
        isA<WorkerException>()
            .having((err) => err.code, 'code', 'TRANSPORT_ERROR')
            .having((err) => err.cause, 'cause', same(sink.error)),
      ),
    );

    await ipc.close();
  });

  test('fails with the error of the transport sink', () async {
    final (left, _) = pair();
    final sink = FailingSink(left.sink);
    final error = Exception('broken');
    final ipc = Ipc(
      left.stream,
      sink,
      kill: left.close,
      grace: const Duration(milliseconds: 10),
    );

    sink.failDone(error);

    await expectLater(
      ipc.ready,
      throwsA(
        isA<WorkerException>()
            .having((err) => err.code, 'code', 'TRANSPORT_ERROR')
            .having((err) => err.cause, 'cause', same(error)),
      ),
    );

    await ipc.close();
  });

  test('treats a broken pipe on write as a closed transport', () async {
    final (left, right) = pair();
    final sink = FailingSink(left.sink);
    final ipc = Ipc(left.stream, sink, kill: left.close);

    Worker(right).send('ready');

    await ipc.ready;

    sink.error = _brokenPipe;

    expect(
      () => ipc.add(utf8.encode('payload')),
      throwsWorkerException('WORKER_CLOSED', cause: _brokenPipe),
    );

    ipc.exited(1);

    await expectLater(
      ipc.toList(),
      throwsWorkerException('WORKER_EXITED', exitCode: 1),
    );
  });

  test('treats a failure after terminate as a closed transport', () async {
    final (left, right) = pair();
    final sink = FailingSink(left.sink);
    final ipc = Ipc(
      left.stream,
      sink,
      kill: left.close,
      grace: const Duration(milliseconds: 10),
    );

    Worker(right).send('ready');

    await ipc.ready;

    sink.error = Exception('broken');

    final closed = ipc.close();

    expect(
      () => ipc.add(utf8.encode('payload')),
      throwsWorkerException('WORKER_CLOSED', cause: sink.error),
    );

    await closed;

    expect(await ipc.toList(), isEmpty);
  });

  test('throws on a write to a closed transport', () async {
    final (left, right) = pair();
    final ipc = wrap(left);

    Worker(right).send('ready');

    await ipc.ready;

    left.close();

    expect(
      () => ipc.add(utf8.encode('payload')),
      throwsA(
        isA<WorkerException>()
            .having((err) => err.code, 'code', 'WORKER_CLOSED')
            .having((err) => err.cause, 'cause', isA<StateError>()),
      ),
    );

    ipc.exited(1);

    await expectLater(
      ipc.toList(),
      throwsWorkerException('WORKER_EXITED', exitCode: 1),
    );
  });

  test('exited is ignored after an exit frame', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);

    worker
      ..send('ready')
      ..send('exit', {'code': 0})
      ..close();

    await ipc.wait();

    ipc.exited(0);

    expect(await ipc.toList(), isEmpty);
  });

  test('exited is ignored while terminating', () async {
    final (left, right) = pair();
    final ipc = wrap(left, grace: const Duration(milliseconds: 10));

    Worker(right).send('ready');

    await ipc.ready;

    final terminated = ipc.terminate();

    ipc.exited(null, signal: 9);

    expect(await terminated, null);
    expect(await ipc.toList(), isEmpty);
  });

  test('holds back the end of a worker that crashes after ready', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);
    final data = ipc.toList();
    var done = false;

    unawaited(ipc.done.then((_) => done = true));

    worker.send('ready');

    await ipc.ready;

    worker.close();

    await pumpEventQueue();

    expect(done, false);

    ipc.exited(null, signal: 11);

    await expectLater(data, throwsWorkerException('WORKER_KILLED', signal: 11));

    await ipc.done;
  });

  test('holds back the end of a worker that crashes before ready', () async {
    final (left, _) = pair();
    final ipc = wrap(left);

    left.close();

    await pumpEventQueue();

    ipc.exited(1);

    await expectLater(
      ipc.ready,
      throwsWorkerException('WORKER_EXITED', exitCode: 1),
    );

    expect(await ipc.wait(), null);
  });

  test('fails a crashed worker with its exit rather than a timeout', () async {
    final (left, _) = pair();
    final ipc = wrap(left, timeout: const Duration(milliseconds: 50));

    left.close();

    await Future<void>.delayed(const Duration(milliseconds: 100));

    ipc.exited(1);

    await expectLater(
      ipc.ready,
      throwsWorkerException('WORKER_EXITED', exitCode: 1),
    );
  });

  test('exited applies to a close after the transport has ended', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);
    final data = ipc.toList();

    worker.send('ready');

    await ipc.ready;

    worker.close();

    await pumpEventQueue();

    final closing = ipc.close();

    ipc.exited(2);

    await expectLater(
      data,
      throwsWorkerException('WORKER_EXITED', exitCode: 2),
    );

    await closing;
  });

  test('does not hold back the end of a worker that exits', () async {
    final (left, right) = pair();
    final ipc = wrap(left);
    final worker = Worker(right);

    worker
      ..send('ready')
      ..send('exit', {'code': 0})
      ..close();

    expect(await ipc.wait(), 0);
    expect(await ipc.toList(), isEmpty);
  });

  test(
    'fails a crashed worker with its exit rather than a transport failure',
    () async {
      final (left, _) = pair();
      final ipc = wrap(left);
      final error = Exception('broken');

      left.fail(error);

      await pumpEventQueue();

      ipc.exited(null, signal: 11);

      await expectLater(
        ipc.ready,
        throwsA(
          isA<WorkerException>()
              .having((err) => err.code, 'code', 'WORKER_KILLED')
              .having(
                (err) => (err.cause as WorkerException?)?.cause,
                'cause.cause',
                same(error),
              ),
        ),
      );
    },
  );

  test('kills a worker whose transport fails and that does not exit', () async {
    final (left, _) = pair();
    final error = Exception('broken');
    var killed = false;

    final ipc = Ipc(
      left.stream,
      left.sink,
      kill: () => killed = true,
      grace: const Duration(milliseconds: 10),
    );

    left.fail(error);

    await expectLater(
      ipc.ready,
      throwsA(
        isA<WorkerException>()
            .having((err) => err.code, 'code', 'TRANSPORT_ERROR')
            .having((err) => err.cause, 'cause', same(error)),
      ),
    );

    expect(killed, true);

    ipc.exited(1);

    await ipc.done;
  });

  test('treats a reset transport as closed', () async {
    final (left, _) = pair();
    final ipc = wrap(left);

    left.fail(_reset);

    await pumpEventQueue();

    ipc.exited(1);

    await expectLater(
      ipc.ready,
      throwsWorkerException('WORKER_EXITED', exitCode: 1),
    );
  });
}
