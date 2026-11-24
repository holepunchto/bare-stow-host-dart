# bare-stow-host-dart

Host side of the [bare-stow](https://github.com/holepunchto/bare-stow) lifecycle for Dart. The Dart counterpart of [bare-stow-host](https://github.com/holepunchto/bare-stow-host): it multiplexes a control channel and a user data channel over a byte transport to a stowed bundle, and layers readiness, termination, and exit reporting on top.

```
dart pub add bare_stow_host
```

## Usage

```dart
import 'dart:convert';

import 'package:bare_kit/bare_kit.dart';
import 'package:bare_stow_host/bare_stow_host.dart';

Future<void> main() async {
  final worklet = Worklet.start('index.bundle');

  final ipc = Ipc(worklet, worklet, kill: worklet.destroy);

  worklet.exitCode.then(ipc.exited);

  await ipc.ready;

  ipc.add(utf8.encode('hello'));

  final code = await ipc.terminate();
}
```

Any `Stream<List<int>>` and `StreamSink<List<int>>` can be wrapped, such as a `Worklet` from [bare-kit-dart](https://github.com/holepunchto/bare-kit-dart). Listening to the `Ipc` yields the user data from the worker, and adding to it sends user data to the worker.

Whoever creates the `Ipc` must call `ipc.exited(code, signal: signal)` once the worker has exited, such as when the process of a sidecar exits. Until then, a transport that ends or fails without the worker saying why is held back, so that the `Ipc` can fail with the exit code or signal. A worker whose transport fails and that hasn't exited within the grace period is killed.

## Protocol

Every frame is a 4-byte little-endian length followed by that many bytes: A 1-byte tag and the payload. The tag is `0x00` for a control frame, whose payload is a UTF-8 JSON object carrying its `type`, and `0x01` for a user data frame. See [bare-stow-protocol](https://github.com/holepunchto/bare-stow-protocol) for the control frames.

## License

Apache-2.0
