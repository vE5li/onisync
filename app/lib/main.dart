// Shared entrypoint for BOTH the onisync Android and Linux desktop apps.
//
// The two apps differ only in how they connect to the backend (Android starts
// an in-process engine; Linux attaches to a daemon over IPC). That difference
// is isolated in bootstrap/*_bootstrap.dart behind the OniSyncBootstrap
// contract, and everything else — the whole UI — is shared (app.dart,
// screens/).
//
// The backend is chosen at build time with a compile-time define:
//
//   flutter run --dart-define=ONISYNC_BACKEND=android   # (default)
//   flutter run --dart-define=ONISYNC_BACKEND=linux -d linux
//
// The flake's run-android / run-linux apps pass the right value.

import 'package:flutter/material.dart';

import 'app.dart';
import 'bootstrap/bootstrap.dart';
import 'bootstrap/android_bootstrap.dart' as android;
import 'bootstrap/linux_bootstrap.dart' as linux;

/// Backend id baked in at build time; defaults to Android.
const String _backend = String.fromEnvironment(
  'ONISYNC_BACKEND',
  defaultValue: 'android',
);

OniSyncBootstrap _selectBootstrap() {
  switch (_backend) {
    case 'linux':
      return linux.createBootstrap();
    case 'android':
      return android.createBootstrap();
    default:
      throw StateError(
        'Unknown ONISYNC_BACKEND "$_backend" '
        '(expected "android" or "linux").',
      );
  }
}

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  // RustLib.init() is done inside each bootstrap's connect(), because Linux
  // needs a custom library loader and Android uses the default.
  runApp(OniSyncApp(bootstrap: _selectBootstrap()));
}
