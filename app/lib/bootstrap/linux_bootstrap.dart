// Linux desktop backend: attach to the running onisync daemon over IPC
// (portability plan sections 6-7, two-process topology).
//
// Unlike Android, this process does NOT start its own sync engine or open the
// database. The systemd daemon owns the DB and serves a Unix control socket
// (/run/onisync/onisync.sock); this app merely ATTACHES to it. So there is no
// config JSON, no data directory, no identity, and no public key to show here —
// they all belong to the daemon. There is likewise no share-intent input, so
// attachInputs/dispose fall back to the no-op defaults in OniSyncBootstrap.
//
// Selected at build time via --dart-define=ONISYNC_BACKEND=linux (see main).

import 'dart:io';

import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart'
    show ExternalLibrary;

import '../rust/frb_generated.dart';
import '../rust/api.dart' as onisync;
import 'bootstrap.dart';

class LinuxBootstrap extends OniSyncBootstrap {
  @override
  Future<OniSyncSession> connect() async {
    // On Linux the .so is built + bundled by the runner's CMake hook
    // (app/linux/CMakeLists.txt); load it explicitly (see _loadBridge).
    await RustLib.init(externalLibrary: _loadBridge());

    // Connect to the daemon's control socket (/run/onisync/onisync.sock). This
    // fails if the daemon is not running. No config/paths: the daemon owns the
    // engine, DB, and identity.
    final app = await onisync.OniSyncApp.attach();
    return OniSyncSession(app: app, publicKey: null);
  }

  /// Resolve libonisync_bridge.so for both run modes.
  ///
  /// frb's default loader derives a dev-only relative path from `rust_root`
  /// (../onisync-bridge/target/release/) that does not exist for a Cargo
  /// *workspace* (which builds to the repo-root target/) nor for a bundled app.
  /// So load it explicitly:
  ///
  /// - Bundled release: the runner's CMake hook installs the .so into `lib/`
  ///   next to the executable (see app/linux/CMakeLists.txt).
  /// - `flutter run -d linux` (dev): the CWD is the Flutter project (app/) and
  ///   the workspace cdylib is at ../target/release/.
  static ExternalLibrary _loadBridge() {
    const soName = 'libonisync_bridge.so';
    final candidates = <String>[
      // Bundled: <bundle>/lib/libonisync_bridge.so
      '${File(Platform.resolvedExecutable).parent.path}/lib/$soName',
      // Dev (flutter run, CWD = app/): repo-root workspace target.
      '../target/release/$soName',
      // Dev fallback if run from repo root.
      'target/release/$soName',
    ];
    final found = candidates.firstWhere(
      (path) => File(path).existsSync(),
      orElse: () => soName, // last resort: let the dynamic loader search.
    );
    return ExternalLibrary.open(found);
  }
}

/// Factory referenced by the backend selector in main.dart.
OniSyncBootstrap createBootstrap() => LinuxBootstrap();
