// Android backend: start an in-process sync engine and accept files shared to
// the app (portability plan section 8).
//
// Unlike Linux (which attaches to a daemon), this process OWNS the engine, DB,
// and identity. It also wires the Android share sheet so "Share to onisync"
// uploads files, and surfaces this device's public key for pairing.
//
// Selected at build time via --dart-define=ONISYNC_BACKEND=android (see main).

import 'dart:async';

import 'package:flutter/services.dart';
import 'package:receive_sharing_intent/receive_sharing_intent.dart';

import '../rust/frb_generated.dart';
import '../rust/api.dart' as onisync;
import 'bootstrap.dart';

/// MethodChannel exposed by [MainActivity] returning the JSON config + paths
/// the Kotlin side built. See OniSyncConfig.kt for the single source of truth.
///
/// The Kotlin foreground service normally starts the runtime before this
/// bootstrap runs; the values fetched here are the *same* values it passed to
/// nativeStart, so OniSyncApp.start attaches to the already-running instance
/// (crate::service::start is idempotent). If the service is somehow slow, the
/// Dart side starts it with identical inputs — no divergence possible.
const _configChannel = MethodChannel('onisync_app/config');

class AndroidBootstrap extends OniSyncBootstrap {
  StreamSubscription<List<SharedMediaFile>>? _shareSub;

  @override
  Future<OniSyncSession> connect() async {
    // Loads libonisync_bridge.so and wires up the generated bindings.
    await RustLib.init();

    // Fetch the runtime startup inputs from Kotlin. Editing the peer config
    // means editing OniSyncConfig.kt — there is no JSON literal on the Dart
    // side any more.
    final inputs = await _configChannel.invokeMapMethod<String, String>(
      'getStartupInputs',
    );
    if (inputs == null) {
      throw StateError('onisync_app/config channel returned no inputs');
    }
    final configJson = inputs['configJson']!;
    final dataDir = inputs['dataDir']!;
    final identityFile = inputs['identityFile']!;

    final app = onisync.OniSyncApp.start(
      configurationJson: configJson,
      dataDir: dataDir,
      identityFile: identityFile,
    );

    return OniSyncSession(app: app, publicKey: app.publicKey());
  }

  /// Wire up the Android share sheet ("Share to onisync"). Two cases:
  ///   1. app already running  -> getMediaStream() pushes new shares,
  ///   2. app launched by share -> getInitialMedia() returns the first batch.
  /// Both funnel into [_uploadSharedFiles].
  @override
  void attachInputs(
    OniSyncSession session, {
    required void Function(String message) showMessage,
    required VoidCallback onChanged,
  }) {
    _shareSub = ReceiveSharingIntent.instance.getMediaStream().listen(
      (files) => _uploadSharedFiles(session.app, files, showMessage, onChanged),
      onError: (Object error) => showMessage('Share error: $error'),
    );

    ReceiveSharingIntent.instance.getInitialMedia().then((files) {
      if (files.isEmpty) return;
      _uploadSharedFiles(session.app, files, showMessage, onChanged);
      // Tell the plugin we consumed the initial intent so it is not redelivered.
      ReceiveSharingIntent.instance.reset();
    });
  }

  /// Hand each shared file's *path* to the sync engine via
  /// [onisync.OniSyncApp.uploadFile]. The engine streams the bytes from disk
  /// (hash then serve-on-demand); they are never buffered whole, matching the
  /// CLI's upload path.
  Future<void> _uploadSharedFiles(
    onisync.OniSyncApp app,
    List<SharedMediaFile> files,
    void Function(String) showMessage,
    VoidCallback onChanged,
  ) async {
    if (files.isEmpty) return;

    var uploaded = 0;
    for (final shared in files) {
      try {
        // Derive a display/logical name from the source path. The engine treats
        // this as the file's logical identity at the ingestion boundary.
        final name = shared.path.split('/').last;
        await app.uploadFile(path: shared.path, pathName: name, tags: const []);
        uploaded++;
      } catch (error) {
        showMessage('Failed to upload ${shared.path}: $error');
      }
    }

    if (uploaded > 0) {
      showMessage('Uploaded $uploaded file${uploaded == 1 ? '' : 's'} to onisync');
      onChanged();
    }
  }

  @override
  void dispose() {
    _shareSub?.cancel();
    // Deliberately do NOT stop the runtime: it is owned by the foreground
    // service (crate::service) so sync keeps running after the UI is closed.
  }
}

/// Factory referenced by the backend selector in main.dart.
OniSyncBootstrap createBootstrap() => AndroidBootstrap();
