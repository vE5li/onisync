// Android backend: start an in-process sync engine and accept files shared to
// the app.
//
// Unlike Linux (which attaches to a daemon), this process OWNS the engine, DB,
// and identity. It also wires the Android share sheet so "Share to onisync"
// uploads files, and surfaces this device's public key for pairing.
//
// Selected at build time via --dart-define=ONISYNC_BACKEND=android (see main).

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:receive_sharing_intent/receive_sharing_intent.dart';

import '../rust/frb_generated.dart';
import '../rust/api.dart' as onisync;
import '../screens/share_review_screen.dart';
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

    // Fetch the runtime startup inputs from Kotlin. The peer config lives
    // in OniSyncConfig.kt (single source of truth); the Dart side never
    // holds a JSON literal.
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

    // The device's public Downloads dir, resolved Kotlin-side via Environment
    // (same mechanism as the sync dir). Best-effort: a null just hides the
    // download button.
    final downloadsDir = await _configChannel.invokeMethod<String>(
      'getDownloadsDir',
    );

    return OniSyncSession(
      app: app,
      publicKey: app.publicKey(),
      downloadsDir: downloadsDir,
    );
  }

  /// Wire up the Android share sheet ("Share to onisync"). Two cases:
  ///   1. app already running  -> getMediaStream() pushes new shares,
  ///   2. app launched by share -> getInitialMedia() returns the first batch.
  /// Both funnel into [_reviewSharedFiles], which opens the share-review
  /// screen so the user can tag the file(s) before they are uploaded.
  @override
  void attachInputs(
    OniSyncSession session, {
    required void Function(String message) showMessage,
    required void Function(Route<dynamic> route) navigate,
    required VoidCallback onChanged,
  }) {
    _shareSub = ReceiveSharingIntent.instance.getMediaStream().listen(
      (files) => _reviewSharedFiles(session, files, navigate),
      onError: (Object error) => showMessage('Share error: $error'),
    );

    ReceiveSharingIntent.instance.getInitialMedia().then((files) {
      if (files.isEmpty) return;
      _reviewSharedFiles(session, files, navigate);
      // Tell the plugin we consumed the initial intent so it is not redelivered.
      ReceiveSharingIntent.instance.reset();
    });
  }

  /// Open the share-review screen for the shared file(s). Rather than upload
  /// immediately, the user first attaches tags there; the actual
  /// [onisync.OniSyncApp.uploadFile] call (which streams the bytes from disk,
  /// never buffering them whole) happens when they confirm.
  void _reviewSharedFiles(
    OniSyncSession session,
    List<SharedMediaFile> files,
    void Function(Route<dynamic> route) navigate,
  ) {
    if (files.isEmpty) return;
    final paths = files.map((f) => f.path).toList();
    navigate(
      MaterialPageRoute<void>(
        builder: (_) => ShareReviewScreen(session: session, paths: paths),
      ),
    );
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
