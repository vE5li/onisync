// Platform-agnostic startup contract shared by the Android and Linux apps.
//
// The ONLY real difference between the two apps is how a live [onisync.OniSyncApp]
// handle is obtained:
//
//   * Android starts an in-process sync engine (owns the DB + identity) and
//     also shows the device public key and accepts files from the share sheet.
//   * Linux attaches to a systemd daemon over IPC; the daemon owns everything,
//     so there is no public key to show and no share intent here.
//
// Everything AFTER a handle exists (listing files/tags, the change-stream loop,
// the widget tree) is identical and lives in the shared UI (../app.dart,
// ../screens/). To keep that UI free of any platform imports, each platform
// implements this [OniSyncBootstrap] and hands back a [OniSyncSession].

import 'package:flutter/widgets.dart';

import '../rust/api.dart' as onisync;

/// A connected backend, ready for the shared UI to drive.
class OniSyncSession {
  /// The live handle every screen calls into (list/create/tag/upload/...).
  final onisync.OniSyncApp app;

  /// This device's base64 public key, or `null` when the platform has no local
  /// identity to show (Linux, where the daemon owns the identity).
  final String? publicKey;

  const OniSyncSession({required this.app, this.publicKey});
}

/// Produces a [OniSyncSession] and wires any platform-only side channels.
///
/// Implementations are selected at build time from [main] via a
/// `--dart-define=ONISYNC_BACKEND=...`; the shared UI never imports them
/// directly.
abstract class OniSyncBootstrap {
  /// Initialise the generated Rust bindings and connect to the backend
  /// (in-process engine on Android, daemon IPC on Linux). Throws on failure;
  /// the caller renders the error.
  Future<OniSyncSession> connect();

  /// Hook for platform-only inputs that need a live session, e.g. the Android
  /// share sheet uploading files. Called once after [connect] succeeds.
  ///
  /// [showMessage] surfaces user feedback (snackbars) from callbacks that fire
  /// outside a build context. [navigate] pushes a route onto the app's
  /// navigator from those same out-of-context callbacks (the share handler
  /// uses it to open the share-review screen). [onChanged] is invoked after
  /// any mutation so the UI can refresh. The default is a no-op (Linux has
  /// nothing to wire).
  void attachInputs(
    OniSyncSession session, {
    required void Function(String message) showMessage,
    required void Function(Route<dynamic> route) navigate,
    required VoidCallback onChanged,
  }) {}

  /// Release any resources acquired by [attachInputs] (stream subscriptions,
  /// etc.). Does NOT stop the backend — its lifetime is owned elsewhere (the
  /// Android foreground service / the Linux daemon). Default no-op.
  void dispose() {}
}
