// Platform-agnostic contract for launching an external editor on a file and
// waiting until the user is done editing it.
//
// The Flutter "edit" action in the file detail screen is a thin driver over
// the daemon's stateless edit protocol (see OniSyncApp.beginEditByString /
// finishEditByString / cancelEdit): it asks the daemon for a path, hands the
// path to a launcher implementing this interface, and — once the launcher
// resolves — hands the path back for hash-and-maybe-upload.
//
// The "wait" part is what differs between platforms:
//
//   * Linux: `Process.start` the resolved editor and `await exitCode`. The
//     editor blocks in the foreground, so the exit is a reliable "user is
//     done" signal.
//   * Android: fire an `ACTION_EDIT` intent for a FileProvider URI and await
//     the next `onResume` of MainActivity. External editors do not reliably
//     return a result to us via `startActivityForResult`, so "the user came
//     back to onisync" is the strongest signal available.
//
// Each platform's bootstrap plugs a concrete [EditorLauncher] into the
// [OniSyncSession] (null on platforms without one). The file detail screen
// only shows its Edit button when the session carries a launcher.

import '../rust/api.dart' as onisync;

/// Handle to launch external editors on files.
///
/// Implementations are constructed at bootstrap time (with any platform state
/// they need — MethodChannels, config lookups, …) and reused for the app's
/// lifetime. Each `launchAndWait` call is one editing session; concurrent
/// calls are not supported (nothing prevents them, but the Android impl in
/// particular has one process-wide "who is expecting the next onResume?" slot
/// and would confuse two overlapping edits).
abstract class EditorLauncher {
  /// Open [path] in an external editor and return once the user is done.
  ///
  /// [rules] is the daemon-configured tag → command mapping (see
  /// [onisync.EditorRuleEntry]); implementations that consult tags (Linux)
  /// walk it in declaration order, first match wins. Implementations that
  /// ignore tags (Android — the OS picks the editor by MIME) may leave it
  /// unused.
  ///
  /// [appliedTagNames] is the *names* of the tags currently applied to the
  /// file, so [rules] can be resolved without the launcher needing a bridge
  /// handle. Order is not significant; matching is by set membership.
  ///
  /// [logicalName] is the file's user-facing name (last component of the
  /// logical path). Used by the Android impl to sniff a MIME hint from the
  /// extension.
  ///
  /// Throws on any launch/wait failure; the caller uses that to distinguish
  /// "abort — clean up the daemon temp" from "editor exited normally — hand
  /// the path back to `finishEditByString`".
  Future<void> launchAndWait({
    required String path,
    required String logicalName,
    required List<String> appliedTagNames,
    required List<onisync.EditorRuleEntry> rules,
  });
}

/// A user-visible reason the launch could not be started.
///
/// Thrown by [EditorLauncher.launchAndWait] when the platform layer refuses
/// the launch outright (no matching editor, missing environment variable, no
/// app installed on Android that handles the MIME). The file detail screen
/// surfaces the [message] in a snackbar. All other failures (editor crashed
/// mid-edit, I/O error) surface as their native exception type.
class EditorLaunchException implements Exception {
  final String message;
  const EditorLaunchException(this.message);
  @override
  String toString() => 'EditorLaunchException: $message';
}
