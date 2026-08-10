// Android launcher: fire `ACTION_EDIT` for a FileProvider URI and wait for
// the user to return to tagsy.
//
// Two Android-specific problems this solves:
//
//   1. External apps cannot read raw filesystem paths from our app-private
//      storage (the daemon's fetch temp dir sits under `filesDir`). We hand
//      out a `content://` URI through our own FileProvider, granting
//      read+write for the duration of the intent. See
//      `EditorChannel.kt` and the manifest `<provider>` for the pieces.
//
//   2. `ACTION_EDIT` targets do not reliably report a result back via
//      `startActivityForResult` — many editors just close the activity, some
//      never return anything. The strongest "user is done" signal we can
//      count on is "MainActivity resumed after we launched", which the
//      Kotlin side surfaces via the `editorReturned` MethodChannel event.
//      First `onResume` after launch wins; if the user got distracted (task
//      switcher, notification) and hit tagsy's icon without ever opening
//      the editor, we still hand the path to `finishEdit`, which
//      re-hashes and no-ops when nothing changed.
//
// A crash between `launch` and the resume event only leaks a temp file, which
// the daemon bulk-wipes on next start (see `Paths::clean_fetch_temp_dir`).

import 'dart:async';

import 'package:flutter/services.dart';

import '../rust/api.dart' as tagsy;
import 'editor_launcher.dart';

/// The MethodChannel name matches the Kotlin side (`EditorChannel.kt`).
///
/// Kept separate from the existing `tagsy_app/config` channel so the two
/// concerns stay independent — `config` is startup-only, `editor` is
/// per-edit and has state (the pending edit's completer).
const _channel = MethodChannel('tagsy_app/editor');

class AndroidEditorLauncher implements EditorLauncher {
  AndroidEditorLauncher() {
    _channel.setMethodCallHandler(_onNativeCall);
  }

  /// The completer of the currently-in-flight `launchAndWait`. Non-null only
  /// while we are waiting for the next `editorReturned` event from Kotlin.
  ///
  /// Only one edit is expected in flight at a time — the file detail
  /// screen's Edit button disables itself while an edit is running. If a
  /// second launch tried to overlap the first, it would replace this
  /// completer and the first would never resolve; the abstraction contract
  /// documents that.
  Completer<void>? _pending;

  @override
  Future<void> launchAndWait({
    required String path,
    required String logicalName,
    required List<String> appliedTagIds,
    required List<tagsy.EditorRuleEntry> rules,
  }) async {
    // Rules exist for the Linux CLI-style "run this argv" model. Android's
    // dispatch is by MIME, resolved by the OS from the picker the user sees;
    // there is no way to exec an arbitrary argv in this environment (and no
    // reason to want to). Ignore `rules` entirely.
    if (_pending != null) {
      throw const EditorLaunchException('another edit is already in progress');
    }
    final completer = Completer<void>();
    _pending = completer;
    try {
      // Kotlin does the FileProvider URI construction, MIME sniff, and the
      // ACTION_EDIT / ACTION_VIEW dispatch. On failure it throws a
      // PlatformException; we surface that as a user-visible error and clear
      // the pending completer so the next launch is not blocked.
      final launched = await _channel.invokeMethod<bool>('launch', {
        'path': path,
        'logicalName': logicalName,
      });
      if (launched != true) {
        _pending = null;
        throw const EditorLaunchException(
          'no app on this device can edit this file type',
        );
      }
    } on PlatformException catch (error) {
      _pending = null;
      throw EditorLaunchException(error.message ?? 'launch failed');
    }

    // The Kotlin side calls back into `editorReturned` on the next resume;
    // await that.
    return completer.future;
  }

  /// MethodChannel entrypoint called by Kotlin. The only expected method is
  /// `editorReturned`, fired on the next MainActivity.onResume after a
  /// successful `launch`.
  Future<void> _onNativeCall(MethodCall call) async {
    switch (call.method) {
      case 'editorReturned':
        final pending = _pending;
        _pending = null;
        pending?.complete();
        return;
    }
  }
}
