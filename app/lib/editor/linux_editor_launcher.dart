// Linux desktop launcher: spawn the editor as a child process and await
// its exit.
//
// The editor to spawn is resolved by consulting the daemon-configured
// tag-based rules first (first rule whose `tag` name is applied to the file
// wins) and falling back to `$VISUAL` / `$EDITOR`. Both mechanisms follow the
// CLI's `open_in_editor` shape: the argv is passed straight to
// `Process.start`, the file path is appended, and we block on `exitCode`.
// This makes the launcher inherit the CLI's contract with the shell: the
// command must run in the foreground and exit when the user is done. A
// backgrounding launcher (`xdg-open`, `nohup`, `gtk-launch`) would return
// immediately and confuse the "editing finished" signal — those are not
// supported.
//
// Command strings are tokenised by whitespace, which is deliberately
// primitive: it covers `code --wait` / `gimp` / `inkscape --file` / etc.
// without needing a shell. Quoted arguments and shell metacharacters are not
// supported; if you need them, wrap the command in a small script and point
// the rule at that script.

import 'dart:io';

import '../rust/api.dart' as onisync;
import 'editor_launcher.dart';

class LinuxEditorLauncher implements EditorLauncher {
  @override
  Future<void> launchAndWait({
    required String path,
    required String logicalName,
    required List<String> appliedTagNames,
    required List<onisync.EditorRuleEntry> rules,
  }) async {
    final argv = _resolveCommand(appliedTagNames: appliedTagNames, rules: rules);
    // Convention matches the CLI's `open_in_editor` (onisync/src/main.rs):
    // path is the last argument. Users writing rules can rely on it.
    final args = [...argv.skip(1), path];

    final ProcessResult result;
    try {
      // `runInShell: false` — we already argv-split the command ourselves and
      // deliberately do not want the shell to re-interpret arguments (globs,
      // env, quoting). If a rule needs shell semantics it should call a
      // wrapper script explicitly.
      result = await Process.run(argv[0], args, runInShell: false);
    } on ProcessException catch (error) {
      throw EditorLaunchException(
        'failed to launch editor "${argv[0]}": ${error.message}',
      );
    }

    if (result.exitCode != 0) {
      // Nonzero is treated as an abort — the CLI does the same. Include a
      // slice of stderr so the user can see why (e.g. GIMP printing a
      // display error). Trim to keep the snackbar short.
      final stderr = result.stderr.toString().trim();
      final tail = stderr.length > 200 ? '${stderr.substring(0, 200)}…' : stderr;
      throw EditorLaunchException(
        'editor "${argv[0]}" exited with code ${result.exitCode}'
        '${tail.isEmpty ? '' : ': $tail'}',
      );
    }
  }

  /// Walk `rules` in declaration order; the first rule whose `tag` is in
  /// `appliedTagNames` wins. Falls back to `$VISUAL` then `$EDITOR`. Throws
  /// [EditorLaunchException] if neither is set and no rule matches — mirroring
  /// the CLI's "no editor configured" failure mode rather than silently
  /// picking a default like `vi` (which would open in-terminal from a GUI
  /// process, going nowhere).
  static List<String> _resolveCommand({
    required List<String> appliedTagNames,
    required List<onisync.EditorRuleEntry> rules,
  }) {
    final applied = appliedTagNames.toSet();
    for (final rule in rules) {
      if (applied.contains(rule.tag)) {
        final argv = _tokenise(rule.command);
        if (argv.isEmpty) continue; // empty command: skip and try the next rule.
        return argv;
      }
    }

    final envEditor =
        Platform.environment['VISUAL'] ?? Platform.environment['EDITOR'];
    if (envEditor != null && envEditor.trim().isNotEmpty) {
      final argv = _tokenise(envEditor);
      if (argv.isNotEmpty) return argv;
    }

    throw const EditorLaunchException(
      'no editor configured: set an editor rule for one of this file\'s tags '
      'in the daemon config, or set \$VISUAL / \$EDITOR',
    );
  }

  /// Whitespace-split a command string into argv. Deliberately dumb — no
  /// quote handling, no escape sequences. See the file-level doc for
  /// rationale.
  static List<String> _tokenise(String command) {
    return command
        .split(RegExp(r'\s+'))
        .where((segment) => segment.isNotEmpty)
        .toList(growable: false);
  }
}
