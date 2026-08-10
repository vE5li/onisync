// Tests for the Linux editor launcher's rule resolution.
//
// These cover the resolution/guard logic only, not the actual spawn: the
// interesting behavior is *which* argv gets chosen and which rules are
// refused, and that is what the security properties described in
// `linux_editor_launcher.dart` rest on. Spawning a real process would test
// `Process.run` rather than anything of ours.

import 'package:flutter_test/flutter_test.dart';

import 'package:tagsy_app/editor/editor_launcher.dart';
import 'package:tagsy_app/editor/linux_editor_launcher.dart';
import 'package:tagsy_app/rust/api.dart' as tagsy;

tagsy.EditorRuleEntry _rule(String tagId, List<String> argv) =>
    tagsy.EditorRuleEntry(tagId: tagId, argv: argv);

void main() {
  group('LinuxEditorLauncher.resolveArgv', () {
    test('returns the argv of the first rule matching an applied tag', () {
      final argv = LinuxEditorLauncher.resolveArgv(
        appliedTagIds: const ['tag-b'],
        rules: [
          _rule('tag-a', const ['/usr/bin/gimp']),
          _rule('tag-b', const ['/usr/bin/code', '--wait']),
        ],
      );
      expect(argv, const ['/usr/bin/code', '--wait']);
    });

    test('declaration order wins when several rules match', () {
      final argv = LinuxEditorLauncher.resolveArgv(
        appliedTagIds: const ['tag-a', 'tag-b'],
        rules: [
          _rule('tag-b', const ['/usr/bin/first']),
          _rule('tag-a', const ['/usr/bin/second']),
        ],
      );
      expect(argv, const ['/usr/bin/first']);
    });

    test('multi-argument argv is preserved verbatim, including spaces', () {
      // The whole point of taking a list instead of a string: an argument
      // containing a space stays one argument.
      final argv = LinuxEditorLauncher.resolveArgv(
        appliedTagIds: const ['tag-a'],
        rules: [
          _rule('tag-a', const ['/opt/my editor/bin/edit', '--flag=a b']),
        ],
      );
      expect(argv, const ['/opt/my editor/bin/edit', '--flag=a b']);
    });

    test('throws when no rule matches', () {
      expect(
        () => LinuxEditorLauncher.resolveArgv(
          appliedTagIds: const ['tag-z'],
          rules: [
            _rule('tag-a', const ['/usr/bin/gimp']),
          ],
        ),
        throwsA(isA<EditorLaunchException>()),
      );
    });

    test('throws when there are no rules at all', () {
      expect(
        () => LinuxEditorLauncher.resolveArgv(
          appliedTagIds: const ['tag-a'],
          rules: const [],
        ),
        throwsA(isA<EditorLaunchException>()),
      );
    });

    test('rejects a relative argv[0] rather than trusting PATH', () {
      // The security guard: a bare name would be resolved through the
      // inherited PATH, so anyone able to prepend a directory to it could
      // substitute the editor.
      expect(
        () => LinuxEditorLauncher.resolveArgv(
          appliedTagIds: const ['tag-a'],
          rules: [
            _rule('tag-a', const ['gimp']),
          ],
        ),
        throwsA(
          isA<EditorLaunchException>().having(
            (error) => error.message,
            'message',
            contains('absolute path'),
          ),
        ),
      );
    });

    test('rejects an empty argv', () {
      expect(
        () => LinuxEditorLauncher.resolveArgv(
          appliedTagIds: const ['tag-a'],
          rules: [_rule('tag-a', const [])],
        ),
        throwsA(isA<EditorLaunchException>()),
      );
    });

    test(
      'a malformed matching rule fails loudly instead of falling through',
      () {
        // Silently skipping to the next rule would launch a *different* editor
        // than the operator configured — least acceptable exactly when the rule
        // failed the absolute-path check.
        expect(
          () => LinuxEditorLauncher.resolveArgv(
            appliedTagIds: const ['tag-a', 'tag-b'],
            rules: [
              _rule('tag-a', const ['relative-editor']),
              _rule('tag-b', const ['/usr/bin/fallback']),
            ],
          ),
          throwsA(isA<EditorLaunchException>()),
        );
      },
    );

    test('non-matching malformed rules are ignored', () {
      // The guard applies to the rule that actually matched, not to every
      // rule in the config.
      final argv = LinuxEditorLauncher.resolveArgv(
        appliedTagIds: const ['tag-b'],
        rules: [
          _rule('tag-a', const ['relative-editor']),
          _rule('tag-b', const ['/usr/bin/ok']),
        ],
      );
      expect(argv, const ['/usr/bin/ok']);
    });
  });
}
