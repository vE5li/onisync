// Minimal smoke test for the tagsy app shell.
//
// The real app boots a native backend (in-process engine on Android, daemon IPC
// on Linux) via flutter_rust_bridge in `initState`, which is not available in a
// plain widget test. So this uses a fake [TagsyBootstrap] whose `connect()`
// never completes, leaving the UI on its initial status; the test only checks
// that the widget tree constructs and shows that status.

import 'dart:async';

import 'package:flutter_test/flutter_test.dart';

import 'package:tagsy_app/app.dart';
import 'package:tagsy_app/bootstrap/bootstrap.dart';

class _PendingBootstrap extends TagsyBootstrap {
  @override
  Future<TagsySession> connect() => Completer<TagsySession>().future;
}

void main() {
  testWidgets('app renders initial status', (WidgetTester tester) async {
    await tester.pumpWidget(TagsyApp(bootstrap: _PendingBootstrap()));
    expect(find.text('Tagsy'), findsOneWidget);
  });
}
