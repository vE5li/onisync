// Shared application shell for both the Android and Linux apps.
//
// This is entirely platform-agnostic: it takes a [OniSyncBootstrap] (chosen in
// main.dart via --dart-define) and drives the lifecycle that is identical on
// every platform — connect, then hand the session to the home screen. Live
// updates and query dispatch are owned by the screens themselves (each opens
// its own change-stream subscription); the actual pixels live in screens/.

import 'package:flutter/material.dart';

import 'bootstrap/bootstrap.dart';
import 'screens/home_screen.dart';

class OniSyncApp extends StatefulWidget {
  const OniSyncApp({super.key, required this.bootstrap});

  /// The platform backend (in-process engine on Android, daemon IPC on Linux).
  final OniSyncBootstrap bootstrap;

  @override
  State<OniSyncApp> createState() => _OniSyncAppState();
}

// Shows feedback (SnackBars) from callbacks that can fire outside a build
// context (share-intent handlers, stream callbacks, cold-start).
final GlobalKey<ScaffoldMessengerState> _messengerKey =
    GlobalKey<ScaffoldMessengerState>();

class _OniSyncAppState extends State<OniSyncApp> {
  OniSyncSession? _session;

  @override
  void initState() {
    super.initState();
    _boot();
  }

  Future<void> _boot() async {
    try {
      final session = await widget.bootstrap.connect();
      setState(() {
        _session = session;
      });

      // Wire any platform-only inputs (Android share sheet); no-op on Linux.
      // `onChanged` is intentionally a no-op: screens watch the change stream
      // directly, so no app-level re-fetch is needed.
      widget.bootstrap.attachInputs(
        session,
        showMessage: _showMessage,
        onChanged: () {},
      );
    } catch (error) {
      // TODO: surface connection failures in the UI once the redesigned
      // status/error surface lands. For now they only appear in logs.
      debugPrint('onisync bootstrap failed: $error');
    }
  }

  void _showMessage(String message) {
    _messengerKey.currentState
      ?..hideCurrentSnackBar()
      ..showSnackBar(
        SnackBar(content: Text(message), duration: const Duration(seconds: 2)),
      );
  }

  @override
  void dispose() {
    widget.bootstrap.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'OniSync',
      scaffoldMessengerKey: _messengerKey,
      home: HomeScreen(session: _session),
    );
  }
}
