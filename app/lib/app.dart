// Shared application shell for both the Android and Linux apps.
//
// This is entirely platform-agnostic: it takes a [OniSyncBootstrap] (chosen in
// main.dart via --dart-define) and drives the lifecycle that is identical on
// every platform — connect, then hand the session to the home screen. Live
// updates and query dispatch are owned by the screens themselves (each opens
// its own change-stream subscription); the actual pixels live in screens/.

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

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

// Lets callbacks that fire outside a build context (the Android share-intent
// handler) push routes onto the app's navigator — e.g. the share-review
// screen that collects tags before uploading a shared file.
final GlobalKey<NavigatorState> _navigatorKey = GlobalKey<NavigatorState>();

class _OniSyncAppState extends State<OniSyncApp> {
  OniSyncSession? _session;

  @override
  void initState() {
    super.initState();
    _boot();
    // Global Ctrl+C = "go back": pop the current route regardless of which
    // screen is focused. We hook `HardwareKeyboard` directly (rather than
    // wrapping the app in Shortcuts/Actions) so the shortcut fires no
    // matter where focus currently sits — mirrors the Ctrl+F handler in
    // home_screen.dart. Suppressed while an editable text widget has
    // focus so it doesn't clobber the standard "copy" affordance.
    HardwareKeyboard.instance.addHandler(_handleGlobalKey);
  }

  bool _handleGlobalKey(KeyEvent event) {
    if (event is! KeyDownEvent) return false;
    if (event.logicalKey != LogicalKeyboardKey.keyC) return false;
    if (!HardwareKeyboard.instance.isControlPressed) return false;
    // Don't hijack Ctrl+C from a text field — the user probably means copy.
    final focus = FocusManager.instance.primaryFocus?.context;
    if (focus != null &&
        focus.findAncestorWidgetOfExactType<EditableText>() != null) {
      return false;
    }
    final nav = _navigatorKey.currentState;
    if (nav == null) return false;
    // `maybePop` returns false when at the root — in that case we don't
    // want to swallow the event (there's nothing to pop, and consuming it
    // could suppress a legitimate downstream handler).
    nav.maybePop();
    return true;
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
        navigate: (route) => _navigatorKey.currentState?.push(route),
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
    HardwareKeyboard.instance.removeHandler(_handleGlobalKey);
    widget.bootstrap.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'OniSync',
      scaffoldMessengerKey: _messengerKey,
      navigatorKey: _navigatorKey,
      themeMode: ThemeMode.dark,
      darkTheme: ThemeData(
        useMaterial3: true,
        brightness: Brightness.dark,
        colorScheme: ColorScheme.fromSeed(
          seedColor: Colors.indigo,
          brightness: Brightness.dark,
        ),
      ),
      home: HomeScreen(session: _session),
    );
  }
}
