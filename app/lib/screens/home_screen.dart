// Shared home screen: a live search bar that renders returned tags at the top
// and returned files immediately below. Both open the corresponding detail
// screen on tap; when a non-empty tag-name-shaped query resolves to zero tags,
// a "Create tag" affordance appears in the tags section so tag creation
// remains reachable without a dedicated management screen.
//
// The screen intentionally does NOT fetch anything on load: an empty
// `runQuery` scans the entire store, which is a real performance hazard as the
// store grows. Results only appear once the user types.
//
// Identical on every platform; the AppBar exposes an (Android-only)
// copy-public-key action that renders only when the session carries a key
// (absent on Linux, where the daemon owns the identity). No platform imports
// here.

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../bootstrap/bootstrap.dart';
import '../rust/api.dart' as onisync;
import '../widgets/tag_chip.dart';
import 'file_detail_screen.dart';
import 'operations_screen.dart';
import 'tag_detail_screen.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key, required this.session});

  final OniSyncSession? session;

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final TextEditingController _query = TextEditingController();

  /// Owned so we can programmatically drop focus when navigating to a detail
  /// screen. Without this, Flutter's automatic focus restoration can re-focus
  /// the search field when the detail route is popped, which re-opens the
  /// soft keyboard on mobile — a jarring UX papercut every time the user
  /// backs out of a detail view.
  final FocusNode _queryFocus = FocusNode();

  /// Debounce timer for keystrokes -> `runQuery` calls. Kept short so results
  /// feel live but the daemon isn't hit on every character.
  Timer? _debounce;

  /// Monotonic counter used to discard stale results if a slower query resolves
  /// after a newer one has already been dispatched.
  int _queryEpoch = 0;

  /// Latest result to render. Null until the user runs a query.
  onisync.QueryEntries? _results;
  String? _error;
  bool _loading = false;

  /// When true, the search runs against soft-deleted (tombstoned) files and
  /// tags instead of live ones — see [onisync.DeletedRule]. Toggled by the
  /// small button next to the search field. Off by default: the standard
  /// search only ever shows live rows.
  bool _showDeleted = false;

  /// Change-stream watcher: re-runs the *current* query whenever the underlying
  /// data changes so the results stay accurate. Deliberately does nothing when
  /// the user has not typed a query yet — we never synthesise an empty query.
  ///
  /// TODO(perf): this refetches on every change event, which is coarse. For
  /// large stores we should either debounce the change-driven refetches or
  /// filter which events actually need a re-query (e.g. only re-run on tag /
  /// file mutations, not on transport heartbeats). Revisit when the redesign
  /// stabilises.
  bool _watching = false;

  @override
  void initState() {
    super.initState();
    _query.addListener(_onQueryChanged);
    if (widget.session != null) _watch();
    // Global Ctrl+F -> focus the search field. We hook `HardwareKeyboard`
    // directly (rather than using Shortcuts/Actions) because the focus tree
    // above HomeScreen doesn't route to a Shortcuts widget placed inside it:
    // shortcut resolution walks up from the currently focused node, so a
    // Shortcuts widget higher than the focus never sees the event. The raw
    // handler runs regardless of focus location, which is exactly what a
    // "global" accelerator wants.
    HardwareKeyboard.instance.addHandler(_handleGlobalKey);
  }

  bool _handleGlobalKey(KeyEvent event) {
    if (event is! KeyDownEvent) return false;
    if (event.logicalKey != LogicalKeyboardKey.keyF) return false;
    if (!HardwareKeyboard.instance.isControlPressed) return false;
    _queryFocus.requestFocus();
    return true; // consume so the browser/OS Ctrl+F is fully suppressed
  }

  @override
  void didUpdateWidget(covariant HomeScreen old) {
    super.didUpdateWidget(old);
    if (old.session == null && widget.session != null) _watch();
  }

  @override
  void dispose() {
    _watching = false;
    _debounce?.cancel();
    HardwareKeyboard.instance.removeHandler(_handleGlobalKey);
    _query.removeListener(_onQueryChanged);
    _query.dispose();
    _queryFocus.dispose();
    super.dispose();
  }

  void _onQueryChanged() {
    _debounce?.cancel();
    _debounce = Timer(const Duration(milliseconds: 200), _runQuery);
    // Rebuild for the clear button in the search bar suffix.
    setState(() {});
  }

  Future<void> _watch() async {
    final session = widget.session;
    if (session == null || _watching) return;
    _watching = true;
    try {
      final events = await session.app.subscribe();
      while (mounted && _watching) {
        final event = await events.next();
        if (event == null) break;
        if (!mounted) break;
        // Only re-run if the user has actually issued a query. We must never
        // fabricate an empty-query listing here (see class doc).
        if (_results != null) await _runQuery();
      }
    } catch (_) {
      // Stream errors are surfaced elsewhere (bootstrap) — ignore here so a
      // transient hiccup doesn't kill the screen.
    }
  }

  Future<void> _runQuery() async {
    final session = widget.session;
    if (session == null) return;
    final epoch = ++_queryEpoch;
    setState(() => _loading = true);
    try {
      final result = await session.app.runQuery(
        query: _query.text,
        subtagRule: onisync.SubtagRule.include,
        deletedRule: _showDeleted
            ? onisync.DeletedRule.include
            : onisync.DeletedRule.exclude,
      );
      if (!mounted || epoch != _queryEpoch) return;
      setState(() {
        _results = result;
        _error = null;
        _loading = false;
      });
    } catch (error) {
      if (!mounted || epoch != _queryEpoch) return;
      // Mid-typing tag tokens (`$fo`) legitimately fail to resolve; treat those
      // as "no matches" so the UI doesn't flash red at every keystroke. Other
      // errors (transport, etc.) still surface.
      final message = '$error';
      final looksLikeUnresolved =
          message.contains('NotFound') || message.contains('Ambiguous');
      setState(() {
        if (looksLikeUnresolved) {
          _results = const onisync.QueryEntries(files: [], tags: []);
          _error = null;
        } else {
          _error = message;
        }
        _loading = false;
      });
    }
  }

  /// If the current query text is a plausible bare tag name (non-empty, no
  /// whitespace, no query sigils) and the search returned zero tags, returns
  /// that name so the results view can offer to create it. Otherwise returns
  /// null and no "create" affordance is shown.
  ///
  /// The affordance is suppressed while [_showDeleted] is on: in that mode
  /// the empty result set means "no *deleted* tag matches", not "no tag by
  /// this name exists", so offering to create one would be misleading.
  String? get _createCandidate {
    if (_showDeleted) return null;
    final text = _query.text.trim();
    if (text.isEmpty) return null;
    if (text.contains(RegExp(r'[\s$!]'))) return null;
    final results = _results;
    if (results == null) return null;
    if (results.tags.isNotEmpty) return null;
    return text;
  }

  Future<void> _createTag(String name) async {
    final session = widget.session;
    if (session == null) return;
    try {
      // Pass an empty color; the engine substitutes its default palette entry
      // (see onisyncd::api::create_tag). The user can recolor via the tag
      // detail screen.
      await session.app.createTag(name: name, color: '');
      // The change stream will re-run the current query and the new tag will
      // appear in the results (matching `name` as a substring).
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to create tag: $error')),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    final publicKey = widget.session?.publicKey;
    return Scaffold(
      appBar: AppBar(
        title: const Text('OniSync'),
        actions: [
          _OperationsButton(session: widget.session),
          if (publicKey != null) _CopyPublicKeyButton(publicKey: publicKey),
        ],
      ),
      body: SafeArea(
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Padding(
              padding: const EdgeInsets.all(16),
              child: Row(
                children: [
                  Expanded(
                    child: _SearchBar(
                      controller: _query,
                      focusNode: _queryFocus,
                      loading: _loading,
                    ),
                  ),
                  const SizedBox(width: 8),
                  // Toggle: search live vs. tombstoned rows. When on, the
                  // daemon returns only soft-deleted files/tags for the same
                  // query text; when off, only live ones.
                  IconButton.filledTonal(
                    isSelected: _showDeleted,
                    tooltip: _showDeleted
                        ? 'Showing deleted — tap to search live'
                        : 'Search deleted files and tags',
                    icon: Icon(
                      _showDeleted
                          ? Icons.delete
                          : Icons.delete_outline,
                    ),
                    onPressed: () {
                      setState(() => _showDeleted = !_showDeleted);
                      // Re-run immediately if a query is already active so
                      // the mode change is visible without waiting for a
                      // keystroke.
                      if (_results != null) _runQuery();
                    },
                  ),
                ],
              ),
            ),
            Expanded(child: _buildResults()),
          ],
        ),
      ),
    );
  }

  Widget _buildResults() {
    final session = widget.session;
    if (session == null) {
      return const Center(child: Text('Connecting…'));
    }
    if (_error != null) {
      return Center(child: Text('Error: $_error'));
    }
    final results = _results;
    if (results == null) {
      // No query has run yet; leave the surface empty rather than
      // pre-populating it (which would require an eager listing).
      return const Center(
        child: Padding(
          padding: EdgeInsets.symmetric(horizontal: 32),
          child: Text(
            'Start typing to search files and tags.',
            textAlign: TextAlign.center,
          ),
        ),
      );
    }
    final createCandidate = _createCandidate;
    final hasTags = results.tags.isNotEmpty;
    final hasFiles = results.files.isNotEmpty;
    if (!hasTags && !hasFiles && createCandidate == null) {
      return const Center(child: Text('No matches.'));
    }
    return ListView(
      children: [
        if (hasTags || createCandidate != null) ...[
          const _SectionHeader('Tags'),
          for (final tag in results.tags)
            _TagRow(tag: tag, session: session),
          if (createCandidate != null)
            _CreateTagRow(
              name: createCandidate,
              onCreate: () => _createTag(createCandidate),
            ),
        ],
        if (hasFiles) ...[
          const _SectionHeader('Files'),
          for (final file in results.files)
            _FileRow(file: file, session: session),
        ],
      ],
    );
  }
}

class _SearchBar extends StatelessWidget {
  const _SearchBar({
    required this.controller,
    required this.focusNode,
    required this.loading,
  });

  final TextEditingController controller;
  final FocusNode focusNode;
  final bool loading;

  @override
  Widget build(BuildContext context) {
    return TextField(
      controller: controller,
      focusNode: focusNode,
      decoration: InputDecoration(
        prefixIcon: const Icon(Icons.search),
        hintText: 'Search files and tags',
        border: const OutlineInputBorder(),
        suffixIcon: loading
            ? const Padding(
                padding: EdgeInsets.all(12),
                child: SizedBox(
                  width: 16,
                  height: 16,
                  child: CircularProgressIndicator(strokeWidth: 2),
                ),
              )
            : (controller.text.isEmpty
                ? null
                : IconButton(
                    icon: const Icon(Icons.clear),
                    tooltip: 'Clear',
                    onPressed: () => controller.clear(),
                  )),
      ),
    );
  }
}

class _SectionHeader extends StatelessWidget {
  const _SectionHeader(this.label);

  final String label;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 12, 16, 4),
      child: Text(
        label,
        style: theme.textTheme.labelMedium?.copyWith(
          color: theme.colorScheme.onSurfaceVariant,
        ),
      ),
    );
  }
}

class _TagRow extends StatelessWidget {
  const _TagRow({required this.tag, required this.session});

  final onisync.TagEntry tag;
  final OniSyncSession session;

  @override
  Widget build(BuildContext context) {
    // Deleted rows only appear under the "show deleted" toggle; strike the
    // name through so the user can tell at a glance that a row is a
    // tombstone rather than a live tag.
    final titleStyle = tag.deleted
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    return ListTile(
      dense: true,
      leading: TagColorSwatch(color: tag.color),
      title: Text(tag.name, style: titleStyle),
      trailing: const Icon(Icons.chevron_right),
      onTap: () {
        // Drop focus from the search field before pushing so the soft keyboard
        // hides during navigation and — importantly — is not re-opened when
        // the user pops back to Home (Flutter otherwise restores focus to the
        // previously-focused text field on route resumption).
        FocusManager.instance.primaryFocus?.unfocus();
        Navigator.push(
          context,
          MaterialPageRoute(
            builder: (_) => TagDetailScreen(
              session: session,
              tagId: tag.tagId,
            ),
          ),
        );
      },
    );
  }
}

/// A one-off row rendered under the Tags section when the current query looks
/// like a plausible tag name and no tag with that name (or any substring
/// match) exists yet. Tapping it creates the tag with the engine's default
/// color; the user can recolor via the tag detail screen.
class _CreateTagRow extends StatelessWidget {
  const _CreateTagRow({required this.name, required this.onCreate});

  final String name;
  final VoidCallback onCreate;

  @override
  Widget build(BuildContext context) {
    return ListTile(
      dense: true,
      leading: const Icon(Icons.add),
      title: Text('Create tag "$name"'),
      onTap: onCreate,
    );
  }
}

class _FileRow extends StatelessWidget {
  const _FileRow({required this.file, required this.session});

  final onisync.FileEntry file;
  final OniSyncSession session;

  @override
  Widget build(BuildContext context) {
    // See `_TagRow` for why we strike deleted rows through.
    final titleStyle = file.deleted
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    return ListTile(
      dense: true,
      title: Text(file.path, style: titleStyle),
      trailing: const Icon(Icons.chevron_right),
      onTap: () {
        // See _TagRow.onTap for why we drop focus before navigating.
        FocusManager.instance.primaryFocus?.unfocus();
        Navigator.push(
          context,
          MaterialPageRoute(
            builder: (_) => FileDetailScreen(
              session: session,
              file: file,
            ),
          ),
        );
      },
    );
  }
}

/// AppBar action that opens the [OperationsScreen] (the live view of what the
/// daemon is doing). Always shown; disabled until a session is available.
///
/// It watches the operation stream itself so it can render a live badge with
/// the number of operations currently active. This includes steady-state
/// `peer_connected_*` rows, so the badge doubles as a connected-peer count when
/// nothing else is in flight.
class _OperationsButton extends StatefulWidget {
  const _OperationsButton({required this.session});

  final OniSyncSession? session;

  @override
  State<_OperationsButton> createState() => _OperationsButtonState();
}

class _OperationsButtonState extends State<_OperationsButton> {
  /// Currently-active operations, keyed by id. Includes steady-state
  /// peer-connection rows (see [_countsAsWork]).
  final Map<BigInt, onisync.OperationEntry> _working = {};

  bool _watching = false;

  @override
  void initState() {
    super.initState();
    if (widget.session != null) _watch();
  }

  @override
  void didUpdateWidget(covariant _OperationsButton oldWidget) {
    super.didUpdateWidget(oldWidget);
    // The session arrives asynchronously after connect; start watching once it
    // first appears.
    if (oldWidget.session == null && widget.session != null) _watch();
  }

  @override
  void dispose() {
    _watching = false;
    super.dispose();
  }

  /// Whether an operation should count toward the badge: any active operation
  /// (including steady-state peer connections).
  static bool _countsAsWork(onisync.OperationEntry op) {
    return op.status is onisync.OperationStatusDto_Active;
  }

  void _apply(onisync.OperationEntry op) {
    if (_countsAsWork(op)) {
      _working[op.id] = op;
    } else {
      _working.remove(op.id);
    }
  }

  Future<void> _watch() async {
    final session = widget.session;
    if (session == null || _watching) return;
    _watching = true;
    try {
      // Seed from a snapshot so an already-in-flight transfer is counted
      // immediately, then apply live updates on top.
      final snapshot = await session.app.listOperations();
      if (!mounted) return;
      setState(() {
        _working.clear();
        for (final op in snapshot) {
          _apply(op);
        }
      });

      final updates = await session.app.subscribeOperations();
      while (mounted && _watching) {
        final update = await updates.next();
        if (update == null) break;
        if (!mounted) break;
        switch (update) {
          case onisync.OperationUpdateDto_Resynced():
            final refreshed = await session.app.listOperations();
            if (!mounted) break;
            setState(() {
              _working.clear();
              for (final op in refreshed) {
                _apply(op);
              }
            });
          case onisync.OperationUpdateDto_Started(:final operation):
            setState(() => _apply(operation));
          case onisync.OperationUpdateDto_Updated(:final operation):
            setState(() => _apply(operation));
        }
      }
    } catch (_) {
      // Transient stream hiccups are surfaced elsewhere; don't kill the button.
    }
  }

  @override
  Widget build(BuildContext context) {
    final session = widget.session;
    final count = _working.length;
    final button = IconButton(
      icon: const Icon(Icons.sync),
      tooltip: 'Operations',
      onPressed: session == null
          ? null
          : () {
              FocusManager.instance.primaryFocus?.unfocus();
              Navigator.push(
                context,
                MaterialPageRoute(
                  builder: (_) => OperationsScreen(session: session),
                ),
              );
            },
    );

    if (count == 0) return button;

    // Overlay a small count badge on the top-right of the icon.
    return Stack(
      alignment: Alignment.center,
      children: [
        button,
        Positioned(
          top: 8,
          right: 6,
          child: Container(
            padding: const EdgeInsets.symmetric(horizontal: 5, vertical: 1),
            decoration: BoxDecoration(
              color: Theme.of(context).colorScheme.error,
              borderRadius: BorderRadius.circular(8),
            ),
            constraints: const BoxConstraints(minWidth: 16),
            child: Text(
              '$count',
              textAlign: TextAlign.center,
              style: TextStyle(
                color: Theme.of(context).colorScheme.onError,
                fontSize: 10,
                fontWeight: FontWeight.bold,
              ),
            ),
          ),
        ),
      ],
    );
  }
}

/// Android-only: AppBar action that copies this device's public key to the
/// clipboard. Rendered by [HomeScreen] only when the session carries a key.
class _CopyPublicKeyButton extends StatelessWidget {
  const _CopyPublicKeyButton({required this.publicKey});

  final String publicKey;

  @override
  Widget build(BuildContext context) {
    return IconButton(
      icon: const Icon(Icons.copy),
      tooltip: 'Copy public key',
      onPressed: () async {
        await Clipboard.setData(ClipboardData(text: publicKey));
      },
    );
  }
}
