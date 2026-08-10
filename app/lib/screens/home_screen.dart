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
import '../rust/api.dart' as tagsy;
import '../widgets/tag_chip.dart';
import 'file_detail_screen.dart';
import 'operations_screen.dart';
import 'tag_detail_screen.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key, required this.session});

  final TagsySession? session;

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
  tagsy.QueryEntries? _results;
  String? _error;
  bool _loading = false;

  /// When true, the search runs against soft-deleted (tombstoned) files and
  /// tags instead of live ones — see [tagsy.DeletedRule]. Toggled by the
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

  /// Stable pool of focus nodes for the visible result rows, in render
  /// order (tags → create-tag → files). The list grows lazily as the query
  /// returns more rows and is never shrunk — extra nodes just don't get
  /// attached — so a rebuild triggered by the change stream mid-navigation
  /// can't dispose the currently-focused node out from under the user
  /// (which used to cause focus to snap back to a previous row on Up).
  ///
  /// All entries are disposed in [dispose]; unused entries are cheap.
  final List<FocusNode> _rowFocus = [];

  /// Number of row focus nodes actually bound to a visible row in the most
  /// recent build. Used by the arrow-key handlers to clamp navigation and
  /// by [_handleSubmit] to decide whether there is anything to focus.
  int _activeRowCount = 0;

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
    for (final node in _rowFocus) {
      node.dispose();
    }
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
        subtagRule: tagsy.SubtagRule.include,
        deletedRule: _showDeleted
            ? tagsy.DeletedRule.include
            : tagsy.DeletedRule.exclude,
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
      final looksLikeUnresolved =
          error is tagsy.ApiError_UnknownId ||
          error is tagsy.ApiError_AmbiguousId;
      setState(() {
        if (looksLikeUnresolved) {
          _results = const tagsy.QueryEntries(files: [], tags: []);
          _error = null;
        } else {
          _error = '$error';
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

  /// Handle Enter in the search field.
  ///
  /// If the results list contains exactly one entry (across tags, the
  /// create-tag affordance, and files combined) we activate it directly —
  /// there's no ambiguity, and this preserves the fast "type + Enter to
  /// open" flow for common cases like resolving a query down to a single
  /// tag or offering to create a fresh tag name. Otherwise (two or more
  /// entries, or none) we hand focus to the first row instead, so the user
  /// can arrow-key their way to the desired result without tabbing past the
  /// AppBar actions.
  ///
  /// Flushes any pending debounced query first so Enter works even when the
  /// user types and immediately hits Enter, before the 200 ms debounce has
  /// fired.
  Future<void> _handleSubmit() async {
    final session = widget.session;
    if (session == null) return;
    if (_debounce?.isActive ?? false) {
      _debounce!.cancel();
      await _runQuery();
    }
    if (!mounted) return;
    final results = _results;
    if (results == null) return;
    final candidate = _createCandidate;
    final total =
        results.tags.length +
        (candidate != null ? 1 : 0) +
        results.files.length;
    if (total == 1) {
      if (results.tags.length == 1) {
        // Sole result is a tag; open it and restore focus to row 0 on
        // return so a subsequent Enter re-opens the same tag.
        await _openTag(results.tags.first, restoreIndex: 0);
      } else if (candidate != null) {
        await _createTag(candidate);
      } else {
        await _openFile(results.files.first, restoreIndex: 0);
      }
      return;
    }
    // Zero or 2+ results: move keyboard focus onto the first row (if any)
    // so arrow keys traverse the list. `_rowFocus[0]` is attached to
    // whichever row renders first in `_buildResults`.
    if (total >= 2 && _rowFocus.isNotEmpty) {
      _rowFocus[0].requestFocus();
    }
  }

  /// Ensure `_rowFocus` has at least [count] entries, creating new nodes on
  /// demand. Never shrinks — see the field docstring.
  void _ensureRowFocusCapacity(int count) {
    while (_rowFocus.length < count) {
      _rowFocus.add(FocusNode(debugLabel: 'row${_rowFocus.length}'));
    }
  }

  /// Index of the currently-focused row within `_rowFocus`, or -1 if none.
  /// We match by primary focus rather than `hasFocus` because parent focus
  /// scopes report `hasFocus == true` on ancestors too.
  int _focusedRowIndex() {
    final primary = FocusManager.instance.primaryFocus;
    if (primary == null) return -1;
    for (var i = 0; i < _activeRowCount; i++) {
      if (identical(_rowFocus[i], primary)) return i;
    }
    return -1;
  }

  /// ArrowDown on the results area: move to the next row, clamped at the
  /// last visible row. No wraparound — reaching the bottom just stays put.
  void _focusNextRow() {
    if (_activeRowCount == 0) return;
    final current = _focusedRowIndex();
    // If focus somehow drifted off-list, fall back to the first row.
    final next = current < 0 ? 0 : (current + 1).clamp(0, _activeRowCount - 1);
    _rowFocus[next].requestFocus();
    _ensureRowVisible(next, ScrollPositionAlignmentPolicy.keepVisibleAtEnd);
  }

  /// ArrowUp on the results area: move to the previous row. From row 0,
  /// jump back to the search field so the user can keep typing without
  /// having to Shift-Tab through anything.
  void _focusPreviousRow() {
    if (_activeRowCount == 0) return;
    final current = _focusedRowIndex();
    if (current <= 0) {
      _queryFocus.requestFocus();
      return;
    }
    final prev = current - 1;
    _rowFocus[prev].requestFocus();
    _ensureRowVisible(prev, ScrollPositionAlignmentPolicy.keepVisibleAtStart);
  }

  /// Escape on the results area: return focus to the search field.
  void _focusSearchField() {
    _queryFocus.requestFocus();
  }

  /// Push the tag detail screen and, on return, put keyboard focus back on
  /// the row the user came from so keyboard navigation resumes where it
  /// left off.
  Future<void> _openTag(
    tagsy.TagEntry tag, {
    required int restoreIndex,
  }) async {
    // Drop focus before pushing so Flutter's automatic focus restoration
    // doesn't re-focus the search field (which would also re-open the soft
    // keyboard on mobile). We put focus back explicitly on return.
    FocusManager.instance.primaryFocus?.unfocus();
    await Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) =>
            TagDetailScreen(session: widget.session!, tagId: tag.tagId),
      ),
    );
    if (!mounted) return;
    _restoreRowFocus(restoreIndex);
  }

  /// See [_openTag].
  Future<void> _openFile(
    tagsy.FileEntry file, {
    required int restoreIndex,
  }) async {
    FocusManager.instance.primaryFocus?.unfocus();
    await Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => FileDetailScreen(session: widget.session!, file: file),
      ),
    );
    if (!mounted) return;
    _restoreRowFocus(restoreIndex);
  }

  /// Best-effort: put keyboard focus back on `_rowFocus[index]`. If the
  /// results have shrunk while we were away, clamp to the last visible
  /// row; if there are no rows at all, refocus the search field. Also
  /// scrolls the row into view since it may have been off-screen when the
  /// user activated it — for restore we center the row rather than doing
  /// a minimal scroll, since the user has lost the visual thread.
  void _restoreRowFocus(int index) {
    if (_activeRowCount == 0) {
      _queryFocus.requestFocus();
      return;
    }
    final clamped = index.clamp(0, _activeRowCount - 1);
    _rowFocus[clamped].requestFocus();
  }

  /// Scroll `_rowFocus[index]` into view. Uses a post-frame callback so
  /// this works both when the row is already laid out (arrow-key
  /// navigation) and when the tree is mid-rebuild (returning from a
  /// detail screen).
  void _ensureRowVisible(int index, ScrollPositionAlignmentPolicy policy) {
    final node = _rowFocus[index];
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (!mounted) return;
      final ctx = node.context;
      if (ctx == null) return;
      Scrollable.ensureVisible(
        ctx,
        alignment: 0.0,
        alignmentPolicy: policy,
        duration: const Duration(milliseconds: 150),
      );
    });
  }

  Future<void> _createTag(String name) async {
    final session = widget.session;
    if (session == null) return;
    try {
      // Pass an empty color; the engine substitutes its default palette entry
      // (see tagsyd::api::create_tag). The user can recolor via the tag
      // detail screen.
      await session.app.createTag(name: name, color: '');
      // The change stream will re-run the current query and the new tag will
      // appear in the results (matching `name` as a substring).
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Failed to create tag: $error')));
    }
  }

  @override
  Widget build(BuildContext context) {
    final publicKey = widget.session?.publicKey;
    return Scaffold(
      appBar: AppBar(
        title: const Text('Tagsy'),
        actions: [
          // Toggle: search live vs. tombstoned rows. When on, the daemon
          // returns only soft-deleted files/tags for the same query text;
          // when off, only live ones.
          IconButton(
            isSelected: _showDeleted,
            tooltip: _showDeleted
                ? 'Showing deleted — tap to search live'
                : 'Search deleted files and tags',
            icon: Icon(_showDeleted ? Icons.delete : Icons.delete_outline),
            onPressed: () {
              setState(() => _showDeleted = !_showDeleted);
              // Re-run immediately if a query is already active so the mode
              // change is visible without waiting for a keystroke.
              if (_results != null) _runQuery();
            },
          ),
          _OperationsButton(session: widget.session),
          _PurgePreviewsButton(session: widget.session),
          if (publicKey != null) _CopyPublicKeyButton(publicKey: publicKey),
        ],
      ),
      body: SafeArea(
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Padding(
              padding: const EdgeInsets.all(16),
              child: _SearchBar(
                controller: _query,
                focusNode: _queryFocus,
                loading: _loading,
                onSubmitted: _handleSubmit,
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
      _activeRowCount = 0;
      return const Center(child: Text('No matches.'));
    }
    // Allocate a stable FocusNode per visible row in render order (tags →
    // create-tag → files). Using state-owned nodes for every row — not just
    // the first — is critical: rows without an explicit FocusNode get an
    // implicit one owned by the ListTile, which is disposed and recreated
    // on every rebuild. Change-stream-driven rebuilds during arrow-key
    // navigation would otherwise pull the focused node out from under the
    // user, causing the "Up bounces back down" symptom.
    final totalRows =
        results.tags.length +
        (createCandidate != null ? 1 : 0) +
        results.files.length;
    _ensureRowFocusCapacity(totalRows);
    _activeRowCount = totalRows;
    // Row index within the flat `_rowFocus` array, incremented as we emit
    // each interactive row so the tap and Enter handlers can pass the
    // right restore index back to `_openTag` / `_openFile`.
    var rowIndex = 0;
    final children = <Widget>[];
    if (hasTags || createCandidate != null) {
      children.add(const _SectionHeader('Tags'));
      for (final tag in results.tags) {
        final index = rowIndex++;
        children.add(
          _TagRow(
            tag: tag,
            focusNode: _rowFocus[index],
            onActivate: () => _openTag(tag, restoreIndex: index),
          ),
        );
      }
      if (createCandidate != null) {
        // The create-tag row doesn't push a route, so nothing to restore
        // focus to; `_createTag` returns and the results list mutates.
        children.add(
          _CreateTagRow(
            name: createCandidate,
            onCreate: () => _createTag(createCandidate),
            focusNode: _rowFocus[rowIndex++],
          ),
        );
      }
    }
    if (hasFiles) {
      children.add(const _SectionHeader('Files'));
      for (final file in results.files) {
        final index = rowIndex++;
        children.add(
          _FileRow(
            file: file,
            focusNode: _rowFocus[index],
            onActivate: () => _openFile(file, restoreIndex: index),
          ),
        );
      }
    }
    return CallbackShortcuts(
      bindings: {
        const SingleActivator(LogicalKeyboardKey.arrowDown): _focusNextRow,
        const SingleActivator(LogicalKeyboardKey.arrowUp): _focusPreviousRow,
        const SingleActivator(LogicalKeyboardKey.escape): _focusSearchField,
      },
      child: ListView(children: children),
    );
  }
}

class _SearchBar extends StatelessWidget {
  const _SearchBar({
    required this.controller,
    required this.focusNode,
    required this.loading,
    required this.onSubmitted,
  });

  final TextEditingController controller;
  final FocusNode focusNode;
  final bool loading;

  /// Invoked when the user presses Enter in the field. Wired to
  /// [_HomeScreenState._handleSubmit], which either activates the sole
  /// result or hands focus to the first result row, so keyboard users
  /// don't have to tab past the AppBar actions to reach the list.
  final Future<void> Function() onSubmitted;

  @override
  Widget build(BuildContext context) {
    return TextField(
      controller: controller,
      focusNode: focusNode,
      onSubmitted: (_) => onSubmitted(),
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
  const _TagRow({
    required this.tag,
    required this.focusNode,
    required this.onActivate,
  });

  final tagsy.TagEntry tag;

  /// Stable state-owned focus node for this row's slot in the list. See
  /// [_HomeScreenState._rowFocus] for why every row needs one.
  final FocusNode focusNode;

  /// Invoked on tap or Enter. Navigation lives on the state so it can
  /// restore focus to this row's slot when the detail screen pops.
  final VoidCallback onActivate;

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
      focusNode: focusNode,
      leading: TagColorSwatch(color: tag.color),
      title: Text(tag.name, style: titleStyle),
      trailing: const Icon(Icons.chevron_right),
      onTap: onActivate,
    );
  }
}

/// A one-off row rendered under the Tags section when the current query looks
/// like a plausible tag name and no tag with that name (or any substring
/// match) exists yet. Tapping it creates the tag with the engine's default
/// color; the user can recolor via the tag detail screen.
class _CreateTagRow extends StatelessWidget {
  const _CreateTagRow({
    required this.name,
    required this.onCreate,
    required this.focusNode,
  });

  final String name;
  final VoidCallback onCreate;

  /// See [_TagRow.focusNode].
  final FocusNode focusNode;

  @override
  Widget build(BuildContext context) {
    return ListTile(
      dense: true,
      focusNode: focusNode,
      leading: const Icon(Icons.add),
      title: Text('Create tag "$name"'),
      onTap: onCreate,
    );
  }
}

class _FileRow extends StatelessWidget {
  const _FileRow({
    required this.file,
    required this.focusNode,
    required this.onActivate,
  });

  final tagsy.FileEntry file;

  /// See [_TagRow.focusNode].
  final FocusNode focusNode;

  /// See [_TagRow.onActivate].
  final VoidCallback onActivate;

  @override
  Widget build(BuildContext context) {
    // See `_TagRow` for why we strike deleted rows through.
    final titleStyle = file.deleted
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    return ListTile(
      dense: true,
      focusNode: focusNode,
      title: Text(file.path, style: titleStyle),
      trailing: const Icon(Icons.chevron_right),
      onTap: onActivate,
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

  final TagsySession? session;

  @override
  State<_OperationsButton> createState() => _OperationsButtonState();
}

class _OperationsButtonState extends State<_OperationsButton> {
  /// Currently-active operations, keyed by id. Includes steady-state
  /// peer-connection rows (see [_countsAsWork]).
  final Map<BigInt, tagsy.OperationEntry> _working = {};

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
  static bool _countsAsWork(tagsy.OperationEntry op) {
    return op.status is tagsy.OperationStatusDto_Active;
  }

  void _apply(tagsy.OperationEntry op) {
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
          case tagsy.OperationUpdateDto_Resynced():
            final refreshed = await session.app.listOperations();
            if (!mounted) break;
            setState(() {
              _working.clear();
              for (final op in refreshed) {
                _apply(op);
              }
            });
          case tagsy.OperationUpdateDto_Started(:final operation):
            setState(() => _apply(operation));
          case tagsy.OperationUpdateDto_Updated(:final operation):
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

/// AppBar action that purges the daemon's cached file previews, forcing them to
/// regenerate on demand. Useful after the set of previewable file types changes
/// (e.g. new PDF/video support). Disabled while no session is attached and while
/// a purge is in flight.
class _PurgePreviewsButton extends StatefulWidget {
  const _PurgePreviewsButton({required this.session});

  final TagsySession? session;

  @override
  State<_PurgePreviewsButton> createState() => _PurgePreviewsButtonState();
}

class _PurgePreviewsButtonState extends State<_PurgePreviewsButton> {
  bool _purging = false;

  Future<void> _purge() async {
    final session = widget.session;
    if (session == null || _purging) return;

    setState(() => _purging = true);
    try {
      final purged = await session.app.purgePreviews();
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Purged $purged cached previews')));
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to purge previews: $error')),
      );
    } finally {
      if (mounted) setState(() => _purging = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return IconButton(
      icon: _purging
          ? const SizedBox(
              width: 18,
              height: 18,
              child: CircularProgressIndicator(strokeWidth: 2),
            )
          : const Icon(Icons.image_not_supported_outlined),
      tooltip: 'Purge cached previews',
      onPressed: widget.session == null || _purging ? null : _purge,
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
