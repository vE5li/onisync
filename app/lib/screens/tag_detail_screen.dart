// Per-tag detail screen: shows every property of a single tag (id, name,
// color swatch), the tag's parent tags (tags applied to this tag), and its
// subtags (children). Tap the Name or Color row to edit; each of the two tag
// sections has an Add button and per-chip remove; the AppBar action deletes
// the tag itself. Live-updates on the change stream so rename / recolor /
// external deletions land immediately (the screen pops itself if the tag
// disappears underneath it).
//
// The screen is keyed by [tagId] rather than by a captured [TagEntry] so it
// always reflects the current state of the store on rebuild.

import 'dart:async';

import 'package:flutter/material.dart';

import '../bootstrap/bootstrap.dart';
import '../rust/api.dart' as onisync;
import '../onisync_service.dart';
import '../widgets/property_tile.dart';
import '../widgets/tag_chip.dart';

class TagDetailScreen extends StatefulWidget {
  const TagDetailScreen({
    super.key,
    required this.session,
    required this.tagId,
  });

  final OniSyncSession session;
  final String tagId;

  @override
  State<TagDetailScreen> createState() => _TagDetailScreenState();
}

class _TagDetailScreenState extends State<TagDetailScreen> {
  onisync.TagEntry? _tag;

  /// Direct parent tags (tags applied to this tag). String ids for the wire,
  /// resolved to entries in [_relatedTags] for rendering.
  List<String> _parentTagIds = [];

  /// Direct subtags (children of this tag).
  List<String> _subtagIds = [];

  /// Name/color lookup for every tag id that appears in either section
  /// above. Bounded by parents + subtags — never a whole-store listing.
  Map<String, onisync.TagEntry> _relatedTags = {};

  bool _loading = true;
  String? _error;
  bool _deleted = false;
  bool _watching = false;
  bool _restoring = false;

  onisync.OniSyncApp get _app => widget.session.app;

  @override
  void initState() {
    super.initState();
    _load();
    _watch();
  }

  @override
  void dispose() {
    _watching = false;
    super.dispose();
  }

  Future<void> _watch() async {
    _watching = true;
    try {
      final events = await _app.subscribe();
      while (mounted && _watching) {
        final event = await events.next();
        if (event == null) break;
        if (!mounted) break;
        await _load();
      }
    } catch (_) {
      // See TagsScreen._watch: intentionally swallowed here.
    }
  }

  Future<void> _load() async {
    try {
      // Direct parents/subtags only (Exclude = no hierarchy walk). Matches the
      // file detail, which also shows direct membership.
      //
      // For the tag itself we pass `Include` so a tombstoned tag opened from
      // the home screen's "show deleted" toggle still loads (with its
      // `deleted` flag set). Parents/subtags are always live-only — a
      // tombstoned tag can't participate in a live hierarchy edge.
      final tag = await _app.getTagEntry(
        tagId: widget.tagId,
        deletedRule: onisync.DeletedRule.include,
      );
      final parents = await _app.tagIdsForTagString(
        tagId: widget.tagId,
        subtagRule: onisync.SubtagRule.exclude,
      );
      final subtags = await _app.subtagIdsForTagString(
        tagId: widget.tagId,
        subtagRule: onisync.SubtagRule.exclude,
      );
      // Resolve every related tag by id. Bounded by parents.length +
      // subtags.length; avoids the whole-store listing that `runQuery('')`
      // would do.
      final relatedIds = {...parents, ...subtags};
      final relatedEntries = await Future.wait(
        relatedIds.map((id) => _app.getTagEntry(
              tagId: id,
              deletedRule: onisync.DeletedRule.exclude,
            )),
      );
      if (!mounted) return;
      setState(() {
        _tag = tag;
        _parentTagIds = parents;
        _subtagIds = subtags;
        _relatedTags = {for (final t in relatedEntries) t.tagId: t};
        _loading = false;
        _error = null;
      });
    } catch (error) {
      if (!mounted) return;
      // `getTagEntry` rejects with `UnknownId` when the tag is gone; treat
      // that as "deleted underneath us" and pop back to the list. Other errors
      // (transport, etc.) surface normally.
      final isMissing = error is onisync.ApiError_UnknownId;
      setState(() {
        if (isMissing) {
          _tag = null;
          _error = null;
          if (!_deleted) {
            _deleted = true;
            WidgetsBinding.instance.addPostFrameCallback((_) {
              if (mounted) Navigator.of(context).maybePop();
            });
          }
        } else {
          _error = '$error';
        }
        _loading = false;
      });
    }
  }

  Future<void> _renameTag() async {
    final tag = _tag;
    if (tag == null) return;
    final result = await showDialog<String>(
      context: context,
      builder: (_) => _RenameTagDialog(initial: tag.name),
    );
    if (result == null) return;
    final trimmed = result.trim();
    if (trimmed.isEmpty || trimmed == tag.name) return;
    try {
      await _app.renameTagByString(tagId: tag.tagId, name: trimmed);
      // Live update flows in via the change stream.
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to rename tag: $error')),
      );
    }
  }

  Future<void> _recolorTag() async {
    final tag = _tag;
    if (tag == null) return;
    final result = await showDialog<String>(
      context: context,
      builder: (_) => _RecolorTagDialog(initial: tag.color),
    );
    if (result == null || result == tag.color) return;
    try {
      await _app.setTagColorByString(tagId: tag.tagId, color: result);
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to change color: $error')),
      );
    }
  }

  /// Open a picker of candidate tags (excluding self and any already-related
   /// ids in [excludeIds]) and return the chosen one, or null on cancel.
  ///
  /// TODO(perf/UX): this runs an empty `runQuery` to list every tag, purely
  /// to power the picker. Fetched on user tap (not on screen open) so the
  /// cost is deferred, but the picker itself still scans the whole tag store.
  /// Revisit — the right shape is likely a small search-in-picker that calls
  /// `runQuery` per keystroke, matching the home screen's model. Same
  /// treatment as FileDetailScreen._addTag.
  Future<onisync.TagEntry?> _pickTag({
    required String title,
    required Set<String> excludeIds,
  }) async {
    final onisync.QueryEntries all;
    try {
      // Tag-picker sheets only surface live tags — you can't apply a
      // tombstoned tag to anything.
      all = await _app.runQuery(
        query: '',
        subtagRule: onisync.SubtagRule.include,
        deletedRule: onisync.DeletedRule.exclude,
      );
    } catch (error) {
      _snack('Failed to load tags: $error');
      return null;
    }
    if (!mounted) return null;
    final candidates = all.tags
        .where((t) => t.tagId != widget.tagId && !excludeIds.contains(t.tagId))
        .toList();
    if (candidates.isEmpty) {
      _snack('No candidate tags.');
      return null;
    }
    return showModalBottomSheet<onisync.TagEntry>(
      context: context,
      builder: (_) => SafeArea(
        child: ListView(
          shrinkWrap: true,
          children: [
            ListTile(
              title: Text(title, style: const TextStyle(fontWeight: FontWeight.bold)),
            ),
            for (final tag in candidates)
              ListTile(
                leading: TagColorSwatch(color: tag.color),
                title: Text(tag.name),
                onTap: () => Navigator.pop(context, tag),
              ),
          ],
        ),
      ),
    );
  }

  Future<void> _addParent() async {
    final chosen = await _pickTag(
      title: 'Add tag',
      excludeIds: _parentTagIds.toSet(),
    );
    if (chosen == null) return;
    try {
      // The chosen tag becomes a parent of this tag: parent = chosen, subtag = this.
      await _app.tagTagByString(
        parentId: chosen.tagId,
        subtagId: widget.tagId,
      );
      // The change stream drives _load().
    } catch (error) {
      _snack('Failed to add parent: $error');
    }
  }

  Future<void> _addSubtag() async {
    final chosen = await _pickTag(
      title: 'Add subtag',
      excludeIds: _subtagIds.toSet(),
    );
    if (chosen == null) return;
    try {
      // The chosen tag becomes a subtag of this tag: parent = this, subtag = chosen.
      await _app.tagTagByString(
        parentId: widget.tagId,
        subtagId: chosen.tagId,
      );
    } catch (error) {
      _snack('Failed to add subtag: $error');
    }
  }

  Future<void> _removeParent(String parentId) async {
    try {
      await _app.untagTagByString(
        parentId: parentId,
        subtagId: widget.tagId,
      );
    } catch (error) {
      _snack('Failed to remove parent: $error');
    }
  }

  /// Push another [TagDetailScreen] for the given related tag. Used by the
  /// chips in the Tags / Subtags sections so the user can navigate the tag
  /// hierarchy without going back to search each time.
  void _openTag(String tagId) {
    Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => TagDetailScreen(
          session: widget.session,
          tagId: tagId,
        ),
      ),
    );
  }

  Future<void> _removeSubtag(String subtagId) async {
    try {
      await _app.untagTagByString(
        parentId: widget.tagId,
        subtagId: subtagId,
      );
    } catch (error) {
      _snack('Failed to remove subtag: $error');
    }
  }

  void _snack(String message) {
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(message)));
  }

  Future<void> _deleteTag() async {
    final tag = _tag;
    if (tag == null) return;
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (_) => AlertDialog(
        title: const Text('Confirmation'),
        content: Text('Delete "${tag.name}"?'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context, false),
            child: const Text('Cancel'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(context, true),
            child: const Text('Delete'),
          ),
        ],
      ),
    );
    if (confirmed != true) return;
    try {
      _deleted = true;
      await _app.deleteTagByString(tag.tagId);
      if (!mounted) return;
      Navigator.of(context).maybePop();
    } catch (error) {
      _deleted = false;
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to delete tag: $error')),
      );
    }
  }

  /// Restore a soft-deleted tag. Unlike a file, a tag carries no content, so
  /// this always succeeds for a known tag (it re-announces the definition with
  /// a fresh timestamp, winning last-writer-wins over the delete). On success
  /// we reload so the view re-renders as live.
  Future<void> _restoreTag() async {
    final tag = _tag;
    if (tag == null) return;
    setState(() => _restoring = true);
    try {
      await _app.restoreTagByString(tag.tagId);
      if (!mounted) return;
      _deleted = false;
      _snack('Restored "${tag.name}".');
      await _load();
    } catch (error) {
      if (!mounted) return;
      _snack('Failed to restore tag: $error');
    } finally {
      if (mounted) setState(() => _restoring = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    final tag = _tag;
    // See FileDetailScreen.build for the strikethrough / restore-swap
    // rationale; the two detail screens keep matching tombstone treatments.
    final titleStyle = tag?.deleted == true
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    return Scaffold(
      appBar: AppBar(
        title: Text(tag?.name ?? 'Tag', style: titleStyle),
        actions: [
          if (tag != null)
            (tag.deleted
                ? IconButton(
                    icon: _restoring
                        ? const SizedBox(
                            width: 20,
                            height: 20,
                            child: CircularProgressIndicator(strokeWidth: 2),
                          )
                        : const Icon(Icons.restore_from_trash),
                    tooltip: 'Restore tag',
                    onPressed: _restoring ? null : _restoreTag,
                  )
                : IconButton(
                    icon: const Icon(Icons.delete_outline),
                    tooltip: 'Delete tag',
                    onPressed: _deleteTag,
                  )),
        ],
      ),
      body: _buildBody(),
    );
  }

  Widget _buildBody() {
    if (_loading) return const Center(child: CircularProgressIndicator());
    if (_error != null) return Center(child: Text('Error: $_error'));
    final tag = _tag;
    if (tag == null) {
      // Post-frame pop is queued; render a neutral state in the meantime.
      return const SizedBox.shrink();
    }
    return ListView(
      padding: const EdgeInsets.symmetric(vertical: 8),
      children: [
        PropertyTile(
          label: 'Name',
          value: tag.name,
          trailing: const Icon(Icons.edit_outlined, size: 20),
          onTap: _renameTag,
        ),
        PropertyTile(
          label: 'Color',
          value: tag.color,
          trailing: TagColorSwatch(color: tag.color),
          onTap: _recolorTag,
          monospace: true,
        ),
        const SizedBox(height: 12),
        _TagsSection(
          title: 'Tags',
          tagIds: _parentTagIds,
          resolved: _relatedTags,
          onAdd: _addParent,
          onRemove: _removeParent,
          onTapTag: _openTag,
          emptyLabel: 'No tags.',
        ),
        const SizedBox(height: 16),
        _TagsSection(
          title: 'Subtags',
          tagIds: _subtagIds,
          resolved: _relatedTags,
          onAdd: _addSubtag,
          onRemove: _removeSubtag,
          onTapTag: _openTag,
          emptyLabel: 'No subtags.',
        ),
        const SizedBox(height: 24),
        PropertyTile(
          label: 'Tag id',
          value: tag.tagId,
          monospace: true,
          dense: true,
        ),
      ],
    );
  }
}

/// Renders a labelled group of tag chips, mirroring the "Tags" block on the
/// file detail screen. The Add button, per-chip tap, and per-chip X are all
/// wired to the parent state via callbacks so this widget stays stateless.
class _TagsSection extends StatelessWidget {
  const _TagsSection({
    required this.title,
    required this.tagIds,
    required this.resolved,
    required this.onAdd,
    required this.onRemove,
    required this.onTapTag,
    required this.emptyLabel,
  });

  final String title;
  final List<String> tagIds;
  final Map<String, onisync.TagEntry> resolved;
  final VoidCallback onAdd;
  final ValueChanged<String> onRemove;
  final ValueChanged<String> onTapTag;
  final String emptyLabel;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Text(
                title,
                style: theme.textTheme.labelMedium?.copyWith(
                  color: theme.colorScheme.onSurfaceVariant,
                  fontWeight: FontWeight.bold,
                ),
              ),
              const Spacer(),
              TextButton.icon(
                icon: const Icon(Icons.add, size: 18),
                label: const Text('Add'),
                onPressed: onAdd,
              ),
            ],
          ),
          if (tagIds.isEmpty)
            Text(emptyLabel)
          else
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                for (final tagId in tagIds) _chipFor(tagId),
              ],
            ),
        ],
      ),
    );
  }

  Widget _chipFor(String tagId) {
    final tag = resolved[tagId];
    if (tag == null) {
      // Related tag not resolved (e.g. race between _load steps). Show the
      // raw id so the row is still meaningful.
      return Chip(
        label: Text(tagId, style: const TextStyle(fontFamily: 'monospace')),
      );
    }
    return TagChip(
      tag: tag,
      onPressed: () => onTapTag(tagId),
      onDeleted: () => onRemove(tagId),
    );
  }
}

/// Prompts the user for a new tag name. Pops the entered string on submit,
/// or `null` on cancel.
class _RenameTagDialog extends StatefulWidget {
  const _RenameTagDialog({required this.initial});

  final String initial;

  @override
  State<_RenameTagDialog> createState() => _RenameTagDialogState();
}

class _RenameTagDialogState extends State<_RenameTagDialog> {
  late final TextEditingController _controller =
      TextEditingController(text: widget.initial);

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  void _submit() => Navigator.pop(context, _controller.text);

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('Rename tag'),
      content: TextField(
        controller: _controller,
        autofocus: true,
        decoration: const InputDecoration(labelText: 'Name'),
        onSubmitted: (_) => _submit(),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(context),
          child: const Text('Cancel'),
        ),
        TextButton(onPressed: _submit, child: const Text('Save')),
      ],
    );
  }
}

/// Lets the user pick a new color from [kTagColorPalette]. Pops the chosen
/// `#RRGGBB` string, or `null` on cancel.
class _RecolorTagDialog extends StatefulWidget {
  const _RecolorTagDialog({required this.initial});

  final String initial;

  @override
  State<_RecolorTagDialog> createState() => _RecolorTagDialogState();
}

class _RecolorTagDialogState extends State<_RecolorTagDialog> {
  late final TextEditingController _controller =
      TextEditingController(text: widget.initial);

  @override
  void initState() {
    super.initState();
    // Rebuild on every keystroke so the preview swatch, preset selection ring,
    // and Save-button enablement all track the live text value.
    _controller.addListener(() => setState(() {}));
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  /// Returns the normalized `#RRGGBB[AA]` form of the current input, or `null`
  /// if it doesn't parse. Accepts input with or without a leading `#` and
  /// treats it case-insensitively.
  String? get _normalized {
    var text = _controller.text.trim();
    if (text.startsWith('#')) text = text.substring(1);
    if (text.length != 6 && text.length != 8) return null;
    if (int.tryParse(text, radix: 16) == null) return null;
    return '#${text.toUpperCase()}';
  }

  @override
  Widget build(BuildContext context) {
    final normalized = _normalized;
    return AlertDialog(
      title: const Text('Tag color'),
      content: Column(
        mainAxisSize: MainAxisSize.min,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Expanded(
                child: TextField(
                  controller: _controller,
                  autofocus: true,
                  decoration: InputDecoration(
                    labelText: 'Hex color',
                    hintText: '#RRGGBB',
                    errorText: normalized == null && _controller.text.isNotEmpty
                        ? 'Expected #RRGGBB or #RRGGBBAA'
                        : null,
                  ),
                  onSubmitted: (_) {
                    if (normalized != null) Navigator.pop(context, normalized);
                  },
                ),
              ),
              const SizedBox(width: 12),
              // Live preview of whatever is currently typed. Falls back to grey
              // via [parseTagColor] when the input is invalid.
              TagColorSwatch(color: normalized ?? _controller.text),
            ],
          ),
          const SizedBox(height: 16),
          const Text('Presets'),
          const SizedBox(height: 8),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              for (final color in kTagColorPalette)
                GestureDetector(
                  onTap: () {
                    _controller.text = color;
                    _controller.selection = TextSelection.collapsed(
                      offset: color.length,
                    );
                  },
                  child: TagColorSwatch(
                    color: color,
                    selected: normalized != null &&
                        color.toUpperCase() == normalized,
                  ),
                ),
            ],
          ),
        ],
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(context),
          child: const Text('Cancel'),
        ),
        TextButton(
          onPressed: normalized == null
              ? null
              : () => Navigator.pop(context, normalized),
          child: const Text('Save'),
        ),
      ],
    );
  }
}
