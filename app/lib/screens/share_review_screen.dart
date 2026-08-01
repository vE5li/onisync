// Share-review: the interstitial shown when files are shared into onisync via
// the Android share sheet. Instead of uploading immediately, this screen lets
// the user attach tags to the incoming file(s) first, then uploads them with
// those tags applied. It mirrors the file detail screen's top preview so the
// user can see what they're about to ingest.
//
// One preview is shown per shared file (each capped in height like the detail
// screen). The chosen tags apply to *all* files in the batch — the common case
// is sharing a handful of related files that want the same tags. The user can
// also create a brand-new tag inline from the picker.

import 'dart:io';

import 'package:flutter/material.dart';

import '../bootstrap/bootstrap.dart';
import '../rust/api.dart' as onisync;
import '../widgets/file_preview.dart';
import '../widgets/tag_chip.dart';

class ShareReviewScreen extends StatefulWidget {
  const ShareReviewScreen({
    super.key,
    required this.session,
    required this.paths,
  });

  final OniSyncSession session;

  /// Absolute on-disk paths of the shared files to review and upload. Never
  /// empty (the share handler drops empty batches before navigating).
  final List<String> paths;

  @override
  State<ShareReviewScreen> createState() => _ShareReviewScreenState();
}

class _ShareReviewScreenState extends State<ShareReviewScreen> {
  onisync.OniSyncApp get _app => widget.session.app;

  /// Tags the user has picked to apply to the whole batch, keyed by string id
  /// so we can render chips and de-dupe against the picker.
  final Map<String, onisync.TagEntry> _selected = {};

  bool _uploading = false;

  /// Derive a display/logical name from a source path (last path segment),
  /// matching the engine's ingestion-boundary convention.
  static String _nameFor(String path) => path.split('/').last;

  Future<void> _addTag() async {
    // Mirror the file detail screen's picker: list every live tag and let the
    // user pick one not already selected. (Same whole-store scan TODO applies.)
    final onisync.QueryEntries all;
    try {
      all = await _app.runQuery(
        query: '',
        subtagRule: onisync.SubtagRule.include,
        deletedRule: onisync.DeletedRule.exclude,
      );
    } catch (error) {
      _snack('Failed to load tags: $error');
      return;
    }
    if (!mounted) return;
    final available =
        all.tags.where((t) => !_selected.containsKey(t.tagId)).toList();

    final chosen = await showModalBottomSheet<onisync.TagEntry>(
      context: context,
      builder: (sheetContext) => SafeArea(
        child: ListView(
          shrinkWrap: true,
          children: [
            const ListTile(
              title: Text(
                'Add tag',
                style: TextStyle(fontWeight: FontWeight.bold),
              ),
            ),
            ListTile(
              leading: const Icon(Icons.add),
              title: const Text('Create new tag'),
              onTap: () async {
                // Capture the sheet's navigator before the async gap so we can
                // dismiss it (returning the created tag) once creation resolves.
                final navigator = Navigator.of(sheetContext);
                final created = await _createTag();
                navigator.pop(created);
              },
            ),
            if (available.isEmpty)
              const ListTile(title: Text('No more tags to add.'))
            else
              for (final tag in available)
                ListTile(
                  leading: TagColorSwatch(color: tag.color),
                  title: Text(tag.name),
                  onTap: () => Navigator.pop(context, tag),
                ),
          ],
        ),
      ),
    );
    if (chosen == null) return;
    setState(() => _selected[chosen.tagId] = chosen);
  }

  /// Prompt for a tag name, create it, and return its fresh [onisync.TagEntry]
  /// (or null on cancel / failure). The engine substitutes a default palette
  /// color for the empty color, matching the home screen's create flow.
  ///
  /// `createTag` hands back the new tag's string id, which we use to fetch its
  /// flattened [onisync.TagEntry] (name + engine-assigned color) for the chip
  /// and later upload-time resolution.
  Future<onisync.TagEntry?> _createTag() async {
    final name = await showDialog<String>(
      context: context,
      builder: (_) => const _CreateTagDialog(),
    );
    final trimmed = name?.trim();
    if (trimmed == null || trimmed.isEmpty) return null;
    try {
      final tagId = await _app.createTag(name: trimmed, color: '');
      return await _app.getTagEntry(
        tagId: tagId,
        deletedRule: onisync.DeletedRule.exclude,
      );
    } catch (error) {
      _snack('Failed to create tag: $error');
      return null;
    }
  }

  void _removeTag(String tagId) {
    setState(() => _selected.remove(tagId));
  }

  Future<void> _upload() async {
    setState(() => _uploading = true);
    // Apply the selected tags (by string id) to every file in the batch. The
    // bridge resolves the ids per call, so the same list is safely reused
    // across uploads — unlike opaque TagId handles, which are consumed on use.
    final tagIds = _selected.keys.toList();

    var uploaded = 0;
    for (final path in widget.paths) {
      try {
        await _app.uploadFile(
          path: path,
          pathName: _nameFor(path),
          tags: tagIds,
        );
        uploaded++;
      } catch (error) {
        _snack('Failed to upload $path: $error');
      }
    }
    if (!mounted) return;
    if (uploaded > 0) {
      _snack('Uploaded $uploaded file${uploaded == 1 ? '' : 's'} to onisync');
    }
    Navigator.of(context).maybePop();
  }

  void _snack(String message) {
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(message)));
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final multiple = widget.paths.length > 1;
    return Scaffold(
      appBar: AppBar(
        title: Text(multiple ? 'Share ${widget.paths.length} files' : 'Share file'),
      ),
      body: ListView(
        padding: const EdgeInsets.symmetric(vertical: 8),
        children: [
          for (final path in widget.paths) _buildPreview(context, path),
          const SizedBox(height: 16),
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    Text(
                      'Tags',
                      style: theme.textTheme.labelMedium?.copyWith(
                        color: theme.colorScheme.onSurfaceVariant,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    const Spacer(),
                    TextButton.icon(
                      icon: const Icon(Icons.add, size: 18),
                      label: const Text('Add'),
                      onPressed: _uploading ? null : _addTag,
                    ),
                  ],
                ),
                if (_selected.isEmpty)
                  const Text('No tags selected.')
                else
                  Wrap(
                    spacing: 8,
                    runSpacing: 8,
                    children: [
                      for (final tag in _selected.values)
                        TagChip(
                          tag: tag,
                          onDeleted:
                              _uploading ? null : () => _removeTag(tag.tagId),
                        ),
                    ],
                  ),
              ],
            ),
          ),
          const SizedBox(height: 24),
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 16),
            child: FilledButton.icon(
              icon: _uploading
                  ? const SizedBox(
                      width: 18,
                      height: 18,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : const Icon(Icons.cloud_upload_outlined),
              label: Text(_uploading ? 'Uploading…' : 'Upload'),
              onPressed: _uploading ? null : _upload,
            ),
          ),
        ],
      ),
    );
  }

  /// A shared file's inline preview, capped in height, mirroring the file
  /// detail screen's top preview. When more than one file is shared, each is
  /// labelled with its name so the user can tell them apart.
  Widget _buildPreview(BuildContext context, String path) {
    final theme = Theme.of(context);
    final header = Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 4),
      child: Text(
        widget.paths.length > 1 ? _nameFor(path) : 'Preview',
        style: theme.textTheme.labelMedium?.copyWith(
          color: theme.colorScheme.onSurfaceVariant,
          fontWeight: FontWeight.bold,
        ),
        overflow: TextOverflow.ellipsis,
      ),
    );
    final body = File(path).existsSync()
        ? ConstrainedBox(
            constraints: const BoxConstraints(maxHeight: 360),
            child: FilePreview(path: path),
          )
        : const ListTile(
            leading: Icon(Icons.error_outline),
            title: Text('File not available'),
            subtitle: Text('The shared file could not be read.'),
          );
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [header, body],
    );
  }
}

/// Prompts the user for a new tag name. Pops the entered string on submit, or
/// `null` on cancel. Empty input is filtered by the caller.
class _CreateTagDialog extends StatefulWidget {
  const _CreateTagDialog();

  @override
  State<_CreateTagDialog> createState() => _CreateTagDialogState();
}

class _CreateTagDialogState extends State<_CreateTagDialog> {
  final TextEditingController _controller = TextEditingController();

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  void _submit() => Navigator.pop(context, _controller.text);

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('Create tag'),
      content: TextField(
        controller: _controller,
        autofocus: true,
        decoration: const InputDecoration(labelText: 'Tag name'),
        onSubmitted: (_) => _submit(),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(context),
          child: const Text('Cancel'),
        ),
        TextButton(onPressed: _submit, child: const Text('Create')),
      ],
    );
  }
}
