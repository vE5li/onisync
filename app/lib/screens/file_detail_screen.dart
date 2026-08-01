// File detail: show a file's fields (path, id, hash, version), the tags
// applied to it, and let the user rename (change the logical path), add/remove
// tags, or delete the file. Live-updates on the change stream so external
// changes / peer syncs / our own mutations all land immediately; if the file
// disappears underneath us the screen pops itself back to the previous route.
//
// Keyed by [fileId] rather than by a captured [FileEntry] so the display
// always reflects the current state of the store on rebuild.

import 'dart:io';

import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';
import 'package:share_plus/share_plus.dart';

import '../bootstrap/bootstrap.dart';
import '../rust/api.dart' as onisync;
import '../onisync_service.dart';
import '../widgets/file_preview.dart';
import '../widgets/property_tile.dart';
import '../widgets/tag_chip.dart';
import 'tag_detail_screen.dart';

class FileDetailScreen extends StatefulWidget {
  FileDetailScreen({
    super.key,
    required this.session,
    required onisync.FileEntry file,
  }) : fileId = file.fileId;

  final OniSyncSession session;

  /// The string id of the file to display. The constructor takes a full
  /// [onisync.FileEntry] for convenience at call sites (list rows already
  /// have one), but the screen retains only its id and refetches the entry
  /// itself so it always reflects the current state of the store.
  final String fileId;

  @override
  State<FileDetailScreen> createState() => _FileDetailScreenState();
}

class _FileDetailScreenState extends State<FileDetailScreen> {
  onisync.FileEntry? _file;

  /// Tags currently applied to this file, keyed by string id (for name/color
  /// lookup when rendering the chips). Bounded by the number of applied tags,
  /// so we fetch these one-by-one rather than pulling every tag in the store.
  Map<String, onisync.TagEntry> _appliedTags = {};

  /// The string ids of tags currently applied to this file (direct only).
  List<String> _appliedTagIds = [];

  /// Absolute on-disk path where this file's bytes currently live locally, or
  /// `null` if no sync directory on this device holds a copy. Refreshed on
  /// every [_load] so a fetch/eviction elsewhere shows up in the preview.
  String? _localPath;

  bool _loading = true;
  String? _error;
  bool _deleted = false;
  bool _watching = false;
  bool _restoring = false;
  bool _sharing = false;
  bool _downloading = false;

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
      // Stream errors are surfaced elsewhere (bootstrap) — ignore here so a
      // transient hiccup doesn't kill the screen.
    }
  }

  Future<void> _load() async {
    try {
      // Fetch the file itself, its applied tag ids, and each applied tag's row.
      // All three stay bounded by "this file"; nothing scans the whole store.
      //
      // For the file itself we pass `Include` so a tombstoned file opened from
      // the home screen's "show deleted" toggle still loads (with its
      // `deleted` flag set). Applied tags are always live-only — a
      // tombstoned tag can't be applied to anything.
      final file = await _app.getFileEntry(
        fileId: widget.fileId,
        deletedRule: onisync.DeletedRule.include,
      );
      // Direct tags only (Exclude = no subtag recursion) — these are the ones
      // the user can meaningfully add/remove on this file.
      final applied = await _app.tagIdsForFileString(
        fileId: widget.fileId,
        subtagRule: onisync.SubtagRule.exclude,
      );
      final entries = await Future.wait(
        applied.map((id) => _app.getTagEntry(
              tagId: id,
              deletedRule: onisync.DeletedRule.exclude,
            )),
      );
      // Best-effort: absence (not-synced-here) is expected, not an error. Any
      // hard failure surfaces below as `_error` via the outer catch.
      final localPath =
          await _app.localPathForFileByString(fileId: widget.fileId);
      if (!mounted) return;
      setState(() {
        _file = file;
        _appliedTagIds = applied;
        _appliedTags = {for (final t in entries) t.tagId: t};
        _localPath = localPath;
        _loading = false;
        _error = null;
      });
    } catch (error) {
      if (!mounted) return;
      // `getFileEntry` (or a tag lookup on a just-deleted-then-recreated race)
      // throws NotFound when the entity is gone; treat NotFound on the file
      // itself as "deleted underneath us" and pop back to the previous route.
      final isMissing = '$error'.contains('NotFound');
      setState(() {
        if (isMissing) {
          _file = null;
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

  Future<void> _renameFile() async {
    final file = _file;
    if (file == null) return;
    final result = await showDialog<String>(
      context: context,
      builder: (_) => _RenameFileDialog(initial: file.path),
    );
    if (result == null) return;
    final trimmed = result.trim();
    if (trimmed.isEmpty || trimmed == file.path) return;
    try {
      await _app.moveFileByString(
        fileId: widget.fileId,
        logicalPath: trimmed,
      );
      // Live update flows in via the change stream.
    } catch (error) {
      _snack('Failed to rename file: $error');
    }
  }

  Future<void> _removeTag(String tagId) async {
    try {
      await _app.untagFileByString(tagId: tagId, fileId: widget.fileId);
    } catch (error) {
      _snack('Failed to remove tag: $error');
    }
  }

  Future<void> _addTag() async {
    // TODO(perf/UX): this runs an empty `runQuery` to list every tag, purely
    // to power the picker. It's deferred until the user taps Add (so file
    // detail opens don't pay the cost), but the picker itself still scans the
    // whole tag store. Revisit — the right shape is likely a small
    // search-in-picker that calls `runQuery` per keystroke, matching the home
    // screen's model. Same TODO applies to TagDetailScreen._pickTag.
    final onisync.QueryEntries all;
    try {
      // Tag-picker sheets only surface live tags — you can't apply a
      // tombstoned tag to a file.
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
    final applied = _appliedTagIds.toSet();
    final available =
        all.tags.where((t) => !applied.contains(t.tagId)).toList();
    if (available.isEmpty) {
      _snack('No more tags to add.');
      return;
    }
    final chosen = await showModalBottomSheet<onisync.TagEntry>(
      context: context,
      builder: (_) => SafeArea(
        child: ListView(
          shrinkWrap: true,
          children: [
            const ListTile(title: Text('Add tag', style: TextStyle(fontWeight: FontWeight.bold))),
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
    try {
      await _app.tagFileByString(tagId: chosen.tagId, fileId: widget.fileId);
    } catch (error) {
      _snack('Failed to add tag: $error');
    }
  }

  Future<void> _deleteFile() async {
    final file = _file;
    if (file == null) return;
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (_) => AlertDialog(
        title: const Text('Confirmation'),
        content: Text('Delete "${file.path}"?'),
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
      await _app.deleteFileByString(widget.fileId);
      if (!mounted) return;
      Navigator.of(context).maybePop();
    } catch (error) {
      _deleted = false;
      _snack('Failed to delete file: $error');
    }
  }

  /// Restore a soft-deleted file. Best-effort: the daemon only succeeds if the
  /// bytes are still recoverable (a local `keep_deleted_files` vault or a
  /// connected peer that still holds them); otherwise it fails and the file
  /// stays deleted. On success we reload so the view re-renders as live.
  Future<void> _restoreFile() async {
    final file = _file;
    if (file == null) return;
    setState(() => _restoring = true);
    try {
      await _app.restoreFileByString(widget.fileId);
      if (!mounted) return;
      _deleted = false;
      _snack('Restored "${file.path}".');
      await _load();
    } catch (error) {
      if (!mounted) return;
      // NotFound means no source still holds the bytes — the best-effort
      // restore failed and the file remains deleted.
      final message = '$error'.contains('NotFound')
          ? 'Cannot restore: the file\'s contents are no longer available on '
              'any device.'
          : 'Failed to restore file: $error';
      _snack(message);
    } finally {
      if (mounted) setState(() => _restoring = false);
    }
  }

  /// Hand this file to the OS share sheet (Android only — the button is gated
  /// on the mobile-only session hint). If a local sync directory already holds
  /// the bytes we share that path directly; otherwise we fetch the content to a
  /// daemon-owned temp file first (from a peer if needed) and share that.
  ///
  /// A locally-held path is shared in place. A fetched temp file is handed to
  /// us with move semantics: we rename it under a dedicated share dir so its
  /// filename matches the logical name (see below), then delete that dir after
  /// the share sheet returns.
  Future<void> _shareFile() async {
    final file = _file;
    if (file == null) return;
    setState(() => _sharing = true);
    // A temp directory we fully own for this share, deleted in `finally`. Only
    // created when we fetch (a local file is shared in place, untouched).
    Directory? shareDir;
    // The logical file name — its extension is what receiving apps use to infer
    // the type, so the shared file on disk MUST carry it (an `XFile.name`
    // override is not enough: many targets read the path's extension, and the
    // daemon's fetched temp file is named with an extension-less UUID).
    final name = file.path.split('/').last;
    try {
      var path = _localPath;
      if (path == null) {
        // Not present locally: fetch the bytes to a daemon-owned temp file...
        final fetched = await _app.fetchFileByString(
          fileId: widget.fileId,
          expectedHash: file.contentHash,
        );
        // ...then move it to `<temp>/onisync_share/<logicalName>` so the shared
        // file has the correct name + extension. A per-share subdir avoids
        // collisions when the logical name repeats across shares.
        final base = await getTemporaryDirectory();
        shareDir = Directory(
          '${base.path}/onisync_share/${DateTime.now().microsecondsSinceEpoch}',
        );
        await shareDir.create(recursive: true);
        final named = '${shareDir.path}/$name';
        final fetchedFile = File(fetched);
        try {
          // Cheap path: same filesystem, just relink.
          await fetchedFile.rename(named);
        } on FileSystemException {
          // The daemon's fetch temp dir may be on a different mount than the
          // app temp dir; rename can't cross filesystems, so copy then delete.
          await fetchedFile.copy(named);
          try {
            await fetchedFile.delete();
          } catch (_) {
            // Best-effort; the original still lives in the daemon temp dir.
          }
        }
        path = named;
      }
      await Share.shareXFiles([XFile(path, name: name)]);
    } catch (error) {
      final message = '$error'.contains('NotFound')
          ? 'Cannot share: the file\'s contents are not available on any '
              'device.'
          : 'Failed to share file: $error';
      _snack(message);
    } finally {
      // Clean up the fetched copy (move semantics); best-effort.
      if (shareDir != null) {
        try {
          await shareDir.delete(recursive: true);
        } catch (_) {
          // Nothing to do if cleanup fails; it lives in a temp dir.
        }
      }
      if (mounted) setState(() => _sharing = false);
    }
  }

  /// Copy this file into the device's public Downloads directory (Android only
  /// — the button is gated on the mobile-only [OniSyncSession.downloadsDir]).
  ///
  /// A locally-held copy is copied out (the original stays in its sync
  /// directory). A file not present locally is fetched to a daemon-owned temp
  /// file (from a peer if needed) and *moved* into Downloads. The destination
  /// keeps the file's logical name, de-duplicated (`name (2).ext`) if a file by
  /// that name already exists in Downloads.
  Future<void> _downloadFile() async {
    final file = _file;
    final downloadsDir = widget.session.downloadsDir;
    if (file == null || downloadsDir == null) return;
    setState(() => _downloading = true);
    String? tempPath;
    try {
      final localPath = _localPath;
      final source = localPath ??
          (tempPath = await _app.fetchFileByString(
            fileId: widget.fileId,
            expectedHash: file.contentHash,
          ));

      final name = file.path.split('/').last;
      final dir = Directory(downloadsDir);
      await dir.create(recursive: true);
      final dest = _uniqueDestination(downloadsDir, name);

      if (localPath != null) {
        // Local file: copy out, leaving the synced original in place.
        await File(source).copy(dest);
      } else {
        // Fetched temp file (move semantics): relink into Downloads, falling
        // back to copy+delete across filesystems.
        final fetched = File(source);
        try {
          await fetched.rename(dest);
          tempPath = null; // consumed by the move
        } on FileSystemException {
          await fetched.copy(dest);
        }
      }
      _snack('Saved "${dest.split('/').last}" to Downloads.');
    } catch (error) {
      final message = '$error'.contains('NotFound')
          ? 'Cannot download: the file\'s contents are not available on any '
              'device.'
          : 'Failed to download file: $error';
      _snack(message);
    } finally {
      // Clean up a fetched temp file we didn't move; best-effort.
      if (tempPath != null) {
        try {
          await File(tempPath).delete();
        } catch (_) {
          // Nothing to do; it lives in a temp dir.
        }
      }
      if (mounted) setState(() => _downloading = false);
    }
  }

  /// Build a destination path in [dir] for [name] that does not collide with an
  /// existing file, inserting ` (n)` before the extension as needed
  /// (`report.pdf` -> `report (2).pdf`).
  static String _uniqueDestination(String dir, String name) {
    if (!File('$dir/$name').existsSync()) return '$dir/$name';
    final dot = name.lastIndexOf('.');
    final stem = dot <= 0 ? name : name.substring(0, dot);
    final ext = dot <= 0 ? '' : name.substring(dot);
    for (var n = 2;; n++) {
      final candidate = '$dir/$stem ($n)$ext';
      if (!File(candidate).existsSync()) return candidate;
    }
  }

  void _snack(String message) {
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(message)));
  }

  @override
  Widget build(BuildContext context) {
    final file = _file;
    // Strike the title through for a tombstoned file, matching the home
    // screen's list-row treatment so the state stays consistent as the user
    // navigates.
    final titleStyle = file?.deleted == true
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    return Scaffold(
      appBar: AppBar(
        title: Text(
          file?.path ?? 'File',
          overflow: TextOverflow.ellipsis,
          style: titleStyle,
        ),
        actions: [
          if (file != null)
            // Delete for live files; Restore for tombstoned ones. Restore is
            // best-effort and disabled while a restore is in flight.
            (file.deleted
                ? IconButton(
                    icon: _restoring
                        ? const SizedBox(
                            width: 20,
                            height: 20,
                            child: CircularProgressIndicator(strokeWidth: 2),
                          )
                        : const Icon(Icons.restore_from_trash),
                    tooltip: 'Restore file',
                    onPressed: _restoring ? null : _restoreFile,
                  )
                : IconButton(
                    icon: const Icon(Icons.delete_outline),
                    tooltip: 'Delete file',
                    onPressed: _deleteFile,
                  )),
          // Download to the device's public Downloads dir, between delete and
          // share. Mobile-only (gated on the session's downloads-dir hint,
          // non-null only on Android) and only for live files. Disabled while a
          // fetch-then-download is in flight.
          if (file != null &&
              !file.deleted &&
              widget.session.downloadsDir != null)
            IconButton(
              icon: _downloading
                  ? const SizedBox(
                      width: 20,
                      height: 20,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : const Icon(Icons.download_outlined),
              tooltip: 'Download file',
              onPressed: _downloading ? null : _downloadFile,
            ),
          // Share to the OS share sheet, to the right of delete/restore.
          // Mobile-only (gated on the session's public-key hint, which is
          // non-null only on Android), and only for live files. Disabled while
          // a fetch-then-share is in flight.
          if (file != null &&
              !file.deleted &&
              widget.session.publicKey != null)
            IconButton(
              icon: _sharing
                  ? const SizedBox(
                      width: 20,
                      height: 20,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : const Icon(Icons.share_outlined),
              tooltip: 'Share file',
              onPressed: _sharing ? null : _shareFile,
            ),
        ],
      ),
      body: _buildBody(context),
    );
  }

  Widget _buildBody(BuildContext context) {
    if (_loading) return const Center(child: CircularProgressIndicator());
    if (_error != null) return Center(child: Text('Error: $_error'));
    final file = _file;
    if (file == null) {
      // Post-frame pop is queued; render a neutral state in the meantime.
      return const SizedBox.shrink();
    }
    final theme = Theme.of(context);
    return ListView(
      padding: const EdgeInsets.symmetric(vertical: 8),
      children: [
        _buildPreview(context, file),
        PropertyTile(
          label: 'Path',
          value: file.path,
          trailing: const Icon(Icons.edit_outlined, size: 20),
          onTap: _renameFile,
        ),
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
                    onPressed: _addTag,
                  ),
                ],
              ),
              if (_appliedTagIds.isEmpty)
                const Text('No tags applied.')
              else
                Wrap(
                  spacing: 8,
                  runSpacing: 8,
                  children: [
                    for (final tagId in _appliedTagIds) _tagChipFor(tagId),
                  ],
                ),
            ],
          ),
        ),
        const SizedBox(height: 24),
        PropertyTile(
          label: 'Version',
          value: '${file.versionNumber}',
          dense: true,
        ),
        PropertyTile(
          label: 'Size',
          value: _formatSize(file.size.toInt()),
          dense: true,
        ),
        PropertyTile(
          label: 'File id',
          value: file.fileId,
          monospace: true,
          dense: true,
        ),
        PropertyTile(
          label: 'Content hash',
          value: file.contentHash,
          monospace: true,
          dense: true,
        ),
      ],
    );
  }

  /// Format a byte count as a human-readable size (binary units: KiB, MiB, …).
  /// Bytes are shown as a plain count; larger sizes use one decimal place.
  static String _formatSize(int bytes) {
    if (bytes < 1024) {
      return '$bytes B';
    }
    const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
    var value = bytes / 1024;
    var unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit++;
    }
    return '${value.toStringAsFixed(1)} ${units[unit]}';
  }

  /// The file's inline preview, or a placeholder if no local copy is present.
  ///
  /// Not every known file has bytes on this device: peers can advertise files
  /// whose content we haven't fetched yet. In that case `_localPath` is null
  /// and we render a neutral "not synced" tile instead of the preview widget.
  /// Preview height is bounded so it never crowds out the tags/properties.
  Widget _buildPreview(BuildContext context, onisync.FileEntry file) {
    final theme = Theme.of(context);
    final path = _localPath;
    final header = Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16),
      child: Text(
        'Preview',
        style: theme.textTheme.labelMedium?.copyWith(
          color: theme.colorScheme.onSurfaceVariant,
          fontWeight: FontWeight.bold,
        ),
      ),
    );
    final body = path == null
        ? ListTile(
            leading: const Icon(Icons.cloud_off_outlined),
            title: const Text('Not available locally'),
            subtitle: const Text('No sync directory on this device holds a copy.'),
          )
        : ConstrainedBox(
            constraints: const BoxConstraints(maxHeight: 360),
            child: FilePreview(path: path),
          );
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [header, body],
    );
  }

  Widget _tagChipFor(String tagId) {
    final tag = _appliedTags[tagId];
    if (tag == null) {
      // Applied tag not resolved (e.g. race between _load steps). Show the
      // raw id so the row is still meaningful.
      return Chip(label: Text(tagId, style: const TextStyle(fontFamily: 'monospace')));
    }
    return TagChip(
      tag: tag,
      onPressed: () => Navigator.push(
        context,
        MaterialPageRoute(
          builder: (_) => TagDetailScreen(
            session: widget.session,
            tagId: tagId,
          ),
        ),
      ),
      onDeleted: () => _removeTag(tagId),
    );
  }
}

/// Prompts the user for a new logical path. Pops the entered string on submit,
/// or `null` on cancel. Empty / unchanged input is filtered by the caller.
class _RenameFileDialog extends StatefulWidget {
  const _RenameFileDialog({required this.initial});

  final String initial;

  @override
  State<_RenameFileDialog> createState() => _RenameFileDialogState();
}

class _RenameFileDialogState extends State<_RenameFileDialog> {
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
      title: const Text('Rename file'),
      content: TextField(
        controller: _controller,
        autofocus: true,
        decoration: const InputDecoration(labelText: 'Logical path'),
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
