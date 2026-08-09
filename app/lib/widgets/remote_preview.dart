import 'package:flutter/material.dart';

import '../rust/api.dart' as onisync;

/// Preview for a file whose bytes are **not** present locally.
///
/// Unlike [FilePreview] (which reads full-fidelity bytes off disk), this asks
/// the daemon for a small, cacheable preview via [OniSyncApp.getPreview]
/// — a low-resolution image or a short text snippet. The daemon generates it
/// from a peer that holds the content (first responder wins) and caches it, so
/// repeat opens are cheap. A file that no peer holds, or whose content is not
/// previewable, resolves to [PreviewKind.none] and renders a neutral tile.
///
/// The fetch may involve a peer round-trip, so it can take a few seconds; a
/// spinner is shown meanwhile. Keyed by [fileId] + [contentHash] so navigating
/// between files (or a content change) restarts the fetch rather than showing a
/// stale result.
class RemotePreview extends StatefulWidget {
  const RemotePreview({
    super.key,
    required this.app,
    required this.fileId,
    required this.contentHash,
  });

  final onisync.OniSyncApp app;
  final String fileId;

  /// The file's current content hash. Not passed to the API (the daemon keys
  /// previews by the file's current hash itself), but used as part of the
  /// widget key so a content change re-triggers the fetch.
  final String contentHash;

  @override
  State<RemotePreview> createState() => _RemotePreviewState();
}

class _RemotePreviewState extends State<RemotePreview> {
  late Future<onisync.PreviewEntry> _future;

  @override
  void initState() {
    super.initState();
    _future = widget.app.getPreview(fileId: widget.fileId);
  }

  @override
  void didUpdateWidget(RemotePreview oldWidget) {
    super.didUpdateWidget(oldWidget);
    // Refetch if the file or its content changed underneath us.
    if (oldWidget.fileId != widget.fileId ||
        oldWidget.contentHash != widget.contentHash) {
      _future = widget.app.getPreview(fileId: widget.fileId);
    }
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<onisync.PreviewEntry>(
      future: _future,
      builder: (context, snapshot) {
        if (snapshot.connectionState != ConnectionState.done) {
          return const _PreviewTile(
            icon: Icons.cloud_download_outlined,
            title: 'Fetching preview…',
            subtitle: 'Requesting a preview from a peer that holds this file.',
            trailing: SizedBox(
              width: 20,
              height: 20,
              child: CircularProgressIndicator(strokeWidth: 2),
            ),
          );
        }
        if (snapshot.hasError) {
          final error = snapshot.error;
          // A file no peer can serve is *not* an error: `getPreview` resolves
          // it to `PreviewKind.none`, handled in `_buildPreview`. The only
          // meaningful rejection here is the file id itself being gone.
          final missing = error is onisync.ApiError_UnknownId;
          return _PreviewTile(
            icon: missing ? Icons.help_outline : Icons.cloud_off_outlined,
            title: missing ? 'File no longer exists' : 'Failed to load preview',
            subtitle: missing
                ? 'It was deleted while this screen was open.'
                : '$error',
          );
        }
        return _buildPreview(context, snapshot.data!);
      },
    );
  }

  Widget _buildPreview(BuildContext context, onisync.PreviewEntry preview) {
    switch (preview.kind) {
      case onisync.PreviewKind.image:
        final bytes = preview.imageBytes;
        if (bytes == null || bytes.isEmpty) {
          return const _PreviewTile(
            icon: Icons.broken_image_outlined,
            title: 'No preview',
            subtitle: 'The preview image could not be decoded.',
          );
        }
        // Low-resolution thumbnail from the daemon. Fill the available box
        // (like the local FilePreview) rather than drawing at the tiny native
        // size; `BoxFit.contain` preserves aspect ratio. It's a small preview,
        // so upscaling looks blocky — that's fine.
        return Padding(
          padding: const EdgeInsets.symmetric(vertical: 8, horizontal: 16),
          child: SizedBox(
            width: double.infinity,
            child: Image.memory(
              bytes,
              fit: BoxFit.contain,
            ),
          ),
        );
      case onisync.PreviewKind.text:
        final text = preview.text ?? '';
        return Padding(
          padding: const EdgeInsets.symmetric(vertical: 8, horizontal: 16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              SelectableText(
                text,
                style: const TextStyle(fontFamily: 'monospace', fontSize: 13),
              ),
            ],
          ),
        );
      case onisync.PreviewKind.none:
        return const _PreviewTile(
          icon: Icons.description_outlined,
          title: 'No preview available',
          subtitle: 'This file type cannot be previewed.',
        );
    }
  }
}

/// A compact status tile used for the non-image preview states (loading, error,
/// unavailable, no-preview), matching the look of the detail screen's other
/// list tiles.
class _PreviewTile extends StatelessWidget {
  const _PreviewTile({
    required this.icon,
    required this.title,
    required this.subtitle,
    this.trailing,
  });

  final IconData icon;
  final String title;
  final String subtitle;
  final Widget? trailing;

  @override
  Widget build(BuildContext context) {
    return ListTile(
      leading: Icon(icon),
      title: Text(title),
      subtitle: Text(subtitle),
      trailing: trailing,
    );
  }
}
