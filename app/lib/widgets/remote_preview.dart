import 'package:flutter/material.dart';

import '../rust/api.dart' as tagsy;

/// Preview for a file whose bytes are **not** present locally.
///
/// Unlike [FilePreview] (which reads full-fidelity bytes off disk), this asks
/// the daemon for a small, cacheable preview via [TagsyApp.getPreview]
/// — a low-resolution image or a short text snippet. The daemon generates it
/// from a peer that holds the content (first responder wins) and caches it, so
/// repeat opens are cheap. Content that is genuinely not previewable resolves
/// to [PreviewKind.none] and renders a neutral tile.
///
/// A file whose bytes no reachable device currently holds is a *transient*
/// failure ([ApiError_ContentUnavailable]) rather than [PreviewKind.none]: the
/// daemon does not cache it, and this widget shows a retry affordance so the
/// preview can be re-requested once a holder comes online.
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

  final tagsy.TagsyApp app;
  final String fileId;

  /// The file's current content hash. Not passed to the API (the daemon keys
  /// previews by the file's current hash itself), but used as part of the
  /// widget key so a content change re-triggers the fetch.
  final String contentHash;

  @override
  State<RemotePreview> createState() => _RemotePreviewState();
}

class _RemotePreviewState extends State<RemotePreview> {
  late Future<tagsy.PreviewEntry> _future;

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

  /// Re-request the preview after a transient failure. The daemon did not cache
  /// the `ContentUnavailable` outcome, so this re-runs the full resolve path
  /// (which may now find a holder online) rather than returning a stale miss.
  void _retry() {
    setState(() {
      _future = widget.app.getPreview(fileId: widget.fileId);
    });
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<tagsy.PreviewEntry>(
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
          // Two rejections are meaningful here, and they are different in kind:
          //
          // - `UnknownId`: the file id itself is gone (deleted while this
          //   screen was open). Permanent — retrying cannot help.
          // - `ContentUnavailable`: the file exists, but no preview could be
          //   obtained *right now* (nothing to generate from locally and no
          //   reachable peer served one). Transient and deliberately not
          //   cached by the daemon, so a retry once a holder is online can
          //   succeed.
          //
          // A file whose content is genuinely un-previewable is *not* an error:
          // it resolves to `PreviewKind.none`, handled in `_buildPreview`.
          if (error is tagsy.ApiError_UnknownId) {
            return const _PreviewTile(
              icon: Icons.help_outline,
              title: 'File no longer exists',
              subtitle: 'It was deleted while this screen was open.',
            );
          }
          if (error is tagsy.ApiError_ContentUnavailable) {
            return _PreviewTile(
              icon: Icons.cloud_off_outlined,
              title: 'Preview unavailable',
              subtitle: 'No device holding this file is reachable right now.',
              trailing: IconButton(
                icon: const Icon(Icons.refresh),
                tooltip: 'Retry',
                onPressed: _retry,
              ),
            );
          }
          return _PreviewTile(
            icon: Icons.cloud_off_outlined,
            title: 'Failed to load preview',
            subtitle: '$error',
            trailing: IconButton(
              icon: const Icon(Icons.refresh),
              tooltip: 'Retry',
              onPressed: _retry,
            ),
          );
        }
        return _buildPreview(context, snapshot.data!);
      },
    );
  }

  Widget _buildPreview(BuildContext context, tagsy.PreviewEntry preview) {
    switch (preview.kind) {
      case tagsy.PreviewKind.image:
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
            child: Image.memory(bytes, fit: BoxFit.contain),
          ),
        );
      case tagsy.PreviewKind.text:
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
      case tagsy.PreviewKind.none:
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
