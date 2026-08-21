// Detail screen for the top-bar storage indicator. Shows the full breakdown of
// local vs. whole-catalog storage — bytes and file counts on each side — as
// labelled property rows. Like the indicator, it seeds from a one-shot fetch
// and then re-fetches on every catalog change so the figures stay live while
// the screen is open.

import 'package:flutter/material.dart';

import '../bootstrap/bootstrap.dart';
import '../rust/api.dart' as tagsy;
import '../widgets/format.dart';
import '../widgets/property_tile.dart';

class StorageStatsScreen extends StatefulWidget {
  const StorageStatsScreen({super.key, required this.session});

  final TagsySession session;

  @override
  State<StorageStatsScreen> createState() => _StorageStatsScreenState();
}

class _StorageStatsScreenState extends State<StorageStatsScreen> {
  tagsy.StorageStatsEntry? _stats;
  String? _error;
  bool _watching = false;

  @override
  void initState() {
    super.initState();
    _watch();
  }

  @override
  void dispose() {
    _watching = false;
    super.dispose();
  }

  Future<void> _refresh() async {
    try {
      final stats = await widget.session.app.storageStats();
      if (!mounted) return;
      setState(() {
        _stats = stats;
        _error = null;
      });
    } catch (error) {
      if (!mounted) return;
      // Keep any previously-shown figures; only surface the error when we have
      // nothing to show yet.
      setState(() {
        if (_stats == null) _error = '$error';
      });
    }
  }

  Future<void> _watch() async {
    if (_watching) return;
    _watching = true;
    await _refresh();
    try {
      final events = await widget.session.app.subscribe();
      while (mounted && _watching) {
        final event = await events.next();
        if (event == null) break;
        if (!mounted) break;
        await _refresh();
      }
    } catch (_) {
      // Stream errors are surfaced elsewhere (bootstrap) — ignore here so a
      // transient hiccup doesn't kill the screen.
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Storage')),
      body: _buildBody(),
    );
  }

  Widget _buildBody() {
    final stats = _stats;
    if (stats == null) {
      if (_error != null) return Center(child: Text('Error: $_error'));
      return const Center(child: CircularProgressIndicator());
    }

    final localBytes = stats.localBytes.toInt();
    final totalBytes = stats.totalBytes.toInt();
    final localFiles = stats.localFiles.toInt();
    final totalFiles = stats.totalFiles.toInt();

    return ListView(
      padding: const EdgeInsets.symmetric(vertical: 8),
      children: [
        PropertyTile(
          label: 'Stored locally',
          value: formatSize(localBytes),
        ),
        PropertyTile(
          label: 'Total in catalog',
          value: formatSize(totalBytes),
        ),
        PropertyTile(
          label: 'Files stored locally',
          value: '$localFiles',
        ),
        PropertyTile(
          label: 'Total files in catalog',
          value: '$totalFiles',
        ),
      ],
    );
  }
}
