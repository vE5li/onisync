// Row for a labelled scalar property (name/color/id/path/...): a small label
// above the value, an optional trailing widget, and single-tap / long-press
// handlers so the whole row is one hit target for both edit (tap) and copy
// (long-press). Used by the tag detail and file detail screens to keep those
// two surfaces visually and behaviorally consistent.

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

class PropertyTile extends StatelessWidget {
  const PropertyTile({
    super.key,
    required this.label,
    required this.value,
    this.trailing,
    this.monospace = false,
    this.onTap,
    this.dense = false,
  });

  final String label;
  final String value;
  final Widget? trailing;
  final bool monospace;
  final VoidCallback? onTap;
  // Renders the tile with smaller text, lighter value color, and reduced
  // vertical padding. Used for secondary/read-only metadata (ids, hashes,
  // version numbers) shown below the tags on the detail screens.
  final bool dense;

  Future<void> _copy() => Clipboard.setData(ClipboardData(text: value));

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    // For dense (secondary metadata) tiles we lighten the text so it reads as
    // less prominent than the primary property rows. Blending the normal text
    // color with the surface color gives us a mid-grey (~#666 on white) that
    // adapts to light/dark themes without hard-coding a specific hex.
    final onSurface = theme.colorScheme.onSurface;
    final mutedColor = dense
        ? Color.alphaBlend(
            onSurface.withValues(alpha: 0.55),
            theme.colorScheme.surface,
          )
        : null;
    final baseValueStyle =
        (dense ? theme.textTheme.bodySmall : theme.textTheme.bodyLarge)
            ?.copyWith(color: mutedColor);
    final valueStyle = monospace
        ? (baseValueStyle ?? const TextStyle()).copyWith(
            fontFamily: 'monospace',
          )
        : baseValueStyle;
    final labelStyle =
        (dense ? theme.textTheme.labelSmall : theme.textTheme.labelMedium)
            ?.copyWith(
              color: dense ? mutedColor : theme.colorScheme.onSurfaceVariant,
              fontWeight: FontWeight.bold,
            );
    return InkWell(
      onTap: onTap,
      onLongPress: _copy,
      child: Padding(
        padding: EdgeInsets.symmetric(horizontal: 16, vertical: dense ? 6 : 12),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.center,
          children: [
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(label, style: labelStyle),
                  SizedBox(height: dense ? 2 : 4),
                  Text(value, style: valueStyle),
                ],
              ),
            ),
            if (trailing != null) ...[const SizedBox(width: 12), trailing!],
          ],
        ),
      ),
    );
  }
}
