// Small shared widgets for rendering tags and their colors.

import 'package:flutter/material.dart';

import '../rust/api.dart' as tagsy;

/// Preset colors offered when creating a tag (stored as #RRGGBB strings).
const List<String> kTagColorPalette = [
  '#F44336', // red
  '#E91E63', // pink
  '#9C27B0', // purple
  '#3F51B5', // indigo
  '#2196F3', // blue
  '#009688', // teal
  '#4CAF50', // green
  '#FF9800', // orange
  '#795548', // brown
  '#607D8B', // blue grey
];

/// Parse a `#RRGGBB` (or `#AARRGGBB`) string into a [Color]. Falls back to grey
/// for anything unparseable, since the core stores colors as free-form strings.
Color parseTagColor(String value) {
  var hex = value.trim();
  if (hex.startsWith('#')) hex = hex.substring(1);
  if (hex.length == 6) hex = 'FF$hex';
  final parsed = int.tryParse(hex, radix: 16);
  if (parsed == null) return Colors.grey;
  return Color(parsed);
}

/// A round color swatch, optionally showing a selection ring.
class TagColorSwatch extends StatelessWidget {
  const TagColorSwatch({super.key, required this.color, this.selected = false});

  final String color;
  final bool selected;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: 28,
      height: 28,
      decoration: BoxDecoration(
        color: parseTagColor(color),
        shape: BoxShape.circle,
        border: selected
            ? Border.all(
                color: Theme.of(context).colorScheme.onSurface,
                width: 3,
              )
            : Border.all(color: Colors.black26),
      ),
    );
  }
}

/// A compact tag pill (color dot + name), used in file listings/detail.
///
/// [onPressed] fires when the pill body is tapped (e.g. to open the tag
/// detail); [onDeleted] fires when the trailing X is tapped (untag). Either
/// or both can be null; when both are non-null the pill body and the X are
/// independent hit targets thanks to [InputChip]'s built-in split behavior.
class TagChip extends StatelessWidget {
  const TagChip({super.key, required this.tag, this.onPressed, this.onDeleted});

  final tagsy.TagEntry tag;
  final VoidCallback? onPressed;
  final VoidCallback? onDeleted;

  @override
  Widget build(BuildContext context) {
    return InputChip(
      avatar: CircleAvatar(backgroundColor: parseTagColor(tag.color)),
      label: Text(tag.name),
      onPressed: onPressed,
      onDeleted: onDeleted,
      deleteIcon: onDeleted == null ? null : const Icon(Icons.close, size: 16),
      materialTapTargetSize: MaterialTapTargetSize.shrinkWrap,
      visualDensity: VisualDensity.compact,
    );
  }
}
