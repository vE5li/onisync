// Shared display formatters used across screens, kept here so surfaces render
// the same values the same way (e.g. the file detail screen and the top-bar
// storage indicator both show byte sizes identically).

/// Format a byte count as a human-readable size (binary units: KiB, MiB, …).
/// Bytes are shown as a plain count; larger sizes use one decimal place.
String formatSize(int bytes) {
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
