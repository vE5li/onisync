<p align="center">
  <img src="./icon/github.png" alt="OniSync" width="128" />
</p>

<h1 align="center">OniSync</h1>

> [!IMPORTANT]
> **Early development — not stable, not feature-complete.** Expect breaking changes in the public APIs. Not recommended for real data yet.

## How it works

OniSync organizes files by **tags** instead of folders:

- You can tag files.
- You can tag tags, so tags can form hierarchies.
- Those relationships can even form cycles — a tag can (transitively) tag itself.

The upshot is that search is intentionally fuzzy: matching on any tag in a chain surfaces the file, so you rarely have to remember exactly where you put something. And because every device holds the full metadata (tags, relationships, file names), search always works — even offline, and even for files whose content lives on another device.

Sync is handled by `onisyncd`, running on every device. It's a true two-way sync (edits on any device propagate to the others, with conflicts resolved per-item) and it's push-based over persistent connections — changes show up on peers as they happen, with no polling.

## Components

OniSync is a Cargo workspace plus a Flutter app:

- **`onisync-core`** — Shared types and schema primitives used by every other crate.
- **`onisyncd`** — The sync daemon: file watching, chunked transfer, and the versioned SQLite store.
- **`onisync`** — The command-line client that talks to `onisyncd`.
- **`onisync-bridge`** — `flutter_rust_bridge` glue that exposes the daemon to Dart as a native library (`.so` on Android, loaded in-process on desktop).
- **`app/`** — The Flutter UI, built on top of `onisync-bridge`.

## Supported platforms

- Linux (desktop)
- Android
- macOS — planned
- iOS — planned
- Windows — not planned

## Building

Use the helper apps defined in `flake.nix` rather than raw `cargo` / `flutter`:

```sh
nix run .#run-linux      # codegen + launch the Linux desktop app
nix run .#run-android    # codegen + native .so + launch on Android
```

See `flake.nix` for the full list of apps and the required environment variables (`ONISYNC_CONFIG`, `ONISYNC_DEVICE`, ...), and `AGENTS.md` for repository conventions.

## License

MIT — see [LICENSE.md](./LICENSE.md).
