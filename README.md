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

## Automatic tagging

Tagging everything by hand gets old, so the daemon config can carry **tag rules**: a regular expression matched against a file's logical path, and the tags to apply when it matches.

```json
{
  "tag_rules": [
    { "pattern": "\\.md$", "tags": ["6450a8fe6eb945cc8b40adf4b97408bd"] },
    { "pattern": "^photos/", "tags": ["b053c022c8a6432eb88acb0452abceb2"] }
  ]
}
```

A few things worth knowing:

- The pattern matches the **full logical path** (`photos/holiday/cat.jpg`), not just the file name, so a rule can key on location as well as on type. It is a search, not a full match — anchor it with `^` / `$` if that matters.
- Every matching rule contributes. `notes/todo.md` picks up the tags of both rules above.
- Tags are named by **id**, not name, so renaming a tag doesn't break a rule. Pair a rule with a `tags` declaration in the same config if you want the tag to be guaranteed to exist.
- Rules run **only when this device first creates a file** — an upload, or a file appearing in a sync directory. Renaming a file afterwards does *not* re-run them, and neither does receiving a file from a peer (its own device's rules already applied).

That last point means editing your rules has no effect on files that already exist. To catch them up:

```sh
onisync retag --dry-run   # show what would be tagged
onisync retag             # actually apply it
onisync retag --check     # just validate the rules
```

`retag` only ever *adds* tags, including for files a rule no longer matches: nothing distinguishes a tag a rule applied from one you applied yourself, so removing them isn't safe.

A rule whose pattern doesn't compile is skipped and reported by `onisync retag --check`; it never stops the daemon from starting or disables the other rules. The daemon reads its config once at startup, so restart it after editing rules.

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
