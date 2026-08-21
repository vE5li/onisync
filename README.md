<p align="center">
  <img src="./icon/github.png" alt="Tagsy" width="128" />
</p>

<h1 align="center">Tagsy</h1>

> [!IMPORTANT]
> **Early development — not stable, not feature-complete.** Expect breaking changes in the public APIs. Not recommended for real data yet.

## How it works

Tagsy ("Taxi") organizes files by **tags** instead of folders:

- You can tag files.
- You can tag tags, so tags can form hierarchies.
- Those relationships can even form cycles — a tag can (transitively) tag itself.

The upshot is that search is intentionally fuzzy: matching on any tag in a chain surfaces the file, so you rarely have to remember exactly where you put something. And because every device holds the full metadata (tags, relationships, file names), search always works — even offline, and even for files whose content lives on another device.

Sync is handled by `tagsyd`, running on every device. It's a true two-way sync (edits on any device propagate to the others, with conflicts resolved per-item) and it's push-based over persistent connections — changes show up on peers as they happen, with no polling.

## Searching

A query is a list of chunks, all of which must match. A chunk is an optional `!` (negate), an optional prefix picking what to match against (`/t` tag, `/l` logical path, nothing at all for "either"), and a payload.

The payload's delimiter picks *how* to match:

| Payload | Meaning |
| --- | --- |
| `cat` | literal substring |
| `"my file"` | literal substring, including spaces |
| `%\.md$%` | regular expression |

So `/l %^photos/\d{4}/%` finds files under a four-digit year directory, and `beach ! %\.tmp$%` finds anything matching `beach` that isn't a temp file. Regexes are case-insensitive unless you start them with `(?-i)`.

`%` rather than the usual `/.../` because paths are full of slashes — `%^photos/raw/%` needs no escaping — and because `/` already introduces the prefixes.

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
tagsy retag --dry-run   # show what would be tagged
tagsy retag             # actually apply it
tagsy retag --check     # just validate the rules
```

`retag` only ever *adds* tags, including for files a rule no longer matches: nothing distinguishes a tag a rule applied from one you applied yourself, so removing them isn't safe.

A rule whose pattern doesn't compile is skipped and reported by `tagsy retag --check`; it never stops the daemon from starting or disables the other rules. The daemon reads its config once at startup, so restart it after editing rules.

## Components

Tagsy is a Cargo workspace plus a Flutter app:

- **`tagsy-core`** — Shared types and schema primitives used by every other crate.
- **`tagsyd`** — The sync daemon: file watching, chunked transfer, and the versioned SQLite store.
- **`tagsy`** — The command-line client that talks to `tagsyd`.
- **`tagsy-bridge`** — `flutter_rust_bridge` glue that exposes the daemon to Dart as a native library (`.so` on Android, loaded in-process on desktop).
- **`app/`** — The Flutter UI, built on top of `tagsy-bridge`.

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

See `flake.nix` for the full list of apps and the required environment variables (`TAGSY_CONFIG`, `ONISYNC_DEVICE`, ...), and `AGENTS.md` for repository conventions.

## License

MIT — see [LICENSE.md](./LICENSE.md).
