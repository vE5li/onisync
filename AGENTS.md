# AGENTS.md

## Project

OniSync is a file synchronization system with tag-based organization. The
repo is a Cargo workspace (`onisync-core`, `onisyncd`, `onisync`,
`onisync-bridge`) plus a Flutter app under `app/`.

## Version control

This repo uses [`jj`](https://jj-vcs.github.io/jj/). Use the `jj` CLI for all
version-control operations; do not invoke `git` directly.

## Build / run

Use the helper apps defined in `flake.nix` instead of raw `cargo` / `flutter`
invocations:

- `nix run .#codegen` — regenerate the Dart↔Rust bindings.
- `nix run .#run-android` — codegen + native `.so` + launch on Android.
- `nix run .#launch-android` — fast path, no rebuild.
- `nix run .#run-android-clean` — uninstall first (wipes local data), then run.
- `nix run .#build-native-android` — just cross-compile the native `.so`.
- `nix run .#run-linux` — codegen + launch the Linux desktop app.
- `nix run .#launch-linux` — fast path, no codegen.

See `flake.nix` for the full list and required env vars (e.g.
`ONISYNC_CONFIG`, `ONISYNC_DEVICE`).

## Database schema versioning

All SQLite tables are versioned by suffix: `files_v1`, `tags_v1`,
`entries_v1`, `file_versions_v1` in the main DB, and `files_v1` in each
per-sync-directory DB. Every SQL literal in `onisyncd/src/database.rs`
uses the suffixed name; grep for `_v1` (or `_v2`, etc.) to find them all.

Schema changes are handled by per-table migration functions on
`FileDatabase` and `SyncDirectoryDatabase`, called in sequence from each
`initialize`:

- `migrate_files_to_v2`, `migrate_tags_to_v2`, `migrate_entries_to_v2`,
  `migrate_file_versions_to_v2` (main DB).
- `SyncDirectoryDatabase::migrate_files_to_v2` (sync-dir DB).

### Adding a new schema version

When the schema needs to change again:

1. Rename the `create_*_vN` function to the newer version and adjust the create statement inside.
2. Don't modify any existing `migrate_*_to_vN` function. They are frozen so that any
   backup at version `N` can still be restored on a newer build and
   walked forward through every intermediate version.
3. For each table whose schema changes, add a `migrate_<table>_to_v<N+1>`
   function alongside the existing one. It should:
   - If `<table>_vN` exists, create `<tabli>_vN+1` and `INSERT INTO <table>_v<N+1> SELECT ...
     FROM <table>_vN` (with whatever column translation the schema change
     requires), then `DROP TABLE <table>_vN`.
   - Not do anything if `<table>_vN` doesn't exist.
4. Call the new function in `initialize` **after** the N-1 migration and **before** the `create_*_vN` calls.
5. Update every SQL literal that references the changed table from
   `<table>_vN` to `<table>_v<N+1>`. Grep for the old suffix; it should
   only survive in the frozen `migrate_*_to_vN` function.
6. Update the two `resolve_id_prefix` call sites in `database.rs` if the
   `files` or `tags` table changed.

This chain lets a restored v1 backup on a v3 build migrate v1 → v2 → v3
on startup, permanently. Only the v0 → v1 bootstrap is temporary.
