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

- `migrate_files_to_v1`, `migrate_tags_to_v1`, `migrate_entries_to_v1`,
  `migrate_file_versions_to_v1` (main DB).
- `SyncDirectoryDatabase::migrate_files_to_v1` (sync-dir DB).

Each function is idempotent: it runs `CREATE TABLE IF NOT EXISTS <name>_vN`,
and if a lower-version predecessor still exists it copies the rows across
with a straight column-listed `INSERT SELECT` and then drops the old
table. Once every predecessor is gone the function is a no-op on
subsequent calls.

The v0 → v1 step is the one-shot **temporary** bootstrap from the
pre-versioning schema (bare `files`, `tags`, ...). It is marked
`TODO: DELETE after all devices migrated` in the code — the plan is to
delete those bootstrap branches (the `if Self::table_exists("<old>")`
blocks inside each `migrate_*_to_v1`, plus the dev-only `ALTER TABLE`
pre-migration block at the top of `FileDatabase::initialize`) once every
dev device has run them at least once. The `_v1` `CREATE TABLE IF NOT
EXISTS` statements stay: they are the permanent v1 schema.

### Adding a new schema version

When the schema needs to change again:

1. Do NOT modify the existing `_vN` `CREATE TABLE` statements or any
   existing `migrate_*_to_vN` function. They are frozen so that any
   backup at version `N` can still be restored on a newer build and
   walked forward through every intermediate version.
2. For each table whose schema changes, add a `migrate_<table>_to_v<N+1>`
   function alongside the existing one. It should:
   - `CREATE TABLE IF NOT EXISTS <table>_v<N+1> (...)` with the new
     schema.
   - If `<table>_vN` still exists, `INSERT INTO <table>_v<N+1> SELECT ...
     FROM <table>_vN` (with whatever column translation the schema change
     requires), then `DROP TABLE <table>_vN`.
   - Be a no-op on the second call.
3. Call the new function in `initialize` **after** the corresponding
   `_vN` migration, so a fresh install / vN-backup walks all the way up.
4. Update every SQL literal that references the changed table from
   `<table>_vN` to `<table>_v<N+1>`. Grep for the old suffix; it should
   only survive in the frozen `migrate_*_to_vN` function.
5. Update the two `resolve_id_prefix` call sites in `database.rs` if the
   `files` or `tags` table changed.

This chain lets a restored v1 backup on a v3 build migrate v1 → v2 → v3
on startup, permanently. Only the v0 → v1 bootstrap is temporary.
