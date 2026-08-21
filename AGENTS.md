# AGENTS.md

## Project

Tagsy is a file synchronization system with tag-based organization. The
repo is a Cargo workspace (`tagsy-core`, `tagsyd`, `tagsy`,
`tagsy-bridge`) plus a Flutter app under `app/`.

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
`TAGSY_CONFIG`, `ONISYNC_DEVICE`).

## Restructuring in progress

`docs/design/restructure.md` is an active, phased plan to realign the module
boundaries with the actor boundaries. Phases 0 and 1 have landed (`database.rs`
is now `store/`); the remaining phases move most of `tagsyd` — `lib.rs` →
`catalog/`, `directory_manager.rs` → `workspace/`, `api.rs` + `control.rs` →
`frontend/`, `configuration.rs` → `config/` — and then split the crate graph.

Read it before any large move or rename, and extend it rather than starting a
parallel plan. It also records decisions that look like oversights but are
deliberate. The most important: the API surface is deliberately expressed
several times over (`Api` → `ControlRequest` → dispatch → `ControlResponse` →
backend impl, plus the CLI and bridge). Each level is meant to be
independently readable — **do not** collapse it into a code-generating macro.

## Database schema versioning

There are two SQLite databases, both owned by `tagsyd/src/store/`:

- the **main catalog** (`CatalogStore`) — `files_v2`, `tags_v1`,
  `entries_v1`, `file_versions_v1`, `previews_v1`.
- a **per-sync-directory index** (`DirectoryIndex`) — one database per sync
  directory, holding a single `files_v1` table. Unrelated to the main
  catalog's former `files_v1`.

Every table name carries a version suffix. All `CREATE TABLE` statements and
all migrations live in `store/schema.rs`, for both databases — that one file
is the entire schema. The SQL that *reads and writes* each table lives in the
module that owns it:

| Table | Owning module |
|---|---|
| `files_v2` | `store/files.rs` |
| `tags_v1` | `store/tags.rs` |
| `entries_v1` | `store/entries.rs` |
| `file_versions_v1` | `store/versions.rs` |
| `previews_v1` | `store/previews.rs` |
| `files_v1` (per-directory) | `store/directory_index.rs` |

Three modules also touch a table they don't own — check them too when
versioning one:

- `store/entries.rs` `LEFT JOIN`s `tags_v1` in its three tag-returning
  traversals, to drop tombstoned tags from a walk.
- `store/files.rs` `JOIN`s `file_versions_v1` to resolve each file's latest
  version.
- `store/short_id.rs` names `files_v2` and `tags_v1` as string arguments
  rather than in SQL (see step 6 below).

`store/query.rs` issues no SQL at all; it composes the primitives above.

Migrations are free functions in `store::schema` taking `&Connection`, called
in sequence from `CatalogStore::initialize` and `DirectoryIndex::initialize`.
**Exactly one exists today:**

- `migrate_files_to_v2` — main catalog `files_v1` → `files_v2`, adding the
  `restored_at` clock.

Every other table is still at its first version and has no migration function.

Because both databases share `schema.rs`, the per-directory creator is named
`create_directory_files_v1` to keep it distinct from the main catalog's
`files_v1` that `migrate_files_to_v2` walks forward. Keep that prefix if you
version that table.

### Adding a new schema version

When the schema needs to change again — steps 1–4 all happen in
`store/schema.rs`:

1. Rename `create_<table>_vN` to `create_<table>_v<N+1>` and adjust the
   `CREATE TABLE` statement inside it.
2. Don't modify any existing `migrate_*_to_vN` function. They are frozen so
   that any backup at version `N` can still be restored on a newer build and
   walked forward through every intermediate version.
3. For each table whose schema changes, add a `migrate_<table>_to_v<N+1>`
   function alongside the existing ones. It should:
   - Do nothing if `<table>_vN` doesn't exist.
   - Otherwise create `<table>_v<N+1>`, `INSERT INTO <table>_v<N+1> SELECT ...
     FROM <table>_vN` (with whatever column translation the schema change
     requires), then `DROP TABLE <table>_vN`.
4. Call the new function from the relevant `initialize` — `CatalogStore` or
   `DirectoryIndex` — **after** the N-1 migration and **before** the
   `create_*` calls.
5. Update every SQL literal that references the changed table from
   `<table>_vN` to `<table>_v<N+1>`. Start with the owning module from the
   table above, then grep the old suffix across `tagsyd/src/store/` to
   confirm you got them all; it should survive only inside the frozen
   `migrate_*_to_vN` functions.
6. If `files_v2` or `tags_v1` changed, update the four hardcoded table names
   in `store/short_id.rs` — two `resolve_id_prefix` calls and two
   `shortest_unique_prefix_length` calls. They take the table name as a
   string argument, so the grep in step 5 still finds them.

This chain lets a restored v1 backup on a v3 build migrate v1 → v2 → v3
on startup, permanently.
