//! Every `CREATE TABLE` and every migration, for both databases, in one file.
//!
//! **This file is the frozen contract.** A `migrate_*_to_vN` function is
//! never edited once shipped: a backup taken at version `N` must still be
//! restorable on a newer build and walked forward through every intermediate
//! version. Adding a version means adding a *new* migration alongside the old
//! ones and calling it from `initialize` before the matching `create_*`.
//! See AGENTS.md for the full procedure.

use rusqlite::{Connection, OptionalExtension};

use super::types::DatabaseError;

// ---------------------------------------------------------------------------
// Main catalog database (`CatalogStore`)
// ---------------------------------------------------------------------------

/// Migrate the main catalog `files_v1` → `files_v2`, adding the
/// `restored_at` clock.
///
/// Frozen once shipped (see AGENTS.md): never edit this. If `files_v1`
/// exists (an older backup restored on this build), create `files_v2` and
/// copy every row across, seeding `restored_at = 0` (no explicit restore
/// has happened), then drop `files_v1`. A no-op when `files_v1` is absent.
pub(super) fn migrate_files_to_v2(connection: &Connection) -> Result<(), DatabaseError> {
    let files_v1_exists: bool = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'files_v1'",
            [],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);

    if !files_v1_exists {
        return Ok(());
    }

    connection.execute(
        "CREATE TABLE IF NOT EXISTS files_v2 (
                id                        TEXT PRIMARY KEY,
                logical_path              TEXT NOT NULL,
                logical_path_modified_at  INTEGER NOT NULL,
                deleted                   INTEGER NOT NULL,
                deleted_at                INTEGER NOT NULL,
                restored_at               INTEGER NOT NULL
            )",
        (),
    )?;

    connection.execute(
        "INSERT INTO files_v2
                (id, logical_path, logical_path_modified_at, deleted, deleted_at, restored_at)
             SELECT id, logical_path, logical_path_modified_at, deleted, deleted_at, 0
             FROM files_v1",
        (),
    )?;

    connection.execute("DROP TABLE files_v1", ())?;

    Ok(())
}

pub(super) fn create_files_v2(connection: &Connection) -> Result<(), DatabaseError> {
    // `logical_path` is the file's logical identity: its human-readable
    // path/name (possibly nested, e.g. `foo/bar/name.txt`), independent
    // of where any individual sync directory stores the bytes on disk.
    // Contrast with `DirectoryIndex`'s `files_v1.physical_path`, which
    // is the on-disk location within a particular sync directory.
    //
    // `deleted` is the soft-delete tombstone flag (0 = live, 1 = deleted).
    // It is a *materialized* view of the three-way last-writer-wins between
    // the delete, the latest content edit, and an explicit restore: the
    // file is live iff `max(latest file_versions_v1.observed_at,
    // restored_at) > deleted_at`. Keeping `deleted` as a stored flag
    // (maintained by `remove_file` / `restore_file`) lets every read keep
    // its simple `WHERE deleted = 0` filter; reconciliation deliberately
    // considers tombstoned rows so a delete can win over a stale peer.
    //
    // The three clocks that decide `deleted`:
    // - `deleted_at`: unix-millis wall clock stamped when the file was deleted,
    //   preserved across the wire.
    // - latest `file_versions_v1.observed_at`: a content edit newer than the delete
    //   resurrects the file (restore-after-edit).
    // - `restored_at`: unix-millis wall clock of an explicit user restore,
    //   preserved across the wire. A restore newer than the delete resurrects the
    //   file *without* fabricating a version. Symmetric to `deleted_at`; never
    //   restamp it when applying a peer's restore. 0 means "never explicitly
    //   restored".
    //
    // `logical_path_modified_at` is the unix-millis wall-clock time the
    // `logical_path` was last changed, stamped on the *originating*
    // device and preserved across the wire. It is the last-writer-wins
    // clock for the path *only* (content has its own clock via
    // `file_versions_v1.observed_at`; deletes use `deleted_at`; restores
    // use `restored_at`; tags are reconciled separately with their own
    // `modified_at`). It exists so a move made while a peer is offline
    // reconciles on reconnect: the manifest advertises this timestamp and
    // the receiver adopts the peer's path only when it is strictly newer.
    // Never restamp it when applying a peer's move. Do NOT fold other
    // metadata into this clock — a bare "modified" clock would let a
    // content edit silently override a path (they are independently
    // edited). See `Change::FileMoved`.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS files_v2 (
                id                        TEXT PRIMARY KEY,
                logical_path              TEXT NOT NULL,
                logical_path_modified_at  INTEGER NOT NULL,
                deleted                   INTEGER NOT NULL,
                deleted_at                INTEGER NOT NULL,
                restored_at               INTEGER NOT NULL
            )",
        (),
    )?;

    Ok(())
}

pub(super) fn create_tags_v1(connection: &Connection) -> Result<(), DatabaseError> {
    // `name` is intentionally NOT `UNIQUE`: two devices editing offline
    // can each mint a tag with the same name but a different `TagId`.
    // When they reconcile, both tags must be able to coexist rather than
    // one insert failing a constraint and leaving the databases
    // divergent. Tag identity is the `TagId`; names are display-only and
    // may collide. (Disambiguation in the UI is handled by tag short-ids
    // — see roadmap pass 2.)
    //
    // `modified_at` is the unix-millis wall-clock time, stamped on the
    // *originating* device and preserved across the wire, that drives
    // last-writer-wins reconciliation of tag definitions. Never restamp
    // it when applying a peer's change.
    //
    // `deleted` is the soft-delete tombstone. Unlike files, a tag
    // already carries `modified_at` as its single last-writer-wins
    // clock, so a delete just sets `deleted = 1` and bumps `modified_at`
    // (mirroring the relationship tombstones in `untag_entry`) — no
    // separate `deleted_at` is needed. All live reads filter
    // `deleted = 0`; reconciliation considers tombstoned rows so a
    // delete can win LWW against a stale peer.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS tags_v1 (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                color       TEXT NOT NULL,
                metadata    TEXT,
                modified_at INTEGER NOT NULL DEFAULT 0,
                deleted     INTEGER NOT NULL
            )",
        (),
    )?;

    Ok(())
}

pub(super) fn create_entries_v1(connection: &Connection) -> Result<(), DatabaseError> {
    // `modified_at` drives last-writer-wins reconciliation of
    // relationships, exactly as for `tags_v1` above.
    //
    // `deleted` is a soft-delete flag (0 = live, 1 = tombstoned).
    // Untagging sets `deleted = 1` and bumps `modified_at` instead of
    // removing the row, so an "absent" relationship still carries a
    // timestamp and can win LWW against a stale "present" from a peer.
    // All *reads* of live relationships must filter `deleted = 0` (see
    // the read helpers below); reconciliation deliberately considers
    // tombstoned rows too. The `UNIQUE(tag_id, target_id, type)`
    // constraint is retained: a relationship reappears by flipping
    // `deleted` back to 0, never by inserting a duplicate row.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS entries_v1 (
                id          TEXT PRIMARY KEY,
                tag_id      TEXT NOT NULL,
                target_id   TEXT NOT NULL,
                type        INTEGER,
                modified_at INTEGER NOT NULL DEFAULT 0,
                deleted     INTEGER NOT NULL DEFAULT 0,
                UNIQUE (tag_id, target_id, type)
            )",
        (),
    )?;

    Ok(())
}

pub(super) fn create_file_versions_v1(connection: &Connection) -> Result<(), DatabaseError> {
    // Append-only log of content hashes per file. The latest row per
    // `file_id` (highest `version_number`) is the current version.
    //
    // - `version_number` is a per-file monotonic counter starting at 1. It is what
    //   we order by; do not order by `observed_at`.
    // - `observed_at` is unix-millis wall-clock at insert time, kept for debugging
    //   / UI.
    // - `origin` is `'local'` for now. When cross-peer conflict resolution lands it
    //   will hold the originating peer's public key.
    // - `size` is the version's content size in bytes, read from disk (or the
    //   in-memory buffer) at the same time the content hash is computed. A 0-byte
    //   file stores `size = 0` — that is a real, known value, distinct from
    //   absence, which is why the column is `NOT NULL` with no default.
    //
    // Intentionally no `FOREIGN KEY` on `file_id`: a version may be
    // recorded by `SyncDirectoryManager` before the corresponding row in
    // `files_v2` exists (which is inserted later, asynchronously, by
    // `handle_changes`). The same ordering will apply when peer-
    // originated versions land. A FK here would fight the message-
    // passing architecture.
    //
    // TODO: When a file is deleted (`CatalogStore::remove_file`), we
    // currently leave its `file_versions_v1` rows behind as a history
    // audit trail. If/when that history grows unwieldy, add a cleanup
    // pass or make `remove_file` cascade the delete here.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS file_versions_v1 (
                file_id         TEXT    NOT NULL,
                content_hash    TEXT    NOT NULL,
                observed_at     INTEGER NOT NULL,
                version_number  INTEGER NOT NULL,
                origin          TEXT    NOT NULL,
                size            INTEGER NOT NULL,
                PRIMARY KEY (file_id, version_number)
            )",
        (),
    )?;

    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_versions_v1_latest
                ON file_versions_v1(file_id, version_number DESC)",
        (),
    )?;

    Ok(())
}

pub(super) fn create_previews_v1(connection: &Connection) -> Result<(), DatabaseError> {
    // Per-peer preview cache, keyed by `(file_id, content_hash)`. Coupling
    // to `content_hash` (not just `file_id`) is what makes the cache
    // self-invalidating across content changes: a new version's hash simply
    // won't match any cached row, so a stale preview can never be served for
    // fresh content. Invalidation on version change / deletion (see
    // `invalidate_previews`) is therefore about bounding table growth and
    // clearing tombstoned files, not correctness.
    //
    // - `kind` is the discriminant of `tagsy_core::Preview` (0 = Image, 1 = Text,
    //   2 = None). The `None` kind is a *cached negative result* ("this content has
    //   no preview"), so an un-previewable file is not re-generated on every
    //   request.
    // - `data` holds the encoded image bytes (kind = Image) or the UTF-8 snippet
    //   (kind = Text); NULL for kind = None.
    // - `width`/`height` are the image's pixel dimensions (kind = Image), NULL
    //   otherwise.
    // - `generated_at` is unix-millis at insert, for eventual eviction / UI.
    //
    // No `FOREIGN KEY` on `file_id`, matching `file_versions_v1`: a preview
    // may be cached from a peer before the local catalog row exists.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS previews_v1 (
                file_id       TEXT    NOT NULL,
                content_hash  TEXT    NOT NULL,
                kind          INTEGER NOT NULL,
                data          BLOB,
                width         INTEGER,
                height        INTEGER,
                generated_at  INTEGER NOT NULL,
                PRIMARY KEY (file_id, content_hash)
            )",
        (),
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Per-sync-directory database (`DirectoryIndex`)
//
// A separate database file per sync directory, so this `files_v1` is unrelated
// to the main catalog's `files_v1` that `migrate_files_to_v2` above walks
// forward. The name carries a `directory_` prefix here only to keep the two
// apart at the call site.
// ---------------------------------------------------------------------------

pub(super) fn create_directory_files_v1(connection: &Connection) -> Result<(), DatabaseError> {
    // `physical_path` is where the bytes live on disk relative to this
    // sync directory's root, and doubles as the reverse index for
    // filesystem events (path -> file_id). For TagBased it equals the
    // logical path; for Universal it is the `file_id`. The logical/human
    // name lives in `CatalogStore`'s `files_v2`.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS files_v1 (
                id              TEXT PRIMARY KEY,
                physical_path   TEXT NOT NULL
            )",
        (),
    )?;

    Ok(())
}
