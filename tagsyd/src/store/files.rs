//! The `files_v2` table: the catalog's record of which files exist, where they
//! live logically, and whether they are tombstoned.
//!
//! Also home to `manifest_entries`, which joins those rows to their version
//! history to build what a peer is told about on connect.

use rusqlite::OptionalExtension;
use tagsy_core::{FileId, FileInfo, LogicalPath};

use super::CatalogStore;
use super::previews::delete_previews_for;
use super::short_id::common_prefix_length;
use super::types::{DatabaseError, DeletedRule, DeletionState};
use super::versions::VersionHistory;

/// One row of [`CatalogStore::manifest_entries`]: a file id, its full
/// [`VersionHistory`], the unix-millis timestamp of its latest version, the
/// file's logical path, the unix-millis time that path was last changed
/// (`logical_path_modified_at`, the path's LWW clock), and its soft-delete
/// tombstone state (`deleted`, `deleted_at`, `restored_at`).
/// Maps directly onto a `state::ManifestEntry`.
pub type ManifestRow = (
    FileId,
    VersionHistory,
    i64,
    LogicalPath,
    i64,
    bool,
    i64,
    i64,
);

impl CatalogStore {
    /// Return every `file_id` in the `files` table together with its full
    /// version history and soft-delete tombstone state (`deleted`,
    /// `deleted_at`).
    ///
    /// Deleted (tombstoned) files *are* included: reconciliation advertises the
    /// tombstone so a delete can win last-writer-wins against a peer's stale
    /// "present", and so a peer offline during the delete learns about it on
    /// reconnect (or restores the file if it holds a newer edit).
    pub fn manifest_entries(&self) -> Result<Vec<ManifestRow>, DatabaseError> {
        // First fetch the file rows we still know about, then for each fetch
        // its history and tags. Two-stage to keep the SQL straightforward;
        // manifest construction is a one-shot at connect time so the N+1 here
        // is acceptable.
        let mut id_statement = self.connection.prepare(
            "SELECT id, logical_path, logical_path_modified_at, deleted, deleted_at, restored_at \
             FROM files_v2",
        )?;
        let file_rows: Vec<(FileId, LogicalPath, i64, bool, i64, i64)> = id_statement
            .query_map([], |row| {
                let deleted: i64 = row.get(3)?;
                Ok((
                    row.get::<_, FileId>(0)?,
                    row.get::<_, LogicalPath>(1)?,
                    row.get::<_, i64>(2)?,
                    deleted != 0,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut entries = Vec::with_capacity(file_rows.len());
        for (file_id, logical_path, logical_path_modified_at, deleted, deleted_at, restored_at) in
            file_rows
        {
            let history = self.version_history(file_id)?;
            // Files in `files` should always have at least one version
            // (every add/change path records one), but be defensive.
            if history.is_empty() {
                log::warn!(
                    "File {} has no recorded versions; skipping manifest entry",
                    file_id.to_string()
                );
                continue;
            }
            let latest_observed_at = self
                .latest_version(file_id)?
                .map(|version| version.observed_at)
                .unwrap_or(0);
            entries.push((
                file_id,
                history,
                latest_observed_at,
                logical_path,
                logical_path_modified_at,
                deleted,
                deleted_at,
                restored_at,
            ));
        }
        Ok(entries)
    }

    /// Cheap existence check for a `file_id` in the `files` table. Used by
    /// `handle_changes` to decide whether an inbound `FileMetadataAdded` should
    /// be treated as new or as an idempotent re-announcement.
    pub fn file_exists(&self, file_id: FileId) -> Result<bool, DatabaseError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM files_v2 WHERE id = ?1",
            [file_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Add a new file.
    /// Insert a newly added file.
    ///
    /// `logical_path_modified_at` is the unix-millis wall-clock time the path
    /// was set (creation time on the originating device, or the peer's stamped
    /// time when materializing a file first seen over the wire). It seeds the
    /// path's last-writer-wins clock so a later move can be ordered against it.
    pub fn add_file(
        &self,
        file_id: FileId,
        logical_path: &LogicalPath,
        logical_path_modified_at: i64,
    ) -> Result<(), DatabaseError> {
        // A freshly added file is always live: `deleted = 0`, `deleted_at = 0`,
        // `restored_at = 0` (never explicitly restored yet).
        self.connection.execute(
            "INSERT INTO files_v2 (id, logical_path, logical_path_modified_at, deleted, \
             deleted_at, restored_at)
                 VALUES (?1, ?2, ?3, 0, 0, 0)",
            (file_id, logical_path, logical_path_modified_at),
        )?;

        Ok(())
    }

    /// Clear a file's soft-delete tombstone when a **newer content edit**
    /// supersedes the delete (restore-after-edit). Called by the
    /// version-arrival paths after recording a version: if the file's
    /// latest version `observed_at` is strictly newer than `deleted_at`,
    /// the edit wins last-writer-wins and the file becomes live again.
    /// Otherwise it is a no-op (a stale/duplicate version does not
    /// resurrect a newer delete).
    ///
    /// This is the edit half of the three-way LWW; explicit user restores go
    /// through [`apply_restore`](Self::apply_restore). `restored_at` is left
    /// untouched here — an edit is not a restore.
    pub fn restore_file(&self, file_id: FileId) -> Result<(), DatabaseError> {
        let Some(state) = self.file_deletion_state(file_id)? else {
            return Ok(());
        };
        if !state.deleted {
            return Ok(());
        }

        let latest_observed_at = self
            .latest_version(file_id)?
            .map(|version| version.observed_at)
            .unwrap_or(0);

        if latest_observed_at <= state.deleted_at {
            // No edit newer than the delete; the tombstone stands.
            return Ok(());
        }

        self.connection
            .execute("UPDATE files_v2 SET deleted = 0 WHERE id = ?1", [file_id])?;

        Ok(())
    }

    /// Apply an **explicit user restore** to a file, last-writer-wins.
    ///
    /// Records `restored_at` as the file's restore clock (only advancing it, so
    /// a duplicate/older restore is a no-op) and clears the tombstone iff the
    /// restore is strictly newer than the delete (`restored_at > deleted_at`).
    /// Unlike [`restore_file`](Self::restore_file), this does not depend on a
    /// version edit — it is the restore half of the three-way LWW and is what
    /// makes a peer's still-present `deleted_at` lose to our un-delete on
    /// reconnect (the manifest advertises `restored_at`).
    ///
    /// Returns `true` if the file is live after this call's changes (either the
    /// restore won or it was already live), `false` if a newer delete still
    /// out-votes the restore (the file stays tombstoned).
    pub fn apply_restore(&self, file_id: FileId, restored_at: i64) -> Result<bool, DatabaseError> {
        let Some(state) = self.file_deletion_state(file_id)? else {
            return Ok(false);
        };

        // Advance the restore clock (monotonic: never move it backward).
        if restored_at > state.restored_at {
            self.connection.execute(
                "UPDATE files_v2 SET restored_at = ?2 WHERE id = ?1",
                (file_id, restored_at),
            )?;
        }

        let effective_restored_at = state.restored_at.max(restored_at);
        let latest_observed_at = self
            .latest_version(file_id)?
            .map(|version| version.observed_at)
            .unwrap_or(0);

        // Live iff the newest of {edit, restore} beats the delete.
        let live = effective_restored_at.max(latest_observed_at) > state.deleted_at;
        if live && state.deleted {
            self.connection
                .execute("UPDATE files_v2 SET deleted = 0 WHERE id = ?1", [file_id])?;
        }

        Ok(live)
    }

    /// The soft-delete tombstone state of a file, or `None` if the file is
    /// unknown. Used by reconciliation to decide delete-vs-edit-vs-restore
    /// last-writer-wins.
    pub fn file_deletion_state(
        &self,
        file_id: FileId,
    ) -> Result<Option<DeletionState>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT deleted, deleted_at, restored_at FROM files_v2 WHERE id = ?1",
                [file_id],
                |row| {
                    let deleted: i64 = row.get(0)?;
                    Ok(DeletionState {
                        deleted: deleted != 0,
                        deleted_at: row.get::<_, i64>(1)?,
                        restored_at: row.get::<_, i64>(2)?,
                    })
                },
            )
            .optional()
            .map_err(DatabaseError::from)
    }

    /// Change a file's logical path, last-writer-wins.
    ///
    /// The move is applied only if `modified_at` is strictly newer than the
    /// file's current `logical_path_modified_at`. This makes local moves and
    /// peer moves (live or reconciled) commute regardless of delivery order:
    /// the latest edit — by wall clock stamped on the originating device —
    /// always wins, and re-applying an older or duplicate move is a no-op.
    /// Never restamp `modified_at` when applying a peer's move; pass its
    /// original value straight through.
    ///
    /// Returns `true` if the move was applied, `false` if it lost (an equal or
    /// newer path change is already recorded, or the file is unknown).
    pub fn update_file_logical_path(
        &self,
        file_id: FileId,
        logical_path: &LogicalPath,
        modified_at: i64,
    ) -> Result<bool, DatabaseError> {
        let rows = self.connection.execute(
            "UPDATE files_v2
                 SET logical_path = ?2, logical_path_modified_at = ?3
                 WHERE id = ?1 AND logical_path_modified_at < ?3",
            (file_id, logical_path, modified_at),
        )?;

        Ok(rows > 0)
    }

    /// Soft-delete a file: set its tombstone (`deleted = 1`, `deleted_at`)
    /// instead of removing the row, so the deletion survives reconciliation
    /// (a peer offline during the delete learns of it on reconnect) and can win
    /// last-writer-wins against a stale "present".
    ///
    /// Precondition: the file is currently live (not already tombstoned).
    /// Callers filter out redundant redeliveries via
    /// [`file_deletion_state`](Self::file_deletion_state) beforehand — a
    /// tombstone is a terminal state, and comparing one `deleted_at` against
    /// another is meaningless (both peers converge on the originator's stamp).
    /// This function therefore only decides live-vs-tombstoned, never
    /// tombstoned-vs-tombstoned.
    ///
    /// Last-writer-wins guard: the delete is applied only if `deleted_at` is
    /// strictly newer than **both** the file's latest recorded version
    /// `observed_at` (restore-after-edit) **and** its `restored_at` (an
    /// explicit restore). Equivalently, the delete wins iff
    /// `deleted_at > max(latest observed_at, restored_at)`. The `file_versions`
    /// history is left intact.
    ///
    /// Returns `true` if the file transitioned from live to tombstoned;
    /// `false` if a newer edit or restore out-dated the delete and the file
    /// stays live.
    pub fn remove_file(&self, file_id: FileId, deleted_at: i64) -> Result<bool, DatabaseError> {
        let latest_observed_at = self
            .latest_version(file_id)?
            .map(|version| version.observed_at)
            .unwrap_or(0);
        let restored_at = self
            .file_deletion_state(file_id)?
            .map(|state| state.restored_at)
            .unwrap_or(0);

        if deleted_at <= latest_observed_at.max(restored_at) {
            // A newer edit or explicit restore supersedes this delete; keep the
            // file live.
            return Ok(false);
        }

        let affected = self.connection.execute(
            "UPDATE files_v2 SET deleted = 1, deleted_at = ?2 WHERE id = ?1",
            (file_id, deleted_at),
        )?;

        if affected > 0 {
            // The file is now tombstoned; drop any cached previews for it. Its
            // version history is intentionally retained, but a preview of a
            // deleted file serves no purpose.
            delete_previews_for(&self.connection, file_id)?;
        }

        Ok(affected > 0)
    }

    /// Get the id of a file by its logical path.
    pub fn file_id_from_logical_path(
        &self,
        logical_path: &LogicalPath,
    ) -> Result<FileId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM files_v2 WHERE logical_path = ?1 AND deleted = 0")?;

        let file_id = statement
            .query_map([logical_path], |row| row.get(0))?
            .map(|id| id.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(file_id)
    }

    /// Get the logical path for `file_id`.
    ///
    /// The inverse of `file_id_from_logical_path`. Errors with `MissingFile` if
    /// the file has no row in `files` (unknown or deleted).
    pub fn logical_path_for_file_id(&self, file_id: FileId) -> Result<LogicalPath, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT logical_path FROM files_v2 WHERE id = ?1 AND deleted = 0")?;

        let logical_path = statement
            .query_map([file_id], |row| row.get(0))?
            .map(|logical_path| logical_path.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(logical_path)
    }

    /// The unix-millis time `file_id`'s `logical_path` was last changed — the
    /// path's last-writer-wins clock — or `None` if the file is unknown.
    /// Includes tombstoned rows (unlike [`Self::logical_path_for_file_id`]) so
    /// reconciliation can order a peer's move against our recorded time
    /// regardless of local delete state. Used to decide whether to adopt a
    /// peer's moved path on reconnect.
    pub fn logical_path_modified_at(&self, file_id: FileId) -> Result<Option<i64>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT logical_path_modified_at FROM files_v2 WHERE id = ?1",
                [file_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(DatabaseError::from)
    }

    /// Get the [`FileInfo`] for a single `file_id` — the single-file
    /// counterpart of [`Self::get_all_files`].
    ///
    /// Joins the file to its latest version and computes the "short id" length
    /// with [`Self::shorten_file_id`] (an indexed neighbour lookup, not a full
    /// scan), so this stays cheap per call. Errors with `MissingFile` if the
    /// file has no row, or no version yet.
    ///
    /// `deleted_rule` mirrors [`Self::get_all_files`]: `Exclude` hides
    /// tombstoned files (they read as `MissingFile`), `Include` returns them
    /// with `FileInfo::deleted = true`.
    pub fn file_info_from_id(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, DatabaseError> {
        // f.deleted is aliased; inline the guard rather than using the shared
        // fragment helpers.
        let extra_clause = match deleted_rule {
            DeletedRule::Exclude => " AND f.deleted = 0",
            DeletedRule::Include => "",
        };
        let sql = format!(
            "SELECT f.logical_path, v.content_hash, v.version_number, v.size, f.deleted
             FROM files_v2 AS f
             JOIN file_versions_v1 AS v
               ON v.file_id = f.id
              AND v.version_number = (
                  SELECT MAX(version_number)
                  FROM file_versions_v1 AS inner
                  WHERE inner.file_id = f.id
              )
             WHERE f.id = ?1{extra_clause}"
        );
        let mut statement = self.connection.prepare(&sql)?;

        let mut file = statement
            .query_map([file_id], |row| {
                Ok(FileInfo {
                    file_id,
                    logical_path: row.get(0)?,
                    content_hash: row.get(1)?,
                    version_number: row.get(2)?,
                    size: row.get::<_, i64>(3)? as u64,
                    // Filled in below.
                    short_id_length: 0,
                    deleted: row.get::<_, i64>(4)? != 0,
                })
            })?
            .map(|file| file.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        file.short_id_length = self.shorten_file_id(file_id)?;

        Ok(file)
    }

    /// List every currently-known file (i.e. with a row in `files`) together
    /// with its latest version's content hash and version number.
    ///
    /// Files are joined to the row in `file_versions` with the highest
    /// `version_number`, using the DESC index on
    /// `(file_id, version_number)`. Files without any recorded version are
    /// excluded (they should not occur in practice, since every add/change
    /// path records a version, but the inner join makes this defensive).
    ///
    /// `deleted_rule` governs tombstone visibility: under
    /// [`DeletedRule::Exclude`] tombstoned files are hidden (the standard
    /// behavior for every non-search caller); under [`DeletedRule::Include`]
    /// they come back too, and callers can distinguish them by the returned
    /// `deleted` flag.
    pub fn get_all_files(&self, deleted_rule: DeletedRule) -> Result<Vec<FileInfo>, DatabaseError> {
        // f.deleted is aliased, so we cannot use the plain " AND deleted = 0"
        // fragment. Inline the guard here.
        let where_clause = match deleted_rule {
            DeletedRule::Exclude => " WHERE f.deleted = 0",
            DeletedRule::Include => "",
        };
        let sql = format!(
            "SELECT f.id, f.logical_path, v.content_hash, v.version_number, v.size, f.deleted
             FROM files_v2 AS f
             JOIN file_versions_v1 AS v
               ON v.file_id = f.id
              AND v.version_number = (
                  SELECT MAX(version_number)
                  FROM file_versions_v1 AS inner
                  WHERE inner.file_id = f.id
              ){where_clause}"
        );
        let mut statement = self.connection.prepare(&sql)?;

        let mut files = Vec::new();
        let rows = statement.query_map([], |row| {
            Ok(FileInfo {
                file_id: row.get(0)?,
                logical_path: row.get(1)?,
                content_hash: row.get(2)?,
                version_number: row.get(3)?,
                size: row.get::<_, i64>(4)? as u64,
                // Filled in below once we have the whole set.
                short_id_length: 0,
                deleted: row.get::<_, i64>(5)? != 0,
            })
        })?;

        for row in rows {
            files.push(row?);
        }

        // Compute each file's shortest unique id prefix. Because this listing
        // already contains *every* file, we can do it in-memory rather than
        // hitting the DB per file: sort the ids, then each id only needs to be
        // distinguished from its two immediate neighbours in that order (same
        // reasoning as `shorten_file_id`, which the single-id path uses).
        let mut sorted_ids: Vec<String> = files.iter().map(|f| f.file_id.to_string()).collect();
        sorted_ids.sort();
        for file in &mut files {
            let id = file.file_id.to_string();
            let position = sorted_ids
                .binary_search(&id)
                .expect("every file's id is in the sorted set");

            let mut required = 1;
            if position > 0 {
                let predecessor = &sorted_ids[position - 1];
                required = required.max(common_prefix_length(&id, predecessor) + 1);
            }
            if position + 1 < sorted_ids.len() {
                let successor = &sorted_ids[position + 1];
                required = required.max(common_prefix_length(&id, successor) + 1);
            }
            file.short_id_length = required.clamp(1, id.len());
        }

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::now_millis;
    use crate::store::fixtures::memory_db;

    #[test]
    fn logical_path_for_file_id_roundtrips() {
        let database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("photos/cat.jpg"), 0)
            .unwrap();

        assert_eq!(
            database.logical_path_for_file_id(file_id).unwrap(),
            LogicalPath::new("photos/cat.jpg")
        );
    }

    #[test]
    fn logical_path_for_file_id_missing_is_not_found() {
        let database = memory_db();
        let missing = FileId::new();
        assert!(matches!(
            database.logical_path_for_file_id(missing),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn get_all_files_reports_latest_version() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        // Two versions; get_all_files must report the latest (higher
        // version_number), not the first.
        let v1 = database
            .record_version(file_id, "hash-v1", "local", 1)
            .unwrap();
        let v2 = database
            .record_version(file_id, "hash-v2", "local", 1)
            .unwrap();
        assert!(v2 > v1);

        let files = database.get_all_files(DeletedRule::Exclude).unwrap();
        assert_eq!(files.len(), 1);
        let info = &files[0];
        assert_eq!(info.file_id, file_id);
        assert_eq!(info.logical_path, LogicalPath::new("a.txt"));
        assert_eq!(info.content_hash, "hash-v2");
        assert_eq!(info.version_number, v2);
    }

    #[test]
    fn reverting_to_an_old_hash_becomes_the_new_latest_version() {
        // Regression: reverting a file's content back to a hash it held earlier
        // must record a *new* latest version with that hash — not be treated as
        // a duplicate/no-op. The change-ingest path keys "do we already hold
        // this?" off `latest_version`, so this is the invariant it relies on:
        // an old hash reappearing is the current version again only after a new
        // version row is recorded.
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        database
            .record_version(file_id, "hash-A", "local", 1)
            .unwrap();
        database
            .record_version(file_id, "hash-B", "local", 1)
            .unwrap();
        // Current version is B, but A is still in history.
        assert_eq!(
            database
                .latest_version(file_id)
                .unwrap()
                .unwrap()
                .content_hash,
            "hash-B"
        );

        // Revert to A: a new version whose hash is the old A.
        let reverted = database
            .record_version(file_id, "hash-A", "local", 1)
            .unwrap();
        let latest = database.latest_version(file_id).unwrap().unwrap();
        assert_eq!(latest.content_hash, "hash-A");
        assert_eq!(latest.version_number, reverted);
        // History now has three entries: A, B, A.
        let history = database.version_history(file_id).unwrap();
        assert_eq!(
            history
                .iter()
                .map(|(_, h, _)| h.as_str())
                .collect::<Vec<_>>(),
            vec!["hash-A", "hash-B", "hash-A"]
        );
    }

    #[test]
    fn get_all_files_excludes_files_without_versions() {
        let database = memory_db();
        // A file row with no recorded version: the inner join drops it.
        database
            .add_file(FileId::new(), &LogicalPath::new("orphan.txt"), 0)
            .unwrap();

        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn get_all_files_empty_when_no_files() {
        let database = memory_db();
        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn file_delete_soft_deletes_and_hides_from_reads() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 1).unwrap();

        // Delete with a timestamp newer than the version's observed_at.
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());

        // Hidden from user-facing reads...
        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            database.file_info_from_id(file_id, DeletedRule::Exclude),
            Err(DatabaseError::MissingFile)
        ));
        // ...but the row (and its history) still exists for reconciliation.
        assert!(database.file_exists(file_id).unwrap());
        assert_eq!(
            database.file_deletion_state(file_id).unwrap(),
            Some(DeletionState {
                deleted: true,
                deleted_at,
                restored_at: 0,
            })
        );
    }

    #[test]
    fn file_delete_loses_to_newer_edit() {
        // Restore-after-delete: an edit whose observed_at is newer than the
        // delete keeps the file live (the delete is a no-op).
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        // record_version stamps observed_at = now; a delete stamped in the
        // past must lose.
        database.record_version(file_id, "h1", "local", 1).unwrap();
        let stale_delete_at = 1; // far in the past

        assert!(!database.remove_file(file_id, stale_delete_at).unwrap());
        // File stays visible.
        assert_eq!(
            database.get_all_files(DeletedRule::Exclude).unwrap().len(),
            1
        );
        assert_eq!(
            database.file_deletion_state(file_id).unwrap(),
            Some(DeletionState {
                deleted: false,
                deleted_at: 0,
                restored_at: 0,
            })
        );
    }

    #[test]
    fn apply_restore_clears_tombstone() {
        // An explicit restore whose stamp beats `deleted_at` clears the
        // tombstone via the `restored_at` clock — no fabricated version.
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 1).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());
        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );

        // A restore stamped after the delete wins and makes the file live.
        let restored_at = deleted_at + 1;
        assert!(database.apply_restore(file_id, restored_at).unwrap());
        assert_eq!(
            database.get_all_files(DeletedRule::Exclude).unwrap().len(),
            1
        );
        assert_eq!(
            database.file_deletion_state(file_id).unwrap(),
            Some(DeletionState {
                deleted: false,
                deleted_at,
                restored_at,
            })
        );
    }

    #[test]
    fn apply_restore_loses_to_newer_delete() {
        // A restore older than the delete does not resurrect the file.
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 1).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());

        // A restore stamped before the delete loses; file stays tombstoned.
        assert!(!database.apply_restore(file_id, deleted_at - 1).unwrap());
        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );

        // ...and a later delete still wins over that restore stamp.
        assert!(database.remove_file(file_id, deleted_at + 5).unwrap());
    }

    #[test]
    fn delete_loses_to_newer_restore() {
        // Three-way LWW: a restore newer than a subsequent delete keeps the
        // file live (the delete is a no-op).
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 1).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());
        // Restore after that delete.
        assert!(database.apply_restore(file_id, deleted_at + 10).unwrap());
        // A delete stamped between the two loses to the restore.
        assert!(!database.remove_file(file_id, deleted_at + 5).unwrap());
        assert_eq!(
            database.get_all_files(DeletedRule::Exclude).unwrap().len(),
            1
        );
    }

    #[test]
    fn update_logical_path_applies_when_newer() {
        let database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("old.txt"), 10)
            .unwrap();

        // A strictly-newer move wins.
        let applied = database
            .update_file_logical_path(file_id, &LogicalPath::new("new.txt"), 20)
            .unwrap();
        assert!(applied);
        assert_eq!(
            database.logical_path_for_file_id(file_id).unwrap(),
            LogicalPath::new("new.txt")
        );
        assert_eq!(
            database.logical_path_modified_at(file_id).unwrap(),
            Some(20)
        );
    }

    #[test]
    fn update_logical_path_rejects_older_or_equal() {
        let database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("current.txt"), 20)
            .unwrap();

        // An older move loses.
        let older = database
            .update_file_logical_path(file_id, &LogicalPath::new("stale.txt"), 10)
            .unwrap();
        assert!(!older);
        // An equal-timestamp move also loses (strict >), making re-delivery a
        // no-op.
        let equal = database
            .update_file_logical_path(file_id, &LogicalPath::new("dup.txt"), 20)
            .unwrap();
        assert!(!equal);

        assert_eq!(
            database.logical_path_for_file_id(file_id).unwrap(),
            LogicalPath::new("current.txt")
        );
        assert_eq!(
            database.logical_path_modified_at(file_id).unwrap(),
            Some(20)
        );
    }

    #[test]
    fn deleted_file_appears_in_manifest_with_tombstone() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 7).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());

        let entries = database.manifest_entries().unwrap();
        assert_eq!(entries.len(), 1);
        let (id, history, _observed, _path, _path_modified, deleted, got_deleted_at, restored_at) =
            &entries[0];
        assert_eq!(*id, file_id);
        assert!(*deleted);
        assert_eq!(*got_deleted_at, deleted_at);
        assert_eq!(*restored_at, 0);
        // History (with sizes) is retained so the file can be restored.
        assert_eq!(history, &vec![(1, "h1".to_owned(), 7)]);
    }

    #[test]
    fn get_all_files_include_returns_tombstoned_with_flag() {
        // Under `Exclude` (default), a tombstoned file is invisible. Under
        // `Include`, it comes back with `FileInfo::deleted = true`, letting
        // the UI's "show deleted" toggle distinguish it from live rows.
        let mut database = memory_db();
        let live = FileId::new();
        let dead = FileId::new();
        database
            .add_file(live, &LogicalPath::new("live.txt"), 0)
            .unwrap();
        database
            .add_file(dead, &LogicalPath::new("dead.txt"), 0)
            .unwrap();
        database.record_version(live, "h1", "local", 1).unwrap();
        database.record_version(dead, "h2", "local", 1).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(dead, deleted_at).unwrap());

        let excluded: Vec<_> = database.get_all_files(DeletedRule::Exclude).unwrap();
        assert_eq!(excluded.len(), 1);
        assert_eq!(excluded[0].file_id, live);
        assert!(!excluded[0].deleted);

        let included: Vec<_> = database.get_all_files(DeletedRule::Include).unwrap();
        assert_eq!(included.len(), 2);
        let dead_info = included.iter().find(|f| f.file_id == dead).unwrap();
        assert!(dead_info.deleted);
        let live_info = included.iter().find(|f| f.file_id == live).unwrap();
        assert!(!live_info.deleted);
    }
}
