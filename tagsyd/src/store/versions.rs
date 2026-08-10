//! The `file_versions_v1` table: an append-only log of content hashes per
//! file.
//!
//! The latest row per `file_id` (highest `version_number`) is the file's
//! current content, and its `observed_at` is the content half of the
//! three-way delete/edit/restore last-writer-wins in [`super::files`].

use std::time::{SystemTime, UNIX_EPOCH};

use tagsy_core::FileId;

use super::CatalogStore;
use super::previews::delete_previews_for;
use super::types::{DatabaseError, FileVersion};

/// A file's version history as `(version_number, content_hash, size)` triples
/// ordered oldest-to-newest. `size` is the version's content size in bytes.
/// Mirrors `state::ManifestEntry::history`.
pub type VersionHistory = Vec<(i64, String, i64)>;

impl CatalogStore {
    /// Append a new version row for `file_id`.
    ///
    /// The `version_number` is computed as `MAX(version_number) + 1` for this
    /// file (starting at 1) inside a transaction so concurrent calls cannot
    /// collide on the PK. Returns the newly assigned `version_number`.
    ///
    /// `origin` is `"local"` when the version was observed on disk by this
    /// daemon; it will later be a peer's public key when the version came in
    /// over the wire.
    ///
    /// `size` is the version's content size in bytes, read at hash time.
    pub fn record_version(
        &mut self,
        file_id: FileId,
        content_hash: &str,
        origin: &str,
        size: i64,
    ) -> Result<i64, DatabaseError> {
        let observed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0);

        let transaction = self.connection.transaction()?;

        // `MAX(version_number)` returns NULL when there are no rows for this
        // file_id, which rusqlite refuses to deserialize into a plain `i64`.
        // Pull it as `Option<i64>` and default to 0 here instead.
        let current_max: Option<i64> = transaction.query_row(
            "SELECT MAX(version_number) FROM file_versions_v1 WHERE file_id = ?1",
            [file_id],
            |row| row.get(0),
        )?;
        let next_version_number: i64 = current_max.unwrap_or(0) + 1;

        transaction.execute(
            "INSERT INTO file_versions_v1
                    (file_id, content_hash, observed_at, version_number, origin, size)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                file_id,
                content_hash,
                observed_at,
                next_version_number,
                origin,
                size,
            ),
        )?;

        // The file's content identity just changed: drop every cached preview
        // for it (of any prior hash) in the same transaction. Correctness does
        // not depend on this — previews are keyed by `content_hash`, so a stale
        // one could never match the new version — but it bounds `previews_v1`
        // growth. This is the single choke point for content change (local
        // edits, peer edits, reconciliation, restore all funnel through here).
        delete_previews_for(&transaction, file_id)?;

        transaction.commit()?;

        Ok(next_version_number)
    }

    /// Return the most recent recorded `content_hash` for every file that has
    /// at least one row in `file_versions`. Used at startup by
    /// `SyncDirectoryManager` to detect files that changed on disk while the
    /// daemon was offline.
    ///
    /// One row per `file_id`; files with no recorded version are absent from
    /// the result.
    pub fn latest_content_hashes(
        &self,
    ) -> Result<std::collections::HashMap<FileId, String>, DatabaseError> {
        // The DESC index on (file_id, version_number) lets SQLite answer this
        // efficiently: for each file_id, take the row with the highest
        // version_number.
        let mut statement = self.connection.prepare(
            "SELECT file_id, content_hash
                 FROM file_versions_v1 AS outer
                 WHERE version_number = (
                     SELECT MAX(version_number)
                     FROM file_versions_v1 AS inner
                     WHERE inner.file_id = outer.file_id
                 )",
        )?;

        let mut hashes = std::collections::HashMap::new();
        let rows = statement.query_map([], |row| {
            let file_id: FileId = row.get(0)?;
            let content_hash: String = row.get(1)?;
            Ok((file_id, content_hash))
        })?;

        for row in rows {
            let (file_id, content_hash) = row?;
            hashes.insert(file_id, content_hash);
        }

        Ok(hashes)
    }

    /// Return the most recent recorded version for `file_id`, or `None` if the
    /// file has never had a version recorded.
    pub fn latest_version(&self, file_id: FileId) -> Result<Option<FileVersion>, DatabaseError> {
        let mut statement = self.connection.prepare(
            "SELECT content_hash, observed_at, version_number, origin, size
                 FROM file_versions_v1
                 WHERE file_id = ?1
                 ORDER BY version_number DESC
                 LIMIT 1",
        )?;

        let mut rows = statement.query_map([file_id], |row| {
            Ok(FileVersion {
                file_id,
                content_hash: row.get(0)?,
                observed_at: row.get(1)?,
                version_number: row.get(2)?,
                origin: row.get(3)?,
                size: row.get(4)?,
            })
        })?;

        match rows.next() {
            Some(Ok(version)) => Ok(Some(version)),
            Some(Err(error)) => Err(error.into()),
            None => Ok(None),
        }
    }

    /// Return the full version history for `file_id`, ordered oldest-first by
    /// `version_number`. Each triple is `(version_number, content_hash, size)`.
    /// An empty vec means the file has no recorded versions.
    ///
    /// Used to build `state::ManifestEntry::history`.
    pub fn version_history(&self, file_id: FileId) -> Result<VersionHistory, DatabaseError> {
        let mut statement = self.connection.prepare(
            "SELECT version_number, content_hash, size
                 FROM file_versions_v1
                 WHERE file_id = ?1
                 ORDER BY version_number ASC",
        )?;

        let mut history = Vec::new();
        let rows = statement.query_map([file_id], |row| {
            let version_number: i64 = row.get(0)?;
            let content_hash: String = row.get(1)?;
            let size: i64 = row.get(2)?;
            Ok((version_number, content_hash, size))
        })?;
        for row in rows {
            history.push(row?);
        }
        Ok(history)
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::LogicalPath;

    use super::*;
    use crate::store::DeletedRule;
    use crate::store::fixtures::memory_db;

    #[test]
    fn size_round_trips_through_version_and_file_info() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database
            .record_version(file_id, "hash-1", "local", 42)
            .unwrap();

        // Latest version carries the size.
        assert_eq!(database.latest_version(file_id).unwrap().unwrap().size, 42);
        // FileInfo surfaces it (as u64).
        assert_eq!(
            database
                .file_info_from_id(file_id, DeletedRule::Exclude)
                .unwrap()
                .size,
            42
        );

        // A new version records its own size; the latest reflects the newest.
        database
            .record_version(file_id, "hash-2", "local", 100)
            .unwrap();
        assert_eq!(
            database
                .file_info_from_id(file_id, DeletedRule::Exclude)
                .unwrap()
                .size,
            100
        );
    }

    #[test]
    fn zero_byte_size_is_a_real_value() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("empty.txt"), 0)
            .unwrap();
        database
            .record_version(file_id, "hash-0", "local", 0)
            .unwrap();
        assert_eq!(
            database
                .file_info_from_id(file_id, DeletedRule::Exclude)
                .unwrap()
                .size,
            0
        );
    }

    #[test]
    fn version_history_carries_per_version_size() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 10).unwrap();
        database.record_version(file_id, "h2", "local", 20).unwrap();

        let history = database.version_history(file_id).unwrap();
        assert_eq!(history, vec![
            (1, "h1".to_owned(), 10),
            (2, "h2".to_owned(), 20),
        ]);
    }
}
