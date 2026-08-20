//! The `previews_v1` cache: rendered previews keyed by
//! `(file_id, content_hash)`.
//!
//! Keying on the content hash is what makes the cache self-invalidating: a new
//! version's hash simply won't match any cached row, so a stale preview can
//! never be served for fresh content. The explicit invalidation below is
//! therefore about bounding table growth, not correctness.

use rusqlite::{Connection, OptionalExtension};
use tagsy_core::{FileId, Preview};

use super::CatalogStore;
use super::types::DatabaseError;
use crate::clock::now_millis;

/// Drop every cached preview for `file_id`, regardless of content hash.
///
/// A free function over `&Connection` rather than a method because the two
/// other places that must invalidate — recording a version and tombstoning a
/// file — do so from inside their own transaction. Keeping one helper is what
/// stops the `DELETE` from being written out three times.
pub(super) fn delete_previews_for(
    connection: &Connection,
    file_id: FileId,
) -> Result<(), DatabaseError> {
    connection.execute("DELETE FROM previews_v1 WHERE file_id = ?1", [file_id])?;

    Ok(())
}

impl CatalogStore {
    /// Look up a cached preview for `(file_id, content_hash)`.
    ///
    /// Returns `Ok(Some(_))` for any cached result — including a cached
    /// [`Preview::None`] (an un-previewable file whose negative result we
    /// remember). `Ok(None)` means nothing is cached and the caller must
    /// generate it locally or request it from peers.
    pub fn preview_for(
        &self,
        file_id: FileId,
        content_hash: &str,
    ) -> Result<Option<Preview>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT kind, data, width, height
                 FROM previews_v1
                 WHERE file_id = ?1 AND content_hash = ?2",
                (file_id, content_hash),
                |row| {
                    let kind: i64 = row.get(0)?;
                    let data: Option<Vec<u8>> = row.get(1)?;
                    let width: Option<i64> = row.get(2)?;
                    let height: Option<i64> = row.get(3)?;
                    Ok((kind, data, width, height))
                },
            )
            .optional()?
            .map(|(kind, data, width, height)| match kind {
                0 => Ok(Preview::Image {
                    bytes: data.unwrap_or_default(),
                    width: width.unwrap_or(0) as u32,
                    height: height.unwrap_or(0) as u32,
                }),
                1 => Ok(Preview::Text(
                    data.and_then(|bytes| String::from_utf8(bytes).ok())
                        .unwrap_or_default(),
                )),
                _ => Ok(Preview::None),
            })
            .transpose()
    }

    /// Cache `preview` for `(file_id, content_hash)`, replacing any existing
    /// row for that key. Idempotent; safe to call whether the preview was
    /// generated locally or received from a peer.
    pub fn record_preview(
        &mut self,
        file_id: FileId,
        content_hash: &str,
        preview: &Preview,
    ) -> Result<(), DatabaseError> {
        let generated_at = now_millis();

        let (kind, data, width, height): (i64, Option<Vec<u8>>, Option<i64>, Option<i64>) =
            match preview {
                Preview::Image {
                    bytes,
                    width,
                    height,
                } => (
                    0,
                    Some(bytes.clone()),
                    Some(*width as i64),
                    Some(*height as i64),
                ),
                Preview::Text(text) => (1, Some(text.clone().into_bytes()), None, None),
                Preview::None => (2, None, None, None),
            };

        self.connection.execute(
            "INSERT OR REPLACE INTO previews_v1
                    (file_id, content_hash, kind, data, width, height, generated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                file_id,
                content_hash,
                kind,
                data,
                width,
                height,
                generated_at,
            ),
        )?;

        Ok(())
    }

    /// Drop every cached preview for `file_id`, regardless of content hash.
    ///
    /// Called when a file's content identity changes (`record_version`) or the
    /// file is deleted. Not required for correctness (previews are hash-keyed)
    /// but bounds table growth and clears previews for tombstoned files.
    pub fn invalidate_previews(&self, file_id: FileId) -> Result<(), DatabaseError> {
        delete_previews_for(&self.connection, file_id)
    }

    /// Delete *every* cached preview, returning how many rows were removed.
    ///
    /// Wipes the whole `previews_v1` cache — successful image/text previews as
    /// well as negative (`Preview::None`) results. Previews are hash-keyed and
    /// regenerated on demand, so this is never required for correctness; it
    /// forces every file to be re-evaluated on its next preview request.
    /// Exposed to operators via the `tagsy purge-previews` CLI command
    /// (useful after changing what the daemon can preview, e.g. new
    /// PDF/video support).
    pub fn purge_previews(&self) -> Result<usize, DatabaseError> {
        self.connection
            .execute("DELETE FROM previews_v1", [])
            .map_err(DatabaseError::from)
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::LogicalPath;

    use super::*;
    use crate::store::fixtures::memory_db;

    #[test]
    fn preview_cache_roundtrips_each_kind() {
        let mut database = memory_db();
        let file_id = FileId::new();

        // Miss before anything is cached.
        assert!(database.preview_for(file_id, "h").unwrap().is_none());

        // Image round-trips including dimensions.
        let image = Preview::Image {
            bytes: vec![1, 2, 3],
            width: 12,
            height: 34,
        };
        database.record_preview(file_id, "h", &image).unwrap();
        assert_eq!(database.preview_for(file_id, "h").unwrap(), Some(image));

        // Text under a different hash coexists.
        let text = Preview::Text("hello".to_owned());
        database.record_preview(file_id, "h2", &text).unwrap();
        assert_eq!(database.preview_for(file_id, "h2").unwrap(), Some(text));

        // A cached `None` is a real (negative) hit, distinct from a miss.
        database
            .record_preview(file_id, "h3", &Preview::None)
            .unwrap();
        assert_eq!(
            database.preview_for(file_id, "h3").unwrap(),
            Some(Preview::None)
        );
    }

    #[test]
    fn record_preview_replaces_same_key() {
        let mut database = memory_db();
        let file_id = FileId::new();

        database
            .record_preview(file_id, "h", &Preview::Text("first".to_owned()))
            .unwrap();
        database
            .record_preview(file_id, "h", &Preview::Text("second".to_owned()))
            .unwrap();

        assert_eq!(
            database.preview_for(file_id, "h").unwrap(),
            Some(Preview::Text("second".to_owned()))
        );
    }

    #[test]
    fn record_version_invalidates_previews() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database
            .record_version(file_id, "hash-v1", "local", 1)
            .unwrap();
        database
            .record_preview(file_id, "hash-v1", &Preview::Text("v1".to_owned()))
            .unwrap();

        // Recording a new version drops every cached preview for the file.
        database
            .record_version(file_id, "hash-v2", "local", 1)
            .unwrap();
        assert!(database.preview_for(file_id, "hash-v1").unwrap().is_none());
    }

    #[test]
    fn remove_file_invalidates_previews() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database
            .record_version(file_id, "hash-v1", "local", 1)
            .unwrap();
        database
            .record_preview(file_id, "hash-v1", &Preview::Text("v1".to_owned()))
            .unwrap();

        // A delete stamped after the version wins and clears the cache.
        let deleted_at = now_millis() + 1_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());
        assert!(database.preview_for(file_id, "hash-v1").unwrap().is_none());
    }
}
