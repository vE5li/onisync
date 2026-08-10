//! The `tags_v1` table: tag definitions, their last-writer-wins clock, and the
//! name/id lookups search resolves user input through.

use std::collections::BTreeSet;

use tagsy_core::TagId;
use tagsy_core::state::TagManifestEntry;
use tagsy_core::tag::MetadataFormat;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};

use super::CatalogStore;
use super::query::TextPattern;
use super::short_id::normalize_id_prefix;
use super::types::{DatabaseError, DeletedRule};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Tag {
    pub id: TagId,
    pub name: String,
    pub color: String,
    pub metadata: Option<MetadataFormat>,
    /// Whether the tag is soft-deleted (tombstoned). Always `false` when the
    /// row was fetched under [`DeletedRule::Exclude`] (the default). Under
    /// [`DeletedRule::Include`] this may be `true`, letting the caller
    /// distinguish live from tombstoned rows in a mixed result set.
    pub deleted: bool,
}

impl CatalogStore {
    /// Every tag definition as a lightweight manifest entry (`tag_id` +
    /// `modified_at` + `deleted`), *including* soft-deleted (tombstoned) tags.
    /// Drives last-writer-wins reconciliation of definitions: the receiver
    /// requests the full definition only for tags whose `modified_at` is newer
    /// than (or absent from) its own, and applies a tombstone directly. A tag
    /// delete bumps `modified_at`, so the existing LWW comparison also decides
    /// delete-vs-edit. Mirrors [`CatalogStore::manifest_entries`] for files.
    pub fn tag_manifest_entries(&self) -> Result<Vec<TagManifestEntry>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, modified_at, deleted FROM tags_v1")?;
        let entries = statement
            .query_map([], |row| {
                let deleted: i64 = row.get(2)?;
                Ok(TagManifestEntry {
                    tag_id: row.get(0)?,
                    modified_at: row.get(1)?,
                    deleted: deleted != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }

    /// The `modified_at` of a tag definition, or `None` if we don't know the
    /// tag. Used by reconciliation to decide whether to request a peer's newer
    /// definition.
    pub fn tag_modified_at(&self, tag_id: TagId) -> Result<Option<i64>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT modified_at FROM tags_v1 WHERE id = ?1",
                [tag_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(DatabaseError::from)
    }

    /// The full stored definition of a tag as `(name, color, modified_at)`, or
    /// `None` if the tag is unknown. Used to answer a peer's `TagRequest` with
    /// a `Change::TagAdded`.
    pub fn tag_definition(
        &self,
        tag_id: TagId,
    ) -> Result<Option<(String, String, i64)>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT name, color, modified_at FROM tags_v1 WHERE id = ?1",
                [tag_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(DatabaseError::from)
    }

    /// Whether a tag with `tag_id` exists. The tag counterpart of
    /// [`file_exists`](Self::file_exists).
    pub fn tag_exists(&self, tag_id: TagId) -> Result<bool, DatabaseError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM tags_v1 WHERE id = ?1",
            [tag_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Add a new tag.
    ///
    /// `modified_at` is the unix-millis last-writer-wins timestamp. For a local
    /// mutation pass [`crate::clock::now_millis`]; for a peer-originated change
    /// pass the timestamp that arrived on the wire (do not restamp).
    ///
    /// If a row with this `tag_id` already exists, this becomes an upsert
    /// resolved by last-writer-wins: the incoming values are applied only if
    /// `modified_at` is newer than the stored one. This keeps reconciliation
    /// idempotent — replaying an old `TagAdded` cannot clobber a newer local
    /// definition.
    pub fn add_tag(
        &self,
        tag_id: TagId,
        name: impl Into<String>,
        color: impl Into<String>,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        let name = name.into();
        let color = color.into();

        // TODO: Check that the tag name is not only numbers.
        if name.is_empty() {
            return Err(DatabaseError::InvalidTagName);
        }

        // TODO: Check that the color is valid.
        if color.is_empty() {
            return Err(DatabaseError::InvalidColor);
        }

        // Upsert with a last-writer-wins guard: on conflict, overwrite only when
        // the incoming `modified_at` is strictly newer. `excluded` refers to the
        // values we tried to insert.
        self.connection.execute(
            "INSERT INTO tags_v1 (id, name, color, modified_at, deleted)
                 VALUES (?1, ?2, ?3, ?4, 0)
                 ON CONFLICT(id) DO UPDATE SET
                     name = excluded.name,
                     color = excluded.color,
                     modified_at = excluded.modified_at,
                     deleted = 0
                 WHERE excluded.modified_at > tags_v1.modified_at",
            (tag_id, &name, &color, modified_at),
        )?;

        Ok(())
    }

    /// Update a tag's name with a last-writer-wins guard: the update is applied
    /// only if `modified_at` is newer than the stored value. See
    /// [`CatalogStore::add_tag`] for the `modified_at` contract.
    pub fn update_tag_name(
        &self,
        tag_id: TagId,
        name: impl Into<String>,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        let name = name.into();

        // TODO: Check that the tag name is not only numbers.
        if name.is_empty() {
            return Err(DatabaseError::InvalidTagName);
        }

        self.connection.execute(
            "UPDATE tags_v1 SET name = ?2, modified_at = ?3
                 WHERE id = ?1 AND ?3 > modified_at",
            (tag_id, name, modified_at),
        )?;

        Ok(())
    }

    /// Update a tag's color with a last-writer-wins guard. See
    /// [`CatalogStore::add_tag`] for the `modified_at` contract.
    pub fn update_tag_color(
        &self,
        tag_id: TagId,
        color: impl Into<String>,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        let color = color.into();

        // TODO: Check that the color is valid.
        if color.is_empty() {
            return Err(DatabaseError::InvalidColor);
        }

        self.connection.execute(
            "UPDATE tags_v1 SET color = ?2, modified_at = ?3
                 WHERE id = ?1 AND ?3 > modified_at",
            (tag_id, color, modified_at),
        )?;

        Ok(())
    }

    /// Soft-delete a tag: set its tombstone (`deleted = 1`) and bump
    /// `modified_at` to `deleted_at`, instead of removing the row. A tag reuses
    /// its `modified_at` as its single last-writer-wins clock, so the delete is
    /// applied only if `deleted_at` is strictly newer than the stored
    /// `modified_at` (a newer rename/recolor resurrects the tag). Mirrors the
    /// relationship tombstones in `untag_entry`.
    ///
    /// Relationship rows referencing the tag are left untouched: they carry
    /// their own tombstones and reconcile independently.
    ///
    /// Returns `true` if the tombstone was applied, `false` if a newer edit
    /// out-dated it (the tag stays live).
    pub fn remove_tag(&self, tag_id: TagId, deleted_at: i64) -> Result<bool, DatabaseError> {
        let affected = self.connection.execute(
            "UPDATE tags_v1 SET deleted = 1, modified_at = ?2
                 WHERE id = ?1 AND ?2 > modified_at",
            (&tag_id, deleted_at),
        )?;

        Ok(affected > 0)
    }

    /// Get all tags.
    ///
    /// `deleted_rule` governs tombstone visibility: `Exclude` hides
    /// tombstoned tags (the standard behavior), `Include` returns them
    /// alongside live ones with `Tag::deleted = true`.
    pub fn get_all_tags(
        &self,
        deleted_rule: DeletedRule,
    ) -> Result<impl IntoIterator<Item = Tag>, DatabaseError> {
        let sql = format!(
            "SELECT id, name, color, deleted FROM tags_v1{}",
            deleted_rule.where_deleted_clause(),
        );
        let mut statement = self.connection.prepare(&sql)?;

        let tag_list = statement
            .query_map([], |row| {
                Ok(Tag {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    color: row.get(2)?,
                    metadata: None,
                    deleted: row.get::<_, i64>(3)? != 0,
                })
            })?
            .map(|tag| tag.unwrap())
            .collect::<Vec<_>>();

        Ok(tag_list)
    }

    /// Get the name of a tag by the id.
    ///
    /// `deleted_rule` mirrors [`Self::get_all_tags`]: `Exclude` hides
    /// tombstoned tags (they read as `MissingTag`), `Include` returns them
    /// with `Tag::deleted = true`.
    pub fn tag_from_id(
        &self,
        tag_id: TagId,
        deleted_rule: DeletedRule,
    ) -> Result<Tag, DatabaseError> {
        let sql = format!(
            "SELECT name, color, deleted FROM tags_v1 WHERE id = ?1{}",
            deleted_rule.and_deleted_clause(),
        );
        let mut statement = self.connection.prepare(&sql)?;

        let tag = statement
            .query_map([tag_id], |row| {
                Ok(Tag {
                    id: tag_id,
                    name: row.get(0)?,
                    color: row.get(1)?,
                    metadata: None,
                    deleted: row.get::<_, i64>(2)? != 0,
                })
            })?
            .map(|tag| tag.unwrap())
            .next()
            .ok_or(DatabaseError::MissingTag)?;

        Ok(tag)
    }

    /// Find every tag id a query token's payload should match.
    ///
    /// A tag matches if **either**:
    /// - its name contains the pattern as a case-insensitive substring (so
    ///   `foo` matches `foo`, `foobar`, and `barfoo`), **or**
    /// - its id starts with the pattern interpreted as a hex id prefix (so a
    ///   pasted partial id still works).
    ///
    /// Returns all matches (deduplicated); an empty result means nothing
    /// matched, which callers treat as "matches no tag" rather than an
    /// error.
    ///
    /// `deleted_rule` controls whether tombstoned tags participate in the
    /// resolution: search callers that want to find deleted tags pass
    /// [`DeletedRule::Include`] so a pattern whose only matches are tombstoned
    /// tags still resolves; every other caller passes
    /// [`DeletedRule::Exclude`].
    pub fn tag_ids_matching_pattern(
        &self,
        pattern: &TextPattern,
        deleted_rule: DeletedRule,
    ) -> Result<Vec<TagId>, DatabaseError> {
        // A regex resolves against tag *names* only, and is evaluated in Rust
        // rather than SQL (SQLite has no regex without an extension).
        //
        // The id-prefix half is deliberately not offered for regexes: ids are
        // opaque hex, so a pattern over them answers no question anyone asks,
        // and supporting it would make `%a%` resolve to a near-arbitrary set
        // of tags on top of its name matches.
        let text = match pattern {
            TextPattern::Substring(text) => text,
            TextPattern::Regex(_) => {
                let compiled = pattern.compile();
                return Ok(self
                    .get_all_tags(deleted_rule)?
                    .into_iter()
                    .filter(|tag| compiled.is_match(&tag.name, &tag.name.to_lowercase()))
                    .map(|tag| tag.id)
                    .collect());
            }
        };

        let mut ids: BTreeSet<TagId> = BTreeSet::new();

        // Name substring, case-insensitive. Escape LIKE metacharacters in the
        // user text so `%`/`_` are matched literally; `LIKE` is case-insensitive
        // for ASCII in SQLite by default.
        let escaped = text
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("%{escaped}%");
        let name_sql = format!(
            "SELECT id FROM tags_v1 WHERE name LIKE ?1 ESCAPE '\\'{}",
            deleted_rule.and_deleted_clause(),
        );
        let mut statement = self.connection.prepare(&name_sql)?;
        let name_matches = statement.query_map([&pattern], |row| row.get::<_, TagId>(0))?;
        for id in name_matches {
            ids.insert(id?);
        }

        // Id prefix (only when the text is a valid hex id prefix at all).
        if let Some(prefix) = normalize_id_prefix(text) {
            let id_pattern = format!("{prefix}%");
            let id_sql = format!(
                "SELECT id FROM tags_v1 WHERE id LIKE ?1{}",
                deleted_rule.and_deleted_clause(),
            );
            let mut statement = self.connection.prepare(&id_sql)?;
            let id_matches = statement.query_map([&id_pattern], |row| row.get::<_, TagId>(0))?;
            for id in id_matches {
                ids.insert(id?);
            }
        }

        Ok(ids.into_iter().collect())
    }

    /// Get the id of a tag by the name. Excludes soft-deleted (tombstoned)
    /// tags.
    pub fn tag_id_from_name(&self, name: &str) -> Result<TagId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM tags_v1 WHERE name = ?1 AND deleted = 0")?;

        let tag_id = statement
            .query_map([name], |row| row.get(0))?
            .map(|id| id.unwrap())
            .next()
            .ok_or(DatabaseError::MissingTag)?;

        Ok(tag_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::fixtures::memory_db;

    #[test]
    fn add_tag_newer_modified_at_wins_older_is_noop() {
        let database = memory_db();
        let tag_id = TagId::new();

        database.add_tag(tag_id, "work", "red", 100).unwrap();
        // A newer definition overwrites.
        database.add_tag(tag_id, "job", "blue", 200).unwrap();
        let (name, color, modified_at) = database.tag_definition(tag_id).unwrap().unwrap();
        assert_eq!(
            (name.as_str(), color.as_str(), modified_at),
            ("job", "blue", 200)
        );

        // A stale definition (older modified_at) must not clobber.
        database.add_tag(tag_id, "stale", "green", 150).unwrap();
        let (name, _, modified_at) = database.tag_definition(tag_id).unwrap().unwrap();
        assert_eq!((name.as_str(), modified_at), ("job", 200));
    }

    #[test]
    fn update_tag_name_respects_lww() {
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "red", 100).unwrap();

        // Older rename loses.
        database.update_tag_name(tag_id, "old", 50).unwrap();
        assert_eq!(database.tag_definition(tag_id).unwrap().unwrap().0, "work");

        // Newer rename wins.
        database.update_tag_name(tag_id, "new", 300).unwrap();
        assert_eq!(database.tag_definition(tag_id).unwrap().unwrap().0, "new");
    }

    #[test]
    fn duplicate_tag_names_coexist() {
        // UNIQUE(name) was relaxed: two tags may share a name.
        let database = memory_db();
        let a = TagId::new();
        let b = TagId::new();
        database.add_tag(a, "work", "red", 100).unwrap();
        database.add_tag(b, "work", "blue", 100).unwrap();

        assert!(database.tag_definition(a).unwrap().is_some());
        assert!(database.tag_definition(b).unwrap().is_some());
        let all: Vec<_> = database
            .get_all_tags(DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn tag_manifest_entries_reports_all_definitions() {
        let database = memory_db();
        let a = TagId::new();
        let b = TagId::new();
        database.add_tag(a, "one", "red", 111).unwrap();
        database.add_tag(b, "two", "blue", 222).unwrap();

        let mut entries = database.tag_manifest_entries().unwrap();
        entries.sort_by_key(|entry| entry.modified_at);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].modified_at, 111);
        assert_eq!(entries[1].modified_at, 222);
    }

    #[test]
    fn tag_ids_matching_token_matches_name_substring_case_insensitively() {
        let database = memory_db();
        let foo = TagId::new();
        let foobar = TagId::new();
        let barfoo = TagId::new();
        let unrelated = TagId::new();
        database.add_tag(foo, "foo", "red", 1).unwrap();
        database.add_tag(foobar, "foobar", "red", 1).unwrap();
        database.add_tag(barfoo, "barfoo", "red", 1).unwrap();
        database.add_tag(unrelated, "baz", "red", 1).unwrap();

        // A different case still matches (case-insensitive substring).
        let matched: BTreeSet<TagId> = database
            .tag_ids_matching_pattern(
                &TextPattern::Substring("FOO".to_owned()),
                DeletedRule::Exclude,
            )
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(
            matched,
            BTreeSet::from([foo, foobar, barfoo]),
            "should match every tag whose name contains 'foo'"
        );
    }

    #[test]
    fn tag_ids_matching_token_empty_when_nothing_matches() {
        let database = memory_db();
        database.add_tag(TagId::new(), "alpha", "red", 1).unwrap();

        assert!(
            database
                .tag_ids_matching_pattern(
                    &TextPattern::Substring("nope".to_owned()),
                    DeletedRule::Exclude
                )
                .unwrap()
                .is_empty(),
            "an unmatched token yields an empty set, not an error"
        );
    }

    #[test]
    fn tag_delete_soft_deletes_and_hides_from_reads() {
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "red", 10).unwrap();

        // Delete with a newer modified_at.
        assert!(database.remove_tag(tag_id, 20).unwrap());

        // Hidden from user-facing reads.
        assert!(
            database
                .get_all_tags(DeletedRule::Exclude)
                .unwrap()
                .into_iter()
                .next()
                .is_none()
        );
        assert!(matches!(
            database.tag_from_id(tag_id, DeletedRule::Exclude),
            Err(DatabaseError::MissingTag)
        ));
        // But the tombstone is advertised in the manifest with a bumped
        // modified_at.
        let entries = database.tag_manifest_entries().unwrap();
        let entry = entries.iter().find(|e| e.tag_id == tag_id).unwrap();
        assert!(entry.deleted);
        assert_eq!(entry.modified_at, 20);
    }

    #[test]
    fn tag_delete_loses_to_newer_edit() {
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "red", 100).unwrap();

        // A delete older than the tag's modified_at is a no-op (LWW).
        assert!(!database.remove_tag(tag_id, 50).unwrap());
        assert!(database.tag_from_id(tag_id, DeletedRule::Exclude).is_ok());
    }

    #[test]
    fn newer_add_revives_deleted_tag() {
        // Restore: a rename/re-add newer than the delete clears the tombstone.
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "red", 10).unwrap();
        assert!(database.remove_tag(tag_id, 20).unwrap());
        assert!(database.tag_from_id(tag_id, DeletedRule::Exclude).is_err());

        // A newer add (upsert) revives it.
        database.add_tag(tag_id, "work", "blue", 30).unwrap();
        let tag = database.tag_from_id(tag_id, DeletedRule::Exclude).unwrap();
        assert_eq!(tag.color, "blue");
    }

    #[test]
    fn restore_tag_preserves_definition() {
        // `Api::restore_tag` reads the tombstoned tag's current name/color and
        // re-announces them via `TagAdded` (add_tag) with a fresh timestamp.
        // This models that round-trip: the revived tag keeps its definition and
        // becomes live again.
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "#123456", 10).unwrap();
        assert!(database.remove_tag(tag_id, 20).unwrap());

        // Read the tombstoned definition (as the API does with `Include`)...
        let deleted = database.tag_from_id(tag_id, DeletedRule::Include).unwrap();
        assert!(deleted.deleted);
        assert_eq!(deleted.name, "work");
        assert_eq!(deleted.color, "#123456");

        // ...and re-announce it with a newer timestamp to restore it.
        database
            .add_tag(tag_id, deleted.name, deleted.color, 30)
            .unwrap();
        let restored = database.tag_from_id(tag_id, DeletedRule::Exclude).unwrap();
        assert!(!restored.deleted);
        assert_eq!(restored.name, "work");
        assert_eq!(restored.color, "#123456");
    }

    #[test]
    fn get_all_tags_include_returns_tombstoned_with_flag() {
        // Sibling of `get_all_files_include_returns_tombstoned_with_flag` on
        // the tag axis.
        let database = memory_db();
        let live = TagId::new();
        let dead = TagId::new();
        database.add_tag(live, "live", "red", 10).unwrap();
        database.add_tag(dead, "dead", "blue", 10).unwrap();
        assert!(database.remove_tag(dead, 20).unwrap());

        let excluded: Vec<_> = database
            .get_all_tags(DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(excluded.len(), 1);
        assert_eq!(excluded[0].id, live);
        assert!(!excluded[0].deleted);

        let included: Vec<_> = database
            .get_all_tags(DeletedRule::Include)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(included.len(), 2);
        let dead_tag = included.iter().find(|t| t.id == dead).unwrap();
        assert!(dead_tag.deleted);
    }

    #[test]
    fn tag_ids_matching_token_include_covers_tombstoned_tags() {
        // A search that wants to see deleted tags must still be able to
        // resolve a name-substring token to a tombstoned tag; otherwise
        // `run_query` with `DeletedRule::Include` would have no way to walk
        // back to a deleted tag by name.
        let database = memory_db();
        let dead = TagId::new();
        database.add_tag(dead, "receipts", "red", 10).unwrap();
        assert!(database.remove_tag(dead, 20).unwrap());

        // Exclude hides the tombstoned tag.
        assert!(
            database
                .tag_ids_matching_pattern(
                    &TextPattern::Substring("receipt".to_owned()),
                    DeletedRule::Exclude
                )
                .unwrap()
                .is_empty()
        );
        // Include finds it.
        let matched = database
            .tag_ids_matching_pattern(
                &TextPattern::Substring("receipt".to_owned()),
                DeletedRule::Include,
            )
            .unwrap();
        assert_eq!(matched, vec![dead]);
    }
}
