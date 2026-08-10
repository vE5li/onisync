//! The `entries_v1` table: the tag graph.
//!
//! One row per relationship — a tag applied to a file, or a tag applied to a
//! tag — carrying its own last-writer-wins clock and tombstone. Untagging
//! flips `deleted` rather than removing the row, so "absent" is a state that
//! can win reconciliation against a peer's stale "present".
//!
//! The `*_inner` walkers traverse that graph transitively under
//! [`SubtagRule::Include`]. The three tag-returning traversals
//! (`tag_ids_for_file`, `subtag_ids_for_tag_inner`, `tag_ids_for_subtag_inner`)
//! `LEFT JOIN tags_v1` so a tombstoned tag drops out of the walk, while a
//! relationship whose tag definition has not reconciled yet is still
//! followed.

use std::collections::BTreeSet;

use tagsy_core::state::{RelationshipKind, RelationshipManifestEntry};
use tagsy_core::{FileId, TagId};
use rusqlite::OptionalExtension;

use super::CatalogStore;
use super::types::{DatabaseError, SubtagRule};

impl CatalogStore {
    /// Every tag relationship (file-tagged and tag-tagged), *including*
    /// soft-deleted (tombstoned) rows. Reconciliation deliberately advertises
    /// tombstones so that an "absent" relationship can win last-writer-wins
    /// against a peer's stale "present". Unlike tag definitions, a relationship
    /// carries its whole state here, so the receiver applies it directly with
    /// no follow-up request.
    pub fn relationship_manifest_entries(
        &self,
    ) -> Result<Vec<RelationshipManifestEntry>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT tag_id, target_id, type, modified_at, deleted FROM entries_v1")?;
        let entries = statement
            .query_map([], |row| {
                let deleted: i64 = row.get(4)?;
                Ok(RelationshipManifestEntry {
                    tag_id: row.get(0)?,
                    target_id: row.get(1)?,
                    kind: row.get(2)?,
                    modified_at: row.get(3)?,
                    deleted: deleted != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }

    /// The `modified_at` of a single relationship, or `None` if we have no row
    /// for it. Used by reconciliation to decide whether an incoming
    /// relationship wins last-writer-wins before applying it.
    pub fn relationship_modified_at(
        &self,
        tag_id: TagId,
        target_id: &str,
        kind: RelationshipKind,
    ) -> Result<Option<i64>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT modified_at FROM entries_v1
                 WHERE tag_id = ?1 AND target_id = ?2 AND type = ?3",
                (&tag_id, &target_id, kind),
                |row| row.get(0),
            )
            .optional()
            .map_err(DatabaseError::from)
    }

    /// Apply an incoming relationship (from a peer's tag manifest) with
    /// last-writer-wins, preserving its `modified_at` and `deleted` state.
    /// Newer-wins is enforced in SQL so replaying stale relationships is a
    /// no-op. `target_id` is the stringified `FileId`/`TagId` per `kind`.
    pub fn apply_relationship(
        &self,
        entry: &RelationshipManifestEntry,
    ) -> Result<(), DatabaseError> {
        let kind = entry.kind;
        self.connection.execute(
            "INSERT INTO entries_v1 (id, tag_id, target_id, type, modified_at, deleted)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(tag_id, target_id, type) DO UPDATE SET
                     modified_at = excluded.modified_at,
                     deleted = excluded.deleted
                 WHERE excluded.modified_at > entries_v1.modified_at",
            (
                TagId::new(),
                &entry.tag_id,
                &entry.target_id,
                kind,
                entry.modified_at,
                entry.deleted as i64,
            ),
        )?;
        Ok(())
    }

    /// Tag a file with the provided tag.
    ///
    /// `modified_at` is the last-writer-wins timestamp; see
    /// [`CatalogStore::add_tag`]. This is an upsert: if a (possibly
    /// tombstoned) row for this `(tag_id, file_id)` relationship already
    /// exists, it is revived (`deleted = 0`) and stamped — but only when
    /// `modified_at` is newer than the stored value. Re-tagging is
    /// therefore idempotent and correctly loses to a newer untag.
    pub fn tag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.upsert_entry(
            tag_id,
            file_id.to_string(),
            RelationshipKind::File,
            modified_at,
        )
    }

    /// Tag a tag with the provided tag. See [`CatalogStore::tag_file`] for the
    /// LWW/upsert semantics and the `modified_at` contract.
    pub fn tag_tag(
        &self,
        tag_id: TagId,
        subtag_id: TagId,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        if tag_id == subtag_id {
            return Err(DatabaseError::CantTagItself);
        }

        self.upsert_entry(
            tag_id,
            subtag_id.to_string(),
            RelationshipKind::Tag,
            modified_at,
        )
    }

    /// Remove a tag from a file (soft delete). See `untag_entry`.
    pub fn untag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.untag_entry(
            tag_id,
            file_id.to_string(),
            RelationshipKind::File,
            modified_at,
        )
    }

    /// Remove a tag from a tag (soft delete). See `untag_entry`.
    pub fn untag_tag(
        &self,
        tag_id: TagId,
        subtag_id: TagId,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.untag_entry(
            tag_id,
            subtag_id.to_string(),
            RelationshipKind::Tag,
            modified_at,
        )
    }

    /// Shared upsert for the two "add relationship" paths. Inserts a live
    /// (`deleted = 0`) entry, or on conflict revives/refreshes the existing row
    /// — gated by last-writer-wins so an older change can't override a newer
    /// one. `target_id` is the stringified `FileId`/`TagId` (both persist as
    /// simple-hex, matching the column's storage).
    fn upsert_entry(
        &self,
        tag_id: TagId,
        target_id: String,
        entry_type: RelationshipKind,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.connection.execute(
            "INSERT INTO entries_v1 (id, tag_id, target_id, type, modified_at, deleted)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0)
                 ON CONFLICT(tag_id, target_id, type) DO UPDATE SET
                     modified_at = excluded.modified_at,
                     deleted = 0
                 WHERE excluded.modified_at > entries_v1.modified_at",
            (TagId::new(), &tag_id, &target_id, entry_type, modified_at),
        )?;

        Ok(())
    }

    /// Shared soft-delete for the two "remove relationship" paths. Marks the
    /// row `deleted = 1` and stamps `modified_at`, gated by
    /// last-writer-wins so a stale untag can't override a newer tag. If the
    /// relationship was never recorded, this is a no-op (there is no row to
    /// tombstone; a peer that only knows the untag can still learn "absent"
    /// once we've seen the tag).
    ///
    /// NOTE: Offline untag propagation is only fully correct once the broader
    /// deletion/tombstone design lands — see roadmap. Today the tombstone is
    /// created locally and reconciled, but there is no tombstone GC.
    fn untag_entry(
        &self,
        tag_id: TagId,
        target_id: String,
        entry_type: RelationshipKind,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.connection.execute(
            "UPDATE entries_v1 SET deleted = 1, modified_at = ?4
                 WHERE tag_id = ?1 AND target_id = ?2 AND type = ?3
                   AND ?4 > modified_at",
            (&tag_id, &target_id, entry_type, modified_at),
        )?;

        Ok(())
    }

    fn file_ids_for_tag_inner(
        &self,
        tag_id: TagId,
        lookup_cache: &mut BTreeSet<TagId>,
        collected_tag_ids: &mut BTreeSet<FileId>,
        subtag_rule: SubtagRule,
    ) -> Result<(), DatabaseError> {
        enum Entry {
            File { file_id: FileId },
            Tag { tag_id: TagId },
        }

        let mut statement = self
            .connection
            .prepare("SELECT target_id, type FROM entries_v1 WHERE tag_id = ?1 AND deleted = 0")?;

        let iterator = statement
            .query_map([tag_id], |row| {
                let r#type: RelationshipKind = row.get(1)?;

                let entry = match r#type {
                    RelationshipKind::File => Entry::File {
                        file_id: row.get(0)?,
                    },
                    RelationshipKind::Tag => Entry::Tag {
                        tag_id: row.get(0)?,
                    },
                };

                Ok(entry)
            })?
            .map(|entry| entry.unwrap());

        lookup_cache.insert(tag_id);

        for entry in iterator {
            match entry {
                Entry::File { file_id } => {
                    collected_tag_ids.insert(file_id);
                }
                Entry::Tag { tag_id } => {
                    if subtag_rule == SubtagRule::Include && !lookup_cache.contains(&tag_id) {
                        self.file_ids_for_tag_inner(
                            tag_id,
                            lookup_cache,
                            collected_tag_ids,
                            subtag_rule,
                        )?
                    }
                }
            }
        }

        Ok(())
    }

    /// Get all files that are tagged with the provided tag.
    pub fn file_ids_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = FileId>, DatabaseError> {
        let mut file_ids = BTreeSet::new();
        let mut lookup_cache = BTreeSet::new();

        self.file_ids_for_tag_inner(tag_id, &mut lookup_cache, &mut file_ids, subtag_rule)?;

        Ok(file_ids)
    }

    /// Get all files that are tagged with the provided tag.
    pub fn tag_ids_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        // Exclude both the relationship tombstone (`entries_v1.deleted`) *and*
        // tags whose own row is tombstoned (`tags_v1.deleted`): deleting a tag
        // is distinct from untagging, so the relationship survives, but a
        // deleted tag must not appear as an applied tag (the UI would then try
        // to fetch a `MissingTag` and error).
        //
        // LEFT JOIN (not INNER): a live relationship can legitimately reference
        // a tag whose definition hasn't been reconciled yet (`FileTagged` can
        // arrive before `TagAdded`). Such a tag has no `tags_v1` row — keep it
        // (`t.deleted IS NULL`); only exclude tags we *know* are tombstoned.
        let mut statement = self.connection.prepare(
            "SELECT e.tag_id FROM entries_v1 AS e
                 LEFT JOIN tags_v1 AS t ON t.id = e.tag_id
                 WHERE e.target_id = ?1 AND e.type = 0 AND e.deleted = 0
                   AND (t.deleted = 0 OR t.deleted IS NULL)",
        )?;

        let mut tag_ids = statement
            .query_map([file_id], |row| row.get::<_, TagId>(0))?
            .map(|tag_id| tag_id.unwrap())
            .collect::<BTreeSet<_>>();

        if subtag_rule == SubtagRule::Include {
            let mut lookup_cache = BTreeSet::new();

            for tag_id in tag_ids.clone() {
                self.tag_ids_for_subtag_inner(
                    tag_id,
                    &mut lookup_cache,
                    &mut tag_ids,
                    subtag_rule,
                )?;
            }
        }

        Ok(tag_ids)
    }

    fn subtag_ids_for_tag_inner(
        &self,
        tag_id: TagId,
        lookup_cache: &mut BTreeSet<TagId>,
        collected_tags: &mut BTreeSet<TagId>,
        subtag_rule: SubtagRule,
    ) -> Result<(), DatabaseError> {
        // Skip subtags whose own tag row is tombstoned (a deleted tag is not a
        // live subtag), alongside the relationship tombstone. LEFT JOIN so a
        // subtag whose definition hasn't reconciled yet (no `tags_v1` row) is
        // still returned; only known-tombstoned tags are excluded.
        let mut statement = self.connection.prepare(
            "SELECT e.target_id FROM entries_v1 AS e
                 LEFT JOIN tags_v1 AS t ON t.id = e.target_id
                 WHERE e.tag_id = ?1 AND e.type = 1 AND e.deleted = 0
                   AND (t.deleted = 0 OR t.deleted IS NULL)",
        )?;

        let iterator = statement
            .query_map([tag_id], |row| row.get::<_, TagId>(0))?
            .map(|entry| entry.unwrap());

        lookup_cache.insert(tag_id);

        for tag_id in iterator {
            collected_tags.insert(tag_id);

            if subtag_rule == SubtagRule::Include && !lookup_cache.contains(&tag_id) {
                self.subtag_ids_for_tag_inner(tag_id, lookup_cache, collected_tags, subtag_rule)?;
            }
        }

        Ok(())
    }

    /// Get all subtags tagged with the provided tag.
    pub fn subtag_ids_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        let mut tags = BTreeSet::new();
        let mut lookup_cache = BTreeSet::new();

        self.subtag_ids_for_tag_inner(tag_id, &mut lookup_cache, &mut tags, subtag_rule)?;

        Ok(tags)
    }

    fn tag_ids_for_subtag_inner(
        &self,
        subtag_id: TagId,
        lookup_cache: &mut BTreeSet<TagId>,
        collected_tags: &mut BTreeSet<TagId>,
        subtag_rule: SubtagRule,
    ) -> Result<(), DatabaseError> {
        // Skip parent tags whose own tag row is tombstoned (a deleted tag is not
        // a live parent), alongside the relationship tombstone. LEFT JOIN so a
        // parent whose definition hasn't reconciled yet (no `tags_v1` row) is
        // still returned; only known-tombstoned tags are excluded.
        let mut statement = self.connection.prepare(
            "SELECT e.tag_id FROM entries_v1 AS e
                 LEFT JOIN tags_v1 AS t ON t.id = e.tag_id
                 WHERE e.target_id = ?1 AND e.type = 1 AND e.deleted = 0
                   AND (t.deleted = 0 OR t.deleted IS NULL)",
        )?;

        let iterator = statement
            .query_map([subtag_id], |row| row.get::<_, TagId>(0))?
            .map(|entry| entry.unwrap());

        lookup_cache.insert(subtag_id);

        for tag_id in iterator {
            collected_tags.insert(tag_id);

            if subtag_rule == SubtagRule::Include && !lookup_cache.contains(&tag_id) {
                self.tag_ids_for_subtag_inner(tag_id, lookup_cache, collected_tags, subtag_rule)?;
            }
        }

        Ok(())
    }

    /// Get all tags that tag the provided tag.
    pub fn tag_ids_for_subtag(
        &self,
        subtag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        let mut tags = BTreeSet::new();
        let mut lookup_cache = BTreeSet::new();

        self.tag_ids_for_subtag_inner(subtag_id, &mut lookup_cache, &mut tags, subtag_rule)?;

        Ok(tags)
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::LogicalPath;

    use super::*;
    use crate::store::fixtures::memory_db;

    #[test]
    fn tag_tag_then_subtag_ids_lists_children() {
        let database = memory_db();
        let parent = TagId::new();
        let child_a = TagId::new();
        let child_b = TagId::new();
        for (id, name) in [(parent, "parent"), (child_a, "a"), (child_b, "b")] {
            database.add_tag(id, name, "red", 1).unwrap();
        }

        database.tag_tag(parent, child_a, 10).unwrap();
        database.tag_tag(parent, child_b, 10).unwrap();

        let subtags: BTreeSet<TagId> = database
            .subtag_ids_for_tag(parent, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(subtags, BTreeSet::from([child_a, child_b]));
    }

    #[test]
    fn subtag_ids_include_walks_transitively() {
        let database = memory_db();
        let grandparent = TagId::new();
        let parent = TagId::new();
        let child = TagId::new();
        for (id, name) in [(grandparent, "gp"), (parent, "p"), (child, "c")] {
            database.add_tag(id, name, "red", 1).unwrap();
        }

        database.tag_tag(grandparent, parent, 10).unwrap();
        database.tag_tag(parent, child, 10).unwrap();

        // Direct only: just `parent`.
        let direct: BTreeSet<TagId> = database
            .subtag_ids_for_tag(grandparent, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(direct, BTreeSet::from([parent]));

        // Transitive: `parent` and `child`.
        let transitive: BTreeSet<TagId> = database
            .subtag_ids_for_tag(grandparent, SubtagRule::Include)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(transitive, BTreeSet::from([parent, child]));
    }

    #[test]
    fn untag_tag_removes_child_from_subtags() {
        let database = memory_db();
        let parent = TagId::new();
        let child = TagId::new();
        database.add_tag(parent, "parent", "red", 1).unwrap();
        database.add_tag(child, "child", "red", 1).unwrap();

        database.tag_tag(parent, child, 10).unwrap();
        database.untag_tag(parent, child, 20).unwrap();

        let subtags: Vec<TagId> = database
            .subtag_ids_for_tag(parent, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(subtags.is_empty());
    }

    #[test]
    fn tag_tag_rejects_self() {
        let database = memory_db();
        let tag = TagId::new();
        database.add_tag(tag, "t", "red", 1).unwrap();
        assert!(matches!(
            database.tag_tag(tag, tag, 10),
            Err(DatabaseError::CantTagItself)
        ));
    }

    #[test]
    fn tag_file_then_untag_soft_deletes_and_hides_from_reads() {
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        database.tag_file(tag_id, file_id, 100).unwrap();
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);

        // Untag soft-deletes: the read no longer sees it...
        database.untag_file(tag_id, file_id, 200).unwrap();
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(tags.is_empty());

        // ...but the tombstone row survives for reconciliation.
        let manifest = database.relationship_manifest_entries().unwrap();
        assert_eq!(manifest.len(), 1);
        assert!(manifest[0].deleted);
        assert_eq!(manifest[0].modified_at, 200);
    }

    #[test]
    fn deleting_a_tag_hides_it_from_applied_tags() {
        // Regression: tagging a file then deleting the *tag* (not untagging)
        // leaves the relationship live but the tag row tombstoned. The applied-
        // tags read must exclude it so the UI doesn't fetch a MissingTag.
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.add_tag(tag_id, "work", "red", 10).unwrap();
        database.tag_file(tag_id, file_id, 100).unwrap();

        // Applied while the tag is live.
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);

        // Delete the tag itself (relationship is untouched).
        assert!(database.remove_tag(tag_id, 200).unwrap());
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(tags.is_empty(), "a deleted tag must not appear as applied");

        // Restoring the tag makes it applied again (the relationship survived).
        database.add_tag(tag_id, "work", "red", 300).unwrap();
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);
    }

    #[test]
    fn applied_tag_without_definition_is_still_returned() {
        // A `FileTagged` relationship can arrive before the tag's `TagAdded`
        // definition during reconciliation, so a live relationship may point at
        // a tag with no `tags_v1` row yet. That tag must still be returned (the
        // filter only excludes *known-tombstoned* tags).
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        // Tag the file without ever defining the tag.
        database.tag_file(tag_id, file_id, 100).unwrap();

        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);
    }

    #[test]
    fn stale_untag_does_not_override_newer_tag() {
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        // Tag at t=200, then a stale untag at t=100 arrives out of order.
        database.tag_file(tag_id, file_id, 200).unwrap();
        database.untag_file(tag_id, file_id, 100).unwrap();

        // The tag must still be present (newer wins).
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);
    }

    #[test]
    fn retag_after_untag_revives_row() {
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        database.tag_file(tag_id, file_id, 100).unwrap();
        database.untag_file(tag_id, file_id, 200).unwrap();
        database.tag_file(tag_id, file_id, 300).unwrap();

        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);
        // Still a single row (revived, not duplicated).
        assert_eq!(database.relationship_manifest_entries().unwrap().len(), 1);
    }

    #[test]
    fn apply_relationship_reconciles_tombstone_with_lww() {
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        // Locally the file is tagged at t=100.
        database.tag_file(tag_id, file_id, 100).unwrap();

        // A peer's manifest carries a newer tombstone (untagged at t=200).
        let incoming = RelationshipManifestEntry {
            tag_id,
            target_id: file_id.to_string(),
            kind: RelationshipKind::File,
            modified_at: 200,
            deleted: true,
        };
        database.apply_relationship(&incoming).unwrap();

        // The newer tombstone wins: the tag is now absent.
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(tags.is_empty());

        // A stale "present" (t=150) from another peer must not resurrect it.
        let stale = RelationshipManifestEntry {
            tag_id,
            target_id: file_id.to_string(),
            kind: RelationshipKind::File,
            modified_at: 150,
            deleted: false,
        };
        database.apply_relationship(&stale).unwrap();
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(tags.is_empty());
    }
}
