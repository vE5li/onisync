//! Reconciling a peer's tag manifest (definitions + relationships) against
//! ours.

use tagsy_core::state::{
    Change, ChangeOrigin, Frame, RelationshipManifestEntry, Sync as SyncMessage, TagManifestEntry,
};
use tagsy_core::{FileId, TagId};
use tokio::sync::mpsc::UnboundedSender;

use crate::bus::{DaemonMessage, Ingest};
use crate::store::CatalogStore;

/// Build our local tag manifest: lightweight definition entries (id +
/// `modified_at`) plus every relationship (with its soft-delete state). The
/// tag counterpart of [`crate::peer::plan::build_local_manifest`].
pub fn build_local_tag_manifest(
    database: &CatalogStore,
) -> Result<(Vec<TagManifestEntry>, Vec<RelationshipManifestEntry>), String> {
    let definitions = database
        .tag_manifest_entries()
        .map_err(|e| format!("tag_manifest_entries: {e:?}"))?;
    let relationships = database
        .relationship_manifest_entries()
        .map_err(|e| format!("relationship_manifest_entries: {e:?}"))?;
    Ok((definitions, relationships))
}

/// Reconcile a peer's tag manifest against ours.
///
/// - **Definitions**: for each tag whose `modified_at` is newer than ours (or
///   that we don't know), enqueue a `TagRequest`; the peer answers with a
///   `Change::TagAdded` carrying the full definition. Older/equal definitions
///   are skipped — the peer will request ours via the symmetric path.
/// - **Relationships**: applied directly. Each carries its whole state, so we
///   translate it into the matching relationship `Change` (tag/untag) stamped
///   with the peer's `modified_at` and hand it to the single DB-writer, which
///   enforces last-writer-wins. This routes through the same code path as a
///   live relationship change, keeping behavior uniform.
///
/// Pure of `.await` and holds no lock: takes `&CatalogStore` synchronously so
/// the caller's future stays `Send`.
pub fn plan_tag_sync(
    peer_name: &str,
    peer_public_key: &str,
    definitions: Vec<TagManifestEntry>,
    relationships: Vec<RelationshipManifestEntry>,
    database: &CatalogStore,
    outbound: &UnboundedSender<Frame>,
    change_sender: &UnboundedSender<DaemonMessage>,
) {
    log::info!(
        "Reconciling {} tag definitions and {} relationships from {peer_name}",
        definitions.len(),
        relationships.len()
    );

    for definition in definitions {
        let ours = match database.tag_modified_at(definition.tag_id) {
            Ok(value) => value,
            Err(error) => {
                log::error!(
                    "tag_modified_at failed for {}: {error:?}",
                    definition.tag_id.to_string()
                );
                continue;
            }
        };
        // Act when we don't know the tag, or the peer's is strictly newer.
        let need = match ours {
            None => true,
            Some(ours) => definition.modified_at > ours,
        };
        if !need {
            continue;
        }

        if definition.deleted {
            // The peer's newer state is a tombstone. Apply it directly through
            // the single DB writer (a tag delete bumps `modified_at`, so the
            // same LWW comparison decides delete-vs-edit). No `TagRequest`: there
            // is no definition to fetch for a deleted tag.
            if let Err(error) = change_sender.send(DaemonMessage::Change(
                Ingest::from_change(Change::TagRemoved {
                    tag_id: definition.tag_id,
                    modified_at: definition.modified_at,
                }),
                ChangeOrigin::Peer {
                    public_key: peer_public_key.to_owned(),
                },
            )) {
                log::error!("change_sender closed; cannot apply reconciled tag delete: {error}");
                return;
            }
        } else {
            // A live tag whose definition is newer than ours: request it.
            let frame = Frame::Sync(SyncMessage::TagRequest {
                tag_id: definition.tag_id,
            });
            if let Err(error) = outbound.send(frame) {
                log::warn!("Failed to enqueue TagRequest for {peer_name}: {error}");
                return;
            }
        }
    }

    for relationship in relationships {
        // LWW pre-check: skip anything not newer than what we hold. The DB layer
        // also enforces this, but skipping here avoids bus traffic for no-ops.
        let ours = match database.relationship_modified_at(
            relationship.tag_id,
            &relationship.target_id,
            relationship.kind,
        ) {
            Ok(value) => value,
            Err(error) => {
                log::error!("relationship_modified_at failed: {error:?}");
                continue;
            }
        };
        if let Some(ours) = ours
            && relationship.modified_at <= ours
        {
            continue;
        }

        let Some(change) = relationship_to_change(&relationship) else {
            log::warn!(
                "Skipping relationship with unparseable target_id {}",
                relationship.target_id
            );
            continue;
        };
        if let Err(error) = change_sender.send(DaemonMessage::Change(
            Ingest::from_change(change),
            ChangeOrigin::Peer {
                public_key: peer_public_key.to_owned(),
            },
        )) {
            log::error!("change_sender closed; cannot apply reconciled relationship: {error}");
            return;
        }
    }
}

/// Translate a reconciled relationship manifest entry into the equivalent
/// relationship `Change`, carrying the entry's `modified_at` so
/// last-writer-wins is preserved. Returns `None` if `target_id` doesn't parse
/// as the id kind.
fn relationship_to_change(entry: &RelationshipManifestEntry) -> Option<Change> {
    use tagsy_core::state::RelationshipKind;
    let change = match (entry.kind, entry.deleted) {
        (RelationshipKind::File, false) => Change::FileTagged {
            file_id: FileId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            metadata: None,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::File, true) => Change::FileUntagged {
            file_id: FileId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::Tag, false) => Change::TagTagged {
            taggee_id: TagId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            metadata: None,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::Tag, true) => Change::TagUntagged {
            taggee_id: TagId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            modified_at: entry.modified_at,
        },
    };
    Some(change)
}

/// Answer a peer's `TagRequest` with the full tag definition as a
/// `Change::TagAdded`, or `TagNotFound` if we no longer hold the tag.
pub fn build_tag_request_response(
    peer_name: &str,
    tag_id: TagId,
    database: &CatalogStore,
) -> Frame {
    match database.tag_definition(tag_id) {
        Ok(Some((name, color, modified_at))) => Frame::Change(Change::TagAdded {
            tag_id,
            tag_name: name,
            color,
            metadata: None,
            modified_at,
        }),
        Ok(None) => {
            log::warn!(
                "TagRequest from {peer_name} for {} but we no longer hold it",
                tag_id.to_string()
            );
            Frame::Sync(SyncMessage::TagNotFound { tag_id })
        }
        Err(error) => {
            log::error!(
                "tag_definition failed for {} requested by {peer_name}: {error:?}",
                tag_id.to_string()
            );
            Frame::Sync(SyncMessage::TagNotFound { tag_id })
        }
    }
}
