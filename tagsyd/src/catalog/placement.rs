//! Deciding which sync directories a file's bytes belong in, and getting them
//! there.
//!
//! Two related but distinct jobs live here:
//!
//! - **Content placement** ([`Placement`], [`placements_for`],
//!   [`place_content`]): given a content-bearing change that already has its
//!   bytes in hand, work out which sync directories should receive them and
//!   apply the move-vs-copy policy.
//! - **Tag-driven placement** ([`plan_placement`],
//!   [`fetch_and_place_deferred`]): given a file whose tag set just changed (or
//!   a connect-time sweep), ask the sync-directory manager whether any
//!   `TagBased` directory now wants it and, if the bytes aren't local, fetch
//!   them on demand.

use std::path::PathBuf;

use tagsy_core::state::ChangeOrigin;
use tagsy_core::{FileId, LogicalPath, PhysicalPath, TagId};
use tokio::sync::mpsc::UnboundedSender;

use crate::bus::{self, DaemonMessage};
use crate::configuration::{Configuration, SyncType};
use crate::directory_manager::SyncDirectoryCommand;
use crate::fetch::PendingFetches;
use crate::file_bytes::FileBytes;
use crate::operations::Operations;
use crate::store::{self, CatalogStore};

/// A resolved sync-directory destination for a content-bearing change, plus how
/// that directory should be told to place the bytes. Produced by
/// `handle_changes` after the origin-skip and tag-match filtering, then turned
/// into a [`SyncDirectoryCommand`] with the actual [`FileBytes`] once the
/// move-vs-copy policy has been applied.
pub(crate) enum Placement {
    Create {
        file_id: FileId,
        physical_path: PhysicalPath,
        sync_directory_path: PathBuf,
    },
    Change {
        file_id: FileId,
        sync_directory_path: PathBuf,
    },
}

impl Placement {
    pub(crate) fn into_command(self, content: FileBytes) -> SyncDirectoryCommand {
        match self {
            Placement::Create {
                file_id,
                physical_path,
                sync_directory_path,
            } => SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content,
                sync_directory_path,
            },
            Placement::Change {
                file_id,
                sync_directory_path,
            } => SyncDirectoryCommand::ChangeFile {
                file_id,
                content,
                sync_directory_path,
            },
        }
    }
}

/// True when every tag a sync directory requires is present in `file_tags`,
/// i.e. the file belongs in that TagBased directory.
pub(crate) fn contains_all_tags(sync_directory_tags: &[TagId], file_tags: &[TagId]) -> bool {
    sync_directory_tags
        .iter()
        .all(|tag_id| file_tags.contains(tag_id))
}

/// The effective tag set to use when deciding `Create` placement for a
/// materialized file: the union of the tags carried on the placement and the
/// file's current tags in the database.
///
/// The carried tags cover a live `FileMetadataAdded` whose `FileTagged`
/// relationships may not be applied yet; the DB tags cover a `Manifest`
/// reconcile pull, which carries empty tags (it cannot know them at pull time)
/// but whose `FileTagged` changes have been applied by the time the pull's
/// `Materialize` runs. Using the union places the file correctly in both cases.
pub(crate) fn effective_placement_tags(carried: &[TagId], db_tags: &[TagId]) -> Vec<TagId> {
    let mut effective = carried.to_vec();
    for tag_id in db_tags {
        if !effective.contains(tag_id) {
            effective.push(*tag_id);
        }
    }
    effective
}

/// Owned data a deferred tag placement needs to fetch and materialize a file's
/// bytes. Produced by the synchronous [`plan_placement`] DB step and consumed
/// by the async [`fetch_and_place_deferred`]. Deliberately holds no `!Send`
/// `&CatalogStore` borrow so the fetch future stays `Send`.
pub(crate) struct DeferredPlacement {
    file_id: FileId,
    logical_path: LogicalPath,
    file_tags: Vec<TagId>,
    /// The file's latest catalog `(content_hash, size)`, or `None` if the file
    /// has no recorded version (in which case there is nothing to fetch by).
    latest_version: Option<(String, u64)>,
    /// Resolves to `true` if the manager deferred placement (wants the file but
    /// has no local bytes to source), `false` otherwise.
    deferred: tokio::sync::oneshot::Receiver<bool>,
}

/// Ask the sync-directory manager to re-evaluate `file_id`'s TagBased placement
/// against its current tag set. Called whenever the file's tags change
/// (`FileTagged` / `FileUntagged`), so a file that gained a directory's tags is
/// placed there and one that lost them is dropped. Also the recovery path for
/// the tag-vs-content reconciliation race (see
/// `SyncDirectoryCommand::ReconcileTagPlacement`).
///
/// This is the **synchronous** DB step: it does all `&CatalogStore` reads and
/// sends the command, returning a [`DeferredPlacement`] for the caller to
/// `await` via [`fetch_and_place_deferred`]. Splitting it this way keeps the
/// caller's future `Send` — no `!Send` database borrow is held across an
/// `.await`. Returns `None` when there is nothing further to do (read error /
/// closed channel).
pub(crate) fn plan_placement(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    database: &CatalogStore,
    file_id: FileId,
) -> Option<DeferredPlacement> {
    let logical_path = match database.logical_path_for_file_id(file_id) {
        Ok(logical_path) => logical_path,
        Err(error) => {
            log::debug!(
                "plan_placement: no logical path for {} ({:?}); skipping",
                file_id.to_string(),
                error
            );
            return None;
        }
    };

    let file_tags = match database.tag_ids_for_file(file_id, store::SubtagRule::Exclude) {
        Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
        Err(error) => {
            log::error!(
                "plan_placement: failed to read tags for {}: {:?}",
                file_id.to_string(),
                error
            );
            return None;
        }
    };

    // Read the latest catalog version hash now, alongside the other DB reads, so
    // the async follow-up needs no database access (and thus no `!Send` borrow
    // across an await). `None` = no recorded version (nothing to fetch by).
    let latest_version = match database.latest_version(file_id) {
        Ok(version) => version.map(|version| (version.content_hash, version.size as u64)),
        Err(error) => {
            log::error!(
                "plan_placement: failed to read latest version for {}: {:?}",
                file_id.to_string(),
                error
            );
            None
        }
    };

    let (respond_to, deferred) = tokio::sync::oneshot::channel();
    if let Err(error) = command_sender.send(SyncDirectoryCommand::ReconcileTagPlacement {
        file_id,
        logical_path: logical_path.clone(),
        file_tags: file_tags.clone(),
        respond_to,
    }) {
        log::error!(
            "plan_placement: command channel closed for {}: {error}",
            file_id.to_string()
        );
        return None;
    }

    Some(DeferredPlacement {
        file_id,
        logical_path,
        file_tags,
        latest_version,
        deferred,
    })
}

/// The async follow-up to [`plan_placement`]. If the manager deferred
/// placement (a TagBased directory wants the file but no local copy exists to
/// source its bytes), fetch the bytes on demand — keyed by the file's latest
/// catalog version hash — and enqueue a [`DaemonMessage::Materialize`] to place
/// them. This is the fix for "adding the tags a sync directory requires doesn't
/// pull a file that isn't local yet".
///
/// Takes only owned, `Send` data so the enclosing future can be spawned.
pub(crate) async fn fetch_and_place_deferred(
    pending_fetches: &PendingFetches,
    change_sender: &UnboundedSender<DaemonMessage>,
    operations: &Operations,
    placement: DeferredPlacement,
) {
    let DeferredPlacement {
        file_id,
        logical_path,
        file_tags,
        latest_version,
        deferred,
    } = placement;

    // Placement fully resolved locally (or the manager task went away): done.
    match deferred.await {
        Ok(true) => {}
        Ok(false) => return,
        Err(_) => {
            log::warn!(
                "plan_placement: manager dropped responder for {}; cannot tell if a fetch is \
                 needed",
                file_id.to_string()
            );
            return;
        }
    }

    // A TagBased directory wants this file but we hold no local copy. Fetch the
    // bytes on demand using the file's latest catalog version hash. Because the
    // catalog is byte-independent, this hash is present for any file we know
    // about even though its bytes are not (yet) local.
    let Some((expected_hash, expected_size)) = latest_version else {
        // No version has ever been recorded. Nothing to fetch by; a future
        // announcement / materialize will place it. Soft deferral.
        log::debug!(
            "plan_placement: no recorded version for {}; leaving placement for a future \
             announcement",
            file_id.to_string()
        );
        return;
    };

    // Drive the content-addressed receive directly against the relay rather
    // than routing a `DaemonMessage::Fetch` back onto the ingest bus: we are
    // *inside* the single-threaded `handle_changes` consumer, so awaiting a
    // reply to a message we enqueue onto our own channel would deadlock. The
    // relay floods the live peer tree and resolves when the bytes arrive.
    log::debug!(
        "plan_placement: fetching {} at catalog hash {} to place it locally",
        file_id.to_string(),
        expected_hash,
    );
    // Surface the placement fetch as a live operation for the UI.
    let placing = operations.begin(crate::operations::OperationKind::placing_file(file_id));

    let content = match crate::fetch_via_relay(
        pending_fetches,
        file_id,
        expected_hash.clone(),
        expected_size,
        None,
    )
    .await
    {
        Ok(content) => {
            log::debug!(
                "plan_placement: fetch of {} succeeded; materializing",
                file_id.to_string()
            );
            placing.complete();
            content
        }
        Err(error) => {
            // No peer had the bytes. Soft deferral: a later reconnect /
            // announcement retries placement.
            log::debug!(
                "plan_placement: fetch of {} failed ({error:?}); placement deferred until a peer \
                 can serve it",
                file_id.to_string()
            );
            placing.fail(format!("{error:?}"));
            return;
        }
    };

    // Place the fetched bytes via the normal materialize pipeline, which creates
    // the file in every matching sync directory (tag-filtered). Fire-and-forget
    // onto our own bus: processed on a later loop iteration, so this does not
    // block or deadlock the current one.
    if let Err(error) = change_sender.send(DaemonMessage::Materialize {
        file_id,
        content,
        content_hash: expected_hash,
        // Bytes sourced by our own on-demand fetch, not a specific announcing
        // peer. `Materialize` does not record a version or forward, so the
        // origin is only a sentinel here.
        origin: ChangeOrigin::Local {
            directory_path: PathBuf::new(),
        },
        placement: bus::MaterializePlacement::Create {
            logical_path,
            tags: file_tags,
        },
    }) {
        log::error!(
            "plan_placement: change channel closed; cannot materialize fetched bytes for {}: \
             {error}",
            file_id.to_string()
        );
    }
}

/// Build the list of sync directories that should receive a `ChangeFile`
/// for `file_id`, applying the origin-skip and tag-match filters.
pub(crate) fn placements_for(
    configuration: &Configuration,
    change_origin: &ChangeOrigin,
    file_id: FileId,
    file_tags: &[TagId],
) -> Vec<Placement> {
    let mut targets = Vec::new();
    for sync_directory in &configuration.sync_directories {
        if let ChangeOrigin::Local { directory_path } = change_origin
            && directory_path == &sync_directory.path
        {
            continue;
        };

        if let SyncType::TagBased {
            tags: sync_directory_tags,
        } = &sync_directory.sync_type
            && !contains_all_tags(sync_directory_tags, file_tags)
        {
            continue;
        }

        targets.push(Placement::Change {
            file_id,
            sync_directory_path: sync_directory.path.clone(),
        });
    }
    targets
}

/// Dispatch a content-bearing change to matching sync directories, applying
/// the move-vs-copy policy.
///
/// `content` is a [`FileBytes`] that may still live on disk. This function
/// decides, per the number of matching sync directories (`N`), how each one
/// obtains the bytes:
///
/// - `N == 0`: nothing is dispatched. A `FileToMove` source is left in place
///   (no auto-cleanup this pass; see `file_bytes` docs).
/// - `N == 1`: the single directory receives the producer's original variant. A
///   `FileToMove` stays a move — zero extra copies, the common single-directory
///   win.
/// - `N > 1`: a `FileToMove` can be honored only once, so every directory
///   instead receives a `FileToCopy` of the source and, after dispatch, the
///   source is removed here (preserving the "move" intent: the source does not
///   survive ingestion). `InMemory` is cloned per directory as before.
pub(crate) async fn place_content(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    targets: Vec<Placement>,
    content: FileBytes,
) {
    let source_path = content.path().map(|path| path.to_path_buf());
    let move_intent = matches!(content, FileBytes::FileToMove(_));

    match targets.len() {
        0 => {
            // No matching sync directory. Drop the content; a move source
            // is intentionally left in place (documented no-cleanup).
        }
        1 => {
            let target = targets.into_iter().next().expect("len checked == 1");
            let _ = command_sender.send(target.into_command(content));
        }
        _ => {
            // Multiple destinations. A destructive move can be honored by
            // exactly one consumer, so give the earlier targets a
            // `FileToCopy` and the LAST target a `FileToMove` (when the
            // source may be discarded), consuming the source.
            //
            // We must NOT eagerly delete the move source here: `CreateFile`
            // commands are processed asynchronously by the sync-directory
            // manager, so a delete issued now would race ahead of the copies
            // and leave them reading a missing file (observed as
            // `FailedAddingFile`, dropping the file everywhere). Commands are
            // processed in FIFO order, so the copies read the source while it
            // still exists and the trailing move consumes it last. If the
            // content was not a move (`FileToCopy`/`InMemory`), no consumer
            // deletes the source and the producer keeps ownership.
            let target_count = targets.len();
            for (index, target) in targets.into_iter().enumerate() {
                let is_last = index + 1 == target_count;
                let per_dir = match &source_path {
                    Some(path) => {
                        if is_last && move_intent {
                            FileBytes::FileToMove(path.clone())
                        } else {
                            FileBytes::FileToCopy(path.clone())
                        }
                    }
                    // No backing path => in-memory: clone the buffer.
                    None => match &content {
                        FileBytes::InMemory(bytes) => FileBytes::InMemory(bytes.clone()),
                        // Unreachable: source_path is None only for InMemory.
                        _ => unreachable!("no source path implies InMemory content"),
                    },
                };
                let _ = command_sender.send(target.into_command(per_dir));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_all_tags_true_when_every_required_tag_is_present() {
        let required = TagId::new();
        let extra = TagId::new();

        assert!(contains_all_tags(&[required], &[required, extra]));
    }

    #[test]
    fn contains_all_tags_false_when_a_required_tag_is_missing() {
        let required = TagId::new();
        let other = TagId::new();

        assert!(!contains_all_tags(&[required], &[other]));
    }

    #[test]
    fn contains_all_tags_vacuously_true_for_universal_directories() {
        // A directory with no tag requirement (Universal) matches every file.
        assert!(contains_all_tags(&[], &[TagId::new()]));
    }

    /// A reconcile pull carries empty tags, but by the time it materializes the
    /// file's tags are in the DB (applied from the TagManifest). The effective
    /// placement tags must include the DB tags so the file lands in its
    /// matching TagBased directories on the first materialize (the
    /// first-connect fix).
    #[test]
    fn effective_tags_include_db_tags_when_carried_is_empty() {
        let db_a = TagId::new();
        let db_b = TagId::new();

        let effective = effective_placement_tags(&[], &[db_a, db_b]);

        assert!(effective.contains(&db_a));
        assert!(effective.contains(&db_b));
        assert_eq!(effective.len(), 2);
    }

    /// The carried tags (from a live `FileMetadataAdded`) are preserved even if
    /// the DB has none yet, and the union deduplicates overlap.
    #[test]
    fn effective_tags_union_dedups_carried_and_db() {
        let shared = TagId::new();
        let carried_only = TagId::new();
        let db_only = TagId::new();

        let effective = effective_placement_tags(&[shared, carried_only], &[shared, db_only]);

        assert!(effective.contains(&shared));
        assert!(effective.contains(&carried_only));
        assert!(effective.contains(&db_only));
        assert_eq!(effective.len(), 3, "shared tag must not be duplicated");
    }
}
