//! Reconciling a peer's file manifest against our local state.
//!
//! [`plan_file_sync`] and its helpers are pure: no `.await`, no channels, no
//! locks. They take only a `&CatalogStore` and a `Vec<ManifestEntry>`, which
//! lets callers hold `&CatalogStore` (which is `!Sync`) without making their
//! future non-`Send`, and keeps the decision testable in isolation from the
//! peer-session machinery that calls it.

use std::collections::HashSet;

use tagsy_core::state::ManifestEntry;
use tagsy_core::{FileId, LogicalPath};

use crate::bus;
use crate::store::{self, CatalogStore};

/// Read every file's full version history from the main DB and pack into a
/// `Vec<ManifestEntry>` suitable for `Sync::Manifest`. Files without any
/// recorded version are skipped (with a warning).
pub fn build_local_manifest(database: &CatalogStore) -> Result<Vec<ManifestEntry>, String> {
    let rows = database
        .manifest_entries()
        .map_err(|e| format!("manifest_entries: {e:?}"))?;
    Ok(rows
        .into_iter()
        .map(
            |(
                file_id,
                history,
                latest_observed_at,
                logical_path,
                logical_path_modified_at,
                deleted,
                deleted_at,
                restored_at,
            )| {
                ManifestEntry {
                    file_id,
                    history,
                    latest_observed_at,
                    logical_path,
                    logical_path_modified_at,
                    deleted,
                    deleted_at,
                    restored_at,
                }
            },
        )
        .collect())
}

/// One reconciliation outcome: pull `content_hash` for `file_id` from the peer
/// and materialize it with `placement`.
///
/// Placement is `Create` when we've never seen this file locally (the
/// manifest's `logical_path` gives us where to put it) and `Change` when we
/// already know the file and are only fetching newer bytes.
#[derive(Debug, Clone)]
pub struct MissingContent {
    pub file_id: FileId,
    pub content_hash: String,
    /// The peer's latest-version content size in bytes (from the manifest
    /// history), recorded alongside the hash when we catalog the version.
    pub size: i64,
    /// The originating device's path-change time (from the manifest entry).
    /// Seeds the path's LWW clock when this file is new to us; ignored when we
    /// already know it (the row's path is reconciled separately via
    /// `PeerMove`).
    pub logical_path_modified_at: i64,
    pub placement: bus::MaterializePlacement,
}

/// A file deletion learned from a peer's manifest that wins last-writer-wins
/// against our local state (the peer's `deleted_at` is newer than the newest of
/// our latest version `observed_at` and our `restored_at`). Applied by
/// enqueuing a `Change::FileDeleted`.
#[derive(Debug, Clone)]
pub struct PeerDeletion {
    pub file_id: FileId,
    pub deleted_at: i64,
}

/// A file restore learned from a peer's manifest that wins last-writer-wins
/// against our local delete (the peer advertises the file as live and its
/// `restored_at` is newer than our `deleted_at`). Applied by enqueuing a
/// `Change::FileRestored`, which reuses the live-restore handler (three-way LWW
/// guard, tombstone clear, byte pull, forward). This is the offline-restore
/// catch-up: an un-delete performed while we were disconnected.
#[derive(Debug, Clone)]
pub struct PeerRestore {
    pub file_id: FileId,
    pub restored_at: i64,
    /// The peer's latest content hash, used to pull the bytes back into any
    /// local sync directory that wants the now-live file.
    pub content_hash: String,
    /// The peer's latest content size in bytes.
    pub size: u64,
}

/// A logical-path change learned from a peer's manifest for a file we already
/// know, whose `logical_path_modified_at` is newer than our own. Applied by
/// enqueuing a `Change::FileMoved`, which reuses the live-move handler (LWW
/// guard, byte repositioning, and forwarding). This is what lets a move made
/// while we were offline reconcile on reconnect. Emitted only for known files:
/// an unknown file adopts the peer's path through its initial `Create`
/// placement, and a deletion tombstone suppresses any move.
#[derive(Debug, Clone)]
pub struct PeerMove {
    pub file_id: FileId,
    pub logical_path: LogicalPath,
    pub modified_at: i64,
}

/// The outcome of reconciling a peer's file manifest against our local state,
/// divided by what the caller must do with each entry.
#[derive(Debug, Clone, Default)]
pub struct SyncPlan {
    pub pulls: Vec<MissingContent>,
    pub deletions: Vec<PeerDeletion>,
    pub restores: Vec<PeerRestore>,
    pub moves: Vec<PeerMove>,
}

/// Compare the peer's manifest against our local `file_versions` table and
/// decide which files we should pull from them.
///
/// Returns each wanted file paired with the peer's latest content hash and a
/// placement describing how the eventual `Materialize` should route the bytes.
/// The caller starts a content-addressed receive per returned entry.
///
/// Pure synchronous function: no `.await`, no `RwLock`, no channels/transfers.
/// Lets callers hold `&CatalogStore` (which is `!Sync`) without making their
/// future non-`Send`, and keeps the decision testable. **Note:** for unknown
/// files this function does *not* insert the `files` row itself; the caller
/// must `add_file` before starting the pull (mirroring the live
/// `FileMetadataAdded` handler).
///
/// Categories per entry:
/// - **Unknown file_id**: we've never seen this file — request it and place
///   with `Create { logical_path, tags: <empty> }`. Tags arrive independently
///   via `Sync::TagManifest`; `plan_placement` will re-place the file into any
///   TagBased sync directory that later matches.
/// - **Equal latest**: identical state — nothing to do.
/// - **Sender's latest hash appears in our history**: they are behind. Their
///   side will request from us when they process our manifest; we do nothing.
/// - **Our latest hash appears in their history**: we are behind — request the
///   newer bytes (`Change` placement, we already hold the file row).
/// - **Divergent**: neither side's latest appears in the other's history. Newer
///   `latest_observed_at` wins. If theirs wins, request. If ours wins, do
///   nothing (their side will accept ours via the symmetric path). Divergence
///   is logged at `error!` level with a TODO for a future deadletter store.
pub fn plan_file_sync(
    peer_name: &str,
    entries: Vec<ManifestEntry>,
    database: &CatalogStore,
) -> SyncPlan {
    log::info!(
        "Reconciling {} manifest entries from {peer_name}",
        entries.len()
    );

    let mut plan = SyncPlan::default();
    for entry in entries {
        // Deletion tombstones take precedence over content reconciliation. If
        // the peer advertises a delete, apply three-way last-writer-wins: the
        // delete wins only if `deleted_at` is newer than BOTH our latest edit
        // `observed_at` AND our explicit `restored_at`. An edit or restore newer
        // than the delete keeps the file live (the content path below handles
        // bytes).
        if entry.deleted {
            // If we already hold a tombstone for this file, we are in the same
            // terminal state as the peer and there is nothing to do — regardless
            // of whose `deleted_at` is larger. Skipping here prevents pointless
            // re-enqueuing of `Change::FileDeleted` on every manifest exchange,
            // which would otherwise re-run the fan-out (`RemoveFile` per sync
            // directory, forward-to-peers) for a delete that has already fully
            // converged.
            let ours = database.file_deletion_state(entry.file_id).ok().flatten();
            if ours.as_ref().is_some_and(|state| state.deleted) {
                continue;
            }
            let ours_observed_at = database
                .latest_version(entry.file_id)
                .ok()
                .flatten()
                .map(|version| version.observed_at)
                .unwrap_or(0);
            let ours_restored_at = ours.map(|state| state.restored_at).unwrap_or(0);
            if entry.deleted_at > ours_observed_at.max(ours_restored_at) {
                log::debug!(
                    "Applying peer delete for {} from {peer_name} (deleted_at={} > \
                     max(ours_observed_at={ours_observed_at}, \
                     ours_restored_at={ours_restored_at}))",
                    entry.file_id.to_string(),
                    entry.deleted_at,
                );
                plan.deletions.push(PeerDeletion {
                    file_id: entry.file_id,
                    deleted_at: entry.deleted_at,
                });
            }
            // Whether or not the delete won, do not also request bytes for a
            // tombstoned entry.
            continue;
        }

        // The peer advertises the file as live. If it carries an explicit
        // restore stamp and we still hold the file tombstoned with an older
        // `deleted_at`, the peer's restore wins last-writer-wins: apply it
        // locally (offline-restore catch-up). We only need this for a file we
        // currently consider deleted; a live-vs-live file is handled by the
        // normal content path below.
        if entry.restored_at > 0
            && let Some(state) = database.file_deletion_state(entry.file_id).ok().flatten()
            && state.deleted
            && entry.restored_at > state.deleted_at
            && let Some((hash, size)) = entry.history.last().map(|(_, h, s)| (h.clone(), *s))
        {
            log::debug!(
                "Applying peer restore for {} from {peer_name} (restored_at={} > \
                 ours_deleted_at={})",
                entry.file_id.to_string(),
                entry.restored_at,
                state.deleted_at,
            );
            plan.restores.push(PeerRestore {
                file_id: entry.file_id,
                restored_at: entry.restored_at,
                content_hash: hash,
                size: size as u64,
            });
            // The restore drives its own byte pull via the FileRestored handler;
            // don't also run the content path for this entry.
            continue;
        }

        let decision = match decide_request(database, &entry) {
            Ok(decision) => decision,
            Err(error) => {
                log::error!(
                    "Reconciliation lookup failed for {}: {error:?}",
                    entry.file_id.to_string()
                );
                continue;
            }
        };
        // The hash and size we want are the peer's latest for this file.
        let their_latest = entry
            .history
            .last()
            .map(|(_, hash, size)| (hash.clone(), *size));
        // Placement depends on whether we already know the file locally: an
        // unknown file must be materialized as `Create` (using the manifest's
        // `logical_path`) so the sync-directory dispatch can place it.
        let known = database.file_exists(entry.file_id).unwrap_or(false);

        // Logical-path reconciliation, independent of the content decision
        // below. A file can be moved without its bytes changing, so this must
        // NOT be gated on `decide_request` (which compares only content). For a
        // file we already know, adopt the peer's path when its
        // `logical_path_modified_at` is strictly newer than ours (last-writer-
        // wins). For an unknown file we do nothing here: it adopts the peer's
        // path through its `Create` placement when the content pull materializes
        // it. Deletion tombstones were already handled and `continue`d above, so
        // a tombstoned entry never reaches this point.
        if known {
            match database.logical_path_modified_at(entry.file_id) {
                Ok(Some(ours)) if entry.logical_path_modified_at > ours => {
                    log::debug!(
                        "Peer {peer_name} has a newer path for {} (theirs={} > ours={ours}); \
                         adopting",
                        entry.file_id.to_string(),
                        entry.logical_path_modified_at,
                    );
                    plan.moves.push(PeerMove {
                        file_id: entry.file_id,
                        logical_path: entry.logical_path.clone(),
                        modified_at: entry.logical_path_modified_at,
                    });
                }
                Ok(_) => {}
                Err(error) => {
                    log::error!(
                        "Reconciliation: path-clock lookup failed for {}: {error:?}",
                        entry.file_id.to_string()
                    );
                }
            }
        }
        let placement_for_request = |entry: &ManifestEntry| -> bus::MaterializePlacement {
            if known {
                bus::MaterializePlacement::Change
            } else {
                // Tags are deliberately empty here; they are reconciled via
                // `Sync::TagManifest` and, when they land, the incoming
                // `FileTagged` handler runs `plan_placement` to re-place the
                // file into any newly-matching TagBased sync directories using
                // the already-materialized bytes as a source. This gives
                // order-independence between the file and tag manifests
                // without enforcing a global ordering.
                bus::MaterializePlacement::Create {
                    logical_path: entry.logical_path.clone(),
                    tags: Vec::new(),
                }
            }
        };
        match decision {
            ReconcileDecision::Nothing => {}
            ReconcileDecision::Request(reason) => {
                log::debug!(
                    "Requesting {} from {peer_name}: {reason}",
                    entry.file_id.to_string()
                );
                if let Some((hash, size)) = their_latest {
                    let placement = placement_for_request(&entry);
                    plan.pulls.push(MissingContent {
                        file_id: entry.file_id,
                        content_hash: hash,
                        size,
                        logical_path_modified_at: entry.logical_path_modified_at,
                        placement,
                    });
                }
            }
            ReconcileDecision::Divergent {
                ours_observed_at,
                request,
            } => {
                // TODO: When a deadletter / conflict store exists, preserve
                // the losing version there instead of just logging.
                log::error!(
                    "Divergent history for {} between us and {peer_name} (our latest \
                     observed_at={ours_observed_at}, theirs={}). {}.",
                    entry.file_id.to_string(),
                    entry.latest_observed_at,
                    if request {
                        "Their version wins; requesting"
                    } else {
                        "Our version wins; keeping"
                    },
                );
                if request && let Some((hash, size)) = their_latest {
                    let placement = placement_for_request(&entry);
                    plan.pulls.push(MissingContent {
                        file_id: entry.file_id,
                        content_hash: hash,
                        size,
                        logical_path_modified_at: entry.logical_path_modified_at,
                        placement,
                    });
                }
            }
        }
    }

    plan
}

enum ReconcileDecision {
    Nothing,
    Request(&'static str),
    Divergent {
        ours_observed_at: i64,
        request: bool,
    },
}

/// Pure decision function: given our local DB and the peer's entry, what
/// should we do? Separated from `plan_file_sync` so it can be reasoned about
/// (and tested) without touching channels.
fn decide_request(
    database: &CatalogStore,
    entry: &ManifestEntry,
) -> Result<ReconcileDecision, store::DatabaseError> {
    let our_history = database.version_history(entry.file_id)?;

    if our_history.is_empty() {
        return Ok(ReconcileDecision::Request("unknown file"));
    }

    let their_latest = match entry.history.last() {
        Some((_, hash, _)) => hash.as_str(),
        None => return Ok(ReconcileDecision::Nothing),
    };
    let our_latest = our_history
        .last()
        .expect("checked non-empty above")
        .1
        .as_str();

    if our_latest == their_latest {
        return Ok(ReconcileDecision::Nothing);
    }

    let our_hashes: HashSet<&str> = our_history
        .iter()
        .map(|(_, hash, _)| hash.as_str())
        .collect();
    let their_hashes: HashSet<&str> = entry
        .history
        .iter()
        .map(|(_, hash, _)| hash.as_str())
        .collect();

    let they_have_our_latest = their_hashes.contains(our_latest);
    let we_have_their_latest = our_hashes.contains(their_latest);

    match (they_have_our_latest, we_have_their_latest) {
        // Their latest is somewhere in our history → they are strictly behind.
        // They'll request from us when they process our manifest.
        (_, true) => Ok(ReconcileDecision::Nothing),
        // Our latest is somewhere in their history → we are strictly behind.
        (true, false) => Ok(ReconcileDecision::Request("we are behind")),
        // Neither side knows the other's latest hash → divergent.
        (false, false) => {
            let ours_observed_at = database
                .latest_version(entry.file_id)?
                .map(|version| version.observed_at)
                .unwrap_or(0);
            let request = entry.latest_observed_at > ours_observed_at;
            Ok(ReconcileDecision::Divergent {
                ours_observed_at,
                request,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::state::ManifestEntry;

    use super::*;
    use crate::clock;

    fn memory_db() -> CatalogStore {
        CatalogStore::initialize(":memory:").expect("open in-memory db")
    }

    /// A file the peer has but we've never seen IS reconciled with a `Create`
    /// placement carrying the manifest's `logical_path`: this is the
    /// offline-creation catch-up case (a file created on the peer while we
    /// were disconnected must sync on reconnect). Tags are left empty; they
    /// are reconciled independently via `Sync::TagManifest` and applied by
    /// `plan_placement` when the corresponding `FileTagged` arrives.
    #[test]
    fn unknown_file_is_requested_as_create() {
        let database = memory_db();
        let file_id = FileId::new();
        let logical_path = LogicalPath::new("subdir/new.txt");
        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "aaaa".to_owned(), 1), (2, "bbbb".to_owned(), 1)],
            latest_observed_at: 100,
            logical_path: logical_path.clone(),
            logical_path_modified_at: 0,
            deleted: false,
            deleted_at: 0,
            restored_at: 0,
        };

        let plan = plan_file_sync("peer", vec![entry], &database);
        assert!(plan.deletions.is_empty());
        assert_eq!(plan.pulls.len(), 1);
        assert_eq!(plan.pulls[0].file_id, file_id);
        assert_eq!(plan.pulls[0].content_hash, "bbbb");
        match &plan.pulls[0].placement {
            bus::MaterializePlacement::Create {
                logical_path: got_logical_path,
                tags,
            } => {
                assert_eq!(got_logical_path, &logical_path);
                assert!(tags.is_empty(), "tags reconcile via Sync::TagManifest");
            }
            other => panic!("expected Create placement, got {other:?}"),
        }
    }

    /// A file whose latest hash we already hold is not wanted.
    #[test]
    fn equal_latest_is_not_wanted() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"), 0)
            .unwrap();
        database
            .record_version(file_id, "bbbb", "local", 1)
            .unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "bbbb".to_owned(), 1)],
            latest_observed_at: 100,
            logical_path: LogicalPath::new("f.txt"),
            logical_path_modified_at: 0,
            deleted: false,
            deleted_at: 0,
            restored_at: 0,
        };

        let plan = plan_file_sync("peer", vec![entry], &database);
        assert!(plan.deletions.is_empty());
        assert!(plan.pulls.is_empty());
    }

    /// When we are strictly behind (our latest is in the peer's history but not
    /// vice versa), the file is wanted at the peer's newer hash. Since the
    /// file is already known locally, placement is `Change`.
    #[test]
    fn behind_is_wanted_as_change() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"), 0)
            .unwrap();
        database.record_version(file_id, "v1", "local", 1).unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "v1".to_owned(), 1), (2, "v2".to_owned(), 1)],
            latest_observed_at: 100,
            logical_path: LogicalPath::new("f.txt"),
            logical_path_modified_at: 0,
            deleted: false,
            deleted_at: 0,
            restored_at: 0,
        };

        let plan = plan_file_sync("peer", vec![entry], &database);
        assert!(plan.deletions.is_empty());
        assert_eq!(plan.pulls.len(), 1);
        assert_eq!(plan.pulls[0].file_id, file_id);
        assert_eq!(plan.pulls[0].content_hash, "v2");
        assert!(
            matches!(plan.pulls[0].placement, bus::MaterializePlacement::Change),
            "known file placement must be Change, got {:?}",
            plan.pulls[0].placement
        );
    }

    /// A known file the peer moved while we were offline (its
    /// `logical_path_modified_at` is newer than ours, content unchanged) is
    /// reconciled as a `PeerMove` — NOT a content pull. This is the offline-
    /// move catch-up: without it, an identical-content rename would compare
    /// equal on hashes and be dropped.
    #[test]
    fn newer_peer_path_is_wanted_as_move() {
        let mut database = memory_db();
        let file_id = FileId::new();
        // We know the file at an old path stamped at t=10.
        database
            .add_file(file_id, &LogicalPath::new("old.txt"), 10)
            .unwrap();
        database.record_version(file_id, "v1", "local", 1).unwrap();

        // The peer has identical content but a newer path (t=20).
        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "v1".to_owned(), 1)],
            latest_observed_at: 100,
            logical_path: LogicalPath::new("new.txt"),
            logical_path_modified_at: 20,
            deleted: false,
            deleted_at: 0,
            restored_at: 0,
        };

        let plan = plan_file_sync("peer", vec![entry], &database);
        assert!(plan.deletions.is_empty());
        assert!(
            plan.pulls.is_empty(),
            "content is identical; the move must not trigger a byte pull"
        );
        assert_eq!(plan.moves.len(), 1);
        assert_eq!(plan.moves[0].file_id, file_id);
        assert_eq!(plan.moves[0].logical_path, LogicalPath::new("new.txt"));
        assert_eq!(plan.moves[0].modified_at, 20);
    }

    /// A peer's path change that is older than (or equal to) ours loses
    /// last-writer-wins: no move is emitted.
    #[test]
    fn stale_peer_path_is_not_wanted_as_move() {
        let mut database = memory_db();
        let file_id = FileId::new();
        // Our path is stamped newer (t=30) than the peer's (t=20).
        database
            .add_file(file_id, &LogicalPath::new("ours.txt"), 30)
            .unwrap();
        database.record_version(file_id, "v1", "local", 1).unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "v1".to_owned(), 1)],
            latest_observed_at: 100,
            logical_path: LogicalPath::new("theirs.txt"),
            logical_path_modified_at: 20,
            deleted: false,
            deleted_at: 0,
            restored_at: 0,
        };

        let plan = plan_file_sync("peer", vec![entry], &database);
        assert!(plan.moves.is_empty(), "a stale peer path must not win LWW");
    }

    /// An unknown file is placed via `Create` (see `unknown_file_is_requested_
    /// as_create`); it must NOT also emit a `PeerMove` (which only applies to
    /// files we already know).
    #[test]
    fn unknown_file_does_not_emit_move() {
        let database = memory_db();
        let entry = ManifestEntry {
            file_id: FileId::new(),
            history: vec![(1, "aaaa".to_owned(), 1)],
            latest_observed_at: 100,
            logical_path: LogicalPath::new("new.txt"),
            logical_path_modified_at: 999,
            deleted: false,
            deleted_at: 0,
            restored_at: 0,
        };

        let plan = plan_file_sync("peer", vec![entry], &database);
        assert!(
            plan.moves.is_empty(),
            "an unknown file adopts the path via Create, not a move"
        );
    }

    /// A peer's delete tombstone whose `deleted_at` is newer than our latest
    /// version's `observed_at` wins: we schedule the deletion and do not pull
    /// bytes for it.
    #[test]
    fn peer_delete_newer_than_our_edit_is_applied() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"), 0)
            .unwrap();
        // Our latest version's observed_at is "now". A delete stamped far in
        // the future beats it.
        database.record_version(file_id, "v1", "local", 1).unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "v1".to_owned(), 1)],
            latest_observed_at: 0,
            logical_path: LogicalPath::new("f.txt"),
            logical_path_modified_at: 0,
            deleted: true,
            deleted_at: clock::now_millis() + 10_000,
            restored_at: 0,
        };

        let plan = plan_file_sync("peer", vec![entry], &database);
        assert!(
            plan.pulls.is_empty(),
            "a tombstoned file must not be pulled"
        );
        assert_eq!(plan.deletions.len(), 1);
        assert_eq!(plan.deletions[0].file_id, file_id);
    }

    /// A peer's delete tombstone older than our latest edit loses: the file is
    /// kept (no deletion scheduled). This is the restore-after-delete rule.
    #[test]
    fn peer_delete_older_than_our_edit_is_ignored() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"), 0)
            .unwrap();
        // Our latest version's observed_at is "now"; a delete stamped in the
        // past loses.
        database.record_version(file_id, "v1", "local", 1).unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "v1".to_owned(), 1)],
            latest_observed_at: 0,
            logical_path: LogicalPath::new("f.txt"),
            logical_path_modified_at: 0,
            deleted: true,
            deleted_at: 1,
            restored_at: 0,
        };

        let plan = plan_file_sync("peer", vec![entry], &database);
        assert!(plan.deletions.is_empty(), "stale delete must not win");
        // Equal latest hash means nothing to pull either.
        assert!(plan.pulls.is_empty());
    }

    /// A peer's delete tombstone for a file we already hold as tombstoned must
    /// not be scheduled again. Without this short-circuit, every manifest
    /// exchange would re-enqueue `Change::FileDeleted` for every dead file,
    /// re-running the per-sync-directory fan-out (spurious
    /// `FailedRemovingFile`) and re-broadcasting the delete to peers, causing
    /// tombstones to pile up across the mesh on every reconnect.
    #[test]
    fn peer_delete_for_already_tombstoned_file_is_ignored() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"), 0)
            .unwrap();
        database.record_version(file_id, "v1", "local", 1).unwrap();
        // Locally tombstone the file at some timestamp.
        let ours_deleted_at = clock::now_millis() + 5_000;
        assert!(database.remove_file(file_id, ours_deleted_at).unwrap());

        // The peer's manifest also reports the file as deleted. Regardless of
        // whether its `deleted_at` matches ours, precedes ours, or exceeds
        // ours, the outcome must be a no-op: we're already in the terminal
        // tombstoned state.
        for peer_deleted_at in [ours_deleted_at - 1, ours_deleted_at, ours_deleted_at + 1] {
            let entry = ManifestEntry {
                file_id,
                history: vec![(1, "v1".to_owned(), 1)],
                latest_observed_at: 0,
                logical_path: LogicalPath::new("f.txt"),
                logical_path_modified_at: 0,
                deleted: true,
                deleted_at: peer_deleted_at,
                restored_at: 0,
            };

            let plan = plan_file_sync("peer", vec![entry], &database);
            assert!(
                plan.pulls.is_empty(),
                "no bytes to pull for a tombstoned file"
            );
            assert!(
                plan.deletions.is_empty(),
                "already-tombstoned file must not schedule another delete \
                 (peer_deleted_at={peer_deleted_at})"
            );
            assert!(plan.moves.is_empty());
            assert!(plan.restores.is_empty());
        }
    }
}
