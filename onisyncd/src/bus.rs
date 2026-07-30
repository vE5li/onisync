//! The daemon ingest bus.
//!
//! One `mpsc` channel carries messages from every producer (the UI-facing
//! [`Api`], the sync-directory watcher, and inbound peer sessions) to the
//! single `handle_changes` task, which is the sole DB writer.
//!
//! [`DaemonMessage`] is an enum so the same ordered FIFO can carry both
//! fire-and-forget mutations and a request-reply message
//! ([`DaemonMessage::Fetch`]) used by `onisync edit` to pull a file's bytes
//! from a peer on demand. Keeping everything on one channel (rather than a
//! second channel + `select!`) preserves the order of a producer's messages
//! relative to each other — e.g. an edit enqueued just before a fetch of the
//! same file — and reuses one set of clone/drain/shutdown wiring.
//!
//! The reply travels back out-of-band via the `oneshot` carried in the `Fetch`
//! variant, mirroring the `SyncDirectoryCommand::ReadFile { respond_to }`
//! idiom. This is what lets the control layer stay decoupled from the peer/sync
//! machinery: it only ever holds an [`Api`](crate::api::Api), which enqueues a
//! `Fetch` and awaits the `oneshot`; the recursive fetch engine that talks to
//! peers lives entirely in `handle_changes`/the peer sessions.

use onisync_core::state::{Change, ChangeOrigin};
use onisync_core::{FileId, LogicalPath, TagId};
use tokio::sync::oneshot;

use crate::file_bytes::FileBytes;

/// A change carried on the daemon ingest bus.
///
/// The wire type [`Change`] (in `onisync-core`) is metadata-only:
/// `FileMetadataAdded` and `FileMetadataChanged` announce a file's identity and
/// content hash but carry no bytes. Bytes reach peers over a separate transfer.
///
/// Locally, though, an ingestion *does* have the bytes — on disk (a watched
/// file) or in memory (an API upload) — and needs to place them into this
/// device's sync directories. [`Ingest`] captures that split between a
/// content-bearing *local* ingestion and a metadata-only change:
///
/// - [`Ingest::Content`] — a local ingestion whose bytes are a [`FileBytes`]
///   (possibly still a path on disk). Produced only by local sources (the UI
///   API and the directory manager). `handle_changes` materializes the bytes
///   into matching sync directories and announces a metadata-only
///   `FileMetadataAdded`/`FileMetadataChanged` to peers.
/// - [`Ingest::Meta`] — any change that carries no local bytes, held as the
///   wire [`Change`]. This includes every inbound peer change: a peer's
///   `FileMetadataAdded`/`FileMetadataChanged` is a metadata announcement, and
///   this device pulls the bytes over a transfer if it wants them.
#[derive(Debug)]
pub enum Ingest {
    /// A local content-bearing ingestion whose bytes are a [`FileBytes`].
    Content(ContentChange),
    /// Any change carrying no local bytes, held as the wire type. Includes all
    /// inbound peer changes.
    Meta(Change),
}

/// A local content-bearing ingestion, with bytes as [`FileBytes`]. Mirrors the
/// (metadata-only) `Change::FileMetadataAdded` / `Change::FileMetadataChanged`
/// but adds the local `content`, which is never serialized.
#[derive(Debug)]
pub enum ContentChange {
    FileAdded {
        file_id: FileId,
        logical_path: LogicalPath,
        content: FileBytes,
        content_hash: String,
        /// Content size in bytes, read at hash time.
        size: u64,
        tags: Vec<TagId>,
    },
    FileChanged {
        file_id: FileId,
        content: FileBytes,
        content_hash: String,
        /// Content size in bytes, read at hash time.
        size: u64,
    },
}

impl Ingest {
    /// Lift a wire [`Change`] onto the bus as [`Ingest::Meta`].
    ///
    /// Since `Change` is metadata-only, a wire change never carries bytes, so
    /// it is always `Meta`. Used by producers that only have a wire change:
    /// the UI API's metadata mutations and inbound peer sessions. Local
    /// content ingestion (API upload, directory manager) constructs
    /// [`Ingest::Content`] directly with its [`FileBytes`].
    pub fn from_change(change: Change) -> Self {
        Ingest::Meta(change)
    }
}

/// A message on the daemon ingest bus.
///
/// One ordered channel; two message kinds: fire-and-forget mutations and a
/// request-reply fetch.
pub enum DaemonMessage {
    /// A mutation to apply. Fire-and-forget: no reply.
    Change(Ingest, ChangeOrigin),
    /// An on-demand request for a file's bytes (used by `onisync edit` when
    /// the file is not present locally). `handle_changes` resolves the
    /// version's size from the catalog and drives a content-addressed
    /// receive that floods `Sync::ChunkRequest`s across the live peer tree
    /// (via the content-keyed relay), resolving `respond_to` when the bytes
    /// arrive (or with an error if no reachable holder can serve them).
    Fetch {
        file_id: FileId,
        /// The BLAKE3 hex digest the requester expects; a peer's bytes are only
        /// accepted if they hash to this. Gates correctness across the flood
        /// and removes any need for divergence handling.
        expected_hash: String,
        respond_to: oneshot::Sender<Result<FileBytes, FetchError>>,
    },
    /// Bytes for `file_id` have arrived over a peer transfer and should be
    /// written into this device's matching sync directories.
    ///
    /// Both the file's logical identity and its **version** were recorded when
    /// the announcement was handled (`FileMetadataAdded`/`Changed` or
    /// `Manifest` reconcile), because `file_versions` is the
    /// byte-independent *catalog* of versions we know exist — not a record
    /// of bytes we hold. `Materialize` is therefore purely about placing
    /// the arrived bytes; it neither records a version nor forwards the
    /// announcement (both already happened).
    Materialize {
        file_id: FileId,
        content: FileBytes,
        /// The hash the bytes were verified against by the transfer receiver.
        content_hash: String,
        /// Which peer announced this. Carried for context/logging; the
        /// version was recorded at announce time.
        origin: ChangeOrigin,
        placement: MaterializePlacement,
    },
    /// Re-evaluate `file_id`'s TagBased placement (and fetch its bytes on
    /// demand if a sync directory now wants it but we do not hold them).
    /// Enqueued by a peer session's connect-time reconciliation sweep so
    /// the fetch runs inside `handle_changes` rather than blocking the
    /// session's frame loop (the fetch needs that loop to relay
    /// `ChunkRequest`/`ChunkData`). Fire-and-forget.
    ReconcilePlacement { file_id: FileId },
    /// Record a file + version into the catalog (`files` + `file_versions`) on
    /// behalf of a peer session's `Manifest` reconciliation. The session
    /// decides *what* to catalog (its divergence/LWW logic stays there) but
    /// must not write the main DB itself — `handle_changes` is the sole
    /// writer — so it hands the write here. Fire-and-forget. Inserts the
    /// `files` row if absent and appends the version; the byte pull happens
    /// separately on the session link.
    CatalogFile {
        file_id: FileId,
        /// The file's logical identity, used to insert the `files` row when the
        /// file is not yet known locally.
        logical_path: LogicalPath,
        /// The originating device's path-change time (from the manifest entry),
        /// used to seed the path's last-writer-wins clock when inserting the
        /// `files` row. Only meaningful for a not-yet-known file; ignored when
        /// the row already exists (its path is reconciled via `WantedMove`).
        logical_path_modified_at: i64,
        content_hash: String,
        /// The version's content size in bytes (from the manifest history).
        size: u64,
        /// The announcing peer (stored in `file_versions.origin`).
        origin: ChangeOrigin,
    },
    /// A locally-provided upload/edit: the client (CLI) holds the bytes and
    /// serves them on demand (a temporary provider), so there is nothing to
    /// place in a local sync directory. `handle_changes` records the file (for
    /// `FileMetadataAdded`) and version, then announces the metadata-only
    /// change to peers, which pull the bytes from the registered provider.
    AnnounceProvided {
        file_id: FileId,
        /// `Some(logical_path)` for a new file (`FileMetadataAdded`); `None`
        /// for an edit of an existing file (`FileMetadataChanged`).
        logical_path: Option<LogicalPath>,
        content_hash: String,
        /// Content size in bytes, read at hash time by the provider (CLI).
        size: u64,
        tags: Vec<TagId>,
    },
    /// User-initiated restore of a soft-deleted file. Request-reply (like
    /// [`DaemonMessage::Fetch`]) because the outcome is only known after an
    /// async *availability probe*.
    ///
    /// `handle_changes` reads the file's latest known version while it is still
    /// tombstoned, then checks whether the bytes are still recoverable — first
    /// the local `keep_deleted_files` vault, then (best-effort) a probe flooded
    /// across the peer tree. Only if the bytes are available does it clear the
    /// tombstone, record the restored version, forward a `Change::FileRestored`
    /// to peers, and drive placement so the bytes land where wanted. If nothing
    /// holds the bytes, the tombstone is left untouched and this resolves
    /// `Err(RestoreError::NotAvailable)`.
    Restore {
        file_id: FileId,
        respond_to: oneshot::Sender<Result<(), RestoreError>>,
    },
    /// Internal follow-up to [`DaemonMessage::Restore`], enqueued by the
    /// spawned availability probe once it has confirmed the bytes are
    /// recoverable. Handled synchronously by the sole DB writer so the catalog
    /// mutation (record the restored version, clear the tombstone), the
    /// `Change::FileRestored` peer-forward, and placement all happen on the
    /// writer loop rather than off it.
    ///
    /// Split out from `Restore` so the (potentially slow) probe never blocks
    /// the single-threaded consumer — mirroring how `Fetch` spawns and
    /// re-enters via `Materialize`.
    ApplyRestore {
        file_id: FileId,
        content_hash: String,
        size: u64,
        /// Wall-clock stamp captured when the restore was initiated; recorded
        /// as the restored version's `observed_at` (beats any peer
        /// `deleted_at`).
        restored_at: i64,
        respond_to: oneshot::Sender<Result<(), RestoreError>>,
    },
}

/// A command sent to a specific peer's live session by `handle_changes`.
///
/// The peer session owns the link and the transfer machinery; `handle_changes`
/// (a separate task) uses this channel to ask the session to start a byte pull
/// once it has recorded a live change announced by that peer. Stored in
/// `RuntimePeer.commands` alongside `outbound`.
#[derive(Debug)]
pub enum PeerCommand {
    /// Start a receiver transfer for `file_id` (verifying `content_hash`) from
    /// this peer, then materialize the result with `placement`.
    StartReceive {
        file_id: FileId,
        content_hash: String,
        /// The file's known content size in bytes (from the catalog/manifest),
        /// used to cap the receiver's request window at EOF.
        expected_size: u64,
        placement: MaterializePlacement,
    },
}

/// How a materialized file should be placed into sync directories.
#[derive(Debug, Clone)]
pub enum MaterializePlacement {
    /// A newly-announced file (`FileMetadataAdded`): create it in each matching
    /// sync directory, placing it at the physical path derived from
    /// `logical_path`.
    Create {
        logical_path: LogicalPath,
        tags: Vec<TagId>,
    },
    /// An updated file (`FileMetadataChanged`): overwrite it in each sync
    /// directory that already holds it (tag-filtered by the file's current
    /// local tags).
    Change,
}

impl DaemonMessage {
    /// Convenience constructor for the common fire-and-forget change case,
    /// lifting a wire [`Change`] onto the bus via [`Ingest::from_change`].
    pub fn change(change: Change, origin: ChangeOrigin) -> Self {
        DaemonMessage::Change(Ingest::from_change(change), origin)
    }
}

/// Why an on-demand fetch failed.
#[derive(Debug, Clone)]
pub enum FetchError {
    /// No connected peer (directly or transitively) reported holding content
    /// that matches `expected_hash` before the timeout.
    NotAvailable,
    /// The fetch did not complete within the overall deadline.
    TimedOut,
    /// The runtime is shutting down; the request cannot be served.
    ShuttingDown,
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::NotAvailable => {
                formatter.write_str("file not available from any connected peer")
            }
            FetchError::TimedOut => formatter.write_str("fetch timed out"),
            FetchError::ShuttingDown => formatter.write_str("runtime is shutting down"),
        }
    }
}

/// Why a restore failed.
#[derive(Debug, Clone)]
pub enum RestoreError {
    /// The file is not soft-deleted (nothing to restore), or is unknown.
    NotDeleted,
    /// Best-effort recovery found no source for the bytes: neither a local
    /// `keep_deleted_files` vault nor any connected peer still holds them. The
    /// tombstone is left in place.
    NotAvailable,
    /// The runtime is shutting down; the request cannot be served.
    ShuttingDown,
}

impl std::fmt::Display for RestoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RestoreError::NotDeleted => {
                formatter.write_str("file is not deleted; nothing to restore")
            }
            RestoreError::NotAvailable => {
                formatter.write_str("no source holds the file's bytes; cannot restore")
            }
            RestoreError::ShuttingDown => formatter.write_str("runtime is shutting down"),
        }
    }
}

impl std::error::Error for FetchError {}
