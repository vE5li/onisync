//! UI-facing API.
//!
//! This is the single, transport-agnostic API surface the UI talks to. It is
//! deliberately a **v1**: every operation maps 1:1 onto capabilities that
//! already exist in [`FileDatabase`](crate::database::FileDatabase) and the
//! change pipeline.
//!
//! ## Architecture
//!
//! The API is split into a read half and a write half because the core
//! enforces a single-writer model:
//!
//! - **Reads** open their own read-only [`FileDatabase`] handle from
//!   `main_db_path`, exactly as peer sessions do. A `&FileDatabase` is never
//!   held across an `.await`.
//! - **Writes** are expressed as [`Change`] values and pushed onto the ingest
//!   bus (`change_sender`). The single `handle_changes` task remains the only
//!   DB writer and performs idempotent persistence plus peer forwarding. This
//!   API adds no business logic and never writes the DB directly.
//!
//! Both process topologies (in-process on Android, IPC-to-daemon on Linux)
//! wrap this same [`Api`] handle; the Dart UI never knows which.

use std::path::PathBuf;
use std::sync::Arc;

use onisync_core::state::{Change, ChangeOrigin};
use onisync_core::{FileId, FileInfo, LogicalPath, Preview, TagId};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{broadcast, oneshot};

use crate::bus::{DaemonMessage, FetchError, Ingest, PreviewError, RestoreError};
use crate::configuration::EditorRule;
use crate::database::{DatabaseError, DeletedRule, FileDatabase, QueryTerm, SubtagRule, Tag};
use crate::directory_manager::SyncDirectoryCommand;
use crate::fetch::PendingFetches;
use crate::transfer::ChunkSource;

/// Errors surfaced to the UI.
///
/// A single serializable error type so the transport can carry one shape over
/// the wire. It wraps the crate's [`DatabaseError`] rather than leaking it raw,
/// and adds UI-facing variants.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum ApiError {
    /// Unknown `FileId`/`TagId`.
    #[error("not found")]
    NotFound,
    /// A short-id prefix matched more than one file, so it could not be
    /// resolved to a single id. Carries the ambiguous prefix.
    #[error("ambiguous id prefix '{0}': matches multiple files")]
    Ambiguous(String),
    /// A caller-supplied argument was invalid (e.g. empty tag name).
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A database-layer failure.
    #[error("database error: {0}")]
    Database(#[source] DatabaseError),
    /// IPC-only: socket/protocol failure. Never produced in-process.
    #[error("transport error: {0}")]
    Transport(String),
    /// An unexpected internal failure (e.g. a change could not be enqueued
    /// because the runtime is shutting down).
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<DatabaseError> for ApiError {
    fn from(error: DatabaseError) -> Self {
        match error {
            DatabaseError::MissingFile | DatabaseError::MissingTag => ApiError::NotFound,
            DatabaseError::AmbiguousIdPrefix(prefix) => ApiError::Ambiguous(prefix),
            DatabaseError::InvalidTagName => {
                ApiError::InvalidArgument("invalid tag name".to_owned())
            }
            DatabaseError::InvalidColor => ApiError::InvalidArgument("invalid color".to_owned()),
            DatabaseError::CantTagItself => {
                ApiError::InvalidArgument("a tag cannot be its own subtag".to_owned())
            }
            // A raw SQL failure is not actionable by the UI, so it is reported
            // as an internal error rather than a structured database error.
            sqlite @ DatabaseError::Sqlite { .. } => ApiError::Internal(sqlite.to_string()),
            other => ApiError::Database(other),
        }
    }
}

impl From<FetchError> for ApiError {
    fn from(error: FetchError) -> Self {
        match error {
            // No peer had the content: surface as a plain not-found to the UI.
            FetchError::NotAvailable => ApiError::NotFound,
            FetchError::TimedOut | FetchError::ShuttingDown => {
                ApiError::Internal(error.to_string())
            }
        }
    }
}

impl From<RestoreError> for ApiError {
    fn from(error: RestoreError) -> Self {
        match error {
            // No source held the bytes, or the file was not deleted: surface as
            // a plain not-found/invalid so the UI can show a clear failure.
            RestoreError::NotAvailable => ApiError::NotFound,
            RestoreError::NotDeleted => ApiError::InvalidArgument(error.to_string()),
            RestoreError::ShuttingDown => ApiError::Internal(error.to_string()),
        }
    }
}

impl From<PreviewError> for ApiError {
    fn from(error: PreviewError) -> Self {
        match error {
            // The file id isn't in the catalog at all.
            PreviewError::UnknownFile => ApiError::NotFound,
            PreviewError::ShuttingDown => ApiError::Internal(error.to_string()),
        }
    }
}

/// The result of a [`Api::run_query`]: the files and tags matching a query.
///
/// Both lists are matched by the same conjunction of query terms (see
/// [`Api::run_query`]); files by their tags/logical path and tags by their
/// place in the tag hierarchy / their name. Full rows (not bare ids) are
/// returned so callers can render results without a second listing round-trip —
/// the daemon does the id→row join once, over just the matched set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub files: Vec<FileInfo>,
    pub tags: Vec<Tag>,
}

/// The result of [`Api::finish_edit`]: did the edit actually change the file?
///
/// `changed = false` means the post-edit bytes hashed to the file's current
/// recorded `content_hash`; either the editor produced no change, or the edit
/// happened in place and the filesystem watcher already published the same
/// content the daemon then saw at `finish_edit` time. `changed = true` means
/// the daemon streamed the new content to peers as a new version. The Dart UI
/// uses this to show a "no changes" hint vs. an "edited" confirmation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditOutcome {
    /// Whether the daemon published a new version from the edited bytes.
    pub changed: bool,
}

/// A live update delivered on the API event stream.
///
/// Delivery is **best-effort**, mirroring the in-process ingest bus. There is
/// no per-event replay or buffering. On (re)connection over IPC the transport
/// emits [`ApiEvent::Resynced`] first; the UI responds by re-fetching current
/// state via the read API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApiEvent {
    /// The stream (re)started; the UI should re-fetch current state. Produced
    /// by the transport layer on connect/reconnect, not by the change bus.
    Resynced,
    /// A change was applied to the store.
    Changed(Change),
    /// A file this connection was temporarily providing (an upload/edit) has
    /// been handed off (a peer completed pulling it); the client may release
    /// the local file. Produced by the control layer, not the change bus.
    ProviderReleased { file_id: FileId },
}

/// The transport-agnostic UI-facing API handle.
///
/// Cheap to clone. Holds the pieces needed to serve reads (the DB path),
/// serve writes (the ingest-bus sender), and produce the event stream (a
/// broadcast subscription source). Constructed by [`run`](crate::run) and
/// wrapped by each transport backend.
#[derive(Clone)]
pub struct Api {
    main_db_path: PathBuf,
    change_sender: UnboundedSender<DaemonMessage>,
    /// Direct handle to the sync-directory manager, used only for read-only
    /// path lookups (`local_path_for_file`). Writes still go via
    /// `change_sender` and the `handle_changes` pipeline.
    command_sender: UnboundedSender<SyncDirectoryCommand>,
    events: broadcast::Sender<Change>,
    /// Fetch/transfer subsystem, used by the control layer to register a
    /// temporary chunk provider for an upload/edit (the client serves the bytes
    /// on demand).
    pending_fetches: PendingFetches,
    /// Directory for daemon-owned temp files produced by `fetch_file`. A
    /// completed fetch materializes here and the path is handed to the caller
    /// with move semantics. See [`crate::paths::Paths::fetch_temp_dir`].
    fetch_temp_dir: PathBuf,
    /// Live sync-operation registry. Reads (`list_operations`) snapshot it;
    /// `subscribe_operations` taps its event broadcast. Fed by the peer
    /// sessions, not by this API.
    operations: crate::operations::Operations,
    /// External-editor rules the desktop UI consults for its "edit" action
    /// (see [`crate::configuration::EditorRule`]). Snapshot of the startup
    /// configuration; the daemon does not act on these but stores them so
    /// every frontend attached to this device sees the same set.
    editor_rules: Vec<EditorRule>,
}

impl Api {
    /// The overall deadline a caller waits for an on-demand fetch to complete.
    /// Must exceed [`crate::fetch::HOP_TIMEOUT`] so intermediate hops can time
    /// out and report before this outer deadline fires.
    const FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Build an API handle from the runtime's shared pieces.
    ///
    /// - `main_db_path`: the main DB path; each read opens its own read-only
    ///   handle on it (SQLite serialises file-level access).
    /// - `change_sender`: the ingest bus every mutation is pushed onto.
    /// - `command_sender`: the sync-directory manager command channel, used for
    ///   read-only path lookups.
    /// - `events`: the broadcast channel `handle_changes` publishes applied
    ///   changes to.
    pub fn new(
        main_db_path: PathBuf,
        change_sender: UnboundedSender<DaemonMessage>,
        command_sender: UnboundedSender<SyncDirectoryCommand>,
        events: broadcast::Sender<Change>,
        pending_fetches: PendingFetches,
        fetch_temp_dir: PathBuf,
        operations: crate::operations::Operations,
        editor_rules: Vec<EditorRule>,
    ) -> Self {
        Self {
            main_db_path,
            change_sender,
            command_sender,
            events,
            pending_fetches,
            fetch_temp_dir,
            operations,
            editor_rules,
        }
    }

    /// Snapshot of the desktop UI's tag-based editor rules (see
    /// [`crate::configuration::EditorRule`]). Read-only; taken from
    /// configuration at startup.
    pub fn editor_rules(&self) -> Vec<EditorRule> {
        self.editor_rules.clone()
    }

    /// Open a fresh read-only DB handle for a single read call.
    ///
    /// `FileDatabase` is `Send + !Sync`; we never share one across `.await`,
    /// so each read opens its own handle and drops it before returning.
    fn open_read(&self) -> Result<FileDatabase, ApiError> {
        FileDatabase::initialize(&self.main_db_path).map_err(ApiError::from)
    }

    /// Enqueue a locally-originated change onto the ingest bus.
    ///
    /// `directory_path` in the [`ChangeOrigin::Local`] is a sentinel that must
    /// not match any configured sync directory, so `handle_changes` dispatches
    /// the change to every matching sync directory rather than skipping one as
    /// the "source". An empty path never matches a real sync-directory path.
    fn enqueue(&self, change: Change) -> Result<(), ApiError> {
        self.change_sender
            .send(DaemonMessage::Change(
                Ingest::from_change(change),
                ChangeOrigin::Local {
                    directory_path: PathBuf::new(),
                },
            ))
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }

    /// Resolve a full-or-short file id `prefix` (as displayed by `list_files`'s
    /// short ids, or a pasted full id) to a single [`FileId`]. Backed by
    /// `FileDatabase::resolve_file_id_prefix`.
    ///
    /// Returns [`ApiError::NotFound`] if nothing matches and
    /// [`ApiError::Ambiguous`] if more than one file matches.
    pub fn resolve_file_id(&self, prefix: &str) -> Result<FileId, ApiError> {
        let database = self.open_read()?;
        Ok(database.resolve_file_id_prefix(prefix)?)
    }

    /// Resolve a full-or-short tag id `prefix` (as displayed by `list_tags`'s
    /// short ids, or a pasted full id) to a single [`TagId`]. The tag
    /// counterpart of [`resolve_file_id`](Self::resolve_file_id). Backed by
    /// `FileDatabase::resolve_tag_id_prefix`.
    ///
    /// Returns [`ApiError::NotFound`] if nothing matches and
    /// [`ApiError::Ambiguous`] if more than one tag matches.
    pub fn resolve_tag_id(&self, prefix: &str) -> Result<TagId, ApiError> {
        let database = self.open_read()?;
        Ok(database.resolve_tag_id_prefix(prefix)?)
    }

    /// List the tags applied to `file_id`. `subtag_rule` controls whether the
    /// tag hierarchy is walked. Backed by `FileDatabase::tag_ids_for_file`.
    pub fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .tag_ids_for_file(file_id, subtag_rule)?
            .into_iter()
            .collect())
    }

    /// Run a free-form query and return both the matching files and tags.
    ///
    /// The query is a whitespace-separated list of *chunks*, combined
    /// conjunctively (a result must satisfy every chunk). Each chunk is
    /// optionally prefixed by `!` (negation) and/or a kind prefix:
    ///
    /// - `/t foo` — require the tag(s) resolved from `foo`. A file matches if
    ///   it carries any such tag; a tag matches if it is a subtag of any.
    /// - `/l foo` — case-insensitive substring against the file's logical path
    ///   (or the tag's name on the tag side).
    /// - `/p foo` — reserved for physical-path search; currently a no-op.
    /// - `foo` (no prefix) — matches on *either* side: logical/name substring
    ///   OR tag membership. This is the "just find anything that looks like
    ///   `foo`" chunk.
    /// - `!` in front of any of the above inverts the filter.
    ///
    /// Chunks with whitespace can be quoted: `/t "foo bar"`.
    ///
    /// Parsing is forgiving — malformed chunks are silently dropped so a
    /// half-typed query in a search box still returns results (see
    /// [`chunk`] for the full grammar and recovery rules). Tag tokens are
    /// resolved to [`TagId`]s here so clients pass the raw string through; an
    /// empty query matches everything. `subtag_rule` controls hierarchy
    /// traversal for the tag terms.
    ///
    /// Returns full [`FileInfo`]/[`Tag`] rows (not bare ids): the daemon joins
    /// each matched id to its row here, over just the result set, so callers
    /// render directly without a second whole-store listing. Backed by
    /// `FileDatabase::file_ids_for_query`/`tag_ids_for_query` plus
    /// `file_info_from_id`/`tag_from_id`.
    ///
    /// `deleted_rule` toggles between the standard live-only view
    /// ([`DeletedRule::Exclude`]) and the "search deleted rows"
    /// view ([`DeletedRule::Include`]). Under `Include`, this method
    /// widens tag-token resolution *and* the candidate pool to include
    /// tombstoned rows, then post-filters the joined `FileInfo`/`Tag` results
    /// to keep only the ones whose `deleted` flag is set — an *only deleted*
    /// result. This lets the UI expose "show deleted" as a toggle without
    /// requiring a separate query grammar. Tag-hierarchy walks and the
    /// file↔tag relationship table stay live-only regardless, since users
    /// searching for deleted files/tags want files whose row itself was
    /// tombstoned, not files that were merely untagged.
    pub fn run_query(
        &self,
        query: &str,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<QueryResult, ApiError> {
        let database = self.open_read()?;
        let terms = Self::parse_query(&database, query, deleted_rule)?;

        // A matched id may not resolve to a full listable row: `file_ids_for_query`
        // draws file ids from the tag `entries` table, which can reference a file
        // that has no `file_versions` row yet (tagged before its content
        // materialized). Such a file is not listable, so skip it rather than
        // failing the whole query with `NotFound`. Same tolerance for tags.
        let mut files = Vec::new();
        for file_id in database.file_ids_for_query(&terms, subtag_rule, deleted_rule)? {
            match database.file_info_from_id(file_id, deleted_rule) {
                Ok(file) => {
                    // Under `Include` we want only the tombstoned files; the
                    // live ones are handled by the standard `Exclude` path.
                    if deleted_rule == DeletedRule::Include && !file.deleted {
                        continue;
                    }
                    files.push(file);
                }
                Err(DatabaseError::MissingFile) => {}
                Err(other) => return Err(other.into()),
            }
        }

        let mut tags = Vec::new();
        for tag_id in database.tag_ids_for_query(&terms, subtag_rule, deleted_rule)? {
            match database.tag_from_id(tag_id, deleted_rule) {
                Ok(tag) => {
                    if deleted_rule == DeletedRule::Include && !tag.deleted {
                        continue;
                    }
                    tags.push(tag);
                }
                Err(DatabaseError::MissingTag) => {}
                Err(other) => return Err(other.into()),
            }
        }

        Ok(QueryResult { files, tags })
    }

    /// Get a single file's [`FileInfo`] by id, or [`ApiError::NotFound`] if no
    /// such file exists. The by-id read that replaces scanning a full listing
    /// (used by `onisync edit`/`download` to find one file's metadata). Backed
    /// by `FileDatabase::file_info_from_id`.
    ///
    /// `deleted_rule` governs tombstone visibility: `Exclude` treats a
    /// tombstoned file as `NotFound` (the standard behavior for pickers and
    /// operational lookups); `Include` returns it with `FileInfo::deleted =
    /// true`, so a detail screen opened from a "search deleted" result can
    /// still render its metadata.
    pub fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, ApiError> {
        let database = self.open_read()?;
        Ok(database.file_info_from_id(file_id, deleted_rule)?)
    }

    /// Get a single tag by id, or [`ApiError::NotFound`] if no such tag exists.
    /// Backed by `FileDatabase::tag_from_id`. See [`Self::get_file`] for the
    /// `deleted_rule` semantics.
    pub fn get_tag(&self, tag_id: TagId, deleted_rule: DeletedRule) -> Result<Tag, ApiError> {
        let database = self.open_read()?;
        Ok(database.tag_from_id(tag_id, deleted_rule)?)
    }

    /// Parse a free-form query string into resolved [`QueryTerm`]s.
    ///
    /// Two stages: [`chunk::lex_query`] tokenises the string into [`Chunk`]s
    /// (pure, no DB access — see the [`chunk`] module docs for the grammar and
    /// error-recovery contract), then this function resolves each chunk into
    /// one [`QueryTerm`], expanding tag references via
    /// [`FileDatabase::tag_ids_matching_token`].
    ///
    /// Both stages are forgiving:
    /// - the lexer silently drops malformed chunks (see its module docs);
    /// - this resolver silently drops any [`ChunkKind::Physical`] chunk, since
    ///   physical-path search is not wired up yet — the grammar accepts `/p` so
    ///   users see consistent parsing, but the filter is a no-op.
    ///
    /// The only remaining fallible step is `tag_ids_matching_token`, which can
    /// surface a real database error; that is propagated as-is.
    ///
    /// `deleted_rule` is forwarded to [`FileDatabase::tag_ids_matching_token`]
    /// so a search that wants to see deleted rows can still resolve tokens
    /// that only match tombstoned tags.
    fn parse_query(
        database: &FileDatabase,
        query: &str,
        deleted_rule: DeletedRule,
    ) -> Result<Vec<QueryTerm>, ApiError> {
        use chunk::{ChunkKind, lex_query};

        let mut terms = Vec::new();
        for chunk in lex_query(query) {
            let term = match (chunk.kind, chunk.negated) {
                (ChunkKind::Tag, false) => {
                    QueryTerm::HasTag(database.tag_ids_matching_token(&chunk.text, deleted_rule)?)
                }
                (ChunkKind::Tag, true) => {
                    QueryTerm::NotTag(database.tag_ids_matching_token(&chunk.text, deleted_rule)?)
                }
                (ChunkKind::Name, false) => QueryTerm::NameContains(chunk.text),
                (ChunkKind::Name, true) => QueryTerm::NotNameContains(chunk.text),
                (ChunkKind::Logical, false) => QueryTerm::LogicalContains(chunk.text),
                (ChunkKind::Logical, true) => QueryTerm::NotLogicalContains(chunk.text),
                (ChunkKind::Any, false) => QueryTerm::AnyMatch(
                    chunk.text.clone(),
                    database.tag_ids_matching_token(&chunk.text, deleted_rule)?,
                ),
                (ChunkKind::Any, true) => QueryTerm::NotAnyMatch(
                    chunk.text.clone(),
                    database.tag_ids_matching_token(&chunk.text, deleted_rule)?,
                ),
                // `/p` is reserved but not yet supported — drop the chunk so
                // the rest of the query still works, matching the "forgiving
                // search box" contract.
                (ChunkKind::Physical, _) => continue,
            };
            terms.push(term);
        }
        Ok(terms)
    }

    /// List the subtags of `tag_id` (its children in the tag hierarchy).
    /// `subtag_rule` controls whether the hierarchy is walked transitively.
    /// Backed by `FileDatabase::subtag_ids_for_tag`.
    pub fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .subtag_ids_for_tag(tag_id, subtag_rule)?
            .into_iter()
            .collect())
    }

    /// List the tags applied to `tag_id` (the tags it is a subtag of) — the tag
    /// analogue of [`tags_for_file`](Self::tags_for_file). `subtag_rule`
    /// controls whether the hierarchy is walked transitively. Backed by
    /// `FileDatabase::tag_ids_for_subtag`.
    pub fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .tag_ids_for_subtag(tag_id, subtag_rule)?
            .into_iter()
            .collect())
    }

    /// Create a tag. Mints a fresh `TagId` and enqueues `Change::TagAdded`;
    /// the id is returned immediately (persistence is asynchronous — observe
    /// the event stream for confirmation).
    pub fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        if name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("tag name is empty".to_owned()));
        }
        // A locally-originated mutation is stamped with our wall clock now; the
        // timestamp then rides the change unchanged to peers for LWW.
        // Hex form (matches the CLI's default and the Flutter app's palette),
        // so tags created with an empty color render consistently everywhere.
        let color = if color.trim().is_empty() {
            "#F44336".to_owned()
        } else {
            color
        };
        let tag_id = TagId::new();
        self.enqueue(Change::TagAdded {
            tag_id,
            tag_name: name,
            color,
            metadata: None,
            modified_at: crate::database::now_millis(),
        })?;
        Ok(tag_id)
    }

    /// Delete a tag. Enqueues `Change::TagRemoved`, stamped with our wall clock
    /// now: a tag reuses `modified_at` as its last-writer-wins clock, so the
    /// delete carries the timestamp here.
    pub fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.enqueue(Change::TagRemoved {
            tag_id,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Restore a soft-deleted tag.
    ///
    /// Unlike a file, a tag carries no content and reuses `modified_at` as its
    /// single last-writer-wins clock, so a restore is simply re-announcing the
    /// tag's current definition with a fresh timestamp: `add_tag` upserts with
    /// `deleted = 0` and wins LWW over the (older) delete, both locally and on
    /// every peer. It therefore reuses the `Change::TagAdded` path rather than
    /// a bespoke wire variant, and is fire-and-forget (no bytes to recover,
    /// so it cannot "fail to find a source" the way a file restore can).
    ///
    /// Returns [`ApiError::NotFound`] if the tag is unknown. Reading it with
    /// `Include` means an already-live tag is re-announced harmlessly (the LWW
    /// guard makes it a no-op if nothing changed).
    pub fn restore_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        let tag = {
            let database = self.open_read()?;
            database.tag_from_id(tag_id, DeletedRule::Include)?
        };
        self.enqueue(Change::TagAdded {
            tag_id,
            tag_name: tag.name,
            color: tag.color,
            metadata: None,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Rename a tag. Enqueues `Change::TagRenamed`, stamped with our wall clock
    /// now for last-writer-wins reconciliation.
    pub fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        if name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("tag name is empty".to_owned()));
        }
        self.enqueue(Change::TagRenamed {
            tag_id,
            tag_name: name,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Change a tag's color. Enqueues `Change::TagRecolored` carrying the full
    /// new color, stamped with our wall clock now for last-writer-wins.
    pub fn set_tag_color(&self, tag_id: TagId, color: String) -> Result<(), ApiError> {
        if color.trim().is_empty() {
            return Err(ApiError::InvalidArgument("color is empty".to_owned()));
        }
        self.enqueue(Change::TagRecolored {
            tag_id,
            color,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Upload a file whose bytes the client provides on demand.
    ///
    /// The client has already computed `content_hash` (by streaming its own
    /// file) and will serve the bytes chunk-by-chunk as a temporary provider;
    /// no bytes are passed here. Mints a `FileId`, records the file + version,
    /// and announces a metadata-only `FileMetadataAdded` to peers, which then
    /// pull the content from the provider the control layer registers.
    pub fn upload_file(
        &self,
        path_name: String,
        content_hash: String,
        size: u64,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        if path_name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        let file_id = FileId::new();
        self.change_sender
            .send(DaemonMessage::AnnounceProvided {
                file_id,
                logical_path: Some(LogicalPath::new(path_name)),
                content_hash,
                size,
                tags,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;
        Ok(file_id)
    }

    /// Register a temporary chunk provider for a file the client is serving on
    /// demand. Delegates to the transfer subsystem's provider registry.
    pub async fn register_provider(
        &self,
        file_id: FileId,
        content_hash: String,
        source: Arc<dyn ChunkSource>,
    ) {
        self.pending_fetches
            .register_provider(file_id, content_hash, source)
            .await;
    }

    /// Remove a temporary provider (the client released the file).
    pub async fn unregister_provider(&self, file_id: FileId, content_hash: &str) {
        self.pending_fetches
            .unregister_provider(file_id, content_hash)
            .await;
    }

    /// Replace the content of an existing file, provided on demand by the
    /// client (see [`Self::upload_file`]). Records the new version and
    /// announces a metadata-only `FileMetadataChanged` to peers, which pull
    /// from the provider.
    pub fn edit_file(
        &self,
        file_id: FileId,
        content_hash: String,
        size: u64,
    ) -> Result<(), ApiError> {
        self.change_sender
            .send(DaemonMessage::AnnounceProvided {
                file_id,
                logical_path: None,
                content_hash,
                size,
                tags: Vec::new(),
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }

    /// Start an external edit: return the on-disk path the caller should hand
    /// to an editor.
    ///
    /// If the file lives in a local sync directory, returns that real
    /// on-disk path (edit-in-place; the watcher propagates the save). Otherwise
    /// fetches the content — from a peer if needed — into an isolated
    /// per-request subdirectory under [`crate::paths::Paths::fetch_temp_dir`],
    /// named with the file's logical basename (extension preserved so editors
    /// dispatch by MIME correctly), and returns that path with move semantics.
    ///
    /// No daemon-side state is kept across the edit. The caller's `file_id`
    /// plus the returned `path` fully describe the follow-up
    /// [`Self::finish_edit`] / [`Self::cancel_edit`]. A caller that crashes
    /// before finishing only leaks the temp file, which the daemon bulk-cleans
    /// on next start (see [`crate::paths::Paths::clean_fetch_temp_dir`]).
    pub async fn begin_edit(&self, file_id: FileId) -> Result<PathBuf, ApiError> {
        // Fast path: the bytes already live in a local sync directory. Give
        // the editor the real file; the watcher will pick up the save.
        if let Some(path) = self.local_path_for_file(file_id).await? {
            return Ok(path);
        }

        // Otherwise materialise into a caller-visible temp. `fetch_file`
        // handles the extension-preserving naming and the on-demand peer
        // pull. Getting the expected hash costs one by-id read; the file
        // must exist for us to fetch it.
        let info = self.get_file(file_id, DeletedRule::Include)?;
        self.fetch_file(file_id, info.content_hash).await
    }

    /// Complete an external edit started with [`Self::begin_edit`]: publish a
    /// new version if the bytes at `path` differ from the file's currently
    /// recorded content.
    ///
    /// Hashing is streaming; the bytes are never buffered whole. The
    /// comparison is against the DB's *current* `content_hash`, so an in-place
    /// edit whose save was already ingested by the watcher no-ops here
    /// automatically.
    ///
    /// # Temp file lifetime
    ///
    /// When the bytes changed, the daemon registers `path` as a chunk provider
    /// (via `FileToCopy`) so peers can pull the new content **on demand** —
    /// reads happen after this call returns, from `path` on disk. The temp
    /// is therefore **not deleted here**: doing so would break peers mid-pull
    /// with a "No such file or directory" error. The temp is left in place
    /// and cleaned up in bulk on the next daemon start (see
    /// [`crate::paths::Paths::clean_fetch_temp_dir`]), matching the "provider
    /// outlives the API call" semantics that
    /// [`crate::transport::TransportBackend::upload_file`] has always had.
    ///
    /// The no-op branch (bytes unchanged) still cleans up: no provider was
    /// registered, no peer will ever read from `path`, so the temp is safe to
    /// remove immediately.
    pub async fn finish_edit(
        &self,
        file_id: FileId,
        path: PathBuf,
    ) -> Result<EditOutcome, ApiError> {
        // Compare the edited bytes against the file's current recorded hash.
        // If they match there is nothing to publish — either the editor
        // produced no change, or the watcher already ingested the in-place
        // save and updated the DB.
        let (edited_hash, edited_size) = crate::control::hash_file(&path).await?;
        let current_hash = self.get_file(file_id, DeletedRule::Include)?.content_hash;

        if edited_hash == current_hash {
            // No-op: nothing was published, nothing else will read `path`.
            self.cleanup_edit_path(&path);
            return Ok(EditOutcome { changed: false });
        }

        // Publish the new content by streaming it from `path` via the
        // usual chunk-provider protocol. Peers pull on demand *after* this
        // call returns, so `path` must remain readable until the daemon
        // restarts. See the method docs.
        let source = crate::file_bytes::FileBytes::FileToCopy(path);
        self.edit_file(file_id, edited_hash.clone(), edited_size)?;
        self.register_provider(file_id, edited_hash, std::sync::Arc::new(source))
            .await;
        Ok(EditOutcome { changed: true })
    }

    /// Abort an external edit started with [`Self::begin_edit`] without
    /// publishing. Cleans up any daemon-owned temp under
    /// [`crate::paths::Paths::fetch_temp_dir`]; other paths are left alone.
    pub fn cancel_edit(&self, path: PathBuf) -> Result<(), ApiError> {
        self.cleanup_edit_path(&path);
        Ok(())
    }

    /// Remove the per-request subdir the daemon created for a Branch B
    /// `begin_edit`, iff `path` lives under `fetch_temp_dir`. A path handed
    /// out for Branch A (a real sync-dir file) is silently ignored — we
    /// never delete user data.
    ///
    /// Best-effort: any I/O failure here is swallowed. The daemon bulk-wipes
    /// `fetch_temp_dir` on its next start regardless, so a missed cleanup is
    /// a bounded leak.
    fn cleanup_edit_path(&self, path: &std::path::Path) {
        // Only touch paths that are actually inside our fetch temp dir.
        // `starts_with` compares path components, so it is not fooled by
        // string-level tricks (e.g. a `..` in a caller-supplied path).
        if !path.starts_with(&self.fetch_temp_dir) {
            return;
        }
        // `fetch_file` materialises as `<fetch_temp_dir>/<uuid>/<basename>`,
        // so removing the parent (`<uuid>`) drops both the file and the
        // now-empty subdir.
        if let Some(parent) = path.parent()
            && parent != self.fetch_temp_dir
        {
            let _ = std::fs::remove_dir_all(parent);
        }
    }

    /// Delete a file. Enqueues `Change::FileDeleted`, stamped with our wall
    /// clock now for last-writer-wins against a later edit.
    pub fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileDeleted {
            file_id,
            deleted_at: crate::database::now_millis(),
        })
    }

    /// Restore a soft-deleted file — best-effort.
    ///
    /// Sends a [`DaemonMessage::Restore`] and awaits its outcome. The daemon
    /// checks whether the file's latest version is still recoverable (its own
    /// `keep_deleted_files` vault first, then a probe flooded across the peer
    /// tree). Only if the bytes are available does it clear the tombstone,
    /// record the restored version, announce a `Change::FileRestored` to peers,
    /// and pull the bytes into whichever local sync directories want them. If
    /// nothing holds the bytes the tombstone is left in place and this returns
    /// [`ApiError::NotFound`].
    ///
    /// Request-reply (unlike `delete_file`) because the outcome is only known
    /// after the async availability probe.
    pub async fn restore_file(&self, file_id: FileId) -> Result<(), ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(DaemonMessage::Restore {
                file_id,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        match tokio::time::timeout(Self::FETCH_TIMEOUT, response).await {
            Ok(Ok(result)) => result.map_err(ApiError::from),
            // The responder was dropped without sending — treat as shutdown.
            Ok(Err(_recv_error)) => Err(ApiError::Internal(RestoreError::ShuttingDown.to_string())),
            Err(_elapsed) => Err(ApiError::Internal(FetchError::TimedOut.to_string())),
        }
    }

    /// Move (rename) a file to a new logical path. Enqueues
    /// `Change::FileMoved`, stamped with our wall clock now as the path's
    /// last-writer-wins clock; each receiving sync directory derives its own
    /// physical placement.
    pub fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        if logical_path.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        self.enqueue(Change::FileMoved {
            file_id,
            logical_path: LogicalPath::new(logical_path),
            modified_at: crate::database::now_millis(),
        })
    }

    /// Fetch a file's content on demand, from a peer if not present locally,
    /// and return the path to a **daemon-owned temp file** holding it.
    ///
    /// Enqueues a [`DaemonMessage::Fetch`] onto the ingest bus;
    /// `handle_changes` checks the local sync directories first
    /// (hash-gated) and, failing that, drives a content-addressed receive that
    /// floods `Sync::ChunkRequest`s across the live peer tree. Awaits
    /// the reply with an overall timeout. `expected_hash` gates which content
    /// is accepted; the caller obtains it from the file's known metadata
    /// (`FileInfo::content_hash`).
    ///
    /// The returned path lives under [`crate::paths::Paths::fetch_temp_dir`]
    /// and is handed to the caller with **move semantics**: the caller must
    /// consume it (rename into place or delete). The whole file is never
    /// buffered into memory — a peer transfer already lands as a temp file
    /// on disk, and a locally-held copy is streamed into the fetch temp
    /// dir.
    ///
    /// The materialised path has the shape
    /// `<fetch_temp_dir>/<uuid>/<logical_basename>` (an isolated
    /// per-request subdirectory whose leaf carries the file's logical name,
    /// including extension). The extension is load-bearing: editors, share
    /// sheets, and downloads all key their behaviour off it. Callers should
    /// clean up the *parent* directory (`<fetch_temp_dir>/<uuid>`) rather
    /// than just the file, so an unmoved temp leaves nothing behind. Any
    /// leftover subdirectories are also wiped in bulk on the next daemon
    /// start (see [`crate::paths::Paths::clean_fetch_temp_dir`]), so a
    /// missed cleanup only leaks until the next restart.
    pub async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        // Read the file's logical basename before enqueuing the fetch so a
        // completed fetch always has a name to land under. This is one extra
        // by-id read (cheap; same shape as `get_file`), and doing it here
        // means every caller — CLI download, UI download/share, and the
        // upcoming edit flow — gets the right on-disk name for free.
        let logical_basename = {
            let database = self.open_read()?;
            let info = database.file_info_from_id(file_id, DeletedRule::Include)?;
            let basename = info.logical_path.basename().to_owned();
            // A pathological empty basename would resolve to
            // `<uuid>/` which some filesystems reject and which would in any
            // case give the editor no extension to dispatch on. Fall back to
            // the file id, which at least yields a stable, unique name.
            if basename.is_empty() {
                file_id.to_string()
            } else {
                basename
            }
        };

        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(DaemonMessage::Fetch {
                file_id,
                expected_hash,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        let content = match tokio::time::timeout(Self::FETCH_TIMEOUT, response).await {
            Ok(Ok(Ok(file_bytes))) => file_bytes,
            Ok(Ok(Err(fetch_error))) => return Err(fetch_error.into()),
            // The responder was dropped without sending — treat as shutdown.
            Ok(Err(_recv_error)) => {
                return Err(ApiError::Internal(FetchError::ShuttingDown.to_string()));
            }
            Err(_elapsed) => return Err(ApiError::Internal(FetchError::TimedOut.to_string())),
        };

        // Materialize into `<fetch_temp_dir>/<uuid>/<logical_basename>`. The
        // per-request `<uuid>` subdirectory isolates the file so it can carry
        // its real name (matching extension included) without colliding with
        // other in-flight fetches of the same logical basename.
        let subdir = self.fetch_temp_dir.join(uuid::Uuid::new_v4().to_string());
        tokio::fs::create_dir_all(&subdir).await.map_err(|error| {
            ApiError::Internal(format!("failed to create fetch temp subdir: {error}"))
        })?;
        let dest = subdir.join(&logical_basename);
        content.materialize_to(&dest).await.map_err(|error| {
            ApiError::Internal(format!("failed to stage fetched file: {error}"))
        })?;
        Ok(dest)
    }

    /// Get the preview for `file_id`'s current content.
    ///
    /// Enqueues a [`DaemonMessage::GetPreview`] onto the ingest bus;
    /// `handle_changes` returns any cached preview, else generates it locally
    /// (if the bytes are present) or requests it from a peer (first responder
    /// wins), caching the result in `previews_v1` before replying.
    ///
    /// A file with no previewable content resolves to [`Preview::None`] — that
    /// is a successful result, not an error. `ApiError::NotFound` means the
    /// file id itself is unknown to the catalog.
    pub async fn get_preview(&self, file_id: FileId) -> Result<Preview, ApiError> {
        // End-to-end stopwatch for the whole daemon-side request (bus enqueue →
        // handle_changes resolution → reply). Combined with the finer-grained
        // logs inside `handle_changes`, this shows how much time is the actual
        // work vs. queueing behind other messages on the single-writer bus.
        let api_start = std::time::Instant::now();
        log::debug!(
            "Api::get_preview: requesting preview for {}",
            file_id.to_string()
        );

        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(DaemonMessage::GetPreview {
                file_id,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        let result = match tokio::time::timeout(Self::FETCH_TIMEOUT, response).await {
            Ok(Ok(result)) => result.map_err(ApiError::from),
            // The responder was dropped without sending — treat as shutdown.
            Ok(Err(_recv_error)) => Err(ApiError::Internal(PreviewError::ShuttingDown.to_string())),
            Err(_elapsed) => Err(ApiError::Internal(FetchError::TimedOut.to_string())),
        };
        log::debug!(
            "Api::get_preview: preview for {} resolved in {:?} (ok={})",
            file_id.to_string(),
            api_start.elapsed(),
            result.is_ok()
        );
        result
    }

    /// Purge the entire preview cache, returning how many cached previews were
    /// removed.
    ///
    /// Enqueues a [`DaemonMessage::PurgePreviews`] onto the ingest bus so the
    /// wipe runs on the sole main-DB writer (`handle_changes`). Previews are
    /// hash-keyed and regenerated on demand, so this never affects correctness;
    /// it forces every file to be re-evaluated on its next preview request.
    /// Exposed to operators via the `onisync purge-previews` CLI command.
    pub async fn purge_previews(&self) -> Result<usize, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(DaemonMessage::PurgePreviews { respond_to })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        match tokio::time::timeout(Self::FETCH_TIMEOUT, response).await {
            Ok(Ok(result)) => result.map_err(ApiError::from),
            // The responder was dropped without sending — treat as shutdown.
            Ok(Err(_recv_error)) => Err(ApiError::Internal("runtime is shutting down".to_owned())),
            Err(_elapsed) => Err(ApiError::Internal(FetchError::TimedOut.to_string())),
        }
    }

    /// Resolve `file_id` to the absolute on-disk path where its bytes currently
    /// live locally, or `None` if no sync directory holds it. Read-only.
    ///
    /// Used by `onisync edit` to detect the "already local" case and open the
    /// real file in place (the watcher then propagates the save).
    pub async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.command_sender
            .send(SyncDirectoryCommand::LocalPath {
                file_id,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;
        response
            .await
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }

    /// Apply `tag_id` to `file_id`. Enqueues `Change::FileTagged`.
    pub fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileTagged {
            file_id,
            tag_id,
            metadata: None,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Remove `tag_id` from `file_id`. Enqueues `Change::FileUntagged`.
    pub fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileUntagged {
            file_id,
            tag_id,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Make `subtag_id` a subtag (child) of `parent_id` in the tag hierarchy.
    /// Enqueues `Change::TagTagged`.
    ///
    /// A tag cannot be its own subtag; that is rejected here (with
    /// [`ApiError::InvalidArgument`]) rather than only being caught by the
    /// database inside the change pipeline, so the caller learns immediately.
    pub fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        if parent_id == subtag_id {
            return Err(ApiError::InvalidArgument(
                "a tag cannot be its own subtag".to_owned(),
            ));
        }
        self.enqueue(Change::TagTagged {
            taggee_id: subtag_id,
            tag_id: parent_id,
            metadata: None,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Remove `subtag_id` as a subtag of `parent_id`. Enqueues
    /// `Change::TagUntagged`.
    pub fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        self.enqueue(Change::TagUntagged {
            taggee_id: subtag_id,
            tag_id: parent_id,
            modified_at: crate::database::now_millis(),
        })
    }

    /// Subscribe to the live change stream.
    ///
    /// Yields every [`Change`] applied by `handle_changes` after this call.
    /// Delivery is best-effort: a slow subscriber that lags beyond the channel
    /// capacity observes a `RecvError::Lagged`, which the transport layer maps
    /// onto an [`ApiEvent::Resynced`] so the UI re-fetches state.
    pub fn subscribe(&self) -> broadcast::Receiver<Change> {
        self.events.subscribe()
    }

    /// Snapshot every currently-active sync operation.
    ///
    /// The read counterpart of
    /// [`subscribe_operations`](Self::subscribe_operations): the UI calls
    /// this for its initial paint (and after an IPC `Resynced`),
    /// then applies live [`OperationEvent`](crate::operations::OperationEvent)s
    /// on top. Order is unspecified; the caller sorts by `started_at`.
    pub fn list_operations(&self) -> Vec<crate::operations::Operation> {
        self.operations.snapshot()
    }

    /// Subscribe to the live sync-operation stream.
    ///
    /// Yields every [`OperationEvent`](crate::operations::OperationEvent)
    /// (started / progress / terminal) produced by the peer sessions after this
    /// call. Best-effort, exactly like [`subscribe`](Self::subscribe): a slow
    /// subscriber that lags past the channel capacity observes a
    /// `RecvError::Lagged`, which the transport maps onto a re-snapshot prompt.
    pub fn subscribe_operations(&self) -> broadcast::Receiver<crate::operations::OperationEvent> {
        self.operations.subscribe()
    }
}

/// Search-query lexer (stage 1 of two — see [`Api::parse_query`]).
///
/// This module is deliberately **pure**: it turns a raw query string into a
/// vector of [`Chunk`]s without ever touching the database. Resolving a chunk's
/// text into concrete [`TagId`](onisync_core::TagId)s or applying it against
/// the stored files happens in the resolver stage.
///
/// # Grammar
///
/// A query is a whitespace-separated sequence of *chunks*. A chunk is:
///
/// 1. An optional `!` (negation) — must be a standalone whitespace-delimited
///    token; `!foo` is **not** a negation, it's a literal chunk whose text
///    starts with `!`.
/// 2. An optional *kind prefix* — one of `/t`, `/l`, `/p`, again standalone:
///    - `/t` — tag chunk: match tags whose name/id resolves from the payload.
///    - `/l` — logical-path chunk: substring match on the file's logical path.
///    - `/p` — physical-path chunk (reserved; not wired up yet).
///
///    Unknown `/x` tokens are **not** prefixes: they become literal chunks
///    whose payload starts with `/`. This keeps `/home/lucas` searchable.
/// 3. A *payload*, either:
///    - A double-quoted string `"..."` — captures whitespace verbatim. Supports
///      backslash escapes `\"` and `\\`; any other `\c` is left as-is (`\c`).
///    - Or a bare run of non-whitespace characters.
///
/// A chunk without a kind prefix is [`ChunkKind::Any`] — the resolver will
/// match its payload against *both* names and tags (union).
///
/// # Error recovery
///
/// Parsing is **infallible**: `lex_query` always returns a `Vec<Chunk>`, never
/// an error. Malformed input is skipped rather than rejected, so a search box
/// stays usable mid-typing. Specifically, when the lexer hits any of the
/// following it *discards the current chunk in progress* and resumes at the
/// next whitespace boundary:
///
/// - a `!` or kind prefix followed by nothing (`!`, `/t`, `! /t` at EOF);
/// - conflicting kind prefixes (`/t /l foo` drops the `/t /l` chunk and
///   continues from `foo`);
/// - a duplicate `!` (`! ! foo` drops that chunk);
/// - an unterminated quoted string (`"foo` at EOF is dropped entirely).
///
/// Diagnostics are intentionally not surfaced: the caller sees only the chunks
/// that parsed cleanly.
pub(crate) mod chunk {
    /// What kind of filter a chunk expresses.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ChunkKind {
        /// Match names *and* tags (union).
        Any,
        /// The payload names a tag.
        Tag,
        /// The payload is a logical-path substring or tag name.
        Name,
        /// The payload is a logical-path substring.
        Logical,
        /// The payload is a physical-path substring.
        Physical,
    }

    /// One parsed chunk of the query.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Chunk {
        pub kind: ChunkKind,
        pub text: String,
        pub negated: bool,
    }

    /// Lex a query string into [`Chunk`]s. See the module docs for the grammar
    /// and the error-recovery contract (this function is infallible; malformed
    /// input is silently dropped).
    pub fn lex_query(query: &str) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        let mut cursor = query;

        while !{
            cursor = cursor.trim_start();
            cursor.is_empty()
        } {
            let (maybe_chunk, rest) = lex_one_chunk(cursor);
            if let Some(chunk) = maybe_chunk {
                chunks.push(chunk);
            }
            cursor = rest;
        }
        chunks
    }

    /// Try to lex one chunk starting at `cursor` (which must be non-empty and
    /// not start with whitespace). Returns the parsed chunk (if any) and the
    /// remainder of the string to keep lexing.
    ///
    /// On any grammar error we return `(None, rest_after_next_whitespace)` —
    /// the whole in-progress chunk is discarded and lexing resumes at the next
    /// token boundary. An unterminated quote is treated as consuming the whole
    /// rest of the string (there is no whitespace boundary that could rescue
    /// half of a broken quote).
    fn lex_one_chunk(cursor: &str) -> (Option<Chunk>, &str) {
        let mut rest = cursor;
        let mut negated = false;
        let mut kind: Option<ChunkKind> = None;

        // Consume prefix tokens until we hit something
        // that isn't a prefix — that becomes the payload. On any grammar error
        // we drop the current chunk and resume at the *next* whitespace
        // boundary (the token that caused the error is itself skipped).
        loop {
            let token_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            let token = &rest[..token_end];

            match token {
                "!" => {
                    if negated {
                        return (None, &rest[token_end..]);
                    }
                    negated = true;
                }
                "/t" | "/n" | "/l" | "/p" => {
                    if kind.is_some() {
                        return (None, &rest[token_end..]);
                    }
                    kind = Some(match token {
                        "/t" => ChunkKind::Tag,
                        "/n" => ChunkKind::Name,
                        "/l" => ChunkKind::Logical,
                        "/p" => ChunkKind::Physical,
                        _ => unreachable!(),
                    });
                }
                _ => break, // not a prefix — treat as payload
            }

            // Advance past the prefix and its trailing whitespace.
            rest = rest[token_end..].trim_start();
            if rest.is_empty() {
                // Prefix with no following chunk: drop it.
                return (None, rest);
            }
        }

        // Read the payload: quoted string or bare token.
        let (text, rest) = if let Some(after_quote) = rest.strip_prefix('"') {
            match read_quoted(after_quote) {
                Some(parsed) => parsed,
                // Unterminated quote: discard the rest of the input entirely.
                None => return (None, ""),
            }
        } else {
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            (rest[..end].to_owned(), &rest[end..])
        };

        (
            Some(Chunk {
                kind: kind.unwrap_or(ChunkKind::Any),
                text,
                negated,
            }),
            rest,
        )
    }

    /// Read a `"..."`-quoted payload starting *after* the opening quote.
    /// Returns `Some((unescaped_text, remainder_after_closing_quote))`, or
    /// `None` if the closing quote is missing (unterminated string).
    fn read_quoted(input: &str) -> Option<(String, &str)> {
        let mut out = String::new();
        let mut chars = input.char_indices();
        while let Some((idx, ch)) = chars.next() {
            match ch {
                '"' => {
                    let rest = &input[idx + ch.len_utf8()..];
                    return Some((out, rest));
                }
                '\\' => match chars.next() {
                    Some((_, esc @ ('"' | '\\'))) => out.push(esc),
                    Some((_, other)) => {
                        // Unknown escape: keep the backslash + char verbatim.
                        out.push('\\');
                        out.push(other);
                    }
                    // Trailing backslash inside a quote: treat as unterminated.
                    None => return None,
                },
                _ => out.push(ch),
            }
        }
        None
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn any(text: &str) -> Chunk {
            Chunk {
                kind: ChunkKind::Any,
                text: text.to_owned(),
                negated: false,
            }
        }

        fn tag(text: &str) -> Chunk {
            Chunk {
                kind: ChunkKind::Tag,
                text: text.to_owned(),
                negated: false,
            }
        }

        fn logical(text: &str) -> Chunk {
            Chunk {
                kind: ChunkKind::Logical,
                text: text.to_owned(),
                negated: false,
            }
        }

        fn negate(mut chunk: Chunk) -> Chunk {
            chunk.negated = true;
            chunk
        }

        #[test]
        fn empty_and_whitespace_only_yield_no_chunks() {
            assert_eq!(lex_query(""), Vec::<Chunk>::new());
            assert_eq!(lex_query("   \t  "), Vec::<Chunk>::new());
        }

        #[test]
        fn bare_words_become_any_chunks() {
            assert_eq!(lex_query("foo bar"), vec![any("foo"), any("bar")]);
        }

        #[test]
        fn quoted_strings_capture_whitespace() {
            assert_eq!(lex_query(r#""foo bar" baz"#), vec![
                any("foo bar"),
                any("baz")
            ],);
        }

        #[test]
        fn quoted_string_supports_escapes() {
            assert_eq!(lex_query(r#""a\"b\\c""#), vec![any(r#"a"b\c"#)]);
        }

        #[test]
        fn unknown_backslash_escape_is_kept_literally() {
            // `\n` is not a recognised escape; we keep it verbatim rather than
            // silently interpreting it, to match the "quotes are only for
            // whitespace capture" contract.
            assert_eq!(lex_query(r#""a\nb""#), vec![any(r"a\nb")]);
        }

        #[test]
        fn kind_prefixes_apply_to_the_next_chunk() {
            assert_eq!(lex_query("/t foo"), vec![tag("foo")]);
            assert_eq!(lex_query("/l foo"), vec![logical("foo")]);
        }

        #[test]
        fn kind_prefix_only_matches_as_standalone_token() {
            // `/tfoo` is not a `/t` prefix — it's a literal chunk starting with `/`.
            assert_eq!(lex_query("/tfoo"), vec![any("/tfoo")]);
        }

        #[test]
        fn negation_alone_and_with_kind_prefix() {
            assert_eq!(lex_query("! foo"), vec![negate(any("foo"))]);
            assert_eq!(lex_query("! /t foo"), vec![negate(tag("foo"))]);
            // Order of `!` and `/t` doesn't matter.
            assert_eq!(lex_query("/t ! foo"), vec![negate(tag("foo"))]);
        }

        #[test]
        fn negation_applies_to_quoted_payload() {
            assert_eq!(lex_query(r#"! /t "foo bar""#), vec![negate(tag("foo bar"))],);
        }

        #[test]
        fn bang_without_space_is_literal_not_negation() {
            // `!foo` is a literal chunk whose text is `!foo`, matching the
            // "prefixes are standalone tokens" rule from the grammar.
            assert_eq!(lex_query("!foo"), vec![any("!foo")]);
        }

        #[test]
        fn unknown_slash_prefix_is_literal() {
            // `/x` isn't a known kind prefix, so it's just a chunk payload.
            // This keeps paths like `/home/lucas` searchable.
            assert_eq!(lex_query("/x foo"), vec![any("/x"), any("foo")]);
            assert_eq!(lex_query("/home/lucas"), vec![any("/home/lucas")]);
        }

        #[test]
        fn mixed_query_parses_end_to_end() {
            // A realistic mix: bare word, tag, quoted logical path, negated tag.
            let got = lex_query(r#"foo /t bar /l "my file.txt" ! /t old"#);
            assert_eq!(got, vec![
                any("foo"),
                tag("bar"),
                logical("my file.txt"),
                negate(tag("old")),
            ],);
        }

        // The lexer is infallible: it drops the current chunk-in-progress on
        // any grammar error and resumes at the next whitespace boundary. The
        // tests below pin down exactly what "resume" means for each error
        // shape.

        #[test]
        fn unterminated_quote_drops_rest_of_input() {
            // Prior chunks are kept; the broken quote and everything after it
            // are discarded (there is no whitespace *inside* the broken quote
            // that could rescue the remainder).
            assert_eq!(lex_query(r#"foo "bar baz"#), vec![any("foo")]);
            assert_eq!(lex_query(r#""foo"#), Vec::<Chunk>::new());
        }

        #[test]
        fn trailing_prefix_is_silently_dropped() {
            // A prefix with no payload (`!`, `/t`, `! /t` at EOF) yields no
            // chunk but doesn't affect chunks already parsed.
            assert_eq!(lex_query("foo !"), vec![any("foo")]);
            assert_eq!(lex_query("foo /t"), vec![any("foo")]);
            assert_eq!(lex_query("foo ! /t"), vec![any("foo")]);
            // Just the bad prefix on its own is an empty result, not an error.
            assert_eq!(lex_query("!"), Vec::<Chunk>::new());
            assert_eq!(lex_query("/t"), Vec::<Chunk>::new());
        }

        #[test]
        fn conflicting_kind_prefixes_drop_that_chunk_only() {
            // `/t /l` conflicts — that chunk-in-progress is discarded at the
            // conflict point, so lexing resumes with `foo bar` intact.
            assert_eq!(lex_query("/t /l foo bar"), vec![any("foo"), any("bar")],);
        }

        #[test]
        fn duplicate_negation_drops_that_chunk_only() {
            // `! !` is a duplicate — drop it, keep everything else.
            assert_eq!(lex_query("first ! ! second third"), vec![
                any("first"),
                any("second"),
                any("third")
            ],);
        }

        #[test]
        fn errors_between_valid_chunks_do_not_bleed() {
            // Interleave several error shapes with valid chunks to prove each
            // recovery is local.
            let got = lex_query(r#"a /t /l b ! ! c "unterminated"#);
            assert_eq!(got, vec![any("a"), any("b"), any("c")]);
        }
    }
}
