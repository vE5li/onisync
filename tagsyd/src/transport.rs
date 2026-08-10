//! Transport abstraction.
//!
//! The UI always talks to the same logical [API](crate::api). Only the
//! transport underneath differs:
//!
//! - **In-process** (Android, and optional single-process desktop): calls
//!   straight into [`Api`](crate::api::Api) / the change pipeline.
//! - **IPC-client** (Linux daemon mode): a thin embedded Rust client that
//!   connects to the daemon's control socket, serializes API calls, and returns
//!   results/events.
//!
//! This module defines the transport-agnostic surface as the
//! [`TransportBackend`] trait and provides the **in-process** implementation
//! ([`InProcessBackend`]). The IPC-client backend
//! ([`IpcClientBackend`](crate::control::IpcClientBackend)) lives in the
//! `control` module.
//!
//! `flutter_rust_bridge` always targets [`Backend`] on both platforms. On
//! Android it wraps the in-process backend; on Linux it will wrap the
//! IPC-client backend. The Dart UI never knows which — a single UI codebase is
//! preserved.
//!
//! ## Async surface
//!
//! Every method is `async`, even the reads which are synchronous on
//! [`Api`](crate::api::Api). This is deliberate: the IPC-client backend is
//! inherently asynchronous (a socket round-trip), so the shared trait must be
//! async for both. The in-process backend simply completes immediately.
//!
//! ## Dispatch
//!
//! [`Backend`] is an `enum` rather than a `dyn TransportBackend`. `async fn` in
//! traits is not yet dyn-compatible without extra machinery, and the set of
//! backends is small, closed, and known at compile time. The enum gives static
//! dispatch and lets the event stream carry a concrete, `Send` type across the
//! FFI boundary.

use std::future::Future;
use std::path::PathBuf;

use tagsy_core::state::Change;
use tagsy_core::{FileId, FileInfo, Preview, TagId};
use tokio::sync::broadcast;

use crate::api::{Api, ApiError, ApiEvent, EditOutcome, QueryResult, RetagSummary, TagRuleReport};
use crate::configuration::EditorRule;
use crate::operations::{Operation, OperationEvent};
use crate::store::{DeletedRule, SubtagRule, Tag};

/// The transport-agnostic UI-facing API.
///
/// This mirrors [`Api`](crate::api::Api) method-for-method, but every operation
/// is `async` so both the in-process backend (immediate) and the IPC-client
/// backend (socket round-trip) can implement it behind one surface.
///
/// Implemented by [`InProcessBackend`] and
/// [`IpcClientBackend`](crate::control::IpcClientBackend), and dispatched
/// through the [`Backend`] enum.
///
/// The returned futures are declared `+ Send` (rather than plain `async fn`)
/// so callers — notably `flutter_rust_bridge`, which spawns them on a
/// multi-threaded runtime — can move them across threads.
pub trait TransportBackend {
    /// Resolve a full-or-short file id `prefix` to a single [`FileId`]. Errors
    /// with `UnknownId` if nothing matches or `AmbiguousId` if several do.
    fn resolve_file_id(
        &self,
        prefix: String,
    ) -> impl Future<Output = Result<FileId, ApiError>> + Send;

    /// Resolve a full-or-short tag id `prefix` to a single [`TagId`]. Errors
    /// with `UnknownId` if nothing matches or `AmbiguousId` if several do.
    fn resolve_tag_id(
        &self,
        prefix: String,
    ) -> impl Future<Output = Result<TagId, ApiError>> + Send;

    /// List the tags applied to `file_id`.
    fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> impl Future<Output = Result<Vec<TagId>, ApiError>> + Send;

    /// Run a free-form query (`$tag`, `!tag`, and name substrings) and return
    /// both the matching files and tags. Tag tokens are resolved in the daemon.
    ///
    /// `deleted_rule` toggles the "show deleted rows" view; see
    /// [`Api::run_query`](crate::api::Api::run_query) for the exact semantics.
    fn run_query(
        &self,
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> impl Future<Output = Result<QueryResult, ApiError>> + Send;

    /// Get a single file's [`FileInfo`] by id (`UnknownId` if unknown).
    /// `deleted_rule` controls whether a tombstoned file reads as `UnknownId`
    /// or is returned with `FileInfo::deleted = true` (see
    /// [`Api::get_file`](crate::api::Api::get_file)).
    fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> impl Future<Output = Result<FileInfo, ApiError>> + Send;

    /// Get a single tag by id (`UnknownId` if unknown). See [`Self::get_file`]
    /// for the `deleted_rule` semantics.
    fn get_tag(
        &self,
        tag_id: TagId,
        deleted_rule: DeletedRule,
    ) -> impl Future<Output = Result<Tag, ApiError>> + Send;

    /// List the subtags (children) of `tag_id` in the tag hierarchy.
    fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> impl Future<Output = Result<Vec<TagId>, ApiError>> + Send;

    /// List the tags applied to `tag_id` (the tags it is a subtag of).
    fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> impl Future<Output = Result<Vec<TagId>, ApiError>> + Send;

    /// Create a tag; returns the freshly-minted id.
    fn create_tag(
        &self,
        name: String,
        color: String,
    ) -> impl Future<Output = Result<TagId, ApiError>> + Send;

    /// Delete a tag.
    fn delete_tag(&self, tag_id: TagId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Restore a soft-deleted tag.
    fn restore_tag(&self, tag_id: TagId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Rename a tag.
    fn rename_tag(
        &self,
        tag_id: TagId,
        name: String,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Change a tag's color.
    fn set_tag_color(
        &self,
        tag_id: TagId,
        color: String,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Upload a file from a path on disk; returns the freshly-minted id.
    ///
    /// The bytes are never buffered whole: the backend hashes `path` by
    /// streaming it and then serves the content chunk-by-chunk on demand (the
    /// IPC backend over the control socket; the in-process backend straight
    /// from disk). `path_name` is the file's logical identity; `path` is
    /// where the bytes currently live.
    fn upload_file(
        &self,
        path: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    ) -> impl Future<Output = Result<FileId, ApiError>> + Send;

    /// Replace the content of an existing file with the bytes at `path`, served
    /// on demand exactly like [`upload_file`](Self::upload_file).
    fn edit_file(
        &self,
        file_id: FileId,
        path: PathBuf,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Start an external edit: return the on-disk path the caller should hand
    /// to an editor.
    ///
    /// Two branches, transparent to the caller:
    ///
    /// - The file is present in a local sync directory → the returned path is
    ///   that real on-disk file, edited in place. The daemon's filesystem
    ///   watcher will pick up the save and propagate a `FileMetadataChanged` on
    ///   its own; [`finish_edit`](Self::finish_edit) is still called and acts
    ///   as a "did the bytes change vs. the current DB hash?" belt-and-braces
    ///   check.
    /// - Otherwise the daemon fetches the content (from a peer if needed) into
    ///   an isolated per-request subdirectory under
    ///   [`crate::paths::Paths::fetch_temp_dir`], named with the file's logical
    ///   basename so an external editor dispatches by extension correctly. Move
    ///   semantics: the caller must consume via
    ///   [`finish_edit`](Self::finish_edit) or
    ///   [`cancel_edit`](Self::cancel_edit).
    ///
    /// No daemon-side state is kept between `begin_edit` and
    /// `finish_edit`/`cancel_edit`; the caller's `file_id`+`path` fully
    /// describe the follow-up. A caller that crashes before finishing leaks
    /// only a temp file, which the daemon bulk-cleans on next start.
    fn begin_edit(&self, file_id: FileId)
    -> impl Future<Output = Result<PathBuf, ApiError>> + Send;

    /// Complete an in-flight external edit.
    ///
    /// `path` is the path returned by [`begin_edit`](Self::begin_edit) (the
    /// bytes at that path are the editor's output). The daemon re-hashes
    /// them, compares to the file's current recorded `content_hash`, and:
    ///
    /// - if equal → nothing to do (either the editor produced no change, or the
    ///   file was edited in place and the watcher already published the
    ///   change);
    /// - if different → publish a new version by streaming `path` to peers via
    ///   the same provider protocol as [`edit_file`](Self::edit_file).
    ///
    /// After that the daemon deletes `path` **only if it lives under**
    /// [`crate::paths::Paths::fetch_temp_dir`] (the isolated per-request
    /// subdirectory it created in `begin_edit`). Paths under sync
    /// directories, or anywhere else the caller may have staged bytes, are
    /// left untouched.
    fn finish_edit(
        &self,
        file_id: FileId,
        path: PathBuf,
    ) -> impl Future<Output = Result<EditOutcome, ApiError>> + Send;

    /// Abort an in-flight external edit without uploading.
    ///
    /// `path` is the path returned by [`begin_edit`](Self::begin_edit).
    /// Cleans up the daemon-owned temp exactly as
    /// [`finish_edit`](Self::finish_edit) does (delete iff under
    /// [`crate::paths::Paths::fetch_temp_dir`]).
    fn cancel_edit(&self, path: PathBuf) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Fetch a file's content on demand (from a peer if not present locally)
    /// and return the path to a temp file holding it. `expected_hash` gates
    /// which content is accepted.
    ///
    /// The path is handed to the caller with **move semantics**: it points at a
    /// daemon-owned temp file (both backends run co-located with the daemon and
    /// share its filesystem) that the caller must consume by renaming it into
    /// place or deleting it. The content is never buffered whole in memory.
    fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> impl Future<Output = Result<PathBuf, ApiError>> + Send;

    /// Get the preview for a file's current content (cached, generated locally,
    /// or fetched from a peer). [`Preview::None`] is a valid result.
    fn get_preview(
        &self,
        file_id: FileId,
    ) -> impl Future<Output = Result<Preview, ApiError>> + Send;

    /// Resolve a file's absolute on-disk path if present locally, else `None`.
    fn local_path_for_file(
        &self,
        file_id: FileId,
    ) -> impl Future<Output = Result<Option<PathBuf>, ApiError>> + Send;

    /// Delete a file.
    fn delete_file(&self, file_id: FileId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Restore a soft-deleted file (best-effort; fails if no source holds its
    /// bytes).
    fn restore_file(&self, file_id: FileId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Move (rename) a file to a new logical path.
    fn move_file(
        &self,
        file_id: FileId,
        logical_path: String,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Apply `tag_id` to `file_id`.
    fn tag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Remove `tag_id` from `file_id`.
    fn untag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Make `subtag_id` a subtag (child) of `parent_id`.
    fn tag_tag(
        &self,
        parent_id: TagId,
        subtag_id: TagId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Remove `subtag_id` as a subtag of `parent_id`.
    fn untag_tag(
        &self,
        parent_id: TagId,
        subtag_id: TagId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Purge the entire preview cache, returning how many cached previews were
    /// removed. Previews are hash-keyed and regenerated on demand, so this only
    /// forces re-evaluation on the next request.
    fn purge_previews(&self) -> impl Future<Output = Result<usize, ApiError>> + Send;

    /// The daemon's configured external-editor rules (see
    /// [`crate::configuration::EditorRule`]). A snapshot read; the desktop UI
    /// calls this once when preparing to launch an editor.
    fn editor_rules(&self) -> impl Future<Output = Result<Vec<EditorRule>, ApiError>> + Send;

    /// Re-apply the configured tag rules to files already in the catalog,
    /// additively. With `dry_run` the work is planned and reported but nothing
    /// is enqueued. See [`crate::api::Api::retag`].
    fn retag(&self, dry_run: bool) -> impl Future<Output = Result<RetagSummary, ApiError>> + Send;

    /// Diagnose the configured tag rules (invalid patterns, unknown tag ids).
    /// See [`crate::api::Api::tag_rule_report`].
    fn tag_rule_report(&self) -> impl Future<Output = Result<TagRuleReport, ApiError>> + Send;

    /// Subscribe to the live change stream. Returns an [`EventStream`] whose
    /// [`recv`](EventStream::recv) yields [`ApiEvent`]s.
    fn subscribe(&self) -> EventStream;

    /// Snapshot every currently-active sync operation (peer transfers,
    /// reconciliation, fetches, ...). The read the UI issues for its initial
    /// paint before applying live [`OperationEvent`]s from
    /// [`subscribe_operations`](Self::subscribe_operations).
    fn list_operations(&self) -> impl Future<Output = Result<Vec<Operation>, ApiError>> + Send;

    /// Subscribe to the live sync-operation stream. Returns an
    /// [`OperationStream`] whose [`recv`](OperationStream::recv) yields
    /// [`OperationUpdate`]s.
    fn subscribe_operations(&self) -> OperationStream;
}

/// The transport-agnostic event stream returned by
/// [`TransportBackend::subscribe`].
///
/// It normalizes the two delivery mechanisms behind one type so the UI (and
/// `flutter_rust_bridge`) sees a single stream shape regardless of transport.
/// Poll it with [`EventStream::recv`].
pub enum EventStream {
    /// In-process delivery: a direct subscription to the runtime's broadcast
    /// bus. Each item is a raw [`Change`] the runtime applied; [`recv`] wraps
    /// it in [`ApiEvent::Changed`].
    InProcess(broadcast::Receiver<Change>),
    /// IPC delivery (section 7): a subscription to the control client's
    /// broadcast of [`ApiEvent`]s decoded off the control socket. The
    /// [`ApiEvent`]s are already fully-formed (the daemon sends
    /// [`ApiEvent::Changed`] per change; a reconnecting client would receive
    /// [`ApiEvent::Resynced`]).
    Ipc(broadcast::Receiver<ApiEvent>),
}

impl EventStream {
    /// Await the next event.
    ///
    /// Returns:
    /// - `Some(ApiEvent::Changed(_))` for each applied change,
    /// - `Some(ApiEvent::Resynced)` when the subscriber lagged past the channel
    ///   capacity (the UI should re-fetch state), and
    /// - `None` once the stream is permanently closed (runtime shut down).
    pub async fn recv(&mut self) -> Option<ApiEvent> {
        match self {
            EventStream::InProcess(receiver) => match receiver.recv().await {
                Ok(change) => Some(ApiEvent::Changed(change)),
                // A slow subscriber fell behind: surface a resync request so
                // the UI re-fetches current state rather than silently
                // dropping changes.
                Err(broadcast::error::RecvError::Lagged(_)) => Some(ApiEvent::Resynced),
                // Sender dropped: the runtime is gone, the stream is done.
                Err(broadcast::error::RecvError::Closed) => None,
            },
            EventStream::Ipc(receiver) => match receiver.recv().await {
                // Already-decoded `ApiEvent`s arrive off the control socket.
                Ok(event) => Some(event),
                // The local client fell behind the daemon's event feed: same
                // remedy as in-process — ask the UI to re-fetch state.
                Err(broadcast::error::RecvError::Lagged(_)) => Some(ApiEvent::Resynced),
                // The control connection dropped (reader task ended).
                Err(broadcast::error::RecvError::Closed) => None,
            },
        }
    }
}

/// A live update on the operation stream, normalized across transports.
///
/// Mirrors the [`ApiEvent`] shape for the change stream: an in-process or IPC
/// subscriber that lags past the channel capacity gets a
/// [`Resynced`](OperationUpdate::Resynced) prompt to re-snapshot via
/// [`list_operations`](TransportBackend::list_operations) rather than silently
/// dropping updates.
#[derive(Debug, Clone)]
pub enum OperationUpdate {
    /// The stream lagged (or reconnected over IPC); the UI should re-snapshot.
    Resynced,
    /// A concrete operation event (started / progress / terminal).
    Event(OperationEvent),
}

/// The transport-agnostic operation stream returned by
/// [`TransportBackend::subscribe_operations`].
///
/// The operation counterpart of [`EventStream`]; same two delivery mechanisms
/// behind one type. Poll it with [`OperationStream::recv`].
pub enum OperationStream {
    /// In-process delivery: a direct subscription to the runtime's operation
    /// broadcast.
    InProcess(broadcast::Receiver<OperationEvent>),
    /// IPC delivery: a subscription to the control client's broadcast of
    /// operation events decoded off the control socket.
    Ipc(broadcast::Receiver<OperationEvent>),
}

impl OperationStream {
    /// Await the next operation update.
    ///
    /// Returns `Some(OperationUpdate::Event(_))` per operation event,
    /// `Some(OperationUpdate::Resynced)` when the subscriber lagged
    /// (re-snapshot needed), and `None` once the stream is permanently
    /// closed.
    pub async fn recv(&mut self) -> Option<OperationUpdate> {
        let receiver = match self {
            OperationStream::InProcess(receiver) | OperationStream::Ipc(receiver) => receiver,
        };
        match receiver.recv().await {
            Ok(event) => Some(OperationUpdate::Event(event)),
            Err(broadcast::error::RecvError::Lagged(_)) => Some(OperationUpdate::Resynced),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}

/// In-process transport backend.
///
/// Thinnest possible wrapper over [`Api`](crate::api::Api): every call
/// delegates directly, completing immediately. Used on Android (single
/// process) and for single-process desktop.
///
/// The wrapped reads perform blocking SQLite work; in-process that is
/// acceptable because each read opens and drops its own short-lived read-only
/// handle (see [`Api`](crate::api::Api) docs) and does not hold it across an
/// `.await`.
#[derive(Clone)]
pub struct InProcessBackend {
    api: Api,
}

impl InProcessBackend {
    /// Wrap an [`Api`](crate::api::Api) handle produced by
    /// [`run`](crate::run).
    pub fn new(api: Api) -> Self {
        Self { api }
    }

    /// Borrow the underlying [`Api`](crate::api::Api).
    pub fn api(&self) -> &Api {
        &self.api
    }
}

impl TransportBackend for InProcessBackend {
    async fn resolve_file_id(&self, prefix: String) -> Result<FileId, ApiError> {
        self.api.resolve_file_id(&prefix)
    }

    async fn resolve_tag_id(&self, prefix: String) -> Result<TagId, ApiError> {
        self.api.resolve_tag_id(&prefix)
    }

    async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        self.api.tags_for_file(file_id, subtag_rule)
    }

    async fn run_query(
        &self,
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<QueryResult, ApiError> {
        self.api.run_query(&query, subtag_rule, deleted_rule)
    }

    async fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, ApiError> {
        self.api.get_file(file_id, deleted_rule)
    }

    async fn get_tag(&self, tag_id: TagId, deleted_rule: DeletedRule) -> Result<Tag, ApiError> {
        self.api.get_tag(tag_id, deleted_rule)
    }

    async fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        self.api.subtags_for_tag(tag_id, subtag_rule)
    }

    async fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        self.api.tags_for_tag(tag_id, subtag_rule)
    }

    async fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        self.api.create_tag(name, color)
    }

    async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.api.delete_tag(tag_id)
    }

    async fn restore_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.api.restore_tag(tag_id)
    }

    async fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        self.api.rename_tag(tag_id, name)
    }

    async fn set_tag_color(&self, tag_id: TagId, color: String) -> Result<(), ApiError> {
        self.api.set_tag_color(tag_id, color)
    }

    async fn upload_file(
        &self,
        path: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        // Hash by streaming the file, announce the upload, then register the
        // on-disk path as a `FileToCopy` chunk provider so peers pull the bytes
        // on demand straight from disk (never buffering the whole file). This is
        // the same provider mechanism the IPC/CLI path uses, sourced from the
        // local filesystem instead of the control socket.
        let (content_hash, size) = crate::file_bytes::hash_and_len(&path).await?;
        let source = crate::file_bytes::FileBytes::FileToCopy(path);
        let file_id = self
            .api
            .upload_file(path_name, content_hash.clone(), size, tags)?;
        self.api
            .register_provider(file_id, content_hash, std::sync::Arc::new(source))
            .await;
        Ok(file_id)
    }

    async fn edit_file(&self, file_id: FileId, path: PathBuf) -> Result<(), ApiError> {
        let (content_hash, size) = crate::file_bytes::hash_and_len(&path).await?;
        let source = crate::file_bytes::FileBytes::FileToCopy(path);
        self.api.edit_file(file_id, content_hash.clone(), size)?;
        self.api
            .register_provider(file_id, content_hash, std::sync::Arc::new(source))
            .await;
        Ok(())
    }

    async fn begin_edit(&self, file_id: FileId) -> Result<PathBuf, ApiError> {
        self.api.begin_edit(file_id).await
    }

    async fn finish_edit(&self, file_id: FileId, path: PathBuf) -> Result<EditOutcome, ApiError> {
        self.api.finish_edit(file_id, path).await
    }

    async fn cancel_edit(&self, path: PathBuf) -> Result<(), ApiError> {
        self.api.cancel_edit(path)
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        self.api.fetch_file(file_id, expected_hash).await
    }

    async fn get_preview(&self, file_id: FileId) -> Result<Preview, ApiError> {
        self.api.get_preview(file_id).await
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        self.api.local_path_for_file(file_id).await
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.api.delete_file(file_id)
    }

    async fn restore_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.api.restore_file(file_id).await
    }

    async fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        self.api.move_file(file_id, logical_path)
    }

    async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.api.tag_file(tag_id, file_id)
    }

    async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.api.untag_file(tag_id, file_id)
    }

    async fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        self.api.tag_tag(parent_id, subtag_id)
    }

    async fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        self.api.untag_tag(parent_id, subtag_id)
    }

    async fn purge_previews(&self) -> Result<usize, ApiError> {
        self.api.purge_previews().await
    }

    async fn editor_rules(&self) -> Result<Vec<EditorRule>, ApiError> {
        Ok(self.api.editor_rules())
    }

    async fn retag(&self, dry_run: bool) -> Result<RetagSummary, ApiError> {
        self.api.retag(dry_run)
    }

    async fn tag_rule_report(&self) -> Result<TagRuleReport, ApiError> {
        self.api.tag_rule_report()
    }

    fn subscribe(&self) -> EventStream {
        EventStream::InProcess(self.api.subscribe())
    }

    async fn list_operations(&self) -> Result<Vec<Operation>, ApiError> {
        Ok(self.api.list_operations())
    }

    fn subscribe_operations(&self) -> OperationStream {
        OperationStream::InProcess(self.api.subscribe_operations())
    }
}

/// The transport-agnostic handle `flutter_rust_bridge` targets on every
/// platform.
///
/// An `enum` over the concrete backends, forwarding the whole
/// [`TransportBackend`] surface to whichever variant is present. The Dart UI
/// holds one `Backend` and never learns which transport backs it.
///
/// [`Backend::InProcess`] is used on Android / single-process desktop;
/// [`Backend::Ipc`] connects to the daemon control socket on the Linux daemon
/// topology.
#[derive(Clone)]
pub enum Backend {
    /// In-process backend (Android / single-process desktop).
    InProcess(InProcessBackend),
    /// IPC-client backend talking to the daemon control socket.
    Ipc(crate::control::IpcClientBackend),
}

impl Backend {
    /// Build an in-process backend from an [`Api`](crate::api::Api) handle.
    pub fn in_process(api: Api) -> Self {
        Backend::InProcess(InProcessBackend::new(api))
    }

    /// Connect an IPC-client backend to the daemon's default control socket
    /// (section 7).
    pub async fn ipc_default() -> Result<Self, ApiError> {
        Ok(Backend::Ipc(
            crate::control::IpcClientBackend::connect_default().await?,
        ))
    }
}

impl TransportBackend for Backend {
    async fn resolve_file_id(&self, prefix: String) -> Result<FileId, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.resolve_file_id(prefix).await,
            Backend::Ipc(backend) => backend.resolve_file_id(prefix).await,
        }
    }

    async fn resolve_tag_id(&self, prefix: String) -> Result<TagId, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.resolve_tag_id(prefix).await,
            Backend::Ipc(backend) => backend.resolve_tag_id(prefix).await,
        }
    }

    async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.tags_for_file(file_id, subtag_rule).await,
            Backend::Ipc(backend) => backend.tags_for_file(file_id, subtag_rule).await,
        }
    }

    async fn run_query(
        &self,
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<QueryResult, ApiError> {
        match self {
            Backend::InProcess(backend) => {
                backend.run_query(query, subtag_rule, deleted_rule).await
            }
            Backend::Ipc(backend) => backend.run_query(query, subtag_rule, deleted_rule).await,
        }
    }

    async fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.get_file(file_id, deleted_rule).await,
            Backend::Ipc(backend) => backend.get_file(file_id, deleted_rule).await,
        }
    }

    async fn get_tag(&self, tag_id: TagId, deleted_rule: DeletedRule) -> Result<Tag, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.get_tag(tag_id, deleted_rule).await,
            Backend::Ipc(backend) => backend.get_tag(tag_id, deleted_rule).await,
        }
    }

    async fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.subtags_for_tag(tag_id, subtag_rule).await,
            Backend::Ipc(backend) => backend.subtags_for_tag(tag_id, subtag_rule).await,
        }
    }

    async fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.tags_for_tag(tag_id, subtag_rule).await,
            Backend::Ipc(backend) => backend.tags_for_tag(tag_id, subtag_rule).await,
        }
    }

    async fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.create_tag(name, color).await,
            Backend::Ipc(backend) => backend.create_tag(name, color).await,
        }
    }

    async fn restore_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.restore_tag(tag_id).await,
            Backend::Ipc(backend) => backend.restore_tag(tag_id).await,
        }
    }

    async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.delete_tag(tag_id).await,
            Backend::Ipc(backend) => backend.delete_tag(tag_id).await,
        }
    }

    async fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.rename_tag(tag_id, name).await,
            Backend::Ipc(backend) => backend.rename_tag(tag_id, name).await,
        }
    }

    async fn set_tag_color(&self, tag_id: TagId, color: String) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.set_tag_color(tag_id, color).await,
            Backend::Ipc(backend) => backend.set_tag_color(tag_id, color).await,
        }
    }

    async fn upload_file(
        &self,
        path: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.upload_file(path, path_name, tags).await,
            Backend::Ipc(backend) => backend.upload_file(path, path_name, tags).await,
        }
    }

    async fn edit_file(&self, file_id: FileId, path: PathBuf) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.edit_file(file_id, path).await,
            Backend::Ipc(backend) => backend.edit_file(file_id, path).await,
        }
    }

    async fn begin_edit(&self, file_id: FileId) -> Result<PathBuf, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.begin_edit(file_id).await,
            Backend::Ipc(backend) => backend.begin_edit(file_id).await,
        }
    }

    async fn finish_edit(&self, file_id: FileId, path: PathBuf) -> Result<EditOutcome, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.finish_edit(file_id, path).await,
            Backend::Ipc(backend) => backend.finish_edit(file_id, path).await,
        }
    }

    async fn cancel_edit(&self, path: PathBuf) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.cancel_edit(path).await,
            Backend::Ipc(backend) => backend.cancel_edit(path).await,
        }
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.fetch_file(file_id, expected_hash).await,
            Backend::Ipc(backend) => backend.fetch_file(file_id, expected_hash).await,
        }
    }

    async fn get_preview(&self, file_id: FileId) -> Result<Preview, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.get_preview(file_id).await,
            Backend::Ipc(backend) => backend.get_preview(file_id).await,
        }
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.local_path_for_file(file_id).await,
            Backend::Ipc(backend) => backend.local_path_for_file(file_id).await,
        }
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.delete_file(file_id).await,
            Backend::Ipc(backend) => backend.delete_file(file_id).await,
        }
    }

    async fn restore_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.restore_file(file_id).await,
            Backend::Ipc(backend) => backend.restore_file(file_id).await,
        }
    }

    async fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.move_file(file_id, logical_path).await,
            Backend::Ipc(backend) => backend.move_file(file_id, logical_path).await,
        }
    }

    async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.tag_file(tag_id, file_id).await,
            Backend::Ipc(backend) => backend.tag_file(tag_id, file_id).await,
        }
    }

    async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.untag_file(tag_id, file_id).await,
            Backend::Ipc(backend) => backend.untag_file(tag_id, file_id).await,
        }
    }

    async fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.tag_tag(parent_id, subtag_id).await,
            Backend::Ipc(backend) => backend.tag_tag(parent_id, subtag_id).await,
        }
    }

    async fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self {
            Backend::InProcess(backend) => backend.untag_tag(parent_id, subtag_id).await,
            Backend::Ipc(backend) => backend.untag_tag(parent_id, subtag_id).await,
        }
    }

    async fn purge_previews(&self) -> Result<usize, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.purge_previews().await,
            Backend::Ipc(backend) => backend.purge_previews().await,
        }
    }

    async fn editor_rules(&self) -> Result<Vec<EditorRule>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.editor_rules().await,
            Backend::Ipc(backend) => backend.editor_rules().await,
        }
    }

    async fn retag(&self, dry_run: bool) -> Result<RetagSummary, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.retag(dry_run).await,
            Backend::Ipc(backend) => backend.retag(dry_run).await,
        }
    }

    async fn tag_rule_report(&self) -> Result<TagRuleReport, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.tag_rule_report().await,
            Backend::Ipc(backend) => backend.tag_rule_report().await,
        }
    }

    fn subscribe(&self) -> EventStream {
        match self {
            Backend::InProcess(backend) => backend.subscribe(),
            Backend::Ipc(backend) => backend.subscribe(),
        }
    }

    async fn list_operations(&self) -> Result<Vec<Operation>, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.list_operations().await,
            Backend::Ipc(backend) => backend.list_operations().await,
        }
    }

    fn subscribe_operations(&self) -> OperationStream {
        match self {
            Backend::InProcess(backend) => backend.subscribe_operations(),
            Backend::Ipc(backend) => backend.subscribe_operations(),
        }
    }
}
