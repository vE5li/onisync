//! Transport abstraction (portability plan section 6).
//!
//! The UI always talks to the same logical [section-5 API](crate::api). Only
//! the transport underneath differs:
//!
//! - **In-process** (Android, and optional single-process desktop): calls
//!   straight into [`Api`](crate::api::Api) / the change pipeline.
//! - **IPC-client** (Linux daemon mode): a thin embedded Rust client that
//!   connects to the daemon's control socket (section 7), serialises API calls,
//!   and returns results/events.
//!
//! This module defines the transport-agnostic surface as the
//! [`TransportBackend`] trait and provides the **in-process** implementation
//! ([`InProcessBackend`]). The IPC-client backend is deferred to section 7
//! (the daemon control socket it would talk to does not exist yet); see the
//! `Backend::Ipc` note below.
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

use onisync_core::state::Change;
use onisync_core::{FileId, FileInfo, TagId};
use tokio::sync::broadcast;

use crate::api::{Api, ApiError, ApiEvent, QueryResult};
use crate::database::{SubtagRule, Tag};

/// The transport-agnostic UI-facing API (portability plan section 6).
///
/// This mirrors [`Api`](crate::api::Api) method-for-method, but every operation
/// is `async` so both the in-process backend (immediate) and the future
/// IPC-client backend (socket round-trip) can implement it behind one surface.
///
/// Implemented by [`InProcessBackend`] today and dispatched through the
/// [`Backend`] enum.
///
/// The returned futures are declared `+ Send` (rather than plain `async fn`)
/// so callers — notably `flutter_rust_bridge`, which spawns them on a
/// multi-threaded runtime — can move them across threads.
pub trait TransportBackend {
    // --- Read API (plan 5.3) -------------------------------------------------

    /// Resolve a full-or-short file id `prefix` to a single [`FileId`]. Errors
    /// with `NotFound` if nothing matches or `Ambiguous` if several do.
    fn resolve_file_id(
        &self,
        prefix: String,
    ) -> impl Future<Output = Result<FileId, ApiError>> + Send;

    /// Resolve a full-or-short tag id `prefix` to a single [`TagId`]. Errors
    /// with `NotFound` if nothing matches or `Ambiguous` if several do.
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
    fn run_query(
        &self,
        query: String,
        subtag_rule: SubtagRule,
    ) -> impl Future<Output = Result<QueryResult, ApiError>> + Send;

    /// Get a single file's [`FileInfo`] by id (`NotFound` if unknown).
    fn get_file(&self, file_id: FileId) -> impl Future<Output = Result<FileInfo, ApiError>> + Send;

    /// Get a single tag by id (`NotFound` if unknown).
    fn get_tag(&self, tag_id: TagId) -> impl Future<Output = Result<Tag, ApiError>> + Send;

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

    // --- Write API (plan 5.4) ------------------------------------------------

    /// Create a tag; returns the freshly-minted id.
    fn create_tag(
        &self,
        name: String,
        color: String,
    ) -> impl Future<Output = Result<TagId, ApiError>> + Send;

    /// Delete a tag.
    fn delete_tag(&self, tag_id: TagId) -> impl Future<Output = Result<(), ApiError>> + Send;

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

    /// Resolve a file's absolute on-disk path if present locally, else `None`.
    fn local_path_for_file(
        &self,
        file_id: FileId,
    ) -> impl Future<Output = Result<Option<PathBuf>, ApiError>> + Send;

    /// Delete a file.
    fn delete_file(&self, file_id: FileId) -> impl Future<Output = Result<(), ApiError>> + Send;

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

    // --- Event stream (plan 5.5) ---------------------------------------------

    /// Subscribe to the live change stream. Returns an [`EventStream`] whose
    /// [`recv`](EventStream::recv) yields [`ApiEvent`]s.
    fn subscribe(&self) -> EventStream;
}

/// The transport-agnostic event stream returned by
/// [`TransportBackend::subscribe`].
///
/// It normalises the two delivery mechanisms behind one type so the UI (and
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
    ///   capacity (plan 5.5: the UI should re-fetch state), and
    /// - `None` once the stream is permanently closed (runtime shut down).
    pub async fn recv(&mut self) -> Option<ApiEvent> {
        match self {
            EventStream::InProcess(receiver) => match receiver.recv().await {
                Ok(change) => Some(ApiEvent::Changed(change)),
                // A slow subscriber fell behind: surface a resync request so
                // the UI re-fetches current state (plan 5.5) rather than
                // silently dropping changes.
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

/// In-process transport backend (plan section 6).
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

/// Map a [`FileBytes::hash`](crate::file_bytes::FileBytes::hash) failure (an
/// I/O error reading the local upload source) into an [`ApiError`].
fn hash_error(error: crate::file_bytes::FileBytesError) -> ApiError {
    ApiError::Transport(format!("hashing upload source: {error}"))
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
    ) -> Result<QueryResult, ApiError> {
        self.api.run_query(&query, subtag_rule)
    }

    async fn get_file(&self, file_id: FileId) -> Result<FileInfo, ApiError> {
        self.api.get_file(file_id)
    }

    async fn get_tag(&self, tag_id: TagId) -> Result<Tag, ApiError> {
        self.api.get_tag(tag_id)
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
        let source = crate::file_bytes::FileBytes::FileToCopy(path);
        let content_hash = source.hash().await.map_err(hash_error)?;
        let size = source.byte_len().await.map_err(hash_error)?;
        let file_id = self
            .api
            .upload_file(path_name, content_hash.clone(), size, tags)?;
        self.api
            .register_provider(file_id, content_hash, std::sync::Arc::new(source))
            .await;
        Ok(file_id)
    }

    async fn edit_file(&self, file_id: FileId, path: PathBuf) -> Result<(), ApiError> {
        let source = crate::file_bytes::FileBytes::FileToCopy(path);
        let content_hash = source.hash().await.map_err(hash_error)?;
        let size = source.byte_len().await.map_err(hash_error)?;
        self.api.edit_file(file_id, content_hash.clone(), size)?;
        self.api
            .register_provider(file_id, content_hash, std::sync::Arc::new(source))
            .await;
        Ok(())
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        self.api.fetch_file(file_id, expected_hash).await
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        self.api.local_path_for_file(file_id).await
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.api.delete_file(file_id)
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

    fn subscribe(&self) -> EventStream {
        EventStream::InProcess(self.api.subscribe())
    }
}

/// The transport-agnostic handle `flutter_rust_bridge` targets on every
/// platform (plan section 6).
///
/// An `enum` over the concrete backends, forwarding the whole
/// [`TransportBackend`] surface to whichever variant is present. The Dart UI
/// holds one `Backend` and never learns which transport backs it.
///
/// [`Backend::InProcess`] is used on Android / single-process desktop;
/// [`Backend::Ipc`] connects to the daemon control socket (section 7) on the
/// Linux daemon topology.
#[derive(Clone)]
pub enum Backend {
    /// In-process backend (Android / single-process desktop).
    InProcess(InProcessBackend),
    /// IPC-client backend talking to the daemon control socket (section 7).
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
    ) -> Result<QueryResult, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.run_query(query, subtag_rule).await,
            Backend::Ipc(backend) => backend.run_query(query, subtag_rule).await,
        }
    }

    async fn get_file(&self, file_id: FileId) -> Result<FileInfo, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.get_file(file_id).await,
            Backend::Ipc(backend) => backend.get_file(file_id).await,
        }
    }

    async fn get_tag(&self, tag_id: TagId) -> Result<Tag, ApiError> {
        match self {
            Backend::InProcess(backend) => backend.get_tag(tag_id).await,
            Backend::Ipc(backend) => backend.get_tag(tag_id).await,
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

    fn subscribe(&self) -> EventStream {
        match self {
            Backend::InProcess(backend) => backend.subscribe(),
            Backend::Ipc(backend) => backend.subscribe(),
        }
    }
}
