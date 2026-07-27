//! Dart-facing API surface (portability plan sections 5, 6, 8).
//!
//! This is the thin layer `flutter_rust_bridge` generates Dart bindings for. It
//! deliberately adds **no** business logic: it owns a [`RuntimeHandle`] and
//! forwards every call to the section-6 [`Backend`], which forwards to the
//! section-5 [`Api`](onisyncd::api::Api). The Dart UI holds one [`OniSyncApp`]
//! and never learns which transport backs it.
//!
//! The `#[flutter_rust_bridge::frb]` annotations are applied only when the
//! `flutter_rust_bridge` feature is enabled (via `cfg_attr`), so the crate
//! still compiles — and `cargo check` still passes — without the generator's
//! dependency present.

// Re-exported (not just `use`d) so the types appearing in this module's
// `#[frb]`-annotated signatures are reachable via `crate::api::*` — which is
// exactly how `flutter_rust_bridge_codegen` references them in the generated
// `frb_generated.rs`. A plain private `use` would not be visible through that
// glob and the generated code fails to compile.
pub use onisync_core::{FileId, FileInfo, TagId};
pub use onisyncd::api::{ApiError, ApiEvent};
pub use onisyncd::database::{SubtagRule, Tag};
use onisyncd::paths::Paths;
use onisyncd::transport::{Backend, EventStream, TransportBackend};
use tokio::sync::Mutex;

use crate::runtime::StartError;

/// A file flattened into primitive fields for the Dart UI.
///
/// The core [`FileInfo`] is crossed to Dart as an *opaque* handle (frb cannot
/// see inside external-crate structs), so its fields are unreadable from Dart.
/// This DTO — defined in the bridge crate with plain `String`/`i64` fields —
/// is generated as a real Dart class the UI can display directly. Ids are
/// rendered as their UUID strings.
pub struct FileEntry {
    pub file_id: String,
    pub path: String,
    pub content_hash: String,
    pub version_number: i64,
    /// The latest version's content size in bytes.
    pub size: i64,
    /// Number of leading characters of `file_id` that uniquely identify this
    /// file among all files in the listing — the "short id" length, à la
    /// `jj`/`git`. The UI highlights `file_id[..short_id_length]` and dims the
    /// rest. Computed on read; not stable across concurrent inserts.
    pub short_id_length: i64,
}

impl From<FileInfo> for FileEntry {
    fn from(info: FileInfo) -> Self {
        Self {
            file_id: info.file_id.to_string(),
            path: info.logical_path.into_string(),
            content_hash: info.content_hash,
            version_number: info.version_number,
            size: info.size as i64,
            short_id_length: info.short_id_length as i64,
        }
    }
}

/// Mirror of [`SubtagRule`] so `flutter_rust_bridge` generates it as a real
/// Dart *enum* (not an opaque handle), letting the UI construct and pass it.
///
/// `SubtagRule` is defined in `onisyncd` (a foreign crate), so frb cannot see
/// its variants to generate an enum directly; the `frb(mirror(...))` attribute
/// re-declares the same shape here and tells frb to treat the foreign type as
/// this enum. The variants MUST stay in sync with
/// `onisyncd::database::SubtagRule`.
///
/// Semantics (see `FileDatabase::file_ids_for_tag`):
///   * `Include` — recurse into subtags (files carrying this tag *or* any of
///     its transitive subtags),
///   * `Exclude` — direct members only (no subtag recursion).
#[cfg(feature = "flutter_rust_bridge")]
#[flutter_rust_bridge::frb(mirror(SubtagRule))]
pub enum _SubtagRule {
    Include,
    Exclude,
}

/// A tag flattened into primitive fields for the Dart UI (see [`FileEntry`]).
pub struct TagEntry {
    pub tag_id: String,
    pub name: String,
    pub color: String,
}

impl From<Tag> for TagEntry {
    fn from(tag: Tag) -> Self {
        Self {
            tag_id: tag.id.to_string(),
            name: tag.name,
            color: tag.color,
        }
    }
}

/// The result of [`OniSyncApp::run_query`] as the flattened [`FileEntry`] /
/// [`TagEntry`] rows the Dart UI renders directly.
///
/// The daemon's [`QueryResult`] carries full `FileInfo`/`Tag` rows for the
/// matched set, so the UI gets everything it needs in one call — no follow-up
/// listing to turn ids into displayable rows.
pub struct QueryEntries {
    pub files: Vec<FileEntry>,
    pub tags: Vec<TagEntry>,
}

/// The handle the Dart UI holds while it is open.
///
/// It does **not** own the runtime. The runtime is a process-global owned by
/// the Android foreground service (see [`crate::service`]) so it survives the
/// UI closing. This handle just reads/writes through that global's
/// [`Backend`]; dropping it (UI closed) leaves the runtime running.
#[cfg_attr(feature = "flutter_rust_bridge", flutter_rust_bridge::frb(opaque))]
pub struct OniSyncApp {
    _private: (),
}

impl OniSyncApp {
    /// Attach to the sync runtime, starting it if it is not already running.
    ///
    /// Normally the foreground service has already started the process-global
    /// runtime (over JNI) by the time the UI opens, in which case this just
    /// attaches. If it is not running yet (e.g. the service is slow, or during
    /// development), this starts it. Either way the runtime keeps running after
    /// this handle is dropped.
    ///
    /// Parses `configuration_json` with
    /// [`Configuration::from_str`](onisyncd::configuration::Configuration::from_str)
    /// and initialises log routing (logcat on Android). Blocks only until the
    /// engine is ready or startup fails.
    #[cfg_attr(feature = "flutter_rust_bridge", flutter_rust_bridge::frb(sync))]
    pub fn start(
        configuration_json: String,
        data_dir: String,
        identity_file: String,
    ) -> Result<OniSyncApp, StartError> {
        crate::service::start(&configuration_json, Paths::new(data_dir, identity_file))?;

        Ok(OniSyncApp { _private: () })
    }

    /// Attach to an already-running onisync daemon over IPC (Linux desktop
    /// topology, plan sections 6-7).
    ///
    /// Unlike [`start`](OniSyncApp::start), this process does **not** own the
    /// sync engine or the database — the systemd daemon does. This opens a
    /// connection to the daemon's control socket (`/run/onisync/onisync.sock`)
    /// and returns a handle that reads/writes through the daemon. No
    /// configuration, data directory, or identity is needed here: they all
    /// belong to the daemon.
    ///
    /// Fails with a transport error if the daemon is not running (the control
    /// socket is absent or refuses the connection).
    pub async fn attach() -> Result<OniSyncApp, StartError> {
        crate::service::attach().await?;
        Ok(OniSyncApp { _private: () })
    }

    /// The backend of the process-global runtime.
    ///
    /// Panics only if the runtime was stopped out from under an open UI, which
    /// should not happen (the service outlives the UI). Callers surface a
    /// transport error instead of unwrapping in that unlikely race.
    fn try_backend(&self) -> Result<Backend, ApiError> {
        crate::service::backend()
            .ok_or_else(|| ApiError::Transport("sync runtime is not running".to_owned()))
    }

    /// This device's base64 ed25519 public key.
    ///
    /// The value a peer must add to its own config to pair with this device.
    /// Synchronous: it is known as soon as the runtime has started. Empty if
    /// the runtime is somehow not running.
    #[cfg_attr(feature = "flutter_rust_bridge", flutter_rust_bridge::frb(sync))]
    pub fn public_key(&self) -> String {
        crate::service::public_key().unwrap_or_default()
    }

    // --- Read API (plan 5.3) -------------------------------------------------

    /// Resolve a full-or-short file id `prefix` `short_id_length`) to a single
    /// [`FileId`]. Errors with `NotFound` if nothing matches or `Ambiguous` if
    /// several do.
    pub async fn resolve_file_id(&self, prefix: String) -> Result<FileId, ApiError> {
        self.try_backend()?.resolve_file_id(prefix).await
    }

    /// Resolve a full-or-short tag id `prefix` to a single [`TagId`]. Errors
    /// with `NotFound` if nothing matches or `Ambiguous` if several do. The
    /// tag counterpart of [`resolve_file_id`].
    ///
    /// This is how the Dart UI turns a `TagEntry.tag_id` string back into the
    /// opaque [`TagId`] that the write/query methods (`delete_tag`, `tag_file`,
    /// `untag_file`) require.
    pub async fn resolve_tag_id(&self, prefix: String) -> Result<TagId, ApiError> {
        self.try_backend()?.resolve_tag_id(prefix).await
    }

    /// List the tags applied to `file_id`.
    pub async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        self.try_backend()?
            .tags_for_file(file_id, subtag_rule)
            .await
    }

    // --- Query helpers for the Dart UI ---------------------------------------
    //
    // The raw `tags_for_file` returns *opaque* id handles the Dart UI cannot
    // render. These variants take id *strings* (full-or-short prefixes resolved
    // by `resolve_file_id`) and return either id strings or flattened
    // FileEntry/TagEntry rows, so the UI never touches an opaque handle.

    /// The string ids of the tags applied to the file identified by `file_id`.
    pub async fn tag_ids_for_file_string(
        &self,
        file_id: String,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<String>, ApiError> {
        let backend = self.try_backend()?;
        let file_id = backend.resolve_file_id(file_id).await?;
        Ok(backend
            .tags_for_file(file_id, subtag_rule)
            .await?
            .into_iter()
            .map(|id| id.to_string())
            .collect())
    }

    /// The string ids of the tags applied to the tag identified by `tag_id`
    /// (its parents in the hierarchy). The tag analogue of
    /// [`Self::tag_ids_for_file_string`].
    pub async fn tag_ids_for_tag_string(
        &self,
        tag_id: String,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<String>, ApiError> {
        let backend = self.try_backend()?;
        let tag_id = backend.resolve_tag_id(tag_id).await?;
        Ok(backend
            .tags_for_tag(tag_id, subtag_rule)
            .await?
            .into_iter()
            .map(|id| id.to_string())
            .collect())
    }

    /// The string ids of the subtags (children) of the tag identified by
    /// `tag_id`.
    pub async fn subtag_ids_for_tag_string(
        &self,
        tag_id: String,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<String>, ApiError> {
        let backend = self.try_backend()?;
        let tag_id = backend.resolve_tag_id(tag_id).await?;
        Ok(backend
            .subtags_for_tag(tag_id, subtag_rule)
            .await?
            .into_iter()
            .map(|id| id.to_string())
            .collect())
    }

    /// Make `subtag_id` a subtag (child) of `parent_id` in the tag hierarchy.
    /// String-id variant of the underlying `tag_tag` call.
    pub async fn tag_tag_by_string(
        &self,
        parent_id: String,
        subtag_id: String,
    ) -> Result<(), ApiError> {
        let backend = self.try_backend()?;
        let parent_id = backend.resolve_tag_id(parent_id).await?;
        let subtag_id = backend.resolve_tag_id(subtag_id).await?;
        backend.tag_tag(parent_id, subtag_id).await
    }

    /// Remove `subtag_id` as a subtag of `parent_id`. String-id variant of
    /// the underlying `untag_tag` call.
    pub async fn untag_tag_by_string(
        &self,
        parent_id: String,
        subtag_id: String,
    ) -> Result<(), ApiError> {
        let backend = self.try_backend()?;
        let parent_id = backend.resolve_tag_id(parent_id).await?;
        let subtag_id = backend.resolve_tag_id(subtag_id).await?;
        backend.untag_tag(parent_id, subtag_id).await
    }

    /// The files and tags matching the free-form `query` (`$tag`, `!tag`, and
    /// name substrings), as flattened [`FileEntry`]/[`TagEntry`] rows. Tag
    /// tokens are resolved in the daemon.
    pub async fn run_query(
        &self,
        query: String,
        subtag_rule: SubtagRule,
    ) -> Result<QueryEntries, ApiError> {
        let backend = self.try_backend()?;
        let result = backend.run_query(query, subtag_rule).await?;
        Ok(QueryEntries {
            files: result.files.into_iter().map(FileEntry::from).collect(),
            tags: result.tags.into_iter().map(TagEntry::from).collect(),
        })
    }

    /// Get a single file's flattened [`FileEntry`] by id string (a full or
    /// short id prefix). Errors `NotFound` if unknown.
    pub async fn get_file_entry(&self, file_id: String) -> Result<FileEntry, ApiError> {
        let backend = self.try_backend()?;
        let file_id = backend.resolve_file_id(file_id).await?;
        Ok(FileEntry::from(backend.get_file(file_id).await?))
    }

    /// Absolute on-disk path where `file_id`'s bytes currently live locally,
    /// or `None` if no sync directory on this device holds a copy (e.g. the
    /// file is known by metadata but hasn't been fetched yet). Read-only.
    ///
    /// The UI uses this to render an inline preview from disk without pulling
    /// bytes across the bridge. Errors `NotFound` if the id itself is unknown.
    pub async fn local_path_for_file_by_string(
        &self,
        file_id: String,
    ) -> Result<Option<String>, ApiError> {
        let backend = self.try_backend()?;
        let file_id = backend.resolve_file_id(file_id).await?;
        Ok(backend
            .local_path_for_file(file_id)
            .await?
            .map(|path| path.to_string_lossy().into_owned()))
    }

    /// Get a single tag's flattened [`TagEntry`] by id string (a full or short
    /// id prefix). Errors `NotFound` if unknown.
    pub async fn get_tag_entry(&self, tag_id: String) -> Result<TagEntry, ApiError> {
        let backend = self.try_backend()?;
        let tag_id = backend.resolve_tag_id(tag_id).await?;
        Ok(TagEntry::from(backend.get_tag(tag_id).await?))
    }

    // --- Write API (plan 5.4) ------------------------------------------------

    /// Create a tag; returns the freshly-minted id.
    pub async fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        self.try_backend()?.create_tag(name, color).await
    }

    /// Delete a tag.
    pub async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.try_backend()?.delete_tag(tag_id).await
    }

    /// Rename a tag. The change propagates through the usual event stream, so
    /// live UI (list, detail) refreshes without an explicit reload.
    pub async fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        self.try_backend()?.rename_tag(tag_id, name).await
    }

    /// Change a tag's color. Same propagation rules as [`Self::rename_tag`].
    pub async fn set_tag_color(&self, tag_id: TagId, color: String) -> Result<(), ApiError> {
        self.try_backend()?.set_tag_color(tag_id, color).await
    }

    /// Upload a file from a path on disk; returns the freshly-minted id.
    ///
    /// The bytes are streamed (hashed and then served on demand), never
    /// buffered whole. `path_name` is the file's logical identity; `path`
    /// is where the bytes currently live (e.g. the shared-file path the
    /// platform hands us).
    pub async fn upload_file(
        &self,
        path: String,
        path_name: String,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        self.try_backend()?
            .upload_file(std::path::PathBuf::from(path), path_name, tags)
            .await
    }

    /// Delete a file.
    pub async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.try_backend()?.delete_file(file_id).await
    }

    /// Move (rename) a file to a new logical path. String-id variant of the
    /// underlying `move_file` call — the Dart UI passes the `FileEntry.fileId`
    /// string it already has.
    pub async fn move_file_by_string(
        &self,
        file_id: String,
        logical_path: String,
    ) -> Result<(), ApiError> {
        let backend = self.try_backend()?;
        let file_id = backend.resolve_file_id(file_id).await?;
        backend.move_file(file_id, logical_path).await
    }

    /// Apply `tag_id` to `file_id`.
    pub async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.try_backend()?.tag_file(tag_id, file_id).await
    }

    /// Remove `tag_id` from `file_id`.
    pub async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.try_backend()?.untag_file(tag_id, file_id).await
    }

    // --- Event stream (plan 5.5) ---------------------------------------------

    /// Subscribe to the live change stream.
    ///
    /// Returns an [`EventSubscription`] the UI polls with
    /// [`EventSubscription::next`]. Each item is an [`ApiEvent`]; a `None`
    /// means the stream is unavailable (runtime not running) or closed.
    pub fn subscribe(&self) -> EventSubscription {
        EventSubscription {
            stream: self
                .try_backend()
                .ok()
                .map(|backend| Mutex::new(backend.subscribe())),
        }
    }
}

/// A live subscription to the change stream (plan 5.5).
///
/// `flutter_rust_bridge` maps [`EventSubscription::next`] onto a Dart
/// `Future<ApiEvent?>` the UI awaits in a loop; on `null` the stream is done.
/// The [`EventStream`] is held behind a [`Mutex`] because the generated Dart
/// binding shares the opaque handle across await points.
#[cfg_attr(feature = "flutter_rust_bridge", flutter_rust_bridge::frb(opaque))]
pub struct EventSubscription {
    /// `None` if the runtime was not running when the subscription was made.
    stream: Option<Mutex<EventStream>>,
}

impl EventSubscription {
    /// Await the next event, or `None` once the stream is permanently closed
    /// (or was never available).
    pub async fn next(&self) -> Option<ApiEvent> {
        // `EventStream::recv` borrows the receiver mutably. A `tokio` mutex
        // (rather than `std`) keeps the resulting future `Send` so
        // `flutter_rust_bridge` can drive it on its multi-thread runtime; the
        // UI drives one `next` at a time per subscription, so contention is
        // nil.
        let stream = self.stream.as_ref()?;
        let mut guard = stream.lock().await;
        guard.recv().await
    }
}
