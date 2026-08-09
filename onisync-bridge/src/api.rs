//! Dart-facing API surface.
//!
//! This is the thin layer `flutter_rust_bridge` generates Dart bindings for. It
//! deliberately adds **no** business logic: it owns a [`RuntimeHandle`] and
//! forwards every call to the [`Backend`], which forwards to the
//! [`Api`](onisyncd::api::Api). The Dart UI holds one [`OniSyncApp`] and never
//! learns which transport backs it.
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
pub use onisync_core::{FileId, FileInfo, Preview, TagId};
pub use onisyncd::api::{ApiError, ApiEvent};
pub use onisyncd::configuration::EditorRule;
pub use onisyncd::database::{DeletedRule, SubtagRule, Tag};
pub use onisyncd::operations::{
    Direction, Operation, OperationEvent, OperationKind, OperationStatus,
};
use onisyncd::paths::Paths;
use onisyncd::transport::{
    Backend, EventStream, OperationStream, OperationUpdate, TransportBackend,
};
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
    /// Whether this file is soft-deleted (tombstoned). Always `false` for rows
    /// returned by the standard live-only listing/search; only set when the
    /// caller opted into the "show deleted" view via
    /// [`DeletedRule::Include`]. The UI uses this to render a "deleted"
    /// badge distinct from live rows.
    pub deleted: bool,
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
            deleted: info.deleted,
        }
    }
}

/// Which kind of content a [`PreviewEntry`] carries. Mirrors the variants of
/// the core [`Preview`] enum as a flat tag the Dart UI can switch on.
pub enum PreviewKind {
    /// A small raster image; `PreviewEntry.image_bytes`/`width`/`height` are
    /// set.
    Image,
    /// A short text snippet; `PreviewEntry.text` is set.
    Text,
    /// No preview for this content (un-previewable type). All payload fields
    /// are empty/None.
    None,
}

/// A file preview flattened for the Dart UI (see [`FileEntry`] for why a DTO).
///
/// The core [`Preview`] is an enum with per-variant payloads; frb cannot see
/// inside a foreign enum, so this flattens it into one struct with a `kind`
/// discriminant plus optional fields. Exactly the fields relevant to `kind` are
/// populated:
/// - `Image`: `image_bytes` (an encoded PNG the UI decodes directly), `width`,
///   `height`.
/// - `Text`: `text`.
/// - `None`: nothing.
pub struct PreviewEntry {
    pub kind: PreviewKind,
    pub image_bytes: Option<Vec<u8>>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub text: Option<String>,
}

impl From<Preview> for PreviewEntry {
    fn from(preview: Preview) -> Self {
        match preview {
            Preview::Image {
                bytes,
                width,
                height,
            } => Self {
                kind: PreviewKind::Image,
                image_bytes: Some(bytes),
                width: Some(width),
                height: Some(height),
                text: None,
            },
            Preview::Text(text) => Self {
                kind: PreviewKind::Text,
                image_bytes: None,
                width: None,
                height: None,
                text: Some(text),
            },
            Preview::None => Self {
                kind: PreviewKind::None,
                image_bytes: None,
                width: None,
                height: None,
                text: None,
            },
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

/// Mirror of [`DeletedRule`] so `flutter_rust_bridge` generates it as a real
/// Dart *enum* the UI can pass to [`OniSyncApp::run_query`]. Variants MUST
/// stay in sync with [`onisyncd::database::DeletedRule`]; see the sibling
/// [`_SubtagRule`] doc for why this mirror declaration exists.
///
/// Semantics (see [`onisyncd::database::DeletedRule`]):
///   * `Exclude` — standard live-only search (default);
///   * `Include` — search over tombstoned rows too; `run_query` returns only
///     the tombstoned matches for the "show deleted" toggle.
#[cfg(feature = "flutter_rust_bridge")]
#[flutter_rust_bridge::frb(mirror(DeletedRule))]
pub enum _DeletedRule {
    Include,
    Exclude,
}

/// An external-editor rule flattened for the Dart UI.
///
/// Mirrors [`EditorRule`] as a DTO with plain fields (rather than an opaque
/// handle) so the Dart side can read `tag_id` / `argv` directly when choosing
/// which command to launch; see [`FileEntry`] for the same DTO-flattening
/// pattern used elsewhere in this crate. The tag id is rendered as its UUID
/// string (matching [`TagEntry::tag_id`]) so the UI can compare it directly
/// against a file's applied tag ids without touching an opaque [`TagId`]
/// handle.
pub struct EditorRuleEntry {
    /// Tag id (UUID string) to match against the file's applied tag ids. Ids
    /// (not names) are the stable identifier: a rule keyed by name would
    /// break silently after a `rename_tag`.
    pub tag_id: String,
    /// The editor command as an explicit `argv` vector; the file path is
    /// appended as the final argument. Crosses the bridge as a list, not a
    /// string, so no side has to agree on a tokenisation rule. See
    /// [`EditorRule::argv`].
    pub argv: Vec<String>,
}

impl From<EditorRule> for EditorRuleEntry {
    fn from(rule: EditorRule) -> Self {
        Self {
            tag_id: rule.tag_id.to_string(),
            argv: rule.argv,
        }
    }
}

/// A tag flattened into primitive fields for the Dart UI (see [`FileEntry`]).
pub struct TagEntry {
    pub tag_id: String,
    pub name: String,
    pub color: String,
    /// Whether this tag is soft-deleted (tombstoned). Mirrors
    /// [`FileEntry::deleted`]: always `false` for standard listings, only
    /// possibly `true` under [`DeletedRule::Include`].
    pub deleted: bool,
}

impl From<Tag> for TagEntry {
    fn from(tag: Tag) -> Self {
        Self {
            tag_id: tag.id.to_string(),
            name: tag.name,
            color: tag.color,
            deleted: tag.deleted,
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

/// A live sync operation flattened into primitive fields for the Dart UI.
///
/// The daemon's [`Operation`] (and its nested [`OperationKind`]/
/// [`OperationStatus`]) live in a foreign crate, so — as with [`FileEntry`] —
/// `flutter_rust_bridge` cannot see inside them to generate readable Dart
/// classes. This DTO re-expresses the same information as flat, displayable
/// fields.
///
/// `kind` is a stable machine string (e.g. `"receiving_file"`,
/// `"connecting_to_peer"`) the UI switches on to choose an icon/label. The
/// optional fields carry whatever that kind provides; a field is empty/None for
/// kinds that do not use it.
pub struct OperationEntry {
    /// Stable id for the life of the operation; the UI keys rows on it so a
    /// row updates in place from start through progress to its terminal state.
    pub id: u64,
    /// Machine-readable kind discriminant (see the type docs).
    pub kind: String,
    /// The peer this operation involves, if any (its configured name).
    pub peer_name: Option<String>,
    /// The file this operation concerns, as its id string, if any.
    pub file_id: Option<String>,
    /// The current lifecycle status.
    pub status: OperationStatusDto,
    /// Bytes/entries done so far, when the operation reports progress.
    pub progress_done: Option<u64>,
    /// Total bytes/entries, when known.
    pub progress_total: Option<u64>,
    /// Wall-clock milliseconds when the operation started.
    pub started_at: i64,
    /// Wall-clock milliseconds of the last update.
    pub updated_at: i64,
}

/// The lifecycle status of an [`OperationEntry`], as a real Dart enum.
pub enum OperationStatusDto {
    /// Running (progress, if any, is on the [`OperationEntry`] fields).
    Active,
    /// Finished successfully.
    Completed,
    /// Finished with an error. Carries the reason.
    Failed { reason: String },
    /// Ended without a terminal outcome (cancelled / link dropped).
    Aborted,
}

impl From<Operation> for OperationEntry {
    fn from(operation: Operation) -> Self {
        let (kind, peer_name, file_id) = flatten_kind(&operation.kind);
        let (status, progress_done, progress_total) = flatten_status(&operation.status);
        Self {
            id: operation.id.as_u64(),
            kind,
            peer_name,
            file_id,
            status,
            progress_done,
            progress_total,
            started_at: operation.started_at,
            updated_at: operation.updated_at,
        }
    }
}

/// Flatten an [`OperationKind`] into `(kind, peer_name, file_id)`.
fn flatten_kind(kind: &OperationKind) -> (String, Option<String>, Option<String>) {
    match kind {
        OperationKind::ConnectingToPeer { peer_name, .. } => (
            "connecting_to_peer".to_owned(),
            Some(peer_name.clone()),
            None,
        ),
        OperationKind::PeerConnected {
            peer_name,
            direction,
            ..
        } => (
            match direction {
                Direction::Outbound => "peer_connected_outbound",
                Direction::Inbound => "peer_connected_inbound",
            }
            .to_owned(),
            Some(peer_name.clone()),
            None,
        ),
        OperationKind::ReceivingFile { file_id, peer_name } => (
            "receiving_file".to_owned(),
            Some(peer_name.clone()),
            Some(file_id.clone()),
        ),
        OperationKind::Fetching { file_id } => ("fetching".to_owned(), None, Some(file_id.clone())),
        OperationKind::ReconcilingManifest { peer_name } => (
            "reconciling_manifest".to_owned(),
            Some(peer_name.clone()),
            None,
        ),
        OperationKind::ReconcilingTags { peer_name } => {
            ("reconciling_tags".to_owned(), Some(peer_name.clone()), None)
        }
        OperationKind::PlacingFile { file_id } => {
            ("placing_file".to_owned(), None, Some(file_id.clone()))
        }
    }
}

/// Flatten an [`OperationStatus`] into `(status, progress_done,
/// progress_total)`.
fn flatten_status(status: &OperationStatus) -> (OperationStatusDto, Option<u64>, Option<u64>) {
    match status {
        OperationStatus::Active { progress } => (
            OperationStatusDto::Active,
            progress.map(|p| p.done),
            progress.and_then(|p| p.total),
        ),
        OperationStatus::Completed => (OperationStatusDto::Completed, None, None),
        OperationStatus::Failed { reason } => (
            OperationStatusDto::Failed {
                reason: reason.clone(),
            },
            None,
            None,
        ),
        OperationStatus::Aborted => (OperationStatusDto::Aborted, None, None),
    }
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
    /// and initializes log routing (logcat on Android). Blocks only until the
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
    /// topology).
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

    /// Resolve a full-or-short file id prefix (see
    /// [`FileInfo::short_id_length`]) to a single [`FileId`]. Errors with
    /// `NotFound` if nothing matches or `Ambiguous` if several do.
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
    ///
    /// `deleted_rule` toggles between the standard live-only view
    /// ([`DeletedRule::Exclude`]) and the "show deleted" view
    /// ([`DeletedRule::Include`], which returns *only* tombstoned files/tags
    /// — see [`Api::run_query`](onisyncd::api::Api::run_query)). This is what
    /// the UI's "search deleted" toggle wires to.
    pub async fn run_query(
        &self,
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<QueryEntries, ApiError> {
        let backend = self.try_backend()?;
        let result = backend.run_query(query, subtag_rule, deleted_rule).await?;
        Ok(QueryEntries {
            files: result.files.into_iter().map(FileEntry::from).collect(),
            tags: result.tags.into_iter().map(TagEntry::from).collect(),
        })
    }

    /// Get a single file's flattened [`FileEntry`] by id string (a full or
    /// short id prefix). Errors `NotFound` if unknown.
    ///
    /// `deleted_rule` mirrors [`Self::run_query`]: under
    /// [`DeletedRule::Exclude`] a tombstoned file reads as `NotFound` (the
    /// default for pickers and operational lookups); under
    /// [`DeletedRule::Include`] it comes back with `FileEntry::deleted =
    /// true`, so a detail screen opened from the "show deleted" search can
    /// render its metadata.
    pub async fn get_file_entry(
        &self,
        file_id: String,
        deleted_rule: DeletedRule,
    ) -> Result<FileEntry, ApiError> {
        let backend = self.try_backend()?;
        let file_id = backend.resolve_file_id(file_id).await?;
        Ok(FileEntry::from(
            backend.get_file(file_id, deleted_rule).await?,
        ))
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

    /// Get the preview for `file_id`'s current content as a flat
    /// [`PreviewEntry`].
    ///
    /// The daemon returns a cached preview, generates one from local bytes, or
    /// fetches one from a peer (first responder wins) — the UI does not need to
    /// know which. A file whose content has no preview comes back with
    /// `PreviewEntry.kind == PreviewKind::None` (a successful result). Errors
    /// `NotFound` only if the id itself is unknown.
    pub async fn get_preview_by_string(&self, file_id: String) -> Result<PreviewEntry, ApiError> {
        let backend = self.try_backend()?;
        let file_id = backend.resolve_file_id(file_id).await?;
        Ok(PreviewEntry::from(backend.get_preview(file_id).await?))
    }

    /// Fetch `file_id`'s content on demand (from a peer if no local sync
    /// directory holds it) and return the path to a **daemon-owned temp file**
    /// holding the bytes.
    ///
    /// `expected_hash` gates which content is accepted; the caller passes the
    /// file's known `FileEntry.content_hash`. The returned path lives under the
    /// daemon's fetch temp dir and is handed over with **move semantics** — the
    /// caller must consume it (e.g. hand it to the OS share sheet) and delete
    /// it afterwards. The UI uses this to share a file that is not present
    /// locally; for a locally-held file prefer
    /// [`Self::local_path_for_file_by_string`].
    pub async fn fetch_file_by_string(
        &self,
        file_id: String,
        expected_hash: String,
    ) -> Result<String, ApiError> {
        let backend = self.try_backend()?;
        let file_id = backend.resolve_file_id(file_id).await?;
        Ok(backend
            .fetch_file(file_id, expected_hash)
            .await?
            .to_string_lossy()
            .into_owned())
    }

    /// Start an external edit of `file_id`: return the on-disk path the UI
    /// should hand to an editor.
    ///
    /// Two branches, transparent to the UI:
    ///
    /// - The file already lives in a local sync directory → the returned path
    ///   is that real file; editing happens in place and the daemon's
    ///   filesystem watcher picks up the save.
    /// - Otherwise the daemon fetches the content into an isolated per-request
    ///   temp path named after the file's logical basename
    ///   (extension-preserving, so external editors dispatch by MIME
    ///   correctly).
    ///
    /// Move semantics: the UI must consume the returned path by calling
    /// [`Self::finish_edit_by_string`] (upload if bytes changed) or
    /// [`Self::cancel_edit`] (abort + cleanup). A missed follow-up only leaks
    /// a temp file that the daemon bulk-cleans on next start; no state is
    /// tracked between calls.
    pub async fn begin_edit_by_string(&self, file_id: String) -> Result<String, ApiError> {
        let backend = self.try_backend()?;
        let file_id = backend.resolve_file_id(file_id).await?;
        Ok(backend
            .begin_edit(file_id)
            .await?
            .to_string_lossy()
            .into_owned())
    }

    /// Complete an external edit started with [`Self::begin_edit_by_string`].
    ///
    /// The daemon re-hashes the bytes at `path`; if different from the file's
    /// current recorded content it publishes a new version (streaming from
    /// `path`, never buffering). Either way it cleans up any daemon-owned
    /// temp under its fetch temp dir; sync-dir paths (Branch A) are left
    /// untouched.
    ///
    /// Returns `true` if a new version was published, `false` for a no-op
    /// (editor produced no change, or the in-place edit was already ingested
    /// by the daemon's filesystem watcher). Lets the UI show "edited" vs.
    /// "no changes".
    ///
    /// The core [`onisyncd::api::EditOutcome`] type is flattened to a bare
    /// bool here so the Dart side gets a plain value rather than an opaque
    /// handle it would then need a getter for; see [`FileEntry`] for the
    /// same DTO-flattening pattern used elsewhere in this crate.
    pub async fn finish_edit_by_string(
        &self,
        file_id: String,
        path: String,
    ) -> Result<bool, ApiError> {
        let backend = self.try_backend()?;
        let file_id = backend.resolve_file_id(file_id).await?;
        Ok(backend
            .finish_edit(file_id, std::path::PathBuf::from(path))
            .await?
            .changed)
    }

    /// Abort an external edit without publishing. Cleans up any daemon-owned
    /// temp at `path`; a sync-dir path is left untouched. Called by the UI
    /// when the launcher itself fails (no editor available, user cancelled,
    /// etc.) so the daemon does not leave a temp behind.
    pub async fn cancel_edit(&self, path: String) -> Result<(), ApiError> {
        self.try_backend()?
            .cancel_edit(std::path::PathBuf::from(path))
            .await
    }

    /// The daemon's configured external-editor rules (see [`EditorRule`]) as
    /// flattened [`EditorRuleEntry`] rows the Dart UI reads directly.
    ///
    /// The desktop UI consults these when preparing an edit: it walks the
    /// list in order, and the first rule whose `tag_id` is among the file's
    /// applied tag ids wins — its `argv` is spawned with the file path as the
    /// final arg. If no rule matches, the UI reports that rather than falling
    /// back to `$VISUAL` / `$EDITOR`: those name terminal editors, which hang
    /// forever when spawned from a GUI process with no controlling TTY. An
    /// empty list therefore means no file is externally editable on this
    /// device.
    pub async fn editor_rules(&self) -> Result<Vec<EditorRuleEntry>, ApiError> {
        Ok(self
            .try_backend()?
            .editor_rules()
            .await?
            .into_iter()
            .map(EditorRuleEntry::from)
            .collect())
    }

    /// Get a single tag's flattened [`TagEntry`] by id string (a full or short
    /// id prefix). Errors `NotFound` if unknown. See
    /// [`Self::get_file_entry`] for the `deleted_rule` semantics.
    pub async fn get_tag_entry(
        &self,
        tag_id: String,
        deleted_rule: DeletedRule,
    ) -> Result<TagEntry, ApiError> {
        let backend = self.try_backend()?;
        let tag_id = backend.resolve_tag_id(tag_id).await?;
        Ok(TagEntry::from(backend.get_tag(tag_id, deleted_rule).await?))
    }

    /// Create a tag; returns the freshly-minted id as a string.
    ///
    /// Returns the id *string* (not the opaque [`TagId`] handle) so the Dart
    /// UI can round-trip it through the usual `*_string` methods — e.g. resolve
    /// it back for `upload_file`, or fetch its flattened [`TagEntry`] to render
    /// a chip. Matches the string-id convention used by the other UI-facing
    /// methods here (see [`Self::tag_ids_for_file_string`]).
    pub async fn create_tag(&self, name: String, color: String) -> Result<String, ApiError> {
        Ok(self
            .try_backend()?
            .create_tag(name, color)
            .await?
            .to_string())
    }

    /// Delete a tag.
    pub async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.try_backend()?.delete_tag(tag_id).await
    }

    /// Restore a soft-deleted tag. Unlike a file restore this always succeeds
    /// for a known tag (a tag carries no content to recover): it re-announces
    /// the tag definition with a fresh timestamp, winning last-writer-wins over
    /// the delete.
    pub async fn restore_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.try_backend()?.restore_tag(tag_id).await
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
    ///
    /// `tags` are the string ids (full-or-short prefixes, as carried by
    /// `TagEntry.tag_id`) to apply to the new file; they are resolved to
    /// [`TagId`] handles here. Taking strings — rather than the opaque
    /// handles — matches the string-id convention of the other UI-facing
    /// methods and, crucially, lets the Dart caller upload several files in a
    /// row with the same tags: an opaque [`TagId`] handle is consumed when it
    /// crosses the bridge, so it cannot be reused across calls, whereas an id
    /// string can.
    pub async fn upload_file(
        &self,
        path: String,
        path_name: String,
        tags: Vec<String>,
    ) -> Result<FileId, ApiError> {
        let backend = self.try_backend()?;
        let mut tag_ids = Vec::with_capacity(tags.len());
        for tag in tags {
            tag_ids.push(backend.resolve_tag_id(tag).await?);
        }
        backend
            .upload_file(std::path::PathBuf::from(path), path_name, tag_ids)
            .await
    }

    /// Delete a file.
    pub async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.try_backend()?.delete_file(file_id).await
    }

    /// Restore a soft-deleted file (best-effort). Fails with
    /// `ApiError::NotFound` if no source (local `keep_deleted_files` vault
    /// or a connected peer) still holds the file's bytes.
    pub async fn restore_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.try_backend()?.restore_file(file_id).await
    }

    /// Purge the daemon's cached file previews, returning how many were
    /// removed.
    ///
    /// Previews are hash-keyed and regenerated on demand, so this only forces
    /// re-evaluation on the next request (useful after the set of previewable
    /// file types changes). Surfaced in the UI as a toolbar action.
    pub async fn purge_previews(&self) -> Result<usize, ApiError> {
        self.try_backend()?.purge_previews().await
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

    /// Snapshot every currently-active sync operation as flattened
    /// [`OperationEntry`] rows the Dart UI renders directly.
    ///
    /// The UI calls this for its initial paint of the "activity" view, then
    /// applies live updates from
    /// [`subscribe_operations`](Self::subscribe_operations) (and re-calls
    /// this on an [`OperationUpdateDto::Resynced`]).
    pub async fn list_operations(&self) -> Result<Vec<OperationEntry>, ApiError> {
        Ok(self
            .try_backend()?
            .list_operations()
            .await?
            .into_iter()
            .map(OperationEntry::from)
            .collect())
    }

    /// Subscribe to the live sync-operation stream.
    ///
    /// Returns an [`OperationSubscription`] the UI polls with
    /// [`OperationSubscription::next`]. Each item is an
    /// [`OperationUpdateDto`]; a `None` means the stream is unavailable
    /// (runtime not running) or closed.
    pub fn subscribe_operations(&self) -> OperationSubscription {
        OperationSubscription {
            stream: self
                .try_backend()
                .ok()
                .map(|backend| Mutex::new(backend.subscribe_operations())),
        }
    }
}

/// A live subscription to the change stream.
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

/// A live update on the operation stream, for the Dart UI.
///
/// The FFI-facing counterpart of
/// [`OperationUpdate`](onisyncd::transport::OperationUpdate).
pub enum OperationUpdateDto {
    /// The stream lagged or reconnected; the UI should re-call
    /// [`OniSyncApp::list_operations`] to re-sync its view.
    Resynced,
    /// A new operation began.
    Started { operation: OperationEntry },
    /// An existing operation changed (progress or terminal outcome).
    Updated { operation: OperationEntry },
}

/// A live subscription to the sync-operation stream.
///
/// The operation counterpart of [`EventSubscription`]:
/// `flutter_rust_bridge` maps [`OperationSubscription::next`] onto a Dart
/// `Future<OperationUpdateDto?>` the UI awaits in a loop; on `null` the stream
/// is done.
#[cfg_attr(feature = "flutter_rust_bridge", flutter_rust_bridge::frb(opaque))]
pub struct OperationSubscription {
    /// `None` if the runtime was not running when the subscription was made.
    stream: Option<Mutex<OperationStream>>,
}

impl OperationSubscription {
    /// Await the next operation update, or `None` once the stream is
    /// permanently closed (or was never available).
    pub async fn next(&self) -> Option<OperationUpdateDto> {
        let stream = self.stream.as_ref()?;
        let mut guard = stream.lock().await;
        match guard.recv().await? {
            OperationUpdate::Resynced => Some(OperationUpdateDto::Resynced),
            OperationUpdate::Event(OperationEvent::Started(operation)) => {
                Some(OperationUpdateDto::Started {
                    operation: OperationEntry::from(operation),
                })
            }
            OperationUpdate::Event(OperationEvent::Updated(operation)) => {
                Some(OperationUpdateDto::Updated {
                    operation: OperationEntry::from(operation),
                })
            }
        }
    }
}
