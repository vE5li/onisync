use std::collections::HashMap;
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use onisync_core::state::Frame;
use onisync_core::{FileId, LogicalPath, PhysicalPath, TagId};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

use crate::bus::PeerCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    /// IP address and port of the peer. None to let the peer establish the
    /// connection.
    pub address: Option<(IpAddr, u16)>,
    /// Human-readable label for this peer, used only to make log messages
    /// readable. Peer identity is always established via `public_key`.
    pub name: String,
    pub public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncType {
    Universal {
        /// When true, a `Change::FileDeleted` does **not** remove the file's
        /// bytes from this directory: the physical copy is kept as a recovery
        /// vault so an accidental delete can be undone. The file is still
        /// removed from the catalog and from every other sync directory; only
        /// this directory's on-disk copy survives.
        ///
        /// Universal-only (files here are stored under their `file_id`, so a
        /// kept copy is unambiguous).
        #[serde(default)]
        keep_deleted_files: bool,
    },
    TagBased {
        tags: Vec<TagId>,
    },
}

impl SyncType {
    /// Decide where a file with the given logical path is stored on disk within
    /// a sync directory of this type — the `LogicalPath -> PhysicalPath`
    /// placement decision.
    ///
    /// - `Universal`: files are stored under their `file_id` on disk, so the
    ///   physical path is the id regardless of the logical name.
    /// - `TagBased`: the on-disk layout mirrors the logical namespace, so the
    ///   physical path equals the logical path.
    ///
    /// This is the only sanctioned way to turn a `LogicalPath` into a
    /// `PhysicalPath`; keeping it here (rather than in `onisync-core`) is why
    /// the core newtypes expose no direct conversion in this direction.
    pub fn physical_for(&self, logical_path: &LogicalPath, file_id: FileId) -> PhysicalPath {
        match self {
            SyncType::Universal { .. } => PhysicalPath::new(file_id.to_string()),
            SyncType::TagBased { .. } => PhysicalPath::new(logical_path.as_str()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpecialType {
    Upload,
    Copy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncDirectory {
    pub path: PathBuf,
    pub sync_type: SyncType,
}

/// A tag declared in the configuration file so its *definition* is guaranteed
/// to exist on startup — the counterpart to referencing a `TagId` from a
/// [`SyncType::TagBased`] directory.
///
/// Tags are otherwise only minted at runtime (UI/API `create_tag`, or
/// reconciled from a peer), which forces an operator to create a tag elsewhere
/// and copy its opaque id into the config by hand. Declaring the tag here —
/// with its id chosen by the operator — makes a `TagBased` directory's `tags`
/// self-contained and lets the *same* tag converge across devices (they all
/// declare the same id).
///
/// Semantics are a last-writer-wins **floor**, not an override: on startup each
/// declaration is replayed as a `Change::TagAdded` stamped with a very low
/// `modified_at`, so it *creates* the tag when absent but never clobbers a
/// newer rename/recolor made through the UI or reconciled from a peer. Config
/// declares existence and initial values; it does not enforce them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagDeclaration {
    /// The tag's id, chosen by the operator (not minted). The same id must be
    /// used on every device that should share this tag.
    pub id: TagId,
    pub name: String,
    /// Hex color (e.g. `#F44336`). Empty is allowed and normalized downstream.
    #[serde(default)]
    pub color: String,
}

/// A rule mapping a tag id to an external editor command.
///
/// Used by the desktop UI's "edit" action: when a file carries a tag whose id
/// matches [`tag_id`], its bytes are handed to [`argv`] instead of the generic
/// `$VISUAL`/`$EDITOR` fallback. Rules are consulted in declaration order; the
/// first match wins.
///
/// The daemon does not use these rules itself — it has no notion of external
/// processes — but stores them so every frontend on this device (and any
/// future non-Flutter client of the same daemon) sees the same set. The
/// Android app currently has no external-editor concept and simply ignores
/// them.
///
/// # Security
///
/// A rule is, by construction, "run this program" — arbitrary code execution
/// with the desktop app's privileges. That is the feature, not a flaw, but it
/// makes **write access to the config file equivalent to code execution**, so
/// the config should be owned by the user running the app and not
/// group/world-writable.
///
/// What the config explicitly is *not* is a place where untrusted data lands:
/// editor rules are read once at startup from the local config and are never
/// synced from peers, stored in the database, or mutated at runtime (there is
/// no setter anywhere in the API or control protocol). A malicious peer
/// therefore cannot introduce or alter a rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditorRule {
    /// Tag id to match against the file's applied tags. Ids (not names) are
    /// the stable, unique identifier: [`crate::api::Api::rename_tag`] can
    /// change a tag's name at any time — a rule keyed by name would silently
    /// stop matching after a rename. Pair a rule with the corresponding
    /// [`TagDeclaration`] to guarantee the id exists on every device.
    pub tag_id: TagId,
    /// The editor command as an explicit `argv` vector, e.g.
    /// `["/run/current-system/sw/bin/gimp"]` or
    /// `["/usr/bin/code", "--wait"]`. The file path is appended as the final
    /// argument, and the vector is passed straight to `execvp` — **no shell is
    /// involved**, so quoting, globbing and metacharacters have no meaning
    /// here.
    ///
    /// This is a list rather than a single string on purpose. A string would
    /// have to be split into `argv` by the launcher, and every splitting rule
    /// is either too naive to express an argument containing a space or
    /// complex enough (quotes, escapes) to be worth getting subtly wrong. A
    /// list sidesteps the question: the operator states the argument
    /// boundaries directly.
    ///
    /// `argv[0]` **must be an absolute path**. See the Linux launcher
    /// (`app/lib/editor/linux_editor_launcher.dart`) for the rationale.
    ///
    /// The command must block until the user is done editing (e.g. `gimp`,
    /// `inkscape`, `code --wait`); one that forks and returns immediately
    /// (`xdg-open`, `nohup ...`) will make the UI think the edit finished as
    /// soon as the launch call returns.
    pub argv: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Configuration {
    /// Synchronized directories on the device itself.
    pub sync_directories: Vec<SyncDirectory>,
    /// Port to listen on. None for not listening.
    pub listen_port: Option<u16>,
    pub peers: Vec<Peer>,
    /// Tags that must exist on startup, so a [`SyncType::TagBased`] directory
    /// can reference them by id without the operator first creating them
    /// through the UI. See [`TagDeclaration`] for the last-writer-wins-floor
    /// semantics.
    #[serde(default)]
    pub tags: Vec<TagDeclaration>,
    /// How (and whether) this device *generates* file previews locally. See
    /// [`PreviewGenerationPolicy`].
    ///
    /// This governs **generation** only — the CPU-heavy decode/rasterize of a
    /// file's bytes into a thumbnail. Serving an already-cached preview to a
    /// peer, caching a preview fetched from a peer, and relaying preview
    /// requests are *always* available regardless of this setting (and require
    /// no generation support compiled in). So a `Never` device still
    /// participates in the preview network: it caches previews it fetches and
    /// can serve them onward.
    #[serde(default)]
    pub preview_generation_policy: PreviewGenerationPolicy,
    /// Tag-id → `argv` mapping consulted by the desktop UI's external-edit
    /// action. See [`EditorRule`]. Empty (the default) means no file has an
    /// external editor and the UI reports that rather than guessing. The
    /// daemon does not act on these rules; they are stored here so every
    /// frontend attached to this device sees the same set.
    #[serde(default)]
    pub editor_rules: Vec<EditorRule>,
}

/// How a device generates file previews.
///
/// Preview generation (image decode/resize, PDF rasterization) is CPU-heavy and
/// pulls in the optional generation stack (the `image` and `pdfium` crates)
/// behind the `preview-generation` cargo feature. This policy lets each device
/// choose its role independently of how it was compiled — with the constraint
/// that a non-`Never` policy requires a binary built *with* that feature (a
/// mismatch is a fail-closed startup error, since a lazy/eager device that
/// cannot actually generate would silently never produce previews).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum PreviewGenerationPolicy {
    /// Never generate. This device produces no previews itself; it only caches
    /// and serves previews obtained from peers. Requires no generation support
    /// compiled in, so a minimal client can drop the `image`/`pdfium` deps
    /// entirely by building without the `preview-generation` feature and
    /// setting this policy.
    #[default]
    #[serde(alias = "never")]
    Never,
    /// Generate on demand: the first `get_preview` for an uncached file (or a
    /// peer's `PreviewRequest` we can serve from local bytes) triggers
    /// generation, which is then cached. The default.
    #[serde(alias = "lazy")]
    Lazy,
    /// Everything `Lazy` does, plus pre-warm: generate as soon as a file's
    /// bytes land locally (a completed peer transfer, a locally-observed
    /// new/changed file) and for every local file during the startup
    /// catch-up sweep. Best for an always-on device (e.g. a home server) so
    /// other devices fetch ready-made previews from its cache instead of
    /// decoding locally.
    #[serde(alias = "eager")]
    Eager,
}

impl PreviewGenerationPolicy {
    /// Whether this policy ever generates previews (i.e. requires the
    /// `preview-generation` feature to be compiled in).
    pub fn generates(self) -> bool {
        !matches!(self, PreviewGenerationPolicy::Never)
    }

    /// Whether this policy eagerly pre-warms previews at ingest / startup.
    pub fn is_eager(self) -> bool {
        matches!(self, PreviewGenerationPolicy::Eager)
    }
}

/// Why a [`Configuration`] could not be produced from its serialized form.
///
/// Frontends that build a [`Configuration`] at runtime (e.g. the Android app,
/// which generates the JSON on first launch and passes it through the bridge)
/// must not panic on malformed input — a panic crashes the app. They use
/// [`Configuration::from_str`], which surfaces failures as this error.
#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    /// The configuration file on disk could not be read.
    #[error("failed to read configuration file {}: {source}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The bytes were not valid configuration JSON.
    #[error("failed to parse configuration JSON: {0}")]
    Parse(#[from] serde_json::Error),
}

impl Configuration {
    // TODO: Return a result
    pub fn new(configuration_file: impl AsRef<Path>) -> Self {
        // TODO: Don't unwrap.
        let file_content = std::fs::read_to_string(configuration_file.as_ref()).unwrap();

        // TODO: We need to make sure that sync directories are not nested.
        // TODO: Make sure that public keys are unique.

        serde_json::from_str(&file_content).unwrap()
    }

    /// Read and parse a [`Configuration`] from a file, returning a [`Result`]
    /// instead of panicking (the fallible counterpart to
    /// [`Configuration::new`]).
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigurationError> {
        let path = path.as_ref();
        let contents =
            std::fs::read_to_string(path).map_err(|source| ConfigurationError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        Self::from_str(&contents)
    }

    // TODO: Return a result
    pub fn write_to_file(&self, file_name: impl AsRef<Path>) {
        let json = serde_json::to_string_pretty(self).unwrap();

        // TODO: Don't unwrap.
        let mut file = std::fs::File::create(file_name.as_ref()).unwrap();
        file.write_all(json.as_bytes()).unwrap();
    }

    /// Human-readable name for the peer with the given public key, falling back
    /// to the public key itself when the peer is unknown. Used purely for logs.
    pub fn peer_name<'a>(&'a self, public_key: &'a str) -> &'a str {
        self.peers
            .iter()
            .find(|peer| peer.public_key == public_key)
            .map(|peer| peer.name.as_str())
            .unwrap_or(public_key)
    }

    pub fn get_external_sync_type(&self) -> SyncType {
        let mut all_synced_tags = Vec::new();

        for sync_directory in &self.sync_directories {
            match &sync_directory.sync_type {
                // If any of the sync directories want to save all files, we don't need the list of
                // tags. (The recovery flag is a local storage concern; it plays
                // no part in what we advertise to peers.)
                SyncType::Universal { .. } => {
                    return SyncType::Universal {
                        keep_deleted_files: false,
                    };
                }
                SyncType::TagBased { tags } => all_synced_tags.extend_from_slice(tags),
            }
        }

        SyncType::TagBased {
            tags: all_synced_tags,
        }
    }
}

impl std::str::FromStr for Configuration {
    type Err = ConfigurationError;

    /// Parse a [`Configuration`] from a JSON string, returning a [`Result`]
    /// instead of panicking.
    ///
    /// This is the non-file, non-panicking entry point: frontends without a
    /// shell/filesystem contract (Android) generate the configuration JSON in
    /// memory and parse it here. [`Configuration::new`] remains the
    /// file-reading desktop path.
    fn from_str(json: &str) -> Result<Self, ConfigurationError> {
        // TODO: We need to make sure that sync directories are not nested.
        // TODO: Make sure that public keys are unique.
        serde_json::from_str(json).map_err(ConfigurationError::Parse)
    }
}

pub struct ConnectionStatistics {}

pub struct RuntimePeer {
    pub sync_type: Option<SyncType>,
    pub statistics: ConnectionStatistics,
    /// Sender into the outbound WebSocket task for this peer.
    /// `None` when no connection is currently established.
    ///
    /// Carries `Frame` (not raw `Change`) because reconciliation and chunk
    /// transfer messages (`Sync::Manifest`, `Sync::ChunkRequest`, ...) share
    /// the same outbound queue as live changes. `forward_to_peers` wraps in
    /// `Frame::Change`.
    pub outbound: Option<UnboundedSender<Frame>>,
    /// Command channel into this peer's live session, used by `handle_changes`
    /// to trigger a byte pull for a change this peer just announced. `None`
    /// when no session is established. Registered/cleared alongside
    /// `outbound`.
    pub commands: Option<UnboundedSender<PeerCommand>>,
}

impl Default for RuntimePeer {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimePeer {
    pub fn new() -> Self {
        Self {
            sync_type: None,
            statistics: ConnectionStatistics {},
            outbound: None,
            commands: None,
        }
    }
}

pub struct RuntimeConfiguration {
    pub peers: HashMap<String, RuntimePeer>,
}

impl RuntimeConfiguration {
    pub fn new(configuration: &Configuration) -> Self {
        let peers = configuration
            .peers
            .iter()
            .map(|peer| (peer.public_key.clone(), RuntimePeer::new()))
            .collect();

        Self { peers }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    /// A config file predating the `tags` field still parses (the field
    /// defaults to empty).
    #[test]
    fn config_without_tags_field_parses() {
        let json = r#"{
            "sync_directories": [],
            "listen_port": null,
            "peers": []
        }"#;
        let configuration = Configuration::from_str(json).unwrap();
        assert!(configuration.tags.is_empty());
    }

    /// Declared tags parse, including the id (a transparent UUID string) and an
    /// omitted color (defaults to empty, normalized downstream).
    #[test]
    fn config_with_declared_tags_parses() {
        let tag_id = TagId::new();
        let json = format!(
            r##"{{
                "sync_directories": [],
                "listen_port": null,
                "peers": [],
                "tags": [
                    {{ "id": "{}", "name": "work", "color": "#00FF00" }},
                    {{ "id": "{}", "name": "photos" }}
                ]
            }}"##,
            tag_id.to_string(),
            TagId::new().to_string()
        );
        let configuration = Configuration::from_str(&json).unwrap();
        assert_eq!(configuration.tags.len(), 2);
        assert_eq!(configuration.tags[0].id, tag_id);
        assert_eq!(configuration.tags[0].name, "work");
        assert_eq!(configuration.tags[0].color, "#00FF00");
        // Omitted color defaults to empty (normalization happens at replay).
        assert_eq!(configuration.tags[1].color, "");
    }

    /// A `TagBased` sync directory can reference a declared tag's id, which is
    /// the whole point: the reference is self-contained within the config.
    #[test]
    fn tag_based_directory_can_reference_declared_tag() {
        let tag_id = TagId::new();
        let json = format!(
            r##"{{
                "sync_directories": [
                    {{ "path": "/tmp/x", "sync_type": {{ "TagBased": {{ "tags": ["{}"] }} }} }}
                ],
                "listen_port": null,
                "peers": [],
                "tags": [ {{ "id": "{}", "name": "work", "color": "#00FF00" }} ]
            }}"##,
            tag_id.to_string(),
            tag_id.to_string()
        );
        let configuration = Configuration::from_str(&json).unwrap();
        let SyncType::TagBased { tags } = &configuration.sync_directories[0].sync_type else {
            panic!("expected a TagBased directory");
        };
        assert_eq!(tags, &vec![tag_id]);
        assert_eq!(configuration.tags[0].id, tag_id);
    }
}
