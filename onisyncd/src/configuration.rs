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
}

/// Why a [`Configuration`] could not be produced from its serialized form.
///
/// Frontends that build a [`Configuration`] at runtime (e.g. the Android app,
/// which generates the JSON on first launch and passes it through the bridge)
/// must not panic on malformed input — a panic crashes the app. They use
/// [`Configuration::from_str`], which surfaces failures as this error.
#[derive(Debug)]
pub enum ConfigurationError {
    /// The configuration file on disk could not be read.
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The bytes were not valid configuration JSON.
    Parse(serde_json::Error),
}

impl std::fmt::Display for ConfigurationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigurationError::Read { path, source } => {
                write!(
                    formatter,
                    "failed to read configuration file {}: {source}",
                    path.display()
                )
            }
            ConfigurationError::Parse(error) => {
                write!(formatter, "failed to parse configuration JSON: {error}")
            }
        }
    }
}

impl std::error::Error for ConfigurationError {}

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
