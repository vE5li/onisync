use rusqlite::ToSql;
use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The peer wire-protocol version. Exchanged in the handshake
/// (`identity::HandshakeMessage::protocol_version`) and required to match
/// **exactly** between two peers before a session is established.
///
/// Bump this on any breaking change to the `Frame`/`Sync`/`Change` wire types
/// **or** to the chunk-transfer contract (chunk boundaries / `CHUNK_SIZE`,
/// which every node derives identically). Since all devices are operated by the
/// same user and updated together, there is no compatibility range — a mismatch
/// is fail-closed.
pub const PROTOCOL_VERSION: u32 = 1;

pub mod tag {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    use crate::{FileId, TagId};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum WeakType {
        String,
        Float,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum WeakData {
        String(String),
        Float(f64),
    }

    pub enum QueryError {
        NotFound,
        WrongType,
    }

    // - Metadata fields cannot be nested
    // e.g.: "file_name: String, folder_name: String"
    // TODO: Maybe use `Cow<'json, str>`
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct MetadataFormat(HashMap<String, WeakType>);

    impl MetadataFormat {
        // TODO: Make this a from.
        pub fn new(_string: &str) -> Self {
            todo!()
        }

        pub fn value_map(
            &self,
            _values: &MetadataValues,
        ) -> Result<HashMap<String, WeakData>, QueryError> {
            todo!()
        }

        pub fn query_value(
            &self,
            _values: &MetadataValues,
            _key: &str,
        ) -> Result<WeakData, QueryError> {
            todo!()
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct MetadataValues(HashMap<String, WeakData>);

    // Fields are not yet read; the struct models planned tag-metadata storage.
    #[allow(dead_code)]
    pub struct TagMetadata {
        file_id: FileId,
        tag_id: TagId, // <-- Tag has the `MetadataFormat`.
        data: MetadataValues,
    }
}

pub mod state {
    use std::path::PathBuf;

    use serde::{Deserialize, Serialize};

    use crate::tag::{MetadataFormat, MetadataValues};
    use crate::{FileId, LogicalPath, TagId};

    pub enum ChangeOrigin {
        Local { directory_path: PathBuf },
        Peer { public_key: String },
    }

    /// Anything a client can request the server to do. Add/edit/remove files
    /// and tags (including tag metadata), tag files or tags.
    ///
    /// The Server is the only entity that has knowledge of the complete state.
    /// It doesn't try to keep every client informed of the entire state, it
    /// only synchronizes the state that is:
    /// - Configured to be synced to the client
    /// - Allowed to be accessed by the user
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Change {
        /// `FileMetadataAdded` / `FileMetadataChanged` are named to make it
        /// explicit that they are **metadata-only announcements** carrying no
        /// file bytes — a receiver pulls the content over a content-addressed
        /// chunk receive.
        FileMetadataAdded {
            file_id: FileId,
            /// The file's logical identity. Receivers store this in the main
            /// database and each derive their own on-disk placement from it.
            logical_path: LogicalPath,
            /// The unix-millis wall-clock time `logical_path` was set, stamped
            /// on the *originating* device and preserved verbatim across the
            /// wire. Seeds the path's last-writer-wins clock on receivers (see
            /// `Change::FileMoved::modified_at`) so a later move orders against
            /// the true creation time rather than each receiver's local
            /// receive time. Never restamp it when forwarding or applying.
            logical_path_modified_at: i64,
            /// BLAKE3 hex digest of the file's content. `FileMetadataAdded` is
            /// a metadata-only announcement: it carries no bytes. A
            /// receiver that does not already hold this hash pulls
            /// the bytes over a separate transfer (keyed by
            /// `file_id` + this hash). The hash is recorded
            /// in `file_versions` so the version chain is authoritative.
            content_hash: String,
            /// The file's content size in bytes, read at hash time. Recorded
            /// alongside `content_hash` in `file_versions`.
            size: u64,
            // TODO: Bundle metadata with the tag.
            tags: Vec<TagId>,
        },
        FileMoved {
            file_id: FileId,
            /// The file's new logical identity. As with `FileMetadataAdded`,
            /// each receiving sync directory derives its own
            /// physical placement.
            logical_path: LogicalPath,
            /// The unix-millis wall-clock time the move happened, stamped on
            /// the *originating* device and preserved verbatim
            /// across the wire. Drives last-writer-wins
            /// reconciliation of the logical path *only*
            /// (content and deletes have their own clocks): a receiver adopts
            /// this path solely when `modified_at` is strictly newer than its
            /// own recorded path-change time. Never restamp it when forwarding
            /// or applying a peer's move. This is what makes a move performed
            /// while a peer was offline reconcile correctly on reconnect.
            modified_at: i64,
        },
        FileMetadataChanged {
            file_id: FileId,
            /// BLAKE3 hex digest of the file's new content. Like
            /// `FileMetadataAdded`, `FileMetadataChanged` is metadata-only and
            /// carries no bytes; the receiver pulls them over a separate
            /// transfer. See `FileMetadataAdded::content_hash`.
            content_hash: String,
            /// The file's new content size in bytes, read at hash time.
            size: u64,
        },
        FileDeleted {
            file_id: FileId,
            /// The unix-millis wall-clock time the file was deleted, stamped on
            /// the originating device and preserved across the wire. Drives
            /// last-writer-wins against a file's latest version `observed_at`:
            /// an edit made after the delete resurrects the file. Never restamp
            /// it when applying a peer's delete.
            deleted_at: i64,
        },
        /// Un-delete a previously soft-deleted file — a user-initiated restore
        /// from the deleted-files view.
        ///
        /// Semantically this is a *version bump*, not a distinct kind of edit:
        /// it re-announces the file's latest known version and clears the
        /// tombstone. It is metadata-only, like `FileMetadataChanged` — the
        /// receiver pulls the bytes over a separate transfer only into the
        /// directories that want them.
        ///
        /// The originating device only emits this after confirming the bytes
        /// are still recoverable somewhere (its own `keep_deleted_files` vault
        /// or a peer that still holds them); a restore with no available source
        /// fails locally and is never announced. Receivers therefore treat it
        /// as authoritative — the version exists in the network.
        FileRestored {
            file_id: FileId,
            /// BLAKE3 hex digest of the version being restored (the file's
            /// latest known version at restore time). Keys the byte pull and is
            /// recorded as the restored version, exactly like
            /// `FileMetadataChanged::content_hash`.
            content_hash: String,
            /// The restored version's content size in bytes.
            size: u64,
            /// The unix-millis wall-clock time the restore happened, stamped on
            /// the originating device and preserved across the wire. Recorded
            /// as the restored version's `observed_at` so it beats
            /// any lingering peer `deleted_at` under
            /// last-writer-wins — this is what makes the
            /// un-delete win reconciliation against a peer still holding the
            /// tombstone. Never restamp it when applying a peer's restore.
            restored_at: i64,
        },
        // Tag-mutation variants each carry `modified_at`: the unix-millis
        // wall-clock time stamped on the *originating* device. It is preserved
        // verbatim as the change propagates and drives last-writer-wins
        // reconciliation of tag state. Receivers must never restamp it.
        TagAdded {
            tag_id: TagId,
            tag_name: String,
            color: String,
            metadata: Option<MetadataFormat>,
            modified_at: i64,
        },
        TagRenamed {
            tag_id: TagId,
            tag_name: String,
            modified_at: i64,
        },
        /// Set a tag's color. Like every other mutation variant, it carries the
        /// complete new value of the field it changes (there is no partial /
        /// keep-existing semantics anywhere in the protocol). Mirrors
        /// `TagRenamed` for the color field.
        TagRecolored {
            tag_id: TagId,
            color: String,
            modified_at: i64,
        },
        TagChanged {
            tag_id: TagId,
            metadata: Option<MetadataValues>,
            modified_at: i64,
        },
        TagRemoved {
            tag_id: TagId,
            /// The unix-millis delete time. A tag reuses its `modified_at` as
            /// its single last-writer-wins clock, so the delete carries a
            /// timestamp here (stored into `modified_at`) rather than a
            /// separate `deleted_at`. A newer rename/recolor
            /// resurrects the tag. Never restamp it when applying a
            /// peer's delete.
            modified_at: i64,
        },
        FileTagged {
            file_id: FileId,
            tag_id: TagId,
            metadata: Option<MetadataValues>,
            modified_at: i64,
        },
        FileTagChanged {
            file_id: FileId,
            tag_id: TagId,
            metadata: Option<MetadataValues>,
            modified_at: i64,
        },
        FileUntagged {
            file_id: FileId,
            tag_id: TagId,
            modified_at: i64,
        },
        TagTagged {
            taggee_id: TagId,
            tag_id: TagId,
            metadata: Option<MetadataValues>,
            modified_at: i64,
        },
        TagTagChanged {
            taggee_id: TagId,
            tag_id: TagId,
            metadata: Option<MetadataValues>,
            modified_at: i64,
        },
        TagUntagged {
            taggee_id: TagId,
            tag_id: TagId,
            modified_at: i64,
        },
    }

    /// One file's full version history as announced in a `Sync::Manifest`.
    ///
    /// `history` is ordered oldest-to-newest: `history[0]` is `version_number`
    /// 1, the last entry is the current version. Each entry pairs the
    /// per-file monotonic `version_number` with the BLAKE3 `content_hash` that
    /// was recorded for it.
    ///
    /// `latest_observed_at` is the wall-clock timestamp (unix millis) of the
    /// latest version on the announcing side. The receiver uses it only as a
    /// tiebreaker when histories have diverged (neither side's latest hash
    /// appears in the other's history).
    ///
    /// `logical_path` carries the file's placement identity, paired with
    /// `logical_path_modified_at` as its last-writer-wins clock. It serves two
    /// reconciliation cases:
    ///   * a file the receiver has never seen (the offline-creation catch-up
    ///     case): the path is used to *place* it. Without it, connect-time
    ///     reconciliation would only work for files both sides already know
    ///     locally — a file created while the peers were disconnected would be
    ///     stranded until re-announced via a live `Change::FileMetadataAdded`.
    ///   * a file the receiver already knows that was *moved* while offline:
    ///     the receiver adopts this path when `logical_path_modified_at` beats
    ///     its own recorded time. Without the timestamp there would be no safe
    ///     way to tell a stale peer path from a newer one, so offline moves
    ///     would never reconcile (they'd only propagate via a live
    ///     `Change::FileMoved` while both peers were connected).
    ///
    /// Tags are deliberately **not** carried here: they are authoritatively
    /// reconciled via [`Sync::TagManifest`] / [`RelationshipManifestEntry`]
    /// (which are LWW with `modified_at`). Duplicating the file→tag edges
    /// in this manifest would create a second, unversioned source of truth
    /// that could resurrect stale associations. When a file's tags arrive
    /// (whether before or after the bytes materialize), the local
    /// `FileTagged` handler runs `reconcile_tag_placement`, which re-places
    /// the file into any newly-matching TagBased sync directories using the
    /// already-materialized bytes as a source. This gives us the desired
    /// order-independence without enforcing manifest ordering.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ManifestEntry {
        pub file_id: FileId,
        /// Oldest-to-newest `(version_number, content_hash, size)` triples.
        /// `size` is the version's content size in bytes.
        pub history: Vec<(i64, String, i64)>,
        pub latest_observed_at: i64,
        pub logical_path: LogicalPath,
        /// The unix-millis time `logical_path` was last changed — the path's
        /// last-writer-wins clock (see `Change::FileMoved::modified_at`). The
        /// receiver adopts this entry's `logical_path` for a file it already
        /// knows only when this timestamp is strictly newer than its own
        /// recorded path-change time. For a file the receiver has never seen it
        /// is simply carried through to the initial placement. This is separate
        /// from content (`history` / `latest_observed_at`) and deletes
        /// (`deleted_at`); a move and an edit reconcile independently.
        pub logical_path_modified_at: i64,
        /// Soft-delete tombstone state. When `deleted` is true this entry
        /// advertises a deletion: the receiver applies it (removing/hiding the
        /// file) unless it holds a version whose `observed_at` beats
        /// `deleted_at`, or an explicit restore whose `restored_at` beats it
        /// (three-way last-writer-wins).
        pub deleted: bool,
        /// The unix-millis time the file was deleted (0 when not deleted).
        pub deleted_at: i64,
        /// The unix-millis time the file was last explicitly restored (0 when
        /// never restored). Advertised so a peer that deleted the file while
        /// offline can be out-voted by our restore under last-writer-wins,
        /// without either side fabricating a content version. Symmetric to
        /// `deleted_at`.
        pub restored_at: i64,
    }

    /// What a tag relationship attaches a tag to. Mirrors the daemon's
    /// `EntryType` (`File = 0`, `Tag = 1`) but lives in the wire crate so the
    /// protocol does not depend on the daemon's database types. The `target_id`
    /// it accompanies is a stringified `FileId` or `TagId` accordingly.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub enum RelationshipKind {
        File,
        Tag,
    }

    /// A tag *definition* as advertised in a tag manifest: just its id and the
    /// last-writer-wins timestamp. Unlike files, tags carry no version chain —
    /// reconciliation compares `modified_at` and requests the full definition
    /// when the peer's is newer (or unknown locally). The lightweight
    /// advertise-then-request split mirrors file reconciliation and leaves room
    /// for tag payloads (metadata) to grow without bloating every manifest.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TagManifestEntry {
        pub tag_id: TagId,
        pub modified_at: i64,
        /// Soft-delete tombstone state. When true, the tag is deleted; the
        /// receiver applies the tombstone directly (no `TagRequest` follow-up)
        /// when this entry's `modified_at` is newer than its own — a tag's
        /// delete bumps `modified_at`, so the existing LWW comparison decides
        /// delete-vs-edit.
        pub deleted: bool,
    }

    /// A tag *relationship* (file-tagged or tag-tagged) as advertised in a tag
    /// manifest. `deleted` carries the soft-delete state so that an "absent"
    /// (untagged) relationship can win last-writer-wins against a peer's stale
    /// "present" — the tombstone is part of the reconcilable state.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RelationshipManifestEntry {
        pub tag_id: TagId,
        /// Stringified `FileId`/`TagId` per `kind`.
        pub target_id: String,
        pub kind: RelationshipKind,
        pub modified_at: i64,
        pub deleted: bool,
    }

    /// Reconciliation messages exchanged between peers, independent of the
    /// live `Change` stream.
    ///
    /// Flow at connection time:
    /// 1. After the public-key handshake, both sides send their `Manifest`
    ///    unprompted.
    /// 2. The receiver compares each entry against its local `file_versions`
    ///    table. For entries where it determines the peer has bytes it doesn't,
    ///    it opens a **content-addressed receive** for `(file_id,
    ///    content_hash)` and pulls each chunk with a [`Sync::ChunkRequest`].
    ///    There is no request/response session: bytes always move over the
    ///    content-addressed chunk protocol below, and
    ///    `Change::FileMetadataAdded`/`FileMetadataChanged` are metadata-only
    ///    announcements that carry no content.
    ///
    /// # Content-addressed chunk transfer
    ///
    /// There is one byte-movement mechanism. A chunk is treated as **pure
    /// content, not a message**: its identity *is* `(file_id, content_hash,
    /// offset)`, and because chunking is deterministic (offset a multiple of
    /// `CHUNK_SIZE`, canonical length `min(CHUNK_SIZE, size - offset)` derived
    /// from the version's authoritative size) that key denotes one exact,
    /// bit-identical byte range on every peer whose copy hashes to
    /// `content_hash`.
    ///
    /// Consequences:
    /// - **No correlation id on the wire.** A [`Sync::ChunkData`] reply is
    ///   matched to a pending request purely by `(file_id, content_hash,
    ///   offset)`. Duplicate replies for the same key are bit-identical; take
    ///   the first, drop the rest.
    /// - **Relays coalesce and multicast.** A relay keeps a content-keyed
    ///   waiter set and forwards a single upstream fetch out to all downstream
    ///   waiters for that key, holding no bytes itself.
    /// - **Discovery folds in.** A `ChunkRequest` *is* the probe: the first
    ///   chunk floods across neighbours; whichever direction returns
    ///   [`Sync::ChunkData`] establishes the route. Even the restore
    ///   availability probe is an offset-0 `ChunkRequest` whose bytes are
    ///   discarded.
    ///
    /// Integrity is **end-to-end**: only the origin receiver verifies the
    /// accumulated BLAKE3 against `content_hash` once the whole file has
    /// arrived. Relays verify nothing (they hold none of the bytes).
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Sync {
        Manifest {
            entries: Vec<ManifestEntry>,
        },

        /// Ask any holder for the canonical chunk of `file_id`/`content_hash`
        /// beginning at `offset`. `offset` MUST be a multiple of the wire-fixed
        /// `CHUNK_SIZE`. There is no `request_id`: the tuple `(file_id,
        /// content_hash, offset)` is both the request's identity and the
        /// reply's routing key. A holder answers [`Sync::ChunkData`] if its
        /// local copy verifies against `content_hash`, else [`Sync::ChunkMiss`];
        /// a relay forwards it to neighbours and fans the reply back to all
        /// waiters for the key.
        ChunkRequest {
            file_id: FileId,
            content_hash: String,
            offset: u64,
        },
        /// The canonical bytes at `offset` for `file_id`/`content_hash`. Length
        /// is `min(CHUNK_SIZE, size - offset)`, derivable identically by every
        /// node, so it is not carried on the wire. The receiver terminates when
        /// it has written the version's authoritative size; there is no
        /// per-chunk EOF flag (a zero-length file is one request at `offset = 0`
        /// returning empty `bytes`).
        ChunkData {
            file_id: FileId,
            content_hash: String,
            offset: u64,
            bytes: Vec<u8>,
        },
        /// This direction cannot (any longer) serve `(file_id, content_hash,
        /// offset)`: the node lacks the content and every upstream it forwarded
        /// to also missed, or it once held it but the file changed. Carries no
        /// reason; the receiver reacts structurally (a chunk missing from *all*
        /// directions fails the receive).
        ChunkMiss {
            file_id: FileId,
            content_hash: String,
            offset: u64,
        },

        /// Tag reconciliation, sent unprompted right after `Manifest` at
        /// connection time (and driving offline catch-up the same way).
        ///
        /// Unlike file reconciliation (which pulls bytes over a transfer), tag
        /// definitions are small, so they use an explicit request/response:
        /// 1. Each side sends its `TagManifest` (lightweight: per-tag id +
        ///    `modified_at`, plus every relationship as a full
        ///    `RelationshipManifestEntry`).
        /// 2. For each *definition* whose `modified_at` is newer than ours (or
        ///    that we don't know), the receiver replies with `TagRequest`.
        ///    Relationships carry their whole state in the manifest, so they
        ///    are applied directly by last-writer-wins with no request needed.
        /// 3. The peer answers each `TagRequest` with a `Change::TagAdded`
        ///    carrying the full current definition (name/color/metadata +
        ///    `modified_at`), re-using the live wire format. If the tag no
        ///    longer exists locally it answers `TagNotFound`.
        TagManifest {
            definitions: Vec<TagManifestEntry>,
            relationships: Vec<RelationshipManifestEntry>,
        },
        TagRequest {
            tag_id: TagId,
        },
        TagNotFound {
            tag_id: TagId,
        },
    }

    /// Top-level wire message wrapper. Every WebSocket text frame between
    /// peers, after the initial plaintext-public-key handshake, is a JSON
    /// `Frame`.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Frame {
        Change(Change),
        Sync(Sync),
    }
}

/// A file as presented to the UI: its id, managed relative path, and the
/// content hash + number of its latest recorded version.
///
/// Produced by `FileDatabase::get_all_files` and returned by the UI-facing
/// read API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub file_id: FileId,
    /// The file's logical path: its human-readable identity (possibly nested,
    /// e.g. `foo/bar/name.txt`), independent of where any sync directory stores
    /// the bytes on disk. Mirrors `FileDatabase.files.logical_path`.
    pub logical_path: LogicalPath,
    pub content_hash: String,
    pub version_number: i64,
    /// The latest version's content size in bytes.
    pub size: u64,
    /// Number of leading characters of `file_id` (in its canonical simple-hex
    /// form) needed to uniquely identify this file among all files known at the
    /// time the listing was produced — the "short id" length, à la `jj`/`git`.
    ///
    /// This is a display hint only: it is computed on read and is not stable
    /// across concurrent inserts. Consumers highlight
    /// `file_id[..short_id_length]` and dim the remainder.
    pub short_id_length: usize,
    /// Whether the file is soft-deleted (tombstoned). Always `false` when the
    /// row was fetched under `DeletedRule::Exclude` (the default). Under
    /// `DeletedRule::Include` this may be `true`, letting the caller
    /// distinguish live from tombstoned rows in a mixed result set (and letting
    /// the UI render a "deleted" badge).
    pub deleted: bool,
}

macro_rules! make_id_type {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn from_string(uuid: &str) -> Option<Self> {
                // Accepts both the simple (32 hex chars) and hyphenated forms,
                // so ids typed or pasted in either shape parse correctly.
                Some(Self(Uuid::try_from(uuid).ok()?))
            }

            pub fn to_string(&self) -> String {
                // Render in the same simple hex form we persist (see `ToSql`),
                // so displayed ids match what's stored and what the short-id
                // prefix logic operates on.
                self.0.simple().to_string()
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                // Persist the UUID in its *simple* form: 32 hex characters, no
                // hyphens (e.g. `7f3a1b2c...` rather than `7f3a-1b2c-...`).
                //
                // This is the canonical on-disk id format. It is chosen so that
                // ids sort and prefix-match cleanly as plain hex strings, which
                // is what the short-id ("shorten"/"resolve") machinery relies on
                // — a hex prefix never straddles a hyphen. `FromSql` still
                // accepts both hyphenated and simple forms, so reads remain
                // backwards compatible; only new writes use this form.
                Ok(self.0.simple().to_string().into())
            }
        }

        impl FromSql for $name {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                // FIX: Don't unwrap.
                Ok(Self(Uuid::try_from(value.as_str()?).unwrap()))
            }
        }
    };
}

make_id_type!(FileId);
make_id_type!(PreviewId);
make_id_type!(TagId);

/// A file's **logical** path: its human-readable identity within onisync's
/// namespace (possibly nested, e.g. `foo/bar/name.txt`). This is what is shown
/// to users, advertised to peers, and stored in the main `FileDatabase`
/// (`files.logical_path`). It is independent of where any individual sync
/// directory stores the bytes on disk.
///
/// Deliberately *not* interchangeable with [`PhysicalPath`]: the only way to
/// obtain a `LogicalPath` from a `PhysicalPath` is
/// [`PhysicalPath::into_logical`] (the ingestion boundary), and the only way to
/// obtain a `PhysicalPath` from a `LogicalPath` is a `SyncType`-aware placement
/// decision that lives in the `onisync` crate (`physical_for`). Keeping them
/// distinct makes the logical-vs-physical confusion a compile error rather than
/// a convention.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct LogicalPath(String);

/// A file's **physical** path: where its bytes live on disk *relative to a
/// particular sync directory's root*. For a `TagBased` directory this equals
/// the logical path; for a `Universal` directory it is the file's `file_id`
/// (files are stored under their id on disk). It also serves as the reverse
/// index for filesystem events (path -> file_id), so it must always reflect the
/// actual on-disk name. Stored in `SyncDirectoryDatabase`
/// (`files.physical_path`).
///
/// See [`LogicalPath`] for why the two are not interchangeable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct PhysicalPath(String);

impl LogicalPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl PhysicalPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    /// The single blessed **ingestion** conversion: a concrete on-disk relative
    /// path becomes a file's logical identity. This is the only way to turn a
    /// `PhysicalPath` into a `LogicalPath`, and is appropriate exactly when a
    /// file first enters onisync's namespace from disk (upload/add, or a move
    /// *into* a sync directory), where the physical location *defines* the
    /// logical path.
    pub fn into_logical(self) -> LogicalPath {
        LogicalPath(self.0)
    }
}

impl std::fmt::Display for LogicalPath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::fmt::Display for PhysicalPath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl ToSql for LogicalPath {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.0.as_str().into())
    }
}

impl FromSql for LogicalPath {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Self(value.as_str()?.to_owned()))
    }
}

impl ToSql for PhysicalPath {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.0.as_str().into())
    }
}

impl FromSql for PhysicalPath {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(Self(value.as_str()?.to_owned()))
    }
}
