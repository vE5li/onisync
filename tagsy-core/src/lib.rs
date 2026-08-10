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
pub const PROTOCOL_VERSION: u32 = 2;

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

    #[derive(Debug, thiserror::Error)]
    pub enum QueryError {
        #[error("not found")]
        NotFound,
        #[error("wrong type")]
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

    use rusqlite::ToSql;
    use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef};
    use serde::{Deserialize, Serialize};

    use crate::tag::{MetadataFormat, MetadataValues};
    use crate::{FileId, LogicalPath, Preview, TagId};

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

    /// What a tag relationship attaches a tag to. The `target_id` it
    /// accompanies is a stringified [`FileId`] or [`TagId`] accordingly.
    ///
    /// This is both the wire representation and the storage representation:
    /// the daemon persists it as the `type` column of `entries_v1`, encoded as
    /// `File = 0` / `Tag = 1` by the [`ToSql`]/[`FromSql`] impls below. Those
    /// two integers are an on-disk format and must not be reassigned.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub enum RelationshipKind {
        File,
        Tag,
    }

    impl ToSql for RelationshipKind {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            match self {
                RelationshipKind::File => Ok(0.into()),
                RelationshipKind::Tag => Ok(1.into()),
            }
        }
    }

    impl FromSql for RelationshipKind {
        fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
            match value.as_i64()? {
                0 => Ok(Self::File),
                1 => Ok(Self::Tag),
                invalid => panic!("invalid relationship kind {}", invalid),
            }
        }
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
        /// local copy verifies against `content_hash`, else
        /// [`Sync::ChunkMiss`]; a relay forwards it to neighbours and
        /// fans the reply back to all waiters for the key.
        ChunkRequest {
            file_id: FileId,
            content_hash: String,
            offset: u64,
        },
        /// The canonical bytes at `offset` for `file_id`/`content_hash`. Length
        /// is `min(CHUNK_SIZE, size - offset)`, derivable identically by every
        /// node, so it is not carried on the wire. The receiver terminates when
        /// it has written the version's authoritative size; there is no
        /// per-chunk EOF flag (a zero-length file is one request at `offset =
        /// 0` returning empty `bytes`).
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

        /// Ask any holder for the canonical preview of `file_id` at exactly
        /// `content_hash`. Like tag reconciliation (and unlike chunk transfers)
        /// this is a small, explicit request/response with no byte-streaming:
        /// the whole preview fits in one reply.
        ///
        /// The `content_hash` is part of the request identity: a holder answers
        /// [`Sync::PreviewData`] **only** if it can produce a preview of that
        /// exact content, else [`Sync::PreviewMiss`]. A holder still on an
        /// older (or newer) version than the requested hash therefore
        /// misses rather than substituting a preview of different
        /// bytes.
        ///
        /// Previews are deterministic *in kind* but not required to be
        /// byte-identical across peers (image encoders may differ by library
        /// version); any valid preview of the requested content is acceptable,
        /// so the first responder wins and later duplicates are dropped.
        PreviewRequest {
            file_id: FileId,
            content_hash: String,
        },
        /// The canonical preview of `file_id` at `content_hash`. The requester
        /// caches it keyed by `(file_id, content_hash)` and ignores any further
        /// replies for the same key.
        PreviewData {
            file_id: FileId,
            content_hash: String,
            preview: Preview,
        },
        /// This direction cannot serve a preview of `(file_id, content_hash)`:
        /// it lacks the content locally (only metadata-known) and every
        /// upstream it forwarded to also missed. A key missing from
        /// *all* directions resolves the request as [`Preview::None`]
        /// to the caller.
        PreviewMiss {
            file_id: FileId,
            content_hash: String,
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

/// A lightweight preview of a file's content, keyed elsewhere by `(file_id,
/// content_hash)`. Crosses both the peer wire (in [`Sync::PreviewData`]) and
/// the UI-facing API boundary, so it lives in `tagsy-core`.
///
/// A preview is always one of: a tiny low-resolution image, a short text
/// snippet, or nothing (the content is un-previewable — binary/video/etc, or
/// generation failed). The `None` variant is a *cacheable* result: it means
/// "there is no preview for this content", not "unknown".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Preview {
    /// A small, low-resolution raster preview. `bytes` is a fully-encoded image
    /// (e.g. WebP/PNG) — self-describing, decoded directly by the UI — and
    /// `width`/`height` are its pixel dimensions for layout hints.
    Image {
        bytes: Vec<u8>,
        width: u32,
        height: u32,
    },
    /// A short UTF-8 snippet from the start of a text file, already truncated
    /// on a character boundary and sanitized. Bounded to a few hundred
    /// bytes.
    Text(String),
    /// The content has no preview (un-previewable type, or generation failed).
    /// A cacheable negative result.
    None,
}

/// A file as presented to the UI: its id, managed relative path, and the
/// content hash + number of its latest recorded version.
///
/// Produced by `CatalogStore::get_all_files` and returned by the UI-facing
/// read API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub file_id: FileId,
    /// The file's logical path: its human-readable identity (possibly nested,
    /// e.g. `foo/bar/name.txt`), independent of where any sync directory stores
    /// the bytes on disk. Mirrors `CatalogStore.files.logical_path`.
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

/// A file's **logical** path: its human-readable identity within tagsy's
/// namespace (possibly nested, e.g. `foo/bar/name.txt`). This is what is shown
/// to users, advertised to peers, and stored in the main `CatalogStore`
/// (`files.logical_path`). It is independent of where any individual sync
/// directory stores the bytes on disk.
///
/// Deliberately *not* interchangeable with [`PhysicalPath`]: the only way to
/// obtain a `LogicalPath` from a `PhysicalPath` is
/// [`PhysicalPath::into_logical`] (the ingestion boundary), and the only way to
/// obtain a `PhysicalPath` from a `LogicalPath` is a `SyncType`-aware placement
/// decision that lives in the `tagsy` crate (`physical_for`). Keeping them
/// distinct makes the logical-vs-physical confusion a compile error rather than
/// a convention.
///
/// # Containment invariant
///
/// A `LogicalPath` is **always a safe relative path** — see
/// [`validate_relative_path`]. This is enforced on every path by which an
/// untrusted value can enter the process: [`Deserialize`] (peer wire data) and
/// [`FromSql`] (a database row, which may have been written by an older,
/// unvalidating build). Upholding it here is what makes
/// `sync_directory.path.join(physical_path)` in the daemon sound: without it a
/// peer could advertise `/etc/cron.d/x` or `../../.bashrc` and, because
/// `Path::join` *replaces* the base when handed an absolute path, steer daemon
/// reads and writes anywhere on the filesystem.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct LogicalPath(String);

/// A file's **physical** path: where its bytes live on disk *relative to a
/// particular sync directory's root*. For a `TagBased` directory this equals
/// the logical path; for a `Universal` directory it is the file's `file_id`
/// (files are stored under their id on disk). It also serves as the reverse
/// index for filesystem events (path -> file_id), so it must always reflect the
/// actual on-disk name. Stored in `DirectoryIndex`
/// (`files.physical_path`).
///
/// See [`LogicalPath`] for why the two are not interchangeable, and for the
/// containment invariant both types uphold.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct PhysicalPath(String);

/// Why a string was rejected as a relative path. See
/// [`validate_relative_path`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RelativePathError {
    #[error("path is empty")]
    Empty,
    #[error("path is absolute")]
    Absolute,
    #[error("path contains a `..` component")]
    ParentDirComponent,
    #[error("path contains a `.` component")]
    CurrentDirComponent,
    #[error("path contains an empty component (`//`)")]
    EmptyComponent,
    #[error("path contains a NUL byte")]
    InteriorNul,
}

/// Check that `path` is a *safe relative path*: one that, when joined onto a
/// sync-directory root, cannot escape it.
///
/// Enforced rules, and why each exists:
///
/// - **Non-empty.** An empty path joins to the root itself, turning a file
///   operation into an operation on the whole sync directory.
/// - **Not absolute.** [`std::path::Path::join`] *discards the base* when its
///   argument is absolute, so `root.join("/etc/passwd")` is `/etc/passwd`. This
///   is the single most dangerous case and is easy to miss when reading the
///   call site.
/// - **No `..` component.** Classic traversal; `root/../..` escapes upward.
/// - **No `.` component and no empty component.** Neither escapes on its own,
///   but both let the same file be spelled many ways (`a/b`, `a/./b`, `a//b`).
///   Since `physical_path` doubles as the reverse index for filesystem events
///   and as a uniqueness key (`physical_path_in_use_by_other`), non-canonical
///   spellings would let one file masquerade as two. Rejecting is simpler than
///   normalizing.
/// - **No NUL byte.** Paths cross into C APIs as NUL-terminated strings; an
///   interior NUL silently truncates, so `"safe.txt\0/../../etc/passwd"` could
///   pass a naive string check and then address a different file.
///
/// A single trailing slash is tolerated and ignored, matching
/// [`LogicalPath::basename`], which documents `foo/bar/` as yielding `bar`.
///
/// # What is deliberately *not* rejected
///
/// A leading `-` on a component (`-rf.txt`). It is tempting to forbid, since
/// such a name looks like an option flag to any tool that receives it
/// positionally. But it is a **legal Linux filename**, and rejecting it here
/// would be unsound in a subtler way: local ingestion builds paths through the
/// infallible [`LogicalPath::new`], so a user's own `-rf.txt` would be accepted
/// on write and then rejected by [`FromSql`] on read, permanently breaking that
/// row. Argument-injection risk belongs to whoever spawns a process, and is
/// handled there — the external editor launcher passes an absolute path, which
/// cannot begin with `-`.
///
/// The rules that *are* enforced above are all things a filesystem walk of a
/// sync directory can never produce, so no such asymmetry arises for them.
pub fn validate_relative_path(path: &str) -> Result<(), RelativePathError> {
    if path.is_empty() {
        return Err(RelativePathError::Empty);
    }
    if path.contains('\0') {
        return Err(RelativePathError::InteriorNul);
    }
    if path.starts_with('/') {
        return Err(RelativePathError::Absolute);
    }

    // Tolerate exactly one trailing slash (`foo/bar/`) so the documented
    // `basename` behavior keeps working, then split the rest strictly.
    let trimmed = path.strip_suffix('/').unwrap_or(path);
    if trimmed.is_empty() {
        return Err(RelativePathError::Empty);
    }

    for component in trimmed.split('/') {
        match component {
            "" => return Err(RelativePathError::EmptyComponent),
            "." => return Err(RelativePathError::CurrentDirComponent),
            ".." => return Err(RelativePathError::ParentDirComponent),
            _ => {}
        }
    }

    Ok(())
}

/// Deserialize a validated relative path, or fail with the validation error.
///
/// Used by the [`Deserialize`] impls of both path newtypes. Peer wire data is
/// the primary untrusted source, and failing here is fail-closed: the frame is
/// rejected and the connection torn down rather than a bad path reaching the
/// filesystem.
fn deserialize_relative_path<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    let raw = String::deserialize(deserializer)?;
    validate_relative_path(&raw)
        .map_err(|error| D::Error::custom(format!("invalid path {raw:?}: {error}")))?;
    Ok(raw)
}

/// Read a validated relative path from a database row.
///
/// Rows written by an older build predate [`validate_relative_path`], so this
/// is not merely paranoia: it stops a value that was persisted before the check
/// existed from being trusted now.
fn column_relative_path(value: ValueRef<'_>) -> FromSqlResult<String> {
    let raw = value.as_str()?.to_owned();
    validate_relative_path(&raw)
        .map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))?;
    Ok(raw)
}

impl<'de> Deserialize<'de> for LogicalPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_relative_path(deserializer).map(Self)
    }
}

impl<'de> Deserialize<'de> for PhysicalPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_relative_path(deserializer).map(Self)
    }
}

impl LogicalPath {
    /// Construct from a **trusted, locally derived** path — one produced by
    /// this process from a filesystem walk, a `FileId`, or an operator-supplied
    /// CLI argument.
    ///
    /// Deliberately infallible, and deliberately *not* the way untrusted data
    /// enters: peer wire data goes through [`Deserialize`] and database rows
    /// through [`FromSql`], both of which enforce
    /// [`validate_relative_path`]. Use [`Self::try_new`] when the input's
    /// provenance is uncertain.
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    /// Construct from a path of uncertain provenance, enforcing
    /// [`validate_relative_path`].
    pub fn try_new(path: impl Into<String>) -> Result<Self, RelativePathError> {
        let path = path.into();
        validate_relative_path(&path)?;
        Ok(Self(path))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    /// The final `/`-separated component of the logical path, ignoring any
    /// trailing slashes and empty segments — so `foo/bar/baz.txt` yields
    /// `baz.txt`, `foo/bar/` yields `bar`, and an all-empty path yields `""`.
    ///
    /// This is the "file name" as far as OS-level tools (editors, share
    /// sheets) are concerned: the extension it carries determines MIME/type
    /// dispatch on both Linux and Android. Used when materializing a file
    /// into a caller-visible temp path so the on-disk name matches the user's
    /// mental model rather than an opaque UUID.
    pub fn basename(&self) -> &str {
        self.0
            .rsplit('/')
            .find(|segment| !segment.is_empty())
            .unwrap_or("")
    }
}

impl PhysicalPath {
    /// Construct from a **trusted, locally derived** path. See
    /// [`LogicalPath::new`] for why this is infallible and where validation
    /// actually happens.
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    /// Construct from a path of uncertain provenance, enforcing
    /// [`validate_relative_path`].
    pub fn try_new(path: impl Into<String>) -> Result<Self, RelativePathError> {
        let path = path.into();
        validate_relative_path(&path)?;
        Ok(Self(path))
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
    /// file first enters tagsy's namespace from disk (upload/add, or a move
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
        column_relative_path(value).map(Self)
    }
}

impl ToSql for PhysicalPath {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(self.0.as_str().into())
    }
}

impl FromSql for PhysicalPath {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        column_relative_path(value).map(Self)
    }
}

#[cfg(test)]
mod relative_path_tests {
    use super::*;

    #[test]
    fn accepts_ordinary_relative_paths() {
        for path in [
            "a.txt",
            "photos/cat.jpg",
            "a/b/c/d.txt",
            "foo/bar/",
            "weird name with spaces.txt",
            "trailing-dash-inside-name.txt",
            "8f14e45fceea167a5a36dedd4bea2543",
        ] {
            assert!(
                validate_relative_path(path).is_ok(),
                "expected {path:?} to be accepted"
            );
        }
    }

    #[test]
    fn rejects_absolute_paths() {
        // The critical case: `Path::join` discards the base for these.
        assert_eq!(
            validate_relative_path("/etc/passwd"),
            Err(RelativePathError::Absolute)
        );
        assert_eq!(
            validate_relative_path("/"),
            Err(RelativePathError::Absolute)
        );
    }

    #[test]
    fn rejects_traversal() {
        assert_eq!(
            validate_relative_path(".."),
            Err(RelativePathError::ParentDirComponent)
        );
        assert_eq!(
            validate_relative_path("../../.bashrc"),
            Err(RelativePathError::ParentDirComponent)
        );
        assert_eq!(
            validate_relative_path("a/../../b"),
            Err(RelativePathError::ParentDirComponent)
        );
        // `..` only as a whole component; a filename may contain dots.
        assert!(validate_relative_path("a/..b/c").is_ok());
        assert!(validate_relative_path("..hidden.txt").is_ok());
    }

    #[test]
    fn rejects_non_canonical_spellings() {
        assert_eq!(
            validate_relative_path("a//b"),
            Err(RelativePathError::EmptyComponent)
        );
        assert_eq!(
            validate_relative_path("a/./b"),
            Err(RelativePathError::CurrentDirComponent)
        );
        assert_eq!(validate_relative_path(""), Err(RelativePathError::Empty));
        assert_eq!(
            validate_relative_path("/"),
            Err(RelativePathError::Absolute)
        );
    }

    #[test]
    fn rejects_interior_nul() {
        // Would truncate when handed to a C API, so the checked string and the
        // addressed file could differ.
        assert_eq!(
            validate_relative_path("safe.txt\0/../../etc/passwd"),
            Err(RelativePathError::InteriorNul)
        );
    }

    #[test]
    fn accepts_leading_dash_component() {
        // Legal Linux filenames. Rejecting them here would break the user's
        // own files on read-back; see `validate_relative_path`'s docs.
        assert!(validate_relative_path("-rf").is_ok());
        assert!(validate_relative_path("a/--config=evil").is_ok());
    }

    #[test]
    fn deserialize_rejects_hostile_peer_paths() {
        // The actual attack shape: a peer advertises a path that escapes the
        // sync root. Both newtypes must refuse to decode it at all.
        for hostile in ["/etc/cron.d/x", "../../.bashrc", "a//b", "x\0y"] {
            let json = serde_json::to_string(hostile).unwrap();
            assert!(
                serde_json::from_str::<LogicalPath>(&json).is_err(),
                "LogicalPath accepted {hostile:?}"
            );
            assert!(
                serde_json::from_str::<PhysicalPath>(&json).is_err(),
                "PhysicalPath accepted {hostile:?}"
            );
        }
    }

    #[test]
    fn deserialize_accepts_valid_paths_and_round_trips() {
        let path = LogicalPath::new("photos/cat.jpg");
        let json = serde_json::to_string(&path).unwrap();
        assert_eq!(json, "\"photos/cat.jpg\"");
        assert_eq!(serde_json::from_str::<LogicalPath>(&json).unwrap(), path);
    }

    #[test]
    fn try_new_matches_validator() {
        assert!(PhysicalPath::try_new("a/b.txt").is_ok());
        assert_eq!(
            PhysicalPath::try_new("/abs"),
            Err(RelativePathError::Absolute)
        );
    }
}
