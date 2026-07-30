use std::collections::BTreeSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use onisync_core::state::{RelationshipKind, RelationshipManifestEntry, TagManifestEntry};
use onisync_core::tag::MetadataFormat;
use onisync_core::{FileId, FileInfo, LogicalPath, PhysicalPath, TagId};
use rusqlite::types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef};
use rusqlite::{Connection, OptionalExtension, ToSql};
use serde::{Deserialize, Serialize};

/// A file's version history as `(version_number, content_hash, size)` triples
/// ordered oldest-to-newest. `size` is the version's content size in bytes.
/// Mirrors `state::ManifestEntry::history`.
pub type VersionHistory = Vec<(i64, String, i64)>;

/// One row of [`FileDatabase::manifest_entries`]: a file id, its full
/// [`VersionHistory`], the unix-millis timestamp of its latest version, the
/// file's logical path, the unix-millis time that path was last changed
/// (`logical_path_modified_at`, the path's LWW clock), and its soft-delete
/// tombstone state (`deleted`, `deleted_at`, `restored_at`).
/// Maps directly onto a `state::ManifestEntry`.
pub type ManifestRow = (
    FileId,
    VersionHistory,
    i64,
    LogicalPath,
    i64,
    bool,
    i64,
    i64,
);

/// A single recorded version of a file's content.
///
/// Rows in `file_versions` are append-only. The `version_number` is a per-file
/// monotonically increasing counter (starts at 1) that defines ordering between
/// versions of the same file. `observed_at` is the unix-millis wall-clock time
/// at which we recorded the version and is metadata only — do not use it for
/// ordering.
#[derive(Debug, Clone)]
pub struct FileVersion {
    pub file_id: FileId,
    pub content_hash: String,
    pub observed_at: i64,
    pub version_number: i64,
    pub origin: String,
    /// The version's content size in bytes. Read from disk (or the in-memory
    /// buffer) at the same time the content hash is computed.
    pub size: i64,
}

/// A file's soft-delete tombstone state: the materialized `deleted` flag plus
/// the two wall-clocks that (together with the latest version `observed_at`)
/// decide it under three-way last-writer-wins.
///
/// The file is live iff `max(latest observed_at, restored_at) > deleted_at`;
/// `deleted` caches that decision so reads keep a simple `WHERE deleted = 0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletionState {
    pub deleted: bool,
    /// Unix-millis of the last delete (0 if never deleted).
    pub deleted_at: i64,
    /// Unix-millis of the last explicit restore (0 if never restored).
    pub restored_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubtagRule {
    Include,
    Exclude,
}

/// Governs whether soft-deleted (tombstoned) rows are visible to a read.
///
/// Applies to `files_v2.deleted` and `tags_v1.deleted`. Relationship-level
/// tombstones (`entries_v1.deleted`) are always filtered — a search for
/// "deleted files" is about files whose own row is tombstoned, not files that
/// were merely untagged.
///
/// - [`Exclude`](DeletedRule::Exclude): behave as before — every read hides
///   tombstoned rows (`WHERE ... deleted = 0`). This is what every non-search
///   caller wants.
/// - [`Include`](DeletedRule::Include): drop that filter so live *and*
///   tombstoned rows come back together. Search callers that want to show
///   deleted rows to the user use this, then post-filter by the returned
///   `deleted` flag to keep only the tombstoned ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeletedRule {
    Include,
    Exclude,
}

impl DeletedRule {
    /// SQL fragment appended after other `WHERE` clauses to enforce the rule.
    /// `Exclude` yields `" AND deleted = 0"`; `Include` yields `""`.
    ///
    /// Callers embed this into a query that already has at least one `WHERE`
    /// clause so the leading `AND` is well-formed. When there is no prior
    /// clause, use [`Self::where_deleted_clause`] instead.
    fn and_deleted_clause(self) -> &'static str {
        match self {
            DeletedRule::Exclude => " AND deleted = 0",
            DeletedRule::Include => "",
        }
    }

    /// SQL fragment for a standalone `WHERE`: `" WHERE deleted = 0"` under
    /// `Exclude`, empty under `Include`.
    fn where_deleted_clause(self) -> &'static str {
        match self {
            DeletedRule::Exclude => " WHERE deleted = 0",
            DeletedRule::Include => "",
        }
    }
}

/// A single clause of a file search (see [`FileDatabase::file_ids_for_query`]).
///
/// Terms are combined conjunctively: a file matches a query only if it matches
/// *every* term. Each term corresponds to one parsed chunk from the API layer
/// (`api::chunk::Chunk`) after its tag references have been resolved to
/// [`TagId`] sets.
///
/// A single tag token can match a *set* of tags (every tag whose name contains
/// the token substring or whose id starts with it), so the tag-bearing terms
/// carry a resolved [`TagId`] set rather than a single id; the set is expanded
/// from the user's syntax in a higher layer (the daemon's API), keeping this
/// database primitive free of name/prefix-resolution concerns. An empty set
/// matches no tag (so `HasTag([])` matches no file and `NotTag([])` excludes
/// nothing).
///
/// The `Any`/`NotAny` variants correspond to a *prefix-less* chunk: the user
/// wrote `foo`, and it should match anything that could reasonably relate to
/// `foo` — its logical path contains `foo`, *or* it carries/subtags a tag
/// resolved from `foo`. These variants carry both the raw substring and the
/// resolved tag set so both sides of the disjunction can be evaluated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryTerm {
    /// Must carry *at least one* tag in this set.
    HasTag(Vec<TagId>),
    /// Must carry *none* of the tags in this set.
    NotTag(Vec<TagId>),
    /// The logical path of the file or name of the tag must contain the
    /// substring (case-insensitive).
    NameContains(String),
    /// The logical path of the file or name of the tag must *not* contain the
    /// substring (case-insensitive).
    NotNameContains(String),
    /// The logical path of the file must contain the substring
    /// (case-insensitive).
    LogicalContains(String),
    /// The logical path of the file must *not* contain the substring
    /// (case-insensitive).
    NotLogicalContains(String),
    /// Matches on *either* substring OR tag (union across
    /// both axes). The [`String`] is the substring; the [`Vec<TagId>`] is the
    /// resolved tag set for the same token. An empty tag set here does **not**
    /// mean "matches nothing" — the substring side still stands.
    AnyMatch(String, Vec<TagId>),
    /// Negation of [`AnyMatch`]: must match *neither* the substring nor any
    /// tag in the set.
    NotAnyMatch(String, Vec<TagId>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    File,
    Tag,
}

impl ToSql for EntryType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            EntryType::File => Ok(0.into()),
            EntryType::Tag => Ok(1.into()),
        }
    }
}

impl FromSql for EntryType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(Self::File),
            1 => Ok(Self::Tag),
            invalid => panic!("invalid entry type {}", invalid),
        }
    }
}

impl From<EntryType> for RelationshipKind {
    fn from(value: EntryType) -> Self {
        match value {
            EntryType::File => RelationshipKind::File,
            EntryType::Tag => RelationshipKind::Tag,
        }
    }
}

impl From<RelationshipKind> for EntryType {
    fn from(value: RelationshipKind) -> Self {
        match value {
            RelationshipKind::File => EntryType::File,
            RelationshipKind::Tag => EntryType::Tag,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Tag {
    pub id: TagId,
    pub name: String,
    pub color: String,
    pub metadata: Option<MetadataFormat>,
    /// Whether the tag is soft-deleted (tombstoned). Always `false` when the
    /// row was fetched under [`DeletedRule::Exclude`] (the default). Under
    /// [`DeletedRule::Include`] this may be `true`, letting the caller
    /// distinguish live from tombstoned rows in a mixed result set.
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatabaseError {
    UnableToOpenOrCreate,
    FailedToExecuteCommand,
    NonUtf8FilePath,
    MissingFile,
    MissingTag,
    InvalidTagName,
    InvalidColor,
    CantTagItself,
    /// A short-id prefix matched more than one row, so it cannot be resolved to
    /// a single id. Carries the ambiguous prefix that was queried.
    AmbiguousIdPrefix(String),
}

/// Current wall-clock time as unix milliseconds.
///
/// Used to stamp `modified_at` on locally-originated tag mutations. Peer
/// changes carry their own `modified_at` and must NOT be restamped with this
/// (that would let a receiver's clock override the last-writer-wins
/// comparison).
pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// Number of leading characters two strings share.
///
/// Operates on `char`s; ids are ASCII hex so this is equivalent to bytes.
fn common_prefix_length(a: &str, b: &str) -> usize {
    a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count()
}

/// Outcome of resolving a short-id prefix against an id column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixResolution {
    /// Exactly one id starts with the prefix.
    Unique(String),
    /// No id starts with the prefix.
    NotFound,
    /// More than one id starts with the prefix; resolution is ambiguous.
    Ambiguous,
}

/// Resolve a short-id `prefix` against `column` of `table`, returning whether
/// it identifies exactly one row.
///
/// This is the generic counterpart to shortening: given the fewest leading hex
/// characters a user typed, find the full id — or report that the prefix is
/// unknown or ambiguous. It backs every "accept a short id" command, so it is
/// deliberately id-type-agnostic (callers wrap it with a typed helper such as
/// [`FileDatabase::resolve_file_id_prefix`]).
///
/// Ids are stored in canonical simple-hex form, so a prefix match is a plain
/// string-prefix test. We fetch up to two matches: zero → `NotFound`, one →
/// `Unique`, two → `Ambiguous`. With the primary-key index on the id column
/// this is a bounded index range scan (`LIMIT 2`), not a full-table scan.
///
/// `prefix` **must** be validated as lowercase hex by the caller (see
/// [`normalise_id_prefix`]); this keeps the `LIKE` pattern free of `%`/`_`
/// wildcards and the query injection-safe (the prefix is still bound as a
/// parameter; `table`/`column` are internal constants, never user input).
fn resolve_id_prefix(
    connection: &Connection,
    table: &str,
    column: &str,
    prefix: &str,
) -> Result<PrefixResolution, DatabaseError> {
    let pattern = format!("{prefix}%");
    let mut statement = connection
        .prepare(&format!(
            "SELECT {column} FROM {table} WHERE {column} LIKE ?1 ORDER BY {column} LIMIT 2"
        ))
        .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
    let matches: Vec<String> = statement
        .query_map([&pattern], |row| row.get::<_, String>(0))
        .map_err(|_| DatabaseError::FailedToExecuteCommand)?
        .collect::<Result<_, _>>()
        .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

    match matches.as_slice() {
        [] => Ok(PrefixResolution::NotFound),
        [only] => Ok(PrefixResolution::Unique(only.clone())),
        _ => Ok(PrefixResolution::Ambiguous),
    }
}

/// Normalise a user-supplied id or short-id into the canonical lowercase-hex
/// form used for prefix matching.
///
/// Accepts hyphenated UUIDs, full simple-hex ids, and short prefixes of either.
/// Hyphens are stripped (so a pasted full UUID resolves) and the result is
/// lowercased. Returns `None` if any remaining character is not a hex digit —
/// this both rejects junk early and guarantees the value is safe to splice into
/// a `LIKE` pattern (no wildcards).
pub fn normalise_id_prefix(input: &str) -> Option<String> {
    let cleaned: String = input
        .chars()
        .filter(|character| *character != '-')
        .collect();
    if cleaned.is_empty()
        || !cleaned
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return None;
    }
    Some(cleaned.to_ascii_lowercase())
}

#[derive(Debug)]
pub struct FileDatabase {
    connection: Connection,
}

impl FileDatabase {
    pub fn initialize(database_path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let connection =
            Connection::open(database_path).map_err(|_| DatabaseError::UnableToOpenOrCreate)?;

        // Run migrations here. Each `migrate_*_to_vN` runs before the matching
        // `create_*_vN` so a restored older-version backup is walked forward
        // through every intermediate version on startup (see AGENTS.md).
        Self::migrate_files_to_v2(&connection)?;

        Self::create_files_v2(&connection)?;
        Self::create_tags_v1(&connection)?;
        Self::create_entries_v1(&connection)?;
        Self::create_file_versions_v1(&connection)?;

        Ok(Self { connection })
    }

    /// Migrate the main catalog `files_v1` → `files_v2`, adding the
    /// `restored_at` clock.
    ///
    /// Frozen once shipped (see AGENTS.md): never edit this. If `files_v1`
    /// exists (an older backup restored on this build), create `files_v2` and
    /// copy every row across, seeding `restored_at = 0` (no explicit restore
    /// has happened), then drop `files_v1`. A no-op when `files_v1` is absent.
    fn migrate_files_to_v2(connection: &Connection) -> Result<(), DatabaseError> {
        let files_v1_exists: bool = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'files_v1'",
                [],
                |_| Ok(true),
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .unwrap_or(false);

        if !files_v1_exists {
            return Ok(());
        }

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS files_v2 (
                    id                        TEXT PRIMARY KEY,
                    logical_path              TEXT NOT NULL,
                    logical_path_modified_at  INTEGER NOT NULL,
                    deleted                   INTEGER NOT NULL,
                    deleted_at                INTEGER NOT NULL,
                    restored_at               INTEGER NOT NULL
                )",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        connection
            .execute(
                "INSERT INTO files_v2
                    (id, logical_path, logical_path_modified_at, deleted, deleted_at, restored_at)
                 SELECT id, logical_path, logical_path_modified_at, deleted, deleted_at, 0
                 FROM files_v1",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        connection
            .execute("DROP TABLE files_v1", ())
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    fn create_files_v2(connection: &Connection) -> Result<(), DatabaseError> {
        // `logical_path` is the file's logical identity: its human-readable
        // path/name (possibly nested, e.g. `foo/bar/name.txt`), independent
        // of where any individual sync directory stores the bytes on disk.
        // Contrast with `SyncDirectoryDatabase.files_v1.physical_path`, which
        // is the on-disk location within a particular sync directory.
        //
        // `deleted` is the soft-delete tombstone flag (0 = live, 1 = deleted).
        // It is a *materialized* view of the three-way last-writer-wins between
        // the delete, the latest content edit, and an explicit restore: the
        // file is live iff `max(latest file_versions_v1.observed_at,
        // restored_at) > deleted_at`. Keeping `deleted` as a stored flag
        // (maintained by `remove_file` / `restore_file`) lets every read keep
        // its simple `WHERE deleted = 0` filter; reconciliation deliberately
        // considers tombstoned rows so a delete can win over a stale peer.
        //
        // The three clocks that decide `deleted`:
        // - `deleted_at`: unix-millis wall clock stamped when the file was deleted,
        //   preserved across the wire.
        // - latest `file_versions_v1.observed_at`: a content edit newer than the delete
        //   resurrects the file (restore-after-edit).
        // - `restored_at`: unix-millis wall clock of an explicit user restore,
        //   preserved across the wire. A restore newer than the delete resurrects the
        //   file *without* fabricating a version. Symmetric to `deleted_at`; never
        //   restamp it when applying a peer's restore. 0 means "never explicitly
        //   restored".
        //
        // `logical_path_modified_at` is the unix-millis wall-clock time the
        // `logical_path` was last changed, stamped on the *originating*
        // device and preserved across the wire. It is the last-writer-wins
        // clock for the path *only* (content has its own clock via
        // `file_versions_v1.observed_at`; deletes use `deleted_at`; restores
        // use `restored_at`; tags are reconciled separately with their own
        // `modified_at`). It exists so a move made while a peer is offline
        // reconciles on reconnect: the manifest advertises this timestamp and
        // the receiver adopts the peer's path only when it is strictly newer.
        // Never restamp it when applying a peer's move. Do NOT fold other
        // metadata into this clock — a bare "modified" clock would let a
        // content edit silently override a path (they are independently
        // edited). See `Change::FileMoved`.
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS files_v2 (
                    id                        TEXT PRIMARY KEY,
                    logical_path              TEXT NOT NULL,
                    logical_path_modified_at  INTEGER NOT NULL,
                    deleted                   INTEGER NOT NULL,
                    deleted_at                INTEGER NOT NULL,
                    restored_at               INTEGER NOT NULL
                )",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    fn create_tags_v1(connection: &Connection) -> Result<(), DatabaseError> {
        // `name` is intentionally NOT `UNIQUE`: two devices editing offline
        // can each mint a tag with the same name but a different `TagId`.
        // When they reconcile, both tags must be able to coexist rather than
        // one insert failing a constraint and leaving the databases
        // divergent. Tag identity is the `TagId`; names are display-only and
        // may collide. (Disambiguation in the UI is handled by tag short-ids
        // — see roadmap pass 2.)
        //
        // `modified_at` is the unix-millis wall-clock time, stamped on the
        // *originating* device and preserved across the wire, that drives
        // last-writer-wins reconciliation of tag definitions. Never restamp
        // it when applying a peer's change.
        //
        // `deleted` is the soft-delete tombstone. Unlike files, a tag
        // already carries `modified_at` as its single last-writer-wins
        // clock, so a delete just sets `deleted = 1` and bumps `modified_at`
        // (mirroring the relationship tombstones in `untag_entry`) — no
        // separate `deleted_at` is needed. All live reads filter
        // `deleted = 0`; reconciliation considers tombstoned rows so a
        // delete can win LWW against a stale peer.
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS tags_v1 (
                    id          TEXT PRIMARY KEY,
                    name        TEXT NOT NULL,
                    color       TEXT NOT NULL,
                    metadata    TEXT,
                    modified_at INTEGER NOT NULL DEFAULT 0,
                    deleted     INTEGER NOT NULL
                )",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    fn create_entries_v1(connection: &Connection) -> Result<(), DatabaseError> {
        // `modified_at` drives last-writer-wins reconciliation of
        // relationships, exactly as for `tags_v1` above.
        //
        // `deleted` is a soft-delete flag (0 = live, 1 = tombstoned).
        // Untagging sets `deleted = 1` and bumps `modified_at` instead of
        // removing the row, so an "absent" relationship still carries a
        // timestamp and can win LWW against a stale "present" from a peer.
        // All *reads* of live relationships must filter `deleted = 0` (see
        // the read helpers below); reconciliation deliberately considers
        // tombstoned rows too. The `UNIQUE(tag_id, target_id, type)`
        // constraint is retained: a relationship reappears by flipping
        // `deleted` back to 0, never by inserting a duplicate row.
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS entries_v1 (
                    id          TEXT PRIMARY KEY,
                    tag_id      TEXT NOT NULL,
                    target_id   TEXT NOT NULL,
                    type        INTEGER,
                    modified_at INTEGER NOT NULL DEFAULT 0,
                    deleted     INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (tag_id, target_id, type)
                )",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    fn create_file_versions_v1(connection: &Connection) -> Result<(), DatabaseError> {
        // Append-only log of content hashes per file. The latest row per
        // `file_id` (highest `version_number`) is the current version.
        //
        // - `version_number` is a per-file monotonic counter starting at 1. It is what
        //   we order by; do not order by `observed_at`.
        // - `observed_at` is unix-millis wall-clock at insert time, kept for debugging
        //   / UI.
        // - `origin` is `'local'` for now. When cross-peer conflict resolution lands it
        //   will hold the originating peer's public key.
        // - `size` is the version's content size in bytes, read from disk (or the
        //   in-memory buffer) at the same time the content hash is computed. A 0-byte
        //   file stores `size = 0` — that is a real, known value, distinct from
        //   absence, which is why the column is `NOT NULL` with no default.
        //
        // Intentionally no `FOREIGN KEY` on `file_id`: a version may be
        // recorded by `SyncDirectoryManager` before the corresponding row in
        // `files_v1` exists (which is inserted later, asynchronously, by
        // `handle_changes`). The same ordering will apply when peer-
        // originated versions land. A FK here would fight the message-
        // passing architecture.
        //
        // TODO: When a file is deleted (`FileDatabase::remove_file`), we
        // currently leave its `file_versions_v1` rows behind as a history
        // audit trail. If/when that history grows unwieldy, add a cleanup
        // pass or make `remove_file` cascade the delete here.
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS file_versions_v1 (
                    file_id         TEXT    NOT NULL,
                    content_hash    TEXT    NOT NULL,
                    observed_at     INTEGER NOT NULL,
                    version_number  INTEGER NOT NULL,
                    origin          TEXT    NOT NULL,
                    size            INTEGER NOT NULL,
                    PRIMARY KEY (file_id, version_number)
                )",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_file_versions_v1_latest
                    ON file_versions_v1(file_id, version_number DESC)",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Append a new version row for `file_id`.
    ///
    /// The `version_number` is computed as `MAX(version_number) + 1` for this
    /// file (starting at 1) inside a transaction so concurrent calls cannot
    /// collide on the PK. Returns the newly assigned `version_number`.
    ///
    /// `origin` is `"local"` when the version was observed on disk by this
    /// daemon; it will later be a peer's public key when the version came in
    /// over the wire.
    ///
    /// `size` is the version's content size in bytes, read at hash time.
    pub fn record_version(
        &mut self,
        file_id: FileId,
        content_hash: &str,
        origin: &str,
        size: i64,
    ) -> Result<i64, DatabaseError> {
        let observed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0);

        let transaction = self
            .connection
            .transaction()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        // `MAX(version_number)` returns NULL when there are no rows for this
        // file_id, which rusqlite refuses to deserialize into a plain `i64`.
        // Pull it as `Option<i64>` and default to 0 here instead.
        let current_max: Option<i64> = transaction
            .query_row(
                "SELECT MAX(version_number) FROM file_versions_v1 WHERE file_id = ?1",
                [file_id],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        let next_version_number: i64 = current_max.unwrap_or(0) + 1;

        transaction
            .execute(
                "INSERT INTO file_versions_v1
                    (file_id, content_hash, observed_at, version_number, origin, size)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                (
                    file_id,
                    content_hash,
                    observed_at,
                    next_version_number,
                    origin,
                    size,
                ),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        transaction
            .commit()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(next_version_number)
    }

    /// Return the most recent recorded `content_hash` for every file that has
    /// at least one row in `file_versions`. Used at startup by
    /// `SyncDirectoryManager` to detect files that changed on disk while the
    /// daemon was offline.
    ///
    /// One row per `file_id`; files with no recorded version are absent from
    /// the result.
    pub fn latest_content_hashes(
        &self,
    ) -> Result<std::collections::HashMap<FileId, String>, DatabaseError> {
        // The DESC index on (file_id, version_number) lets SQLite answer this
        // efficiently: for each file_id, take the row with the highest
        // version_number.
        let mut statement = self
            .connection
            .prepare(
                "SELECT file_id, content_hash
                 FROM file_versions_v1 AS outer
                 WHERE version_number = (
                     SELECT MAX(version_number)
                     FROM file_versions_v1 AS inner
                     WHERE inner.file_id = outer.file_id
                 )",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut hashes = std::collections::HashMap::new();
        let rows = statement
            .query_map([], |row| {
                let file_id: FileId = row.get(0)?;
                let content_hash: String = row.get(1)?;
                Ok((file_id, content_hash))
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        for row in rows {
            let (file_id, content_hash) = row.map_err(|_| DatabaseError::FailedToExecuteCommand)?;
            hashes.insert(file_id, content_hash);
        }

        Ok(hashes)
    }

    /// Return the most recent recorded version for `file_id`, or `None` if the
    /// file has never had a version recorded.
    pub fn latest_version(&self, file_id: FileId) -> Result<Option<FileVersion>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT content_hash, observed_at, version_number, origin, size
                 FROM file_versions_v1
                 WHERE file_id = ?1
                 ORDER BY version_number DESC
                 LIMIT 1",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut rows = statement
            .query_map([file_id], |row| {
                Ok(FileVersion {
                    file_id,
                    content_hash: row.get(0)?,
                    observed_at: row.get(1)?,
                    version_number: row.get(2)?,
                    origin: row.get(3)?,
                    size: row.get(4)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        match rows.next() {
            Some(Ok(version)) => Ok(Some(version)),
            Some(Err(_)) => Err(DatabaseError::FailedToExecuteCommand),
            None => Ok(None),
        }
    }

    /// Return the full version history for `file_id`, ordered oldest-first by
    /// `version_number`. Each triple is `(version_number, content_hash, size)`.
    /// An empty vec means the file has no recorded versions.
    ///
    /// Used to build `state::ManifestEntry::history`.
    pub fn version_history(&self, file_id: FileId) -> Result<VersionHistory, DatabaseError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT version_number, content_hash, size
                 FROM file_versions_v1
                 WHERE file_id = ?1
                 ORDER BY version_number ASC",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut history = Vec::new();
        let rows = statement
            .query_map([file_id], |row| {
                let version_number: i64 = row.get(0)?;
                let content_hash: String = row.get(1)?;
                let size: i64 = row.get(2)?;
                Ok((version_number, content_hash, size))
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        for row in rows {
            history.push(row.map_err(|_| DatabaseError::FailedToExecuteCommand)?);
        }
        Ok(history)
    }

    /// Return every `file_id` in the `files` table together with its full
    /// version history and soft-delete tombstone state (`deleted`,
    /// `deleted_at`).
    ///
    /// Deleted (tombstoned) files *are* included: reconciliation advertises the
    /// tombstone so a delete can win last-writer-wins against a peer's stale
    /// "present", and so a peer offline during the delete learns about it on
    /// reconnect (or restores the file if it holds a newer edit).
    pub fn manifest_entries(&self) -> Result<Vec<ManifestRow>, DatabaseError> {
        // First fetch the file rows we still know about, then for each fetch
        // its history and tags. Two-stage to keep the SQL straightforward;
        // manifest construction is a one-shot at connect time so the N+1 here
        // is acceptable.
        let mut id_statement = self
            .connection
            .prepare("SELECT id, logical_path, logical_path_modified_at, deleted, deleted_at, restored_at FROM files_v2")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        let file_rows: Vec<(FileId, LogicalPath, i64, bool, i64, i64)> = id_statement
            .query_map([], |row| {
                let deleted: i64 = row.get(3)?;
                Ok((
                    row.get::<_, FileId>(0)?,
                    row.get::<_, LogicalPath>(1)?,
                    row.get::<_, i64>(2)?,
                    deleted != 0,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut entries = Vec::with_capacity(file_rows.len());
        for (file_id, logical_path, logical_path_modified_at, deleted, deleted_at, restored_at) in
            file_rows
        {
            let history = self.version_history(file_id)?;
            // Files in `files` should always have at least one version
            // (every add/change path records one), but be defensive.
            if history.is_empty() {
                log::warn!(
                    "File {} has no recorded versions; skipping manifest entry",
                    file_id.to_string()
                );
                continue;
            }
            let latest_observed_at = self
                .latest_version(file_id)?
                .map(|version| version.observed_at)
                .unwrap_or(0);
            entries.push((
                file_id,
                history,
                latest_observed_at,
                logical_path,
                logical_path_modified_at,
                deleted,
                deleted_at,
                restored_at,
            ));
        }
        Ok(entries)
    }

    /// Every tag definition as a lightweight manifest entry (`tag_id` +
    /// `modified_at` + `deleted`), *including* soft-deleted (tombstoned) tags.
    /// Drives last-writer-wins reconciliation of definitions: the receiver
    /// requests the full definition only for tags whose `modified_at` is newer
    /// than (or absent from) its own, and applies a tombstone directly. A tag
    /// delete bumps `modified_at`, so the existing LWW comparison also decides
    /// delete-vs-edit. Mirrors [`manifest_entries`] for files.
    pub fn tag_manifest_entries(&self) -> Result<Vec<TagManifestEntry>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, modified_at, deleted FROM tags_v1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        let entries = statement
            .query_map([], |row| {
                let deleted: i64 = row.get(2)?;
                Ok(TagManifestEntry {
                    tag_id: row.get(0)?,
                    modified_at: row.get(1)?,
                    deleted: deleted != 0,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        Ok(entries)
    }

    /// Every tag relationship (file-tagged and tag-tagged), *including*
    /// soft-deleted (tombstoned) rows. Reconciliation deliberately advertises
    /// tombstones so that an "absent" relationship can win last-writer-wins
    /// against a peer's stale "present". Unlike tag definitions, a relationship
    /// carries its whole state here, so the receiver applies it directly with
    /// no follow-up request.
    pub fn relationship_manifest_entries(
        &self,
    ) -> Result<Vec<RelationshipManifestEntry>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT tag_id, target_id, type, modified_at, deleted FROM entries_v1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        let entries = statement
            .query_map([], |row| {
                let kind: EntryType = row.get(2)?;
                let deleted: i64 = row.get(4)?;
                Ok(RelationshipManifestEntry {
                    tag_id: row.get(0)?,
                    target_id: row.get(1)?,
                    kind: kind.into(),
                    modified_at: row.get(3)?,
                    deleted: deleted != 0,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        Ok(entries)
    }

    /// The `modified_at` of a single relationship, or `None` if we have no row
    /// for it. Used by reconciliation to decide whether an incoming
    /// relationship wins last-writer-wins before applying it.
    pub fn relationship_modified_at(
        &self,
        tag_id: TagId,
        target_id: &str,
        kind: EntryType,
    ) -> Result<Option<i64>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT modified_at FROM entries_v1
                 WHERE tag_id = ?1 AND target_id = ?2 AND type = ?3",
                (&tag_id, &target_id, kind),
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)
    }

    /// The `modified_at` of a tag definition, or `None` if we don't know the
    /// tag. Used by reconciliation to decide whether to request a peer's newer
    /// definition.
    pub fn tag_modified_at(&self, tag_id: TagId) -> Result<Option<i64>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT modified_at FROM tags_v1 WHERE id = ?1",
                [tag_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)
    }

    /// The full stored definition of a tag as `(name, color, modified_at)`, or
    /// `None` if the tag is unknown. Used to answer a peer's `TagRequest` with
    /// a `Change::TagAdded`.
    pub fn tag_definition(
        &self,
        tag_id: TagId,
    ) -> Result<Option<(String, String, i64)>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT name, color, modified_at FROM tags_v1 WHERE id = ?1",
                [tag_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)
    }

    /// Apply an incoming relationship (from a peer's tag manifest) with
    /// last-writer-wins, preserving its `modified_at` and `deleted` state.
    /// Newer-wins is enforced in SQL so replaying stale relationships is a
    /// no-op. `target_id` is the stringified `FileId`/`TagId` per `kind`.
    pub fn apply_relationship(
        &self,
        entry: &RelationshipManifestEntry,
    ) -> Result<(), DatabaseError> {
        let kind: EntryType = entry.kind.into();
        self.connection
            .execute(
                "INSERT INTO entries_v1 (id, tag_id, target_id, type, modified_at, deleted)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(tag_id, target_id, type) DO UPDATE SET
                     modified_at = excluded.modified_at,
                     deleted = excluded.deleted
                 WHERE excluded.modified_at > entries_v1.modified_at",
                (
                    TagId::new(),
                    &entry.tag_id,
                    &entry.target_id,
                    kind,
                    entry.modified_at,
                    entry.deleted as i64,
                ),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        Ok(())
    }

    /// Cheap existence check for a `file_id` in the `files` table. Used by
    /// `handle_changes` to decide whether an inbound `FileMetadataAdded` should
    /// be treated as new or as an idempotent re-announcement.
    pub fn file_exists(&self, file_id: FileId) -> Result<bool, DatabaseError> {
        let count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM files_v2 WHERE id = ?1",
                [file_id],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        Ok(count > 0)
    }

    /// Compute the shortest unique prefix of `file_id` among **all** files in
    /// the database — the "short id" shown in listings, à la `jj`/`git`.
    ///
    /// The result is the fewest leading hex characters of `file_id` that no
    /// other file's id shares. The trick that makes this scale: an id only ever
    /// needs to be distinguished from its two lexicographic *neighbours* (the
    /// id immediately before and after it, sorted). If a prefix separates
    /// you from both neighbours, it separates you from everyone. So this is
    /// two indexed range lookups against the `files.id` primary-key index —
    /// O(log n) — not a scan over all files.
    ///
    /// Ids are stored in canonical simple-hex form (see `FileId`'s `ToSql`), so
    /// lexicographic ordering on the stored strings is a clean hex ordering and
    /// prefixes never straddle a separator.
    ///
    /// Note: the returned length reflects the database *at call time*. It is
    /// not stored and not stable across concurrent inserts — a prefix that
    /// is unique now may become ambiguous if a colliding file is added
    /// later. That is the intended behaviour (resolution re-checks
    /// uniqueness on use).
    ///
    /// Returns the full id length if the file has no neighbours (e.g. it is the
    /// only file). Returns `MissingFile` if `file_id` is not in `files`.
    pub fn shorten_file_id(&self, file_id: FileId) -> Result<usize, DatabaseError> {
        let full = file_id.to_string();

        if !self.file_exists(file_id)? {
            return Err(DatabaseError::MissingFile);
        }

        // Immediate lexicographic predecessor, if any.
        let predecessor: Option<String> = self
            .connection
            .query_row(
                "SELECT id FROM files_v2 WHERE id < ?1 ORDER BY id DESC LIMIT 1",
                [&full],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        // Immediate lexicographic successor, if any.
        let successor: Option<String> = self
            .connection
            .query_row(
                "SELECT id FROM files_v2 WHERE id > ?1 ORDER BY id ASC LIMIT 1",
                [&full],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        // The prefix must be one longer than the longest prefix we share with
        // either neighbour, so that it excludes both of them.
        let mut required = 0;
        for neighbour in [predecessor, successor].into_iter().flatten() {
            let shared = common_prefix_length(&full, &neighbour);
            required = required.max(shared + 1);
        }

        Ok(required.clamp(1, full.len()))
    }

    /// Resolve a full-or-short file id `prefix` to a single [`FileId`].
    ///
    /// The inverse of [`shorten_file_id`](Self::shorten_file_id): given the
    /// characters a user typed (a short id from a listing, or a full id pasted
    /// in either hyphenated or hex form), find the one file it identifies.
    ///
    /// Errors:
    /// - [`DatabaseError::MissingFile`] if no file matches the prefix.
    /// - [`DatabaseError::AmbiguousIdPrefix`] if more than one file matches
    ///   (e.g. a colliding file was added since the short id was displayed).
    pub fn resolve_file_id_prefix(&self, prefix: &str) -> Result<FileId, DatabaseError> {
        let normalised = normalise_id_prefix(prefix).ok_or(DatabaseError::MissingFile)?;
        match resolve_id_prefix(&self.connection, "files_v2", "id", &normalised)? {
            PrefixResolution::Unique(id) => {
                FileId::from_string(&id).ok_or(DatabaseError::MissingFile)
            }
            PrefixResolution::NotFound => Err(DatabaseError::MissingFile),
            PrefixResolution::Ambiguous => Err(DatabaseError::AmbiguousIdPrefix(normalised)),
        }
    }

    /// Whether a tag with `tag_id` exists. The tag counterpart of
    /// [`file_exists`](Self::file_exists).
    pub fn tag_exists(&self, tag_id: TagId) -> Result<bool, DatabaseError> {
        let count: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM tags_v1 WHERE id = ?1",
                [tag_id],
                |row| row.get(0),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        Ok(count > 0)
    }

    /// Compute the shortest unique prefix of `tag_id` among **all** tags — the
    /// "short id" shown in listings. The tag counterpart of
    /// [`shorten_file_id`](Self::shorten_file_id); see it for the
    /// neighbour-based reasoning and the caveats about the length not being
    /// stable across concurrent inserts.
    ///
    /// Returns `MissingTag` if `tag_id` is not in `tags`.
    pub fn shorten_tag_id(&self, tag_id: TagId) -> Result<usize, DatabaseError> {
        let full = tag_id.to_string();

        if !self.tag_exists(tag_id)? {
            return Err(DatabaseError::MissingTag);
        }

        let predecessor: Option<String> = self
            .connection
            .query_row(
                "SELECT id FROM tags_v1 WHERE id < ?1 ORDER BY id DESC LIMIT 1",
                [&full],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let successor: Option<String> = self
            .connection
            .query_row(
                "SELECT id FROM tags_v1 WHERE id > ?1 ORDER BY id ASC LIMIT 1",
                [&full],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut required = 0;
        for neighbour in [predecessor, successor].into_iter().flatten() {
            let shared = common_prefix_length(&full, &neighbour);
            required = required.max(shared + 1);
        }

        Ok(required.clamp(1, full.len()))
    }

    /// Resolve a full-or-short tag id `prefix` to a single [`TagId`]. The tag
    /// counterpart of [`resolve_file_id_prefix`](Self::resolve_file_id_prefix).
    ///
    /// Errors:
    /// - [`DatabaseError::MissingTag`] if no tag matches the prefix.
    /// - [`DatabaseError::AmbiguousIdPrefix`] if more than one tag matches.
    pub fn resolve_tag_id_prefix(&self, prefix: &str) -> Result<TagId, DatabaseError> {
        let normalised = normalise_id_prefix(prefix).ok_or(DatabaseError::MissingTag)?;
        match resolve_id_prefix(&self.connection, "tags_v1", "id", &normalised)? {
            PrefixResolution::Unique(id) => {
                TagId::from_string(&id).ok_or(DatabaseError::MissingTag)
            }
            PrefixResolution::NotFound => Err(DatabaseError::MissingTag),
            PrefixResolution::Ambiguous => Err(DatabaseError::AmbiguousIdPrefix(normalised)),
        }
    }

    /// Add a new file.
    /// Insert a newly added file.
    ///
    /// `logical_path_modified_at` is the unix-millis wall-clock time the path
    /// was set (creation time on the originating device, or the peer's stamped
    /// time when materializing a file first seen over the wire). It seeds the
    /// path's last-writer-wins clock so a later move can be ordered against it.
    pub fn add_file(
        &self,
        file_id: FileId,
        logical_path: &LogicalPath,
        logical_path_modified_at: i64,
    ) -> Result<(), DatabaseError> {
        // A freshly added file is always live: `deleted = 0`, `deleted_at = 0`,
        // `restored_at = 0` (never explicitly restored yet).
        self.connection
            .execute(
                "INSERT INTO files_v2 (id, logical_path, logical_path_modified_at, deleted, deleted_at, restored_at)
                 VALUES (?1, ?2, ?3, 0, 0, 0)",
                (file_id, logical_path, logical_path_modified_at),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Clear a file's soft-delete tombstone when a **newer content edit**
    /// supersedes the delete (restore-after-edit). Called by the
    /// version-arrival paths after recording a version: if the file's
    /// latest version `observed_at` is strictly newer than `deleted_at`,
    /// the edit wins last-writer-wins and the file becomes live again.
    /// Otherwise it is a no-op (a stale/duplicate version does not
    /// resurrect a newer delete).
    ///
    /// This is the edit half of the three-way LWW; explicit user restores go
    /// through [`apply_restore`](Self::apply_restore). `restored_at` is left
    /// untouched here — an edit is not a restore.
    pub fn restore_file(&self, file_id: FileId) -> Result<(), DatabaseError> {
        let Some(state) = self.file_deletion_state(file_id)? else {
            return Ok(());
        };
        if !state.deleted {
            return Ok(());
        }

        let latest_observed_at = self
            .latest_version(file_id)?
            .map(|version| version.observed_at)
            .unwrap_or(0);

        if latest_observed_at <= state.deleted_at {
            // No edit newer than the delete; the tombstone stands.
            return Ok(());
        }

        self.connection
            .execute("UPDATE files_v2 SET deleted = 0 WHERE id = ?1", [file_id])
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Apply an **explicit user restore** to a file, last-writer-wins.
    ///
    /// Records `restored_at` as the file's restore clock (only advancing it, so
    /// a duplicate/older restore is a no-op) and clears the tombstone iff the
    /// restore is strictly newer than the delete (`restored_at > deleted_at`).
    /// Unlike [`restore_file`](Self::restore_file), this does not depend on a
    /// version edit — it is the restore half of the three-way LWW and is what
    /// makes a peer's still-present `deleted_at` lose to our un-delete on
    /// reconnect (the manifest advertises `restored_at`).
    ///
    /// Returns `true` if the file is live after this call's changes (either the
    /// restore won or it was already live), `false` if a newer delete still
    /// out-votes the restore (the file stays tombstoned).
    pub fn apply_restore(&self, file_id: FileId, restored_at: i64) -> Result<bool, DatabaseError> {
        let Some(state) = self.file_deletion_state(file_id)? else {
            return Ok(false);
        };

        // Advance the restore clock (monotonic: never move it backward).
        if restored_at > state.restored_at {
            self.connection
                .execute(
                    "UPDATE files_v2 SET restored_at = ?2 WHERE id = ?1",
                    (file_id, restored_at),
                )
                .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        }

        let effective_restored_at = state.restored_at.max(restored_at);
        let latest_observed_at = self
            .latest_version(file_id)?
            .map(|version| version.observed_at)
            .unwrap_or(0);

        // Live iff the newest of {edit, restore} beats the delete.
        let live = effective_restored_at.max(latest_observed_at) > state.deleted_at;
        if live && state.deleted {
            self.connection
                .execute("UPDATE files_v2 SET deleted = 0 WHERE id = ?1", [file_id])
                .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        }

        Ok(live)
    }

    /// The soft-delete tombstone state of a file, or `None` if the file is
    /// unknown. Used by reconciliation to decide delete-vs-edit-vs-restore
    /// last-writer-wins.
    pub fn file_deletion_state(
        &self,
        file_id: FileId,
    ) -> Result<Option<DeletionState>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT deleted, deleted_at, restored_at FROM files_v2 WHERE id = ?1",
                [file_id],
                |row| {
                    let deleted: i64 = row.get(0)?;
                    Ok(DeletionState {
                        deleted: deleted != 0,
                        deleted_at: row.get::<_, i64>(1)?,
                        restored_at: row.get::<_, i64>(2)?,
                    })
                },
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)
    }

    /// Change a file's logical path, last-writer-wins.
    ///
    /// The move is applied only if `modified_at` is strictly newer than the
    /// file's current `logical_path_modified_at`. This makes local moves and
    /// peer moves (live or reconciled) commute regardless of delivery order:
    /// the latest edit — by wall clock stamped on the originating device —
    /// always wins, and re-applying an older or duplicate move is a no-op.
    /// Never restamp `modified_at` when applying a peer's move; pass its
    /// original value straight through.
    ///
    /// Returns `true` if the move was applied, `false` if it lost (an equal or
    /// newer path change is already recorded, or the file is unknown).
    pub fn update_file_logical_path(
        &self,
        file_id: FileId,
        logical_path: &LogicalPath,
        modified_at: i64,
    ) -> Result<bool, DatabaseError> {
        let rows = self
            .connection
            .execute(
                "UPDATE files_v2
                 SET logical_path = ?2, logical_path_modified_at = ?3
                 WHERE id = ?1 AND logical_path_modified_at < ?3",
                (file_id, logical_path, modified_at),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(rows > 0)
    }

    /// Soft-delete a file: set its tombstone (`deleted = 1`, `deleted_at`)
    /// instead of removing the row, so the deletion survives reconciliation
    /// (a peer offline during the delete learns of it on reconnect) and can win
    /// last-writer-wins against a stale "present".
    ///
    /// Three-way last-writer-wins guard: the delete is applied only if
    /// `deleted_at` is strictly newer than **both** the file's latest recorded
    /// version `observed_at` (restore-after-edit) **and** its `restored_at`
    /// (an explicit restore). Equivalently, the delete wins iff
    /// `deleted_at > max(latest observed_at, restored_at)`. The `file_versions`
    /// history is left intact.
    ///
    /// Returns `true` if the tombstone was applied, `false` if a newer edit or
    /// restore out-dated it (the file stays live).
    pub fn remove_file(&self, file_id: FileId, deleted_at: i64) -> Result<bool, DatabaseError> {
        let latest_observed_at = self
            .latest_version(file_id)?
            .map(|version| version.observed_at)
            .unwrap_or(0);
        let restored_at = self
            .file_deletion_state(file_id)?
            .map(|state| state.restored_at)
            .unwrap_or(0);

        if deleted_at <= latest_observed_at.max(restored_at) {
            // A newer edit or explicit restore supersedes this delete; keep the
            // file live.
            return Ok(false);
        }

        let affected = self
            .connection
            .execute(
                "UPDATE files_v2 SET deleted = 1, deleted_at = ?2 WHERE id = ?1",
                (file_id, deleted_at),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(affected > 0)
    }

    /// Add a new tag.
    ///
    /// `modified_at` is the unix-millis last-writer-wins timestamp. For a local
    /// mutation pass [`now_millis`]; for a peer-originated change pass the
    /// timestamp that arrived on the wire (do not restamp).
    ///
    /// If a row with this `tag_id` already exists, this becomes an upsert
    /// resolved by last-writer-wins: the incoming values are applied only if
    /// `modified_at` is newer than the stored one. This keeps reconciliation
    /// idempotent — replaying an old `TagAdded` cannot clobber a newer local
    /// definition.
    pub fn add_tag(
        &self,
        tag_id: TagId,
        name: impl Into<String>,
        color: impl Into<String>,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        let name = name.into();
        let color = color.into();

        // TODO: Check that the tag name is not only numbers.
        if name.is_empty() {
            return Err(DatabaseError::InvalidTagName);
        }

        // TODO: Check that the color is valid.
        if color.is_empty() {
            return Err(DatabaseError::InvalidColor);
        }

        // Upsert with a last-writer-wins guard: on conflict, overwrite only when
        // the incoming `modified_at` is strictly newer. `excluded` refers to the
        // values we tried to insert.
        self.connection
            .execute(
                "INSERT INTO tags_v1 (id, name, color, modified_at, deleted)
                 VALUES (?1, ?2, ?3, ?4, 0)
                 ON CONFLICT(id) DO UPDATE SET
                     name = excluded.name,
                     color = excluded.color,
                     modified_at = excluded.modified_at,
                     deleted = 0
                 WHERE excluded.modified_at > tags_v1.modified_at",
                (tag_id, &name, &color, modified_at),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Update a tag's name with a last-writer-wins guard: the update is applied
    /// only if `modified_at` is newer than the stored value. See [`add_tag`]
    /// for the `modified_at` contract.
    pub fn update_tag_name(
        &self,
        tag_id: TagId,
        name: impl Into<String>,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        let name = name.into();

        // TODO: Check that the tag name is not only numbers.
        if name.is_empty() {
            return Err(DatabaseError::InvalidTagName);
        }

        self.connection
            .execute(
                "UPDATE tags_v1 SET name = ?2, modified_at = ?3
                 WHERE id = ?1 AND ?3 > modified_at",
                (tag_id, name, modified_at),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Update a tag's color with a last-writer-wins guard. See [`add_tag`] for
    /// the `modified_at` contract.
    pub fn update_tag_color(
        &self,
        tag_id: TagId,
        color: impl Into<String>,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        let color = color.into();

        // TODO: Check that the color is valid.
        if color.is_empty() {
            return Err(DatabaseError::InvalidColor);
        }

        self.connection
            .execute(
                "UPDATE tags_v1 SET color = ?2, modified_at = ?3
                 WHERE id = ?1 AND ?3 > modified_at",
                (tag_id, color, modified_at),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Soft-delete a tag: set its tombstone (`deleted = 1`) and bump
    /// `modified_at` to `deleted_at`, instead of removing the row. A tag reuses
    /// its `modified_at` as its single last-writer-wins clock, so the delete is
    /// applied only if `deleted_at` is strictly newer than the stored
    /// `modified_at` (a newer rename/recolor resurrects the tag). Mirrors the
    /// relationship tombstones in [`untag_entry`].
    ///
    /// Relationship rows referencing the tag are left untouched: they carry
    /// their own tombstones and reconcile independently.
    ///
    /// Returns `true` if the tombstone was applied, `false` if a newer edit
    /// out-dated it (the tag stays live).
    pub fn remove_tag(&self, tag_id: TagId, deleted_at: i64) -> Result<bool, DatabaseError> {
        let affected = self
            .connection
            .execute(
                "UPDATE tags_v1 SET deleted = 1, modified_at = ?2
                 WHERE id = ?1 AND ?2 > modified_at",
                (&tag_id, deleted_at),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(affected > 0)
    }

    /// Tag a file with the provided tag.
    ///
    /// `modified_at` is the last-writer-wins timestamp; see [`add_tag`]. This
    /// is an upsert: if a (possibly tombstoned) row for this `(tag_id,
    /// file_id)` relationship already exists, it is revived (`deleted = 0`)
    /// and stamped — but only when `modified_at` is newer than the stored
    /// value. Re-tagging is therefore idempotent and correctly loses to a
    /// newer untag.
    pub fn tag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.upsert_entry(tag_id, file_id.to_string(), EntryType::File, modified_at)
    }

    /// Tag a tag with the provided tag. See [`tag_file`] for the LWW/upsert
    /// semantics and the `modified_at` contract.
    pub fn tag_tag(
        &self,
        tag_id: TagId,
        subtag_id: TagId,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        if tag_id == subtag_id {
            return Err(DatabaseError::CantTagItself);
        }

        self.upsert_entry(tag_id, subtag_id.to_string(), EntryType::Tag, modified_at)
    }

    /// Remove a tag from a file (soft delete). See [`untag_entry`].
    pub fn untag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.untag_entry(tag_id, file_id.to_string(), EntryType::File, modified_at)
    }

    /// Remove a tag from a tag (soft delete). See [`untag_entry`].
    pub fn untag_tag(
        &self,
        tag_id: TagId,
        subtag_id: TagId,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.untag_entry(tag_id, subtag_id.to_string(), EntryType::Tag, modified_at)
    }

    /// Shared upsert for the two "add relationship" paths. Inserts a live
    /// (`deleted = 0`) entry, or on conflict revives/refreshes the existing row
    /// — gated by last-writer-wins so an older change can't override a newer
    /// one. `target_id` is the stringified `FileId`/`TagId` (both persist as
    /// simple-hex, matching the column's storage).
    fn upsert_entry(
        &self,
        tag_id: TagId,
        target_id: String,
        entry_type: EntryType,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "INSERT INTO entries_v1 (id, tag_id, target_id, type, modified_at, deleted)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0)
                 ON CONFLICT(tag_id, target_id, type) DO UPDATE SET
                     modified_at = excluded.modified_at,
                     deleted = 0
                 WHERE excluded.modified_at > entries_v1.modified_at",
                (TagId::new(), &tag_id, &target_id, entry_type, modified_at),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Shared soft-delete for the two "remove relationship" paths. Marks the
    /// row `deleted = 1` and stamps `modified_at`, gated by
    /// last-writer-wins so a stale untag can't override a newer tag. If the
    /// relationship was never recorded, this is a no-op (there is no row to
    /// tombstone; a peer that only knows the untag can still learn "absent"
    /// once we've seen the tag).
    ///
    /// NOTE: Offline untag propagation is only fully correct once the broader
    /// deletion/tombstone design lands — see roadmap. Today the tombstone is
    /// created locally and reconciled, but there is no tombstone GC.
    fn untag_entry(
        &self,
        tag_id: TagId,
        target_id: String,
        entry_type: EntryType,
        modified_at: i64,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "UPDATE entries_v1 SET deleted = 1, modified_at = ?4
                 WHERE tag_id = ?1 AND target_id = ?2 AND type = ?3
                   AND ?4 > modified_at",
                (&tag_id, &target_id, entry_type, modified_at),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    fn file_ids_for_tag_inner(
        &self,
        tag_id: TagId,
        lookup_cache: &mut BTreeSet<TagId>,
        collected_tag_ids: &mut BTreeSet<FileId>,
        subtag_rule: SubtagRule,
    ) -> Result<(), DatabaseError> {
        enum Entry {
            File { file_id: FileId },
            Tag { tag_id: TagId },
        }

        let mut statement = self
            .connection
            .prepare("SELECT target_id, type FROM entries_v1 WHERE tag_id = ?1 AND deleted = 0")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([tag_id], |row| {
                let r#type: EntryType = row.get(1)?;

                let entry = match r#type {
                    EntryType::File => Entry::File {
                        file_id: row.get(0)?,
                    },
                    EntryType::Tag => Entry::Tag {
                        tag_id: row.get(0)?,
                    },
                };

                Ok(entry)
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|entry| entry.unwrap());

        lookup_cache.insert(tag_id);

        for entry in iterator {
            match entry {
                Entry::File { file_id } => {
                    collected_tag_ids.insert(file_id);
                }
                Entry::Tag { tag_id } => {
                    if subtag_rule == SubtagRule::Include && !lookup_cache.contains(&tag_id) {
                        self.file_ids_for_tag_inner(
                            tag_id,
                            lookup_cache,
                            collected_tag_ids,
                            subtag_rule,
                        )?
                    }
                }
            }
        }

        Ok(())
    }

    /// Get all files that are tagged with the provided tag.
    pub fn file_ids_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = FileId>, DatabaseError> {
        let mut file_ids = BTreeSet::new();
        let mut lookup_cache = BTreeSet::new();

        self.file_ids_for_tag_inner(tag_id, &mut lookup_cache, &mut file_ids, subtag_rule)?;

        Ok(file_ids)
    }

    /// Search files by a conjunction of [`QueryTerm`]s.
    ///
    /// A file is returned only if it satisfies every term:
    /// - [`QueryTerm::HasTag`]: it carries *at least one* tag in the set
    ///   (subtag traversal governed by `subtag_rule`).
    /// - [`QueryTerm::NotTag`]: it carries *none* of the tags in the set (same
    ///   traversal).
    /// - [`QueryTerm::LogicalContains`] / [`QueryTerm::NotLogicalContains`]:
    ///   its logical path contains / does not contain the substring, compared
    ///   case-insensitively.
    /// - [`QueryTerm::AnyMatch`] / [`QueryTerm::NotAnyMatch`]: its logical path
    ///   contains the substring **or** it carries any tag in the set — the
    ///   "prefix-less" chunk semantics.
    ///
    /// An empty term list matches every file; an empty tag set inside a term
    /// matches no tag (so `HasTag([])` matches nothing and `NotTag([])`
    /// excludes nothing). For [`QueryTerm::AnyMatch`] the substring side
    /// still stands when the tag set is empty. Composes
    /// [`Self::file_ids_for_tag`] and [`Self::get_all_files`]; no new SQL.
    ///
    /// `deleted_rule` is passed through to [`Self::get_all_files`] so that
    /// tombstoned files can participate in the candidate pool under
    /// [`DeletedRule::Include`]. Relationship traversal (`entries_v1`)
    /// stays live-only regardless — only the file's own row visibility is
    /// affected. Callers filter the returned ids by their `deleted` flag if
    /// they want an only-deleted view.
    pub fn file_ids_for_query(
        &self,
        terms: &[QueryTerm],
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<impl IntoIterator<Item = FileId>, DatabaseError> {
        // The set of files carrying at least one tag in `tag_ids` (the union of
        // each tag's files). An empty set yields no files.
        let files_for_any_tag = |tag_ids: &[TagId]| -> Result<BTreeSet<FileId>, DatabaseError> {
            let mut union = BTreeSet::new();
            for tag_id in tag_ids {
                union.extend(self.file_ids_for_tag(*tag_id, subtag_rule)?);
            }
            Ok(union)
        };

        // Seed the candidate set. If there is at least one positive tag-only
        // term (`HasTag`), start from the intersection of those (each
        // contributing the union of its matched tags' files); otherwise start
        // from all files. `AnyMatch` is *not* a seed — its substring half can
        // match files that carry no tags at all, so we always need every file
        // in the candidate pool for the filter pass below.
        let positive: Vec<&[TagId]> = terms
            .iter()
            .filter_map(|term| match term {
                QueryTerm::HasTag(tag_ids) => Some(tag_ids.as_slice()),
                _ => None,
            })
            .collect();

        let mut candidates: BTreeSet<FileId> = if let Some((first, rest)) = positive.split_first() {
            let mut set = files_for_any_tag(first)?;
            for tag_ids in rest {
                let next = files_for_any_tag(tag_ids)?;
                set.retain(|file_id| next.contains(file_id));
            }
            // A `HasTag` seed comes from `entries_v1` (always live) and does
            // not consult `files_v1.deleted`, so it can include ids for
            // tombstoned files. We do NOT drop them here — `Api::run_query`
            // joins each id through `file_info_from_id` (which honors the
            // requested `deleted_rule`) and skips misses. This preserves the
            // long-standing tolerance for files referenced from `entries_v1`
            // without a matching `file_versions` row yet.
            set
        } else {
            self.get_all_files(deleted_rule)?
                .into_iter()
                .map(|file| file.file_id)
                .collect()
        };

        // Subtract every negative tag term (the union of its matched tags' files).
        for term in terms {
            if let QueryTerm::NotTag(tag_ids) = term {
                let excluded = files_for_any_tag(tag_ids)?;
                candidates.retain(|file_id| !excluded.contains(file_id));
            }
        }

        // Text-bearing terms (`LogicalContains`, `NotLogicalContains`,
        // `AnyMatch`, `NotAnyMatch`) need each candidate's logical path *and*,
        // for the `Any` variants, the union of files-carrying-any-of-the-set.
        // Build both lookups once, then apply every text term as one retain
        // pass — cheaper than rebuilding per term and keeps the semantics
        // obvious.
        let has_text_term = terms.iter().any(|term| {
            matches!(
                term,
                QueryTerm::NameContains(_)
                    | QueryTerm::NotNameContains(_)
                    | QueryTerm::LogicalContains(_)
                    | QueryTerm::NotLogicalContains(_)
                    | QueryTerm::AnyMatch(_, _)
                    | QueryTerm::NotAnyMatch(_, _),
            )
        });

        if has_text_term {
            let paths: std::collections::BTreeMap<FileId, String> = self
                .get_all_files(deleted_rule)?
                .into_iter()
                .map(|file| (file.file_id, file.logical_path.as_str().to_lowercase()))
                .collect();

            // Precompute the file set for every `Any` variant so we don't hit
            // the DB inside the retain closure.
            let mut any_tag_sets: Vec<(String, BTreeSet<FileId>, bool)> = Vec::new();
            for term in terms {
                let (needle, tag_ids, negated) = match term {
                    QueryTerm::AnyMatch(needle, tag_ids) => (needle, tag_ids, false),
                    QueryTerm::NotAnyMatch(needle, tag_ids) => (needle, tag_ids, true),
                    _ => continue,
                };
                any_tag_sets.push((needle.to_lowercase(), files_for_any_tag(tag_ids)?, negated));
            }

            candidates.retain(|file_id| {
                let Some(path) = paths.get(file_id) else {
                    return false;
                };
                for term in terms {
                    let ok = match term {
                        QueryTerm::NameContains(needle) | QueryTerm::LogicalContains(needle) => {
                            path.contains(&needle.to_lowercase())
                        }
                        QueryTerm::NotNameContains(needle)
                        | QueryTerm::NotLogicalContains(needle) => {
                            !path.contains(&needle.to_lowercase())
                        }
                        _ => continue,
                    };
                    if !ok {
                        return false;
                    }
                }
                for (needle, tagged_files, negated) in &any_tag_sets {
                    let hit = path.contains(needle) || tagged_files.contains(file_id);
                    if hit == *negated {
                        return false;
                    }
                }
                true
            });
        }

        Ok(candidates)
    }

    /// Search *tags* by the same conjunction of [`QueryTerm`]s used for files,
    /// mirroring [`Self::file_ids_for_query`] onto the tag hierarchy:
    ///
    /// - [`QueryTerm::HasTag`]: the tag must be a subtag of *at least one* tag
    ///   in the set (subtag traversal governed by `subtag_rule`) — the tag
    ///   analogue of "a file carries this tag".
    /// - [`QueryTerm::NotTag`]: the tag must *not* be a subtag of any tag in
    ///   the set.
    /// - [`QueryTerm::NameContains`] / [`QueryTerm::NotNameContains`]: the
    ///   tag's name contains / does not contain the substring, compared
    ///   case-insensitively.
    /// - [`QueryTerm::LogicalContains`] / [`QueryTerm::NotLogicalContains`]:
    ///   doesn't match.
    /// - [`QueryTerm::AnyMatch`] / [`QueryTerm::NotAnyMatch`]: the tag's name
    ///   contains the substring **or** the tag is a subtag of any tag in the
    ///   set — the tag analogue of the file-side `Any` semantics.
    ///
    /// An empty term list matches every tag; an empty tag set inside a term
    /// matches no tag. For [`QueryTerm::AnyMatch`] the substring side still
    /// stands when the tag set is empty. Composes [`Self::subtag_ids_for_tag`]
    /// and [`Self::get_all_tags`]; no new SQL.
    ///
    /// `deleted_rule` is passed through to [`Self::get_all_tags`] so that
    /// tombstoned tags can participate in the candidate pool under
    /// [`DeletedRule::Include`]. Relationship traversal (`entries_v1`)
    /// stays live-only regardless — only the tag's own row visibility is
    /// affected. Callers filter the returned ids by their `deleted` flag if
    /// they want an only-deleted view.
    pub fn tag_ids_for_query(
        &self,
        terms: &[QueryTerm],
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        // The set of tags that are a subtag of at least one tag in `tag_ids`
        // (the union of each tag's subtags). An empty set yields no tags.
        let subtags_of_any = |tag_ids: &[TagId]| -> Result<BTreeSet<TagId>, DatabaseError> {
            let mut union = BTreeSet::new();
            for tag_id in tag_ids {
                union.extend(self.subtag_ids_for_tag(*tag_id, subtag_rule)?);
            }
            Ok(union)
        };

        // Seed on `HasTag` only, same reasoning as `file_ids_for_query`:
        // `AnyMatch` can match on the name side without any tag membership, so
        // we can't use it to narrow the seed.
        let positive: Vec<&[TagId]> = terms
            .iter()
            .filter_map(|term| match term {
                QueryTerm::HasTag(tag_ids) => Some(tag_ids.as_slice()),
                _ => None,
            })
            .collect();

        let mut candidates: BTreeSet<TagId> = if let Some((first, rest)) = positive.split_first() {
            let mut set = subtags_of_any(first)?;
            for tag_ids in rest {
                let next = subtags_of_any(tag_ids)?;
                set.retain(|tag_id| next.contains(tag_id));
            }
            // Subtag traversal comes from `entries_v1` (always live) and does
            // not consult `tags_v1.deleted`, so it can include ids for
            // tombstoned tags. We do NOT drop them here — `Api::run_query`
            // joins each id through `tag_from_id` (which honors the requested
            // `deleted_rule`) and skips misses, matching the tolerance in
            // `file_ids_for_query`.
            set
        } else {
            self.get_all_tags(deleted_rule)?
                .into_iter()
                .map(|tag| tag.id)
                .collect()
        };

        for term in terms {
            match term {
                QueryTerm::LogicalContains(..) | QueryTerm::NotLogicalContains(..) => {
                    candidates.clear();
                    return Ok(candidates);
                }
                QueryTerm::NotTag(tag_ids) => {
                    let excluded = subtags_of_any(tag_ids)?;
                    candidates.retain(|tag_id| !excluded.contains(tag_id));
                }
                _ => {}
            }
        }

        let has_text_term = terms.iter().any(|term| {
            matches!(
                term,
                QueryTerm::NameContains(_)
                    | QueryTerm::NotNameContains(_)
                    | QueryTerm::AnyMatch(_, _)
                    | QueryTerm::NotAnyMatch(_, _),
            )
        });

        if has_text_term {
            let names: std::collections::BTreeMap<TagId, String> = self
                .get_all_tags(deleted_rule)?
                .into_iter()
                .map(|tag| (tag.id, tag.name.to_lowercase()))
                .collect();

            let mut any_tag_sets: Vec<(String, BTreeSet<TagId>, bool)> = Vec::new();
            for term in terms {
                let (needle, tag_ids, negated) = match term {
                    QueryTerm::AnyMatch(needle, tag_ids) => (needle, tag_ids, false),
                    QueryTerm::NotAnyMatch(needle, tag_ids) => (needle, tag_ids, true),
                    _ => continue,
                };
                any_tag_sets.push((needle.to_lowercase(), subtags_of_any(tag_ids)?, negated));
            }

            candidates.retain(|tag_id| {
                let Some(name) = names.get(tag_id) else {
                    return false;
                };
                for term in terms {
                    let ok = match term {
                        QueryTerm::NameContains(needle) => name.contains(&needle.to_lowercase()),
                        QueryTerm::NotNameContains(needle) => {
                            !name.contains(&needle.to_lowercase())
                        }
                        _ => continue,
                    };
                    if !ok {
                        return false;
                    }
                }
                for (needle, subtagged, negated) in &any_tag_sets {
                    let hit = name.contains(needle) || subtagged.contains(tag_id);
                    if hit == *negated {
                        return false;
                    }
                }
                true
            });
        }

        Ok(candidates)
    }

    /// Get all files that are tagged with the provided tag.
    pub fn tag_ids_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        // Exclude both the relationship tombstone (`entries_v1.deleted`) *and*
        // tags whose own row is tombstoned (`tags_v1.deleted`): deleting a tag
        // is distinct from untagging, so the relationship survives, but a
        // deleted tag must not appear as an applied tag (the UI would then try
        // to fetch a `MissingTag` and error).
        //
        // LEFT JOIN (not INNER): a live relationship can legitimately reference
        // a tag whose definition hasn't been reconciled yet (`FileTagged` can
        // arrive before `TagAdded`). Such a tag has no `tags_v1` row — keep it
        // (`t.deleted IS NULL`); only exclude tags we *know* are tombstoned.
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.tag_id FROM entries_v1 AS e
                 LEFT JOIN tags_v1 AS t ON t.id = e.tag_id
                 WHERE e.target_id = ?1 AND e.type = 0 AND e.deleted = 0
                   AND (t.deleted = 0 OR t.deleted IS NULL)",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut tag_ids = statement
            .query_map([file_id], |row| row.get::<_, TagId>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|tag_id| tag_id.unwrap())
            .collect::<BTreeSet<_>>();

        if subtag_rule == SubtagRule::Include {
            let mut lookup_cache = BTreeSet::new();

            for tag_id in tag_ids.clone() {
                self.tag_ids_for_subtag_inner(
                    tag_id,
                    &mut lookup_cache,
                    &mut tag_ids,
                    subtag_rule,
                )?;
            }
        }

        Ok(tag_ids)
    }

    fn subtag_ids_for_tag_inner(
        &self,
        tag_id: TagId,
        lookup_cache: &mut BTreeSet<TagId>,
        collected_tags: &mut BTreeSet<TagId>,
        subtag_rule: SubtagRule,
    ) -> Result<(), DatabaseError> {
        // Skip subtags whose own tag row is tombstoned (a deleted tag is not a
        // live subtag), alongside the relationship tombstone. LEFT JOIN so a
        // subtag whose definition hasn't reconciled yet (no `tags_v1` row) is
        // still returned; only known-tombstoned tags are excluded.
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.target_id FROM entries_v1 AS e
                 LEFT JOIN tags_v1 AS t ON t.id = e.target_id
                 WHERE e.tag_id = ?1 AND e.type = 1 AND e.deleted = 0
                   AND (t.deleted = 0 OR t.deleted IS NULL)",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([tag_id], |row| row.get::<_, TagId>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|entry| entry.unwrap());

        lookup_cache.insert(tag_id);

        for tag_id in iterator {
            collected_tags.insert(tag_id);

            if subtag_rule == SubtagRule::Include && !lookup_cache.contains(&tag_id) {
                self.subtag_ids_for_tag_inner(tag_id, lookup_cache, collected_tags, subtag_rule)?;
            }
        }

        Ok(())
    }

    /// Get all subtags tagged with the provided tag.
    pub fn subtag_ids_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        let mut tags = BTreeSet::new();
        let mut lookup_cache = BTreeSet::new();

        self.subtag_ids_for_tag_inner(tag_id, &mut lookup_cache, &mut tags, subtag_rule)?;

        Ok(tags)
    }

    fn tag_ids_for_subtag_inner(
        &self,
        subtag_id: TagId,
        lookup_cache: &mut BTreeSet<TagId>,
        collected_tags: &mut BTreeSet<TagId>,
        subtag_rule: SubtagRule,
    ) -> Result<(), DatabaseError> {
        // Skip parent tags whose own tag row is tombstoned (a deleted tag is not
        // a live parent), alongside the relationship tombstone. LEFT JOIN so a
        // parent whose definition hasn't reconciled yet (no `tags_v1` row) is
        // still returned; only known-tombstoned tags are excluded.
        let mut statement = self
            .connection
            .prepare(
                "SELECT e.tag_id FROM entries_v1 AS e
                 LEFT JOIN tags_v1 AS t ON t.id = e.tag_id
                 WHERE e.target_id = ?1 AND e.type = 1 AND e.deleted = 0
                   AND (t.deleted = 0 OR t.deleted IS NULL)",
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let iterator = statement
            .query_map([subtag_id], |row| row.get::<_, TagId>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|entry| entry.unwrap());

        lookup_cache.insert(subtag_id);

        for tag_id in iterator {
            collected_tags.insert(tag_id);

            if subtag_rule == SubtagRule::Include && !lookup_cache.contains(&tag_id) {
                self.tag_ids_for_subtag_inner(tag_id, lookup_cache, collected_tags, subtag_rule)?;
            }
        }

        Ok(())
    }

    /// Get all tags that tag the provided tag.
    pub fn tag_ids_for_subtag(
        &self,
        subtag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        let mut tags = BTreeSet::new();
        let mut lookup_cache = BTreeSet::new();

        self.tag_ids_for_subtag_inner(subtag_id, &mut lookup_cache, &mut tags, subtag_rule)?;

        Ok(tags)
    }

    /// Get all tags.
    ///
    /// `deleted_rule` governs tombstone visibility: `Exclude` hides
    /// tombstoned tags (the standard behavior), `Include` returns them
    /// alongside live ones with `Tag::deleted = true`.
    pub fn get_all_tags(
        &self,
        deleted_rule: DeletedRule,
    ) -> Result<impl IntoIterator<Item = Tag>, DatabaseError> {
        let sql = format!(
            "SELECT id, name, color, deleted FROM tags_v1{}",
            deleted_rule.where_deleted_clause(),
        );
        let mut statement = self
            .connection
            .prepare(&sql)
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let tag_list = statement
            .query_map([], |row| {
                Ok(Tag {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    color: row.get(2)?,
                    metadata: None,
                    deleted: row.get::<_, i64>(3)? != 0,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|tag| tag.unwrap())
            .collect::<Vec<_>>();

        Ok(tag_list)
    }

    /// Get the name of a tag by the id.
    ///
    /// `deleted_rule` mirrors [`Self::get_all_tags`]: `Exclude` hides
    /// tombstoned tags (they read as `MissingTag`), `Include` returns them
    /// with `Tag::deleted = true`.
    pub fn tag_from_id(
        &self,
        tag_id: TagId,
        deleted_rule: DeletedRule,
    ) -> Result<Tag, DatabaseError> {
        let sql = format!(
            "SELECT name, color, deleted FROM tags_v1 WHERE id = ?1{}",
            deleted_rule.and_deleted_clause(),
        );
        let mut statement = self
            .connection
            .prepare(&sql)
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let tag = statement
            .query_map([tag_id], |row| {
                Ok(Tag {
                    id: tag_id,
                    name: row.get(0)?,
                    color: row.get(1)?,
                    metadata: None,
                    deleted: row.get::<_, i64>(2)? != 0,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|tag| tag.unwrap())
            .next()
            .ok_or(DatabaseError::MissingTag)?;

        Ok(tag)
    }

    /// Get the id of a file by its logical path.
    pub fn file_id_from_logical_path(
        &self,
        logical_path: &LogicalPath,
    ) -> Result<FileId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM files_v2 WHERE logical_path = ?1 AND deleted = 0")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file_id = statement
            .query_map([logical_path], |row| row.get(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|id| id.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(file_id)
    }

    /// Find every tag id a query token `$foo`/`!foo` should match.
    ///
    /// A tag matches if **either**:
    /// - its name contains `token` as a case-insensitive substring (so `foo`
    ///   matches `foo`, `foobar`, and `barfoo`), **or**
    /// - its id starts with `token` interpreted as a hex id prefix (so a pasted
    ///   partial id still works).
    ///
    /// Returns all matches (deduplicated); an empty result means nothing
    /// matched, which callers treat as "matches no tag" rather than an
    /// error.
    ///
    /// `deleted_rule` controls whether tombstoned tags participate in the
    /// resolution: search callers that want to find deleted tags pass
    /// [`DeletedRule::Include`] so a token whose only matches are tombstoned
    /// tags still resolves; every other caller passes
    /// [`DeletedRule::Exclude`].
    pub fn tag_ids_matching_token(
        &self,
        token: &str,
        deleted_rule: DeletedRule,
    ) -> Result<Vec<TagId>, DatabaseError> {
        let mut ids: BTreeSet<TagId> = BTreeSet::new();

        // Name substring, case-insensitive. Escape LIKE metacharacters in the
        // user token so `%`/`_` are matched literally; `LIKE` is case-insensitive
        // for ASCII in SQLite by default.
        let escaped = token
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("%{escaped}%");
        let name_sql = format!(
            "SELECT id FROM tags_v1 WHERE name LIKE ?1 ESCAPE '\\'{}",
            deleted_rule.and_deleted_clause(),
        );
        let mut statement = self
            .connection
            .prepare(&name_sql)
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        let name_matches = statement
            .query_map([&pattern], |row| row.get::<_, TagId>(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
        for id in name_matches {
            ids.insert(id.map_err(|_| DatabaseError::FailedToExecuteCommand)?);
        }

        // Id prefix (only when the token is a valid hex id prefix at all).
        if let Some(prefix) = normalise_id_prefix(token) {
            let id_pattern = format!("{prefix}%");
            let id_sql = format!(
                "SELECT id FROM tags_v1 WHERE id LIKE ?1{}",
                deleted_rule.and_deleted_clause(),
            );
            let mut statement = self
                .connection
                .prepare(&id_sql)
                .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
            let id_matches = statement
                .query_map([&id_pattern], |row| row.get::<_, TagId>(0))
                .map_err(|_| DatabaseError::FailedToExecuteCommand)?;
            for id in id_matches {
                ids.insert(id.map_err(|_| DatabaseError::FailedToExecuteCommand)?);
            }
        }

        Ok(ids.into_iter().collect())
    }

    /// Get the id of a tag by the name. Excludes soft-deleted (tombstoned)
    /// tags.
    pub fn tag_id_from_name(&self, name: &str) -> Result<TagId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM tags_v1 WHERE name = ?1 AND deleted = 0")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let tag_id = statement
            .query_map([name], |row| row.get(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|id| id.unwrap())
            .next()
            .ok_or(DatabaseError::MissingTag)?;

        Ok(tag_id)
    }

    /// Get the logical path for `file_id`.
    ///
    /// The inverse of `file_id_from_logical_path`. Errors with `MissingFile` if
    /// the file has no row in `files` (unknown or deleted).
    pub fn logical_path_for_file_id(&self, file_id: FileId) -> Result<LogicalPath, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT logical_path FROM files_v2 WHERE id = ?1 AND deleted = 0")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let logical_path = statement
            .query_map([file_id], |row| row.get(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|logical_path| logical_path.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(logical_path)
    }

    /// The unix-millis time `file_id`'s `logical_path` was last changed — the
    /// path's last-writer-wins clock — or `None` if the file is unknown.
    /// Includes tombstoned rows (unlike [`Self::logical_path_for_file_id`]) so
    /// reconciliation can order a peer's move against our recorded time
    /// regardless of local delete state. Used to decide whether to adopt a
    /// peer's moved path on reconnect.
    pub fn logical_path_modified_at(&self, file_id: FileId) -> Result<Option<i64>, DatabaseError> {
        self.connection
            .query_row(
                "SELECT logical_path_modified_at FROM files_v2 WHERE id = ?1",
                [file_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|_| DatabaseError::FailedToExecuteCommand)
    }

    /// Get the [`FileInfo`] for a single `file_id` — the single-file
    /// counterpart of [`Self::get_all_files`].
    ///
    /// Joins the file to its latest version and computes the "short id" length
    /// with [`Self::shorten_file_id`] (an indexed neighbour lookup, not a full
    /// scan), so this stays cheap per call. Errors with `MissingFile` if the
    /// file has no row, or no version yet.
    ///
    /// `deleted_rule` mirrors [`Self::get_all_files`]: `Exclude` hides
    /// tombstoned files (they read as `MissingFile`), `Include` returns them
    /// with `FileInfo::deleted = true`.
    pub fn file_info_from_id(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, DatabaseError> {
        // f.deleted is aliased; inline the guard rather than using the shared
        // fragment helpers.
        let extra_clause = match deleted_rule {
            DeletedRule::Exclude => " AND f.deleted = 0",
            DeletedRule::Include => "",
        };
        let sql = format!(
            "SELECT f.logical_path, v.content_hash, v.version_number, v.size, f.deleted
             FROM files_v2 AS f
             JOIN file_versions_v1 AS v
               ON v.file_id = f.id
              AND v.version_number = (
                  SELECT MAX(version_number)
                  FROM file_versions_v1 AS inner
                  WHERE inner.file_id = f.id
              )
             WHERE f.id = ?1{extra_clause}"
        );
        let mut statement = self
            .connection
            .prepare(&sql)
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut file = statement
            .query_map([file_id], |row| {
                Ok(FileInfo {
                    file_id,
                    logical_path: row.get(0)?,
                    content_hash: row.get(1)?,
                    version_number: row.get(2)?,
                    size: row.get::<_, i64>(3)? as u64,
                    // Filled in below.
                    short_id_length: 0,
                    deleted: row.get::<_, i64>(4)? != 0,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|file| file.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        file.short_id_length = self.shorten_file_id(file_id)?;

        Ok(file)
    }

    /// List every currently-known file (i.e. with a row in `files`) together
    /// with its latest version's content hash and version number.
    ///
    /// Files are joined to the row in `file_versions` with the highest
    /// `version_number`, using the DESC index on
    /// `(file_id, version_number)`. Files without any recorded version are
    /// excluded (they should not occur in practice, since every add/change
    /// path records a version, but the inner join makes this defensive).
    ///
    /// `deleted_rule` governs tombstone visibility: under
    /// [`DeletedRule::Exclude`] tombstoned files are hidden (the standard
    /// behavior for every non-search caller); under [`DeletedRule::Include`]
    /// they come back too, and callers can distinguish them by the returned
    /// `deleted` flag.
    pub fn get_all_files(&self, deleted_rule: DeletedRule) -> Result<Vec<FileInfo>, DatabaseError> {
        // f.deleted is aliased, so we cannot use the plain " AND deleted = 0"
        // fragment. Inline the guard here.
        let where_clause = match deleted_rule {
            DeletedRule::Exclude => " WHERE f.deleted = 0",
            DeletedRule::Include => "",
        };
        let sql = format!(
            "SELECT f.id, f.logical_path, v.content_hash, v.version_number, v.size, f.deleted
             FROM files_v2 AS f
             JOIN file_versions_v1 AS v
               ON v.file_id = f.id
              AND v.version_number = (
                  SELECT MAX(version_number)
                  FROM file_versions_v1 AS inner
                  WHERE inner.file_id = f.id
              ){where_clause}"
        );
        let mut statement = self
            .connection
            .prepare(&sql)
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let mut files = Vec::new();
        let rows = statement
            .query_map([], |row| {
                Ok(FileInfo {
                    file_id: row.get(0)?,
                    logical_path: row.get(1)?,
                    content_hash: row.get(2)?,
                    version_number: row.get(3)?,
                    size: row.get::<_, i64>(4)? as u64,
                    // Filled in below once we have the whole set.
                    short_id_length: 0,
                    deleted: row.get::<_, i64>(5)? != 0,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        for row in rows {
            files.push(row.map_err(|_| DatabaseError::FailedToExecuteCommand)?);
        }

        // Compute each file's shortest unique id prefix. Because this listing
        // already contains *every* file, we can do it in-memory rather than
        // hitting the DB per file: sort the ids, then each id only needs to be
        // distinguished from its two immediate neighbours in that order (same
        // reasoning as `shorten_file_id`, which the single-id path uses).
        let mut sorted_ids: Vec<String> = files.iter().map(|f| f.file_id.to_string()).collect();
        sorted_ids.sort();
        for file in &mut files {
            let id = file.file_id.to_string();
            let position = sorted_ids
                .binary_search(&id)
                .expect("every file's id is in the sorted set");

            let mut required = 1;
            if position > 0 {
                let predecessor = &sorted_ids[position - 1];
                required = required.max(common_prefix_length(&id, predecessor) + 1);
            }
            if position + 1 < sorted_ids.len() {
                let successor = &sorted_ids[position + 1];
                required = required.max(common_prefix_length(&id, successor) + 1);
            }
            file.short_id_length = required.clamp(1, id.len());
        }

        Ok(files)
    }
}

#[derive(Debug, Clone)]
pub struct SyncDirectoryFile {
    pub file_id: FileId,
    /// The file's physical path on disk, relative to this sync directory's
    /// root. For a `TagBased` directory this equals the file's logical path;
    /// for a `Universal` directory it is the file's `file_id` (files are stored
    /// under their id on disk). This also serves as the reverse index for
    /// filesystem events (path -> file_id), so it must always reflect the
    /// actual on-disk name. It is NOT the value to advertise to peers or
    /// show to users; for that use the logical path from `FileDatabase`
    /// (`FileDatabase.files.logical_path`).
    pub physical_path: PhysicalPath,
}

#[derive(Debug)]
pub struct SyncDirectoryDatabase {
    connection: Connection,
}

impl SyncDirectoryDatabase {
    pub fn initialize(database_path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let connection =
            Connection::open(database_path).map_err(|_| DatabaseError::UnableToOpenOrCreate)?;

        // Run migrations here.

        Self::create_files_v1(&connection)?;

        Ok(Self { connection })
    }

    fn create_files_v1(connection: &Connection) -> Result<(), DatabaseError> {
        // `physical_path` is where the bytes live on disk relative to this
        // sync directory's root, and doubles as the reverse index for
        // filesystem events (path -> file_id). For TagBased it equals the
        // logical path; for Universal it is the `file_id`. The logical/human
        // name lives in `FileDatabase.files_v1`.
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS files_v1 (
                    id              TEXT PRIMARY KEY,
                    physical_path   TEXT NOT NULL
                )",
                (),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    /// Add a new file.
    ///
    /// Note: content hashes are stored in `FileDatabase::file_versions`, not
    /// here. After calling this you typically also want to call
    /// `FileDatabase::record_version`.
    pub fn add_file(
        &self,
        file_id: FileId,
        physical_path: &PhysicalPath,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "INSERT INTO files_v1 (id, physical_path) VALUES (?1, ?2)",
                (file_id, physical_path),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn update_file_physical_path(
        &self,
        file_id: FileId,
        physical_path: &PhysicalPath,
    ) -> Result<(), DatabaseError> {
        self.connection
            .execute(
                "UPDATE files_v1 SET physical_path = ?2 WHERE id = ?1",
                (file_id, physical_path),
            )
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    pub fn remove_file_by_id(&self, file_id: FileId) -> Result<(), DatabaseError> {
        self.connection
            .execute("DELETE FROM files_v1 WHERE id = ?1", [file_id])
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(())
    }

    // pub fn remove_file_by_physical_path(&self, physical_path: impl AsRef<str>) ->
    // Result<(), DatabaseError> {     self.connection
    //         .execute("DELETE FROM files_v1 WHERE physical_path = ?1",
    // [physical_path.as_ref()])         .map_err(|_|
    // DatabaseError::FailedToExecuteCommand)?;
    //
    //     Ok(())
    // }

    pub fn get_file(&self, file_id: FileId) -> Result<SyncDirectoryFile, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT physical_path FROM files_v1 WHERE id = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let file = statement
            .query_map([file_id], |row| {
                Ok(SyncDirectoryFile {
                    file_id,
                    physical_path: row.get(0)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|preview| preview.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(file)
    }

    /// Whether some *other* file (any `file_id` except `except_file_id`)
    /// already occupies `physical_path` in this sync directory. Used to
    /// resolve on-disk naming collisions: two files may share a logical
    /// path, but their bytes must live at distinct physical paths, so
    /// placement appends a suffix until this returns `false`.
    /// Self-exclusion keeps re-placement/no-op moves of an already-placed
    /// file from being treated as a collision with themselves.
    pub fn physical_path_in_use_by_other(
        &self,
        physical_path: &PhysicalPath,
        except_file_id: FileId,
    ) -> Result<bool, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT 1 FROM files_v1 WHERE physical_path = ?1 AND id != ?2")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let in_use = statement
            .query_map((physical_path, except_file_id), |_row| Ok(()))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .next()
            .is_some();

        Ok(in_use)
    }

    pub fn get_file_id(&self, physical_path: &PhysicalPath) -> Result<FileId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM files_v1 WHERE physical_path = ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let id = statement
            .query_map([physical_path], |row| row.get(0))
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|preview| preview.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(id)
    }

    pub fn get_all_files(&self) -> Result<Vec<SyncDirectoryFile>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, physical_path FROM files_v1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        Ok(statement
            .query_map([], |row| {
                Ok(SyncDirectoryFile {
                    file_id: row.get(0)?,
                    physical_path: row.get(1)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|file| file.unwrap())
            .collect())
    }

    pub fn get_all_files_at(
        &self,
        physical_path: &PhysicalPath,
    ) -> Result<Vec<SyncDirectoryFile>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, physical_path FROM files_v1 WHERE physical_path LIKE ?1")
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?;

        let matcher = format!("{}%", physical_path.as_str());

        Ok(statement
            .query_map([matcher], |row| {
                Ok(SyncDirectoryFile {
                    file_id: row.get(0)?,
                    physical_path: row.get(1)?,
                })
            })
            .map_err(|_| DatabaseError::FailedToExecuteCommand)?
            .map(|file| file.unwrap())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memory_db() -> FileDatabase {
        FileDatabase::initialize(":memory:").expect("open in-memory db")
    }

    #[test]
    fn logical_path_for_file_id_roundtrips() {
        let database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("photos/cat.jpg"), 0)
            .unwrap();

        assert_eq!(
            database.logical_path_for_file_id(file_id).unwrap(),
            LogicalPath::new("photos/cat.jpg")
        );
    }

    #[test]
    fn logical_path_for_file_id_missing_is_not_found() {
        let database = memory_db();
        let missing = FileId::new();
        assert!(matches!(
            database.logical_path_for_file_id(missing),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn get_all_files_reports_latest_version() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        // Two versions; get_all_files must report the latest (higher
        // version_number), not the first.
        let v1 = database
            .record_version(file_id, "hash-v1", "local", 1)
            .unwrap();
        let v2 = database
            .record_version(file_id, "hash-v2", "local", 1)
            .unwrap();
        assert!(v2 > v1);

        let files = database.get_all_files(DeletedRule::Exclude).unwrap();
        assert_eq!(files.len(), 1);
        let info = &files[0];
        assert_eq!(info.file_id, file_id);
        assert_eq!(info.logical_path, LogicalPath::new("a.txt"));
        assert_eq!(info.content_hash, "hash-v2");
        assert_eq!(info.version_number, v2);
    }

    #[test]
    fn reverting_to_an_old_hash_becomes_the_new_latest_version() {
        // Regression: reverting a file's content back to a hash it held earlier
        // must record a *new* latest version with that hash — not be treated as
        // a duplicate/no-op. The change-ingest path keys "do we already hold
        // this?" off `latest_version`, so this is the invariant it relies on:
        // an old hash reappearing is the current version again only after a new
        // version row is recorded.
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        database
            .record_version(file_id, "hash-A", "local", 1)
            .unwrap();
        database
            .record_version(file_id, "hash-B", "local", 1)
            .unwrap();
        // Current version is B, but A is still in history.
        assert_eq!(
            database
                .latest_version(file_id)
                .unwrap()
                .unwrap()
                .content_hash,
            "hash-B"
        );

        // Revert to A: a new version whose hash is the old A.
        let reverted = database
            .record_version(file_id, "hash-A", "local", 1)
            .unwrap();
        let latest = database.latest_version(file_id).unwrap().unwrap();
        assert_eq!(latest.content_hash, "hash-A");
        assert_eq!(latest.version_number, reverted);
        // History now has three entries: A, B, A.
        let history = database.version_history(file_id).unwrap();
        assert_eq!(
            history
                .iter()
                .map(|(_, h, _)| h.as_str())
                .collect::<Vec<_>>(),
            vec!["hash-A", "hash-B", "hash-A"]
        );
    }

    #[test]
    fn get_all_files_excludes_files_without_versions() {
        let database = memory_db();
        // A file row with no recorded version: the inner join drops it.
        database
            .add_file(FileId::new(), &LogicalPath::new("orphan.txt"), 0)
            .unwrap();

        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn get_all_files_empty_when_no_files() {
        let database = memory_db();
        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );
    }

    /// Build a `FileId` from a 32-char hex string so tests can control the
    /// exact prefix relationships between ids.
    fn file_id_from_hex(hex: &str) -> FileId {
        FileId::from_string(hex).expect("valid hex uuid")
    }

    #[test]
    fn shorten_file_id_single_file_needs_one_char() {
        let database = memory_db();
        let only = file_id_from_hex("00000000000000000000000000000001");
        database.add_file(only, &LogicalPath::new("a"), 0).unwrap();

        // No neighbours -> a single character already uniquely identifies it.
        assert_eq!(database.shorten_file_id(only).unwrap(), 1);
    }

    #[test]
    fn shorten_file_id_grows_prefix_to_disambiguate_neighbours() {
        let database = memory_db();
        // Three ids: two share the leading `abcd`, one is far away.
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        let far = file_id_from_hex("ffff000000000000000000000000000f");
        for (id, name) in [(shared_a, "a"), (shared_b, "b"), (far, "c")] {
            database.add_file(id, &LogicalPath::new(name), 0).unwrap();
        }

        // shared_a and shared_b agree on `abcd00...000` up to the final hex
        // char, so they must be distinguished at the last differing position.
        let len_a = database.shorten_file_id(shared_a).unwrap();
        let len_b = database.shorten_file_id(shared_b).unwrap();
        let a = shared_a.to_string();
        let b = shared_b.to_string();
        // The prefix of each must exclude the other.
        assert!(!b.starts_with(&a[..len_a]));
        assert!(!a.starts_with(&b[..len_b]));

        // The far id only needs one char (`f`), since neither neighbour shares
        // its first character.
        assert_eq!(database.shorten_file_id(far).unwrap(), 1);
    }

    #[test]
    fn shorten_file_id_missing_is_not_found() {
        let database = memory_db();
        assert!(matches!(
            database.shorten_file_id(FileId::new()),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn get_all_files_reports_short_id_length() {
        let mut database = memory_db();
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        for (id, name) in [(shared_a, "a"), (shared_b, "b")] {
            database.add_file(id, &LogicalPath::new(name), 0).unwrap();
            database.record_version(id, "hash", "local", 1).unwrap();
        }

        let files = database.get_all_files(DeletedRule::Exclude).unwrap();
        // Both ids share all but the final character, so the short id must be
        // the full length to disambiguate.
        for info in &files {
            let full = info.file_id.to_string();
            assert_eq!(info.short_id_length, full.len());
        }
    }

    #[test]
    fn resolve_file_id_prefix_unique_short_prefix() {
        let database = memory_db();
        let far_a = file_id_from_hex("aaaa000000000000000000000000000a");
        let far_b = file_id_from_hex("bbbb000000000000000000000000000b");
        database.add_file(far_a, &LogicalPath::new("a"), 0).unwrap();
        database.add_file(far_b, &LogicalPath::new("b"), 0).unwrap();

        // A single leading char is enough to pick each out.
        assert_eq!(database.resolve_file_id_prefix("a").unwrap(), far_a);
        assert_eq!(database.resolve_file_id_prefix("b").unwrap(), far_b);
    }

    #[test]
    fn resolve_file_id_prefix_accepts_full_and_hyphenated_forms() {
        let database = memory_db();
        let id = file_id_from_hex("7f3a1b2c4d5e6f708192a3b4c5d6e7f8");
        database.add_file(id, &LogicalPath::new("a"), 0).unwrap();

        // Full hex form.
        assert_eq!(
            database
                .resolve_file_id_prefix("7f3a1b2c4d5e6f708192a3b4c5d6e7f8")
                .unwrap(),
            id
        );
        // Hyphenated form (hyphens are stripped before matching).
        assert_eq!(
            database
                .resolve_file_id_prefix("7f3a1b2c-4d5e-6f70-8192-a3b4c5d6e7f8")
                .unwrap(),
            id
        );
    }

    #[test]
    fn resolve_file_id_prefix_ambiguous_is_reported() {
        let database = memory_db();
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        database
            .add_file(shared_a, &LogicalPath::new("a"), 0)
            .unwrap();
        database
            .add_file(shared_b, &LogicalPath::new("b"), 0)
            .unwrap();

        // `abcd` matches both.
        assert!(matches!(
            database.resolve_file_id_prefix("abcd"),
            Err(DatabaseError::AmbiguousIdPrefix(prefix)) if prefix == "abcd"
        ));
    }

    #[test]
    fn resolve_file_id_prefix_unknown_is_missing() {
        let database = memory_db();
        database
            .add_file(
                file_id_from_hex("aaaa000000000000000000000000000a"),
                &LogicalPath::new("a"),
                0,
            )
            .unwrap();

        assert!(matches!(
            database.resolve_file_id_prefix("ffff"),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn resolve_file_id_prefix_rejects_non_hex() {
        let database = memory_db();
        // `zzzz` is not hex; normalisation fails and it resolves to nothing.
        assert!(matches!(
            database.resolve_file_id_prefix("zzzz"),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn shorten_then_resolve_roundtrips() {
        let mut database = memory_db();
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        let far = file_id_from_hex("ffff000000000000000000000000000f");
        for (id, name) in [(shared_a, "a"), (shared_b, "b"), (far, "c")] {
            database.add_file(id, &LogicalPath::new(name), 0).unwrap();
            database.record_version(id, "hash", "local", 1).unwrap();
        }

        // Each file's displayed short id must resolve back to exactly itself.
        for info in database.get_all_files(DeletedRule::Exclude).unwrap() {
            let full = info.file_id.to_string();
            let short = &full[..info.short_id_length];
            assert_eq!(
                database.resolve_file_id_prefix(short).unwrap(),
                info.file_id,
                "short id {short} should resolve to its own file"
            );
        }
    }

    /// Build a `TagId` from a 32-char hex string so tests can control the exact
    /// prefix relationships between ids.
    fn tag_id_from_hex(hex: &str) -> TagId {
        TagId::from_string(hex).expect("valid hex uuid")
    }

    #[test]
    fn resolve_tag_id_prefix_unique_short_prefix() {
        let database = memory_db();
        let far_a = tag_id_from_hex("aaaa000000000000000000000000000a");
        let far_b = tag_id_from_hex("bbbb000000000000000000000000000b");
        database.add_tag(far_a, "a", "red", 1).unwrap();
        database.add_tag(far_b, "b", "red", 1).unwrap();

        assert_eq!(database.resolve_tag_id_prefix("a").unwrap(), far_a);
        assert_eq!(database.resolve_tag_id_prefix("b").unwrap(), far_b);
    }

    #[test]
    fn resolve_tag_id_prefix_ambiguous_is_reported() {
        let database = memory_db();
        let shared_a = tag_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = tag_id_from_hex("abcd000000000000000000000000000b");
        database.add_tag(shared_a, "a", "red", 1).unwrap();
        database.add_tag(shared_b, "b", "red", 1).unwrap();

        assert!(matches!(
            database.resolve_tag_id_prefix("abcd"),
            Err(DatabaseError::AmbiguousIdPrefix(prefix)) if prefix == "abcd"
        ));
    }

    #[test]
    fn resolve_tag_id_prefix_unknown_is_missing() {
        let database = memory_db();
        database
            .add_tag(
                tag_id_from_hex("aaaa000000000000000000000000000a"),
                "a",
                "red",
                1,
            )
            .unwrap();

        assert!(matches!(
            database.resolve_tag_id_prefix("ffff"),
            Err(DatabaseError::MissingTag)
        ));
    }

    #[test]
    fn shorten_then_resolve_tag_roundtrips() {
        let database = memory_db();
        let shared_a = tag_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = tag_id_from_hex("abcd000000000000000000000000000b");
        let far = tag_id_from_hex("ffff000000000000000000000000000f");
        for (id, name) in [(shared_a, "a"), (shared_b, "b"), (far, "c")] {
            database.add_tag(id, name, "red", 1).unwrap();
        }

        // Each tag's displayed short id must resolve back to exactly itself.
        for tag in database.get_all_tags(DeletedRule::Exclude).unwrap() {
            let full = tag.id.to_string();
            let length = database.shorten_tag_id(tag.id).unwrap();
            let short = &full[..length];
            assert_eq!(
                database.resolve_tag_id_prefix(short).unwrap(),
                tag.id,
                "short id {short} should resolve to its own tag"
            );
        }
    }

    #[test]
    fn tag_tag_then_subtag_ids_lists_children() {
        let database = memory_db();
        let parent = TagId::new();
        let child_a = TagId::new();
        let child_b = TagId::new();
        for (id, name) in [(parent, "parent"), (child_a, "a"), (child_b, "b")] {
            database.add_tag(id, name, "red", 1).unwrap();
        }

        database.tag_tag(parent, child_a, 10).unwrap();
        database.tag_tag(parent, child_b, 10).unwrap();

        let subtags: BTreeSet<TagId> = database
            .subtag_ids_for_tag(parent, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(subtags, BTreeSet::from([child_a, child_b]));
    }

    #[test]
    fn subtag_ids_include_walks_transitively() {
        let database = memory_db();
        let grandparent = TagId::new();
        let parent = TagId::new();
        let child = TagId::new();
        for (id, name) in [(grandparent, "gp"), (parent, "p"), (child, "c")] {
            database.add_tag(id, name, "red", 1).unwrap();
        }

        database.tag_tag(grandparent, parent, 10).unwrap();
        database.tag_tag(parent, child, 10).unwrap();

        // Direct only: just `parent`.
        let direct: BTreeSet<TagId> = database
            .subtag_ids_for_tag(grandparent, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(direct, BTreeSet::from([parent]));

        // Transitive: `parent` and `child`.
        let transitive: BTreeSet<TagId> = database
            .subtag_ids_for_tag(grandparent, SubtagRule::Include)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(transitive, BTreeSet::from([parent, child]));
    }

    #[test]
    fn untag_tag_removes_child_from_subtags() {
        let database = memory_db();
        let parent = TagId::new();
        let child = TagId::new();
        database.add_tag(parent, "parent", "red", 1).unwrap();
        database.add_tag(child, "child", "red", 1).unwrap();

        database.tag_tag(parent, child, 10).unwrap();
        database.untag_tag(parent, child, 20).unwrap();

        let subtags: Vec<TagId> = database
            .subtag_ids_for_tag(parent, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(subtags.is_empty());
    }

    #[test]
    fn tag_tag_rejects_self() {
        let database = memory_db();
        let tag = TagId::new();
        database.add_tag(tag, "t", "red", 1).unwrap();
        assert!(matches!(
            database.tag_tag(tag, tag, 10),
            Err(DatabaseError::CantTagItself)
        ));
    }

    #[test]
    fn add_tag_newer_modified_at_wins_older_is_noop() {
        let database = memory_db();
        let tag_id = TagId::new();

        database.add_tag(tag_id, "work", "red", 100).unwrap();
        // A newer definition overwrites.
        database.add_tag(tag_id, "job", "blue", 200).unwrap();
        let (name, color, modified_at) = database.tag_definition(tag_id).unwrap().unwrap();
        assert_eq!(
            (name.as_str(), color.as_str(), modified_at),
            ("job", "blue", 200)
        );

        // A stale definition (older modified_at) must not clobber.
        database.add_tag(tag_id, "stale", "green", 150).unwrap();
        let (name, _, modified_at) = database.tag_definition(tag_id).unwrap().unwrap();
        assert_eq!((name.as_str(), modified_at), ("job", 200));
    }

    #[test]
    fn update_tag_name_respects_lww() {
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "red", 100).unwrap();

        // Older rename loses.
        database.update_tag_name(tag_id, "old", 50).unwrap();
        assert_eq!(database.tag_definition(tag_id).unwrap().unwrap().0, "work");

        // Newer rename wins.
        database.update_tag_name(tag_id, "new", 300).unwrap();
        assert_eq!(database.tag_definition(tag_id).unwrap().unwrap().0, "new");
    }

    #[test]
    fn duplicate_tag_names_coexist() {
        // UNIQUE(name) was relaxed: two tags may share a name.
        let database = memory_db();
        let a = TagId::new();
        let b = TagId::new();
        database.add_tag(a, "work", "red", 100).unwrap();
        database.add_tag(b, "work", "blue", 100).unwrap();

        assert!(database.tag_definition(a).unwrap().is_some());
        assert!(database.tag_definition(b).unwrap().is_some());
        let all: Vec<_> = database
            .get_all_tags(DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn tag_file_then_untag_soft_deletes_and_hides_from_reads() {
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        database.tag_file(tag_id, file_id, 100).unwrap();
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);

        // Untag soft-deletes: the read no longer sees it...
        database.untag_file(tag_id, file_id, 200).unwrap();
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(tags.is_empty());

        // ...but the tombstone row survives for reconciliation.
        let manifest = database.relationship_manifest_entries().unwrap();
        assert_eq!(manifest.len(), 1);
        assert!(manifest[0].deleted);
        assert_eq!(manifest[0].modified_at, 200);
    }

    #[test]
    fn deleting_a_tag_hides_it_from_applied_tags() {
        // Regression: tagging a file then deleting the *tag* (not untagging)
        // leaves the relationship live but the tag row tombstoned. The applied-
        // tags read must exclude it so the UI doesn't fetch a MissingTag.
        let mut database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.add_tag(tag_id, "work", "red", 10).unwrap();
        database.tag_file(tag_id, file_id, 100).unwrap();

        // Applied while the tag is live.
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);

        // Delete the tag itself (relationship is untouched).
        assert!(database.remove_tag(tag_id, 200).unwrap());
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(tags.is_empty(), "a deleted tag must not appear as applied");

        // Restoring the tag makes it applied again (the relationship survived).
        database.add_tag(tag_id, "work", "red", 300).unwrap();
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);
    }

    #[test]
    fn applied_tag_without_definition_is_still_returned() {
        // A `FileTagged` relationship can arrive before the tag's `TagAdded`
        // definition during reconciliation, so a live relationship may point at
        // a tag with no `tags_v1` row yet. That tag must still be returned (the
        // filter only excludes *known-tombstoned* tags).
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        // Tag the file without ever defining the tag.
        database.tag_file(tag_id, file_id, 100).unwrap();

        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);
    }

    #[test]
    fn stale_untag_does_not_override_newer_tag() {
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        // Tag at t=200, then a stale untag at t=100 arrives out of order.
        database.tag_file(tag_id, file_id, 200).unwrap();
        database.untag_file(tag_id, file_id, 100).unwrap();

        // The tag must still be present (newer wins).
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);
    }

    #[test]
    fn retag_after_untag_revives_row() {
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        database.tag_file(tag_id, file_id, 100).unwrap();
        database.untag_file(tag_id, file_id, 200).unwrap();
        database.tag_file(tag_id, file_id, 300).unwrap();

        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, vec![tag_id]);
        // Still a single row (revived, not duplicated).
        assert_eq!(database.relationship_manifest_entries().unwrap().len(), 1);
    }

    #[test]
    fn apply_relationship_reconciles_tombstone_with_lww() {
        let database = memory_db();
        let file_id = FileId::new();
        let tag_id = TagId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();

        // Locally the file is tagged at t=100.
        database.tag_file(tag_id, file_id, 100).unwrap();

        // A peer's manifest carries a newer tombstone (untagged at t=200).
        let incoming = RelationshipManifestEntry {
            tag_id,
            target_id: file_id.to_string(),
            kind: RelationshipKind::File,
            modified_at: 200,
            deleted: true,
        };
        database.apply_relationship(&incoming).unwrap();

        // The newer tombstone wins: the tag is now absent.
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(tags.is_empty());

        // A stale "present" (t=150) from another peer must not resurrect it.
        let stale = RelationshipManifestEntry {
            tag_id,
            target_id: file_id.to_string(),
            kind: RelationshipKind::File,
            modified_at: 150,
            deleted: false,
        };
        database.apply_relationship(&stale).unwrap();
        let tags: Vec<_> = database
            .tag_ids_for_file(file_id, SubtagRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(tags.is_empty());
    }

    #[test]
    fn tag_manifest_entries_reports_all_definitions() {
        let database = memory_db();
        let a = TagId::new();
        let b = TagId::new();
        database.add_tag(a, "one", "red", 111).unwrap();
        database.add_tag(b, "two", "blue", 222).unwrap();

        let mut entries = database.tag_manifest_entries().unwrap();
        entries.sort_by_key(|entry| entry.modified_at);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].modified_at, 111);
        assert_eq!(entries[1].modified_at, 222);
    }

    #[test]
    fn tag_ids_matching_token_matches_name_substring_case_insensitively() {
        let database = memory_db();
        let foo = TagId::new();
        let foobar = TagId::new();
        let barfoo = TagId::new();
        let unrelated = TagId::new();
        database.add_tag(foo, "foo", "red", 1).unwrap();
        database.add_tag(foobar, "foobar", "red", 1).unwrap();
        database.add_tag(barfoo, "barfoo", "red", 1).unwrap();
        database.add_tag(unrelated, "baz", "red", 1).unwrap();

        // A different case still matches (case-insensitive substring).
        let matched: BTreeSet<TagId> = database
            .tag_ids_matching_token("FOO", DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(
            matched,
            BTreeSet::from([foo, foobar, barfoo]),
            "should match every tag whose name contains 'foo'"
        );
    }

    #[test]
    fn tag_ids_matching_token_empty_when_nothing_matches() {
        let database = memory_db();
        database.add_tag(TagId::new(), "alpha", "red", 1).unwrap();

        assert!(
            database
                .tag_ids_matching_token("nope", DeletedRule::Exclude)
                .unwrap()
                .is_empty(),
            "an unmatched token yields an empty set, not an error"
        );
    }

    #[test]
    fn file_ids_for_query_positive_term_unions_all_matching_tags() {
        let database = memory_db();
        // Two distinct tags both containing the substring 'foo'.
        let foo = TagId::new();
        let foobar = TagId::new();
        database.add_tag(foo, "foo", "red", 1).unwrap();
        database.add_tag(foobar, "foobar", "red", 1).unwrap();

        // file_a carries `foo`, file_b carries `foobar`, file_c carries neither.
        let file_a = FileId::new();
        let file_b = FileId::new();
        let file_c = FileId::new();
        for (id, path) in [(file_a, "a"), (file_b, "b"), (file_c, "c")] {
            database.add_file(id, &LogicalPath::new(path), 0).unwrap();
        }
        database.tag_file(foo, file_a, 1).unwrap();
        database.tag_file(foobar, file_b, 1).unwrap();

        // `$foo` should match files carrying either tag (union), not require both.
        let terms = vec![QueryTerm::HasTag(vec![foo, foobar])];
        let matched: BTreeSet<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(matched, BTreeSet::from([file_a, file_b]));
    }

    #[test]
    fn file_ids_for_query_empty_positive_set_matches_nothing() {
        let database = memory_db();
        let file = FileId::new();
        database.add_file(file, &LogicalPath::new("a"), 0).unwrap();

        // A `$foo` term that matched no tag (empty set) matches no file.
        let terms = vec![QueryTerm::HasTag(vec![])];
        let matched: Vec<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert!(matched.is_empty());
    }

    #[test]
    fn file_ids_for_query_negative_term_excludes_union_of_matching_tags() {
        let mut database = memory_db();
        let foo = TagId::new();
        let foobar = TagId::new();
        database.add_tag(foo, "foo", "red", 1).unwrap();
        database.add_tag(foobar, "foobar", "red", 1).unwrap();

        let file_a = FileId::new();
        let file_b = FileId::new();
        let file_c = FileId::new();
        for (id, path) in [(file_a, "a"), (file_b, "b"), (file_c, "c")] {
            database.add_file(id, &LogicalPath::new(path), 0).unwrap();
            // A negative-only query seeds from all files, which requires each to
            // have a recorded version to appear in the listing.
            database.record_version(id, "hash", "test", 1).unwrap();
        }
        database.tag_file(foo, file_a, 1).unwrap();
        database.tag_file(foobar, file_b, 1).unwrap();

        // `!foo` excludes files carrying either matching tag, leaving only file_c.
        let terms = vec![QueryTerm::NotTag(vec![foo, foobar])];
        let matched: BTreeSet<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(matched, BTreeSet::from([file_c]));
    }

    #[test]
    fn size_round_trips_through_version_and_file_info() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database
            .record_version(file_id, "hash-1", "local", 42)
            .unwrap();

        // Latest version carries the size.
        assert_eq!(database.latest_version(file_id).unwrap().unwrap().size, 42);
        // FileInfo surfaces it (as u64).
        assert_eq!(
            database
                .file_info_from_id(file_id, DeletedRule::Exclude)
                .unwrap()
                .size,
            42
        );

        // A new version records its own size; the latest reflects the newest.
        database
            .record_version(file_id, "hash-2", "local", 100)
            .unwrap();
        assert_eq!(
            database
                .file_info_from_id(file_id, DeletedRule::Exclude)
                .unwrap()
                .size,
            100
        );
    }

    #[test]
    fn zero_byte_size_is_a_real_value() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("empty.txt"), 0)
            .unwrap();
        database
            .record_version(file_id, "hash-0", "local", 0)
            .unwrap();
        assert_eq!(
            database
                .file_info_from_id(file_id, DeletedRule::Exclude)
                .unwrap()
                .size,
            0
        );
    }

    #[test]
    fn version_history_carries_per_version_size() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 10).unwrap();
        database.record_version(file_id, "h2", "local", 20).unwrap();

        let history = database.version_history(file_id).unwrap();
        assert_eq!(
            history,
            vec![(1, "h1".to_owned(), 10), (2, "h2".to_owned(), 20),]
        );
    }

    #[test]
    fn file_delete_soft_deletes_and_hides_from_reads() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 1).unwrap();

        // Delete with a timestamp newer than the version's observed_at.
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());

        // Hidden from user-facing reads...
        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            database.file_info_from_id(file_id, DeletedRule::Exclude),
            Err(DatabaseError::MissingFile)
        ));
        // ...but the row (and its history) still exists for reconciliation.
        assert!(database.file_exists(file_id).unwrap());
        assert_eq!(
            database.file_deletion_state(file_id).unwrap(),
            Some(DeletionState {
                deleted: true,
                deleted_at,
                restored_at: 0,
            })
        );
    }

    #[test]
    fn file_delete_loses_to_newer_edit() {
        // Restore-after-delete: an edit whose observed_at is newer than the
        // delete keeps the file live (the delete is a no-op).
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        // record_version stamps observed_at = now; a delete stamped in the
        // past must lose.
        database.record_version(file_id, "h1", "local", 1).unwrap();
        let stale_delete_at = 1; // far in the past

        assert!(!database.remove_file(file_id, stale_delete_at).unwrap());
        // File stays visible.
        assert_eq!(
            database.get_all_files(DeletedRule::Exclude).unwrap().len(),
            1
        );
        assert_eq!(
            database.file_deletion_state(file_id).unwrap(),
            Some(DeletionState {
                deleted: false,
                deleted_at: 0,
                restored_at: 0,
            })
        );
    }

    #[test]
    fn apply_restore_clears_tombstone() {
        // An explicit restore whose stamp beats `deleted_at` clears the
        // tombstone via the `restored_at` clock — no fabricated version.
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 1).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());
        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );

        // A restore stamped after the delete wins and makes the file live.
        let restored_at = deleted_at + 1;
        assert!(database.apply_restore(file_id, restored_at).unwrap());
        assert_eq!(
            database.get_all_files(DeletedRule::Exclude).unwrap().len(),
            1
        );
        assert_eq!(
            database.file_deletion_state(file_id).unwrap(),
            Some(DeletionState {
                deleted: false,
                deleted_at,
                restored_at,
            })
        );
    }

    #[test]
    fn apply_restore_loses_to_newer_delete() {
        // A restore older than the delete does not resurrect the file.
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 1).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());

        // A restore stamped before the delete loses; file stays tombstoned.
        assert!(!database.apply_restore(file_id, deleted_at - 1).unwrap());
        assert!(
            database
                .get_all_files(DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );

        // ...and a later delete still wins over that restore stamp.
        assert!(database.remove_file(file_id, deleted_at + 5).unwrap());
    }

    #[test]
    fn delete_loses_to_newer_restore() {
        // Three-way LWW: a restore newer than a subsequent delete keeps the
        // file live (the delete is a no-op).
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 1).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());
        // Restore after that delete.
        assert!(database.apply_restore(file_id, deleted_at + 10).unwrap());
        // A delete stamped between the two loses to the restore.
        assert!(!database.remove_file(file_id, deleted_at + 5).unwrap());
        assert_eq!(
            database.get_all_files(DeletedRule::Exclude).unwrap().len(),
            1
        );
    }

    #[test]
    fn update_logical_path_applies_when_newer() {
        let database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("old.txt"), 10)
            .unwrap();

        // A strictly-newer move wins.
        let applied = database
            .update_file_logical_path(file_id, &LogicalPath::new("new.txt"), 20)
            .unwrap();
        assert!(applied);
        assert_eq!(
            database.logical_path_for_file_id(file_id).unwrap(),
            LogicalPath::new("new.txt")
        );
        assert_eq!(
            database.logical_path_modified_at(file_id).unwrap(),
            Some(20)
        );
    }

    #[test]
    fn update_logical_path_rejects_older_or_equal() {
        let database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("current.txt"), 20)
            .unwrap();

        // An older move loses.
        let older = database
            .update_file_logical_path(file_id, &LogicalPath::new("stale.txt"), 10)
            .unwrap();
        assert!(!older);
        // An equal-timestamp move also loses (strict >), making re-delivery a
        // no-op.
        let equal = database
            .update_file_logical_path(file_id, &LogicalPath::new("dup.txt"), 20)
            .unwrap();
        assert!(!equal);

        assert_eq!(
            database.logical_path_for_file_id(file_id).unwrap(),
            LogicalPath::new("current.txt")
        );
        assert_eq!(
            database.logical_path_modified_at(file_id).unwrap(),
            Some(20)
        );
    }

    #[test]
    fn deleted_file_appears_in_manifest_with_tombstone() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("a.txt"), 0)
            .unwrap();
        database.record_version(file_id, "h1", "local", 7).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(file_id, deleted_at).unwrap());

        let entries = database.manifest_entries().unwrap();
        assert_eq!(entries.len(), 1);
        let (id, history, _observed, _path, _path_modified, deleted, got_deleted_at, restored_at) =
            &entries[0];
        assert_eq!(*id, file_id);
        assert!(*deleted);
        assert_eq!(*got_deleted_at, deleted_at);
        assert_eq!(*restored_at, 0);
        // History (with sizes) is retained so the file can be restored.
        assert_eq!(history, &vec![(1, "h1".to_owned(), 7)]);
    }

    #[test]
    fn tag_delete_soft_deletes_and_hides_from_reads() {
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "red", 10).unwrap();

        // Delete with a newer modified_at.
        assert!(database.remove_tag(tag_id, 20).unwrap());

        // Hidden from user-facing reads.
        assert!(
            database
                .get_all_tags(DeletedRule::Exclude)
                .unwrap()
                .into_iter()
                .next()
                .is_none()
        );
        assert!(matches!(
            database.tag_from_id(tag_id, DeletedRule::Exclude),
            Err(DatabaseError::MissingTag)
        ));
        // But the tombstone is advertised in the manifest with a bumped
        // modified_at.
        let entries = database.tag_manifest_entries().unwrap();
        let entry = entries.iter().find(|e| e.tag_id == tag_id).unwrap();
        assert!(entry.deleted);
        assert_eq!(entry.modified_at, 20);
    }

    #[test]
    fn tag_delete_loses_to_newer_edit() {
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "red", 100).unwrap();

        // A delete older than the tag's modified_at is a no-op (LWW).
        assert!(!database.remove_tag(tag_id, 50).unwrap());
        assert!(database.tag_from_id(tag_id, DeletedRule::Exclude).is_ok());
    }

    #[test]
    fn newer_add_revives_deleted_tag() {
        // Restore: a rename/re-add newer than the delete clears the tombstone.
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "red", 10).unwrap();
        assert!(database.remove_tag(tag_id, 20).unwrap());
        assert!(database.tag_from_id(tag_id, DeletedRule::Exclude).is_err());

        // A newer add (upsert) revives it.
        database.add_tag(tag_id, "work", "blue", 30).unwrap();
        let tag = database.tag_from_id(tag_id, DeletedRule::Exclude).unwrap();
        assert_eq!(tag.color, "blue");
    }

    #[test]
    fn restore_tag_preserves_definition() {
        // `Api::restore_tag` reads the tombstoned tag's current name/color and
        // re-announces them via `TagAdded` (add_tag) with a fresh timestamp.
        // This models that round-trip: the revived tag keeps its definition and
        // becomes live again.
        let database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "work", "#123456", 10).unwrap();
        assert!(database.remove_tag(tag_id, 20).unwrap());

        // Read the tombstoned definition (as the API does with `Include`)...
        let deleted = database.tag_from_id(tag_id, DeletedRule::Include).unwrap();
        assert!(deleted.deleted);
        assert_eq!(deleted.name, "work");
        assert_eq!(deleted.color, "#123456");

        // ...and re-announce it with a newer timestamp to restore it.
        database
            .add_tag(tag_id, deleted.name, deleted.color, 30)
            .unwrap();
        let restored = database.tag_from_id(tag_id, DeletedRule::Exclude).unwrap();
        assert!(!restored.deleted);
        assert_eq!(restored.name, "work");
        assert_eq!(restored.color, "#123456");
    }

    #[test]
    fn get_all_files_include_returns_tombstoned_with_flag() {
        // Under `Exclude` (default), a tombstoned file is invisible. Under
        // `Include`, it comes back with `FileInfo::deleted = true`, letting
        // the UI's "show deleted" toggle distinguish it from live rows.
        let mut database = memory_db();
        let live = FileId::new();
        let dead = FileId::new();
        database
            .add_file(live, &LogicalPath::new("live.txt"), 0)
            .unwrap();
        database
            .add_file(dead, &LogicalPath::new("dead.txt"), 0)
            .unwrap();
        database.record_version(live, "h1", "local", 1).unwrap();
        database.record_version(dead, "h2", "local", 1).unwrap();
        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(dead, deleted_at).unwrap());

        let excluded: Vec<_> = database.get_all_files(DeletedRule::Exclude).unwrap();
        assert_eq!(excluded.len(), 1);
        assert_eq!(excluded[0].file_id, live);
        assert!(!excluded[0].deleted);

        let included: Vec<_> = database.get_all_files(DeletedRule::Include).unwrap();
        assert_eq!(included.len(), 2);
        let dead_info = included.iter().find(|f| f.file_id == dead).unwrap();
        assert!(dead_info.deleted);
        let live_info = included.iter().find(|f| f.file_id == live).unwrap();
        assert!(!live_info.deleted);
    }

    #[test]
    fn get_all_tags_include_returns_tombstoned_with_flag() {
        // Sibling of `get_all_files_include_returns_tombstoned_with_flag` on
        // the tag axis.
        let database = memory_db();
        let live = TagId::new();
        let dead = TagId::new();
        database.add_tag(live, "live", "red", 10).unwrap();
        database.add_tag(dead, "dead", "blue", 10).unwrap();
        assert!(database.remove_tag(dead, 20).unwrap());

        let excluded: Vec<_> = database
            .get_all_tags(DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(excluded.len(), 1);
        assert_eq!(excluded[0].id, live);
        assert!(!excluded[0].deleted);

        let included: Vec<_> = database
            .get_all_tags(DeletedRule::Include)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(included.len(), 2);
        let dead_tag = included.iter().find(|t| t.id == dead).unwrap();
        assert!(dead_tag.deleted);
    }

    #[test]
    fn file_ids_for_query_include_returns_tombstoned_candidates() {
        // A `HasTag` search under `DeletedRule::Include` must surface files
        // whose row is tombstoned in `files_v1` (they still have live
        // `entries_v1` rows pointing at them). The caller is expected to
        // post-filter by `FileInfo::deleted` to keep only the tombstoned
        // ones; the evaluator does not do that step itself.
        let mut database = memory_db();
        let tag_id = TagId::new();
        database.add_tag(tag_id, "photos", "red", 10).unwrap();

        let live = FileId::new();
        let dead = FileId::new();
        database
            .add_file(live, &LogicalPath::new("live.jpg"), 0)
            .unwrap();
        database
            .add_file(dead, &LogicalPath::new("dead.jpg"), 0)
            .unwrap();
        database.record_version(live, "h1", "local", 1).unwrap();
        database.record_version(dead, "h2", "local", 1).unwrap();
        database.tag_file(tag_id, live, 20).unwrap();
        database.tag_file(tag_id, dead, 20).unwrap();

        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(dead, deleted_at).unwrap());

        let terms = vec![QueryTerm::HasTag(vec![tag_id])];

        // Exclude does not drop the tombstoned candidate at the evaluator
        // layer either; the "hide deleted" filtering happens downstream in
        // `Api::run_query` (via `file_info_from_id` returning MissingFile).
        // What we assert here is that both ids reach the caller and the
        // downstream filter has enough information to distinguish them.
        let candidates: BTreeSet<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Include)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(candidates, BTreeSet::from([live, dead]));

        // Joining under `Include` returns both, tagged with `deleted`.
        let live_info = database
            .file_info_from_id(live, DeletedRule::Include)
            .unwrap();
        let dead_info = database
            .file_info_from_id(dead, DeletedRule::Include)
            .unwrap();
        assert!(!live_info.deleted);
        assert!(dead_info.deleted);
    }

    #[test]
    fn tag_ids_matching_token_include_covers_tombstoned_tags() {
        // A search that wants to see deleted tags must still be able to
        // resolve a name-substring token to a tombstoned tag; otherwise
        // `run_query` with `DeletedRule::Include` would have no way to walk
        // back to a deleted tag by name.
        let database = memory_db();
        let dead = TagId::new();
        database.add_tag(dead, "receipts", "red", 10).unwrap();
        assert!(database.remove_tag(dead, 20).unwrap());

        // Exclude hides the tombstoned tag.
        assert!(
            database
                .tag_ids_matching_token("receipt", DeletedRule::Exclude)
                .unwrap()
                .is_empty()
        );
        // Include finds it.
        let matched = database
            .tag_ids_matching_token("receipt", DeletedRule::Include)
            .unwrap();
        assert_eq!(matched, vec![dead]);
    }
}
