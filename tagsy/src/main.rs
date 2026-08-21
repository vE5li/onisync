//! Tagsy CLI client

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use owo_colors::OwoColorize;
use serde::Serialize;
use serde_json::json;
use tagsy_core::{FileId, FileInfo, TagId};
use tagsyd::control::IpcClientBackend;
use tagsyd::operations::{Operation, OperationKind, OperationStatus};
use tagsyd::store::{DeletedRule, SubtagRule, Tag};
use tagsyd::transport::TransportBackend;

/// How command results are rendered to stdout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    /// Human-friendly tables and prose.
    Human,
    /// Machine-readable JSON (one value per command, pretty-printed).
    Json,
}

/// A serializable tag row, shared by every command that prints tags. Mirrors
/// the human [`tag_table`] columns: the tag's id, name, color, and the names
/// of the tags applied to it.
///
/// `deleted` mirrors the daemon's tombstone flag and is always `false` for
/// commands that only surface live rows; commands that can surface deleted
/// rows (currently `search --deleted`) set it accordingly so scripts can
/// distinguish tombstones from live tags without a second lookup.
#[derive(Debug, Serialize)]
struct TagRow {
    id: TagId,
    name: String,
    color: String,
    tags: Vec<String>,
    deleted: bool,
}

/// A serializable file row, shared by every command that prints files. Mirrors
/// the human [`file_table`] columns plus the raw fields useful to scripts. See
/// [`TagRow`] for the `deleted` field's semantics.
#[derive(Debug, Serialize)]
struct FileRow {
    id: FileId,
    path: String,
    version: i64,
    content_hash: String,
    size: u64,
    tags: Vec<String>,
    deleted: bool,
}

impl TagRow {
    /// Build a row from a tag and its applied-tag names (see [`tags_by_tag`]).
    fn new(tag: &Tag, tags: Vec<String>) -> Self {
        Self {
            id: tag.id,
            name: tag.name.clone(),
            color: tag.color.clone(),
            tags,
            deleted: tag.deleted,
        }
    }
}

impl FileRow {
    /// Build a row from a file's info and its tag names (see [`tags_by_file`]).
    fn new(file: &FileInfo, tags: Vec<String>) -> Self {
        Self {
            id: file.file_id,
            path: file.logical_path.to_string(),
            version: file.version_number,
            content_hash: file.content_hash.clone(),
            size: file.size,
            tags,
            deleted: file.deleted,
        }
    }
}

/// Print a serializable value as pretty JSON to stdout.
fn print_json(value: &impl Serialize) {
    match serde_json::to_string_pretty(value) {
        Ok(text) => println!("{text}"),
        Err(error) => eprintln!("{{\"error\":\"failed to serialize output: {error}\"}}"),
    }
}

/// Number of leading characters needed to uniquely identify `target` among
/// `all` ids (jj-style short change ids).
fn unique_prefix_length(target: &str, all: &[String]) -> usize {
    for length in 1..=target.len() {
        let prefix = &target[..length];
        let collisions = all
            .iter()
            .filter(|other| other.as_str() != target && other.starts_with(prefix))
            .count();

        if collisions == 0 {
            return length;
        }
    }

    target.len()
}

/// Render an id with its unique prefix highlighted and the remainder dimmed,
/// mirroring how `jj` displays change ids.
fn highlight_id(id: &str, prefix_length: usize) -> String {
    let (unique, rest) = id.split_at(prefix_length.min(id.len()));
    format!("{}{}", unique.magenta().bold(), rest.bright_black())
}

/// Translate the `--include-subtags` (or `--recursive`) flag into a
/// [`SubtagRule`].
fn subtag_rule(include: bool) -> SubtagRule {
    if include {
        SubtagRule::Include
    } else {
        SubtagRule::Exclude
    }
}

/// Translate the `--deleted` flag into a [`DeletedRule`]. `true` means
/// search-over-tombstones (`Include`, which returns *only* tombstoned rows
/// per `Api::run_query`'s semantics); `false` is the standard live-only
/// search.
fn deleted_rule(deleted: bool) -> DeletedRule {
    if deleted {
        DeletedRule::Include
    } else {
        DeletedRule::Exclude
    }
}

/// The single tag table used by *every* command that prints a set of tags
/// (`list-tags`, `tags-for-file`, `subtags`).
///
/// Short-id prefixes are highlighted the way `jj`/`git` show change ids. The
/// prefix length is computed against `tags`, so pass the full set you intend to
/// display; the highlighted prefix is a valid lookup key for the tag commands.
///
/// The `Tags` column shows the tags applied to each tag (the tags it is a
/// subtag of), the tag analogue of the file table's per-file tags.
/// `tags_by_tag` supplies those names; a tag absent from the map renders with
/// an empty column.
fn tag_table(tags: &[Tag], tags_by_tag: &HashMap<TagId, Vec<String>>) -> Table {
    let ids: Vec<String> = tags.iter().map(|tag| tag.id.to_string()).collect();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["Tag id", "Name", "Color", "Tags"]);

    for tag in tags {
        let id = tag.id.to_string();
        let prefix_length = unique_prefix_length(&id, &ids);
        let tags_column = tags_by_tag
            .get(&tag.id)
            .map(|names| names.join(", "))
            .unwrap_or_default();

        table.add_row(vec![
            Cell::new(highlight_id(&id, prefix_length)),
            Cell::new(&tag.name),
            Cell::new(&tag.color),
            // TODO: Store the ids instead of the names.
            Cell::new(tags_column),
        ]);
    }

    table
}

/// The single file table used by *every* command that prints a set of files
/// (`list-files`, `files-for-tag`).
///
/// The short-id prefix comes from the daemon-computed `short_id_length` (unique
/// against *all* files, so it is a valid global lookup key). `tags_by_file`
/// supplies the human-readable tag names shown per file; a file absent from the
/// map renders with an empty tag column.
fn file_table(files: &[FileInfo], tags_by_file: &HashMap<FileId, Vec<String>>) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["File id", "Path", "Version", "Size", "Tags"]);

    for file in files {
        let id = file.file_id.to_string();
        let tags = tags_by_file
            .get(&file.file_id)
            .map(|names| names.join(", "))
            .unwrap_or_default();

        table.add_row(vec![
            Cell::new(highlight_id(&id, file.short_id_length)),
            Cell::new(&file.logical_path),
            Cell::new(format!("v{}", file.version_number)),
            Cell::new(format!("{}b", file.size)),
            Cell::new(tags),
        ]);
    }

    table
}

/// Emit a set of tags in the selected [`OutputMode`]: the shared [`tag_table`]
/// (or `(no tags)`) for humans, or a JSON array of [`TagRow`]s for scripts.
fn emit_tags(output_mode: OutputMode, tags: &[Tag], tags_by_tag: &HashMap<TagId, Vec<String>>) {
    match output_mode {
        OutputMode::Human => {
            if tags.is_empty() {
                println!("(no tags)");
            } else {
                println!("{}", tag_table(tags, tags_by_tag));
            }
        }
        OutputMode::Json => {
            let rows: Vec<TagRow> = tags
                .iter()
                .map(|tag| TagRow::new(tag, tags_by_tag.get(&tag.id).cloned().unwrap_or_default()))
                .collect();

            print_json(&rows);
        }
    }
}

/// Emit a set of files in the selected [`OutputMode`]: the shared
/// [`file_table`] (or `(no files)`) for humans, or a JSON array of [`FileRow`]s
/// for scripts.
fn emit_files(
    output_mode: OutputMode,
    files: &[FileInfo],
    tags_by_file: &HashMap<FileId, Vec<String>>,
) {
    match output_mode {
        OutputMode::Human => {
            if files.is_empty() {
                println!("(no files)");
            } else {
                println!("{}", file_table(files, tags_by_file));
            }
        }
        OutputMode::Json => {
            let rows: Vec<FileRow> = files
                .iter()
                .map(|file| {
                    FileRow::new(
                        file,
                        tags_by_file.get(&file.file_id).cloned().unwrap_or_default(),
                    )
                })
                .collect();

            print_json(&rows);
        }
    }
}

/// Human-readable label for an [`OperationKind`]: a short verb phrase for the
/// "Action" column of the operations table.
fn operation_kind_label(kind: &OperationKind) -> String {
    match kind {
        OperationKind::ConnectingToPeer { url, .. } => format!("Connecting ({url})"),
        OperationKind::PeerConnected { direction, .. } => match direction {
            tagsyd::operations::Direction::Outbound => "Connected (outbound)".to_owned(),
            tagsyd::operations::Direction::Inbound => "Connected (inbound)".to_owned(),
        },
        OperationKind::ReceivingFile { .. } => "Receiving".to_owned(),
        OperationKind::Fetching { .. } => "Fetching".to_owned(),
        OperationKind::ReconcilingManifest { .. } => "Reconciling manifest".to_owned(),
        OperationKind::ReconcilingTags { .. } => "Reconciling tags".to_owned(),
        OperationKind::PlacingFile { .. } => "Placing file".to_owned(),
    }
}

/// The peer an operation involves, if any (its configured name).
fn operation_peer(kind: &OperationKind) -> Option<&str> {
    match kind {
        OperationKind::ConnectingToPeer { peer_name, .. }
        | OperationKind::PeerConnected { peer_name, .. }
        | OperationKind::ReceivingFile { peer_name, .. }
        | OperationKind::ReconcilingManifest { peer_name }
        | OperationKind::ReconcilingTags { peer_name } => Some(peer_name),
        OperationKind::Fetching { .. } | OperationKind::PlacingFile { .. } => None,
    }
}

/// The file an operation concerns, if any (its id string).
fn operation_file(kind: &OperationKind) -> Option<&str> {
    match kind {
        OperationKind::ReceivingFile { file_id, .. }
        | OperationKind::Fetching { file_id }
        | OperationKind::PlacingFile { file_id } => Some(file_id),
        OperationKind::ConnectingToPeer { .. }
        | OperationKind::PeerConnected { .. }
        | OperationKind::ReconcilingManifest { .. }
        | OperationKind::ReconcilingTags { .. } => None,
    }
}

/// Human-readable label for an [`OperationStatus`], including a `done/total`
/// progress fragment for active operations that report one.
fn operation_status_label(status: &OperationStatus) -> String {
    match status {
        OperationStatus::Active { progress: None } => "active".to_owned(),
        OperationStatus::Active {
            progress: Some(progress),
        } => match progress.total {
            Some(total) => format!("active ({}/{})", progress.done, total),
            None => format!("active ({})", progress.done),
        },
        OperationStatus::Completed => "completed".to_owned(),
        OperationStatus::Failed { reason } => format!("failed: {reason}"),
        OperationStatus::Aborted => "aborted".to_owned(),
    }
}

/// Build the operations table (see [`file_table`] for the shared pattern).
fn operation_table(operations: &[Operation]) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["Id", "Action", "Peer", "File", "Status"]);

    for operation in operations {
        table.add_row(vec![
            Cell::new(operation.id.as_u64()),
            Cell::new(operation_kind_label(&operation.kind)),
            Cell::new(operation_peer(&operation.kind).unwrap_or("")),
            Cell::new(operation_file(&operation.kind).unwrap_or("")),
            Cell::new(operation_status_label(&operation.status)),
        ]);
    }

    table
}

/// Emit the currently-active operations in the selected [`OutputMode`]: the
/// shared [`operation_table`] (or `(no operations)`) for humans, or the raw
/// [`Operation`]s as a JSON array for scripts (they already derive
/// `Serialize`).
fn emit_operations(output_mode: OutputMode, operations: &[Operation]) {
    match output_mode {
        OutputMode::Human => {
            if operations.is_empty() {
                println!("(no operations)");
            } else {
                println!("{}", operation_table(operations));
            }
        }
        OutputMode::Json => print_json(&operations),
    }
}

/// Resolve `tag_ids` to display names, one `get_tag` per *distinct* id,
/// memoized in `cache` across calls so a tag seen on many files/tags is fetched
/// once. An id that no longer resolves (deleted) falls back to its stringified
/// form.
async fn resolve_tag_names(
    backend: &IpcClientBackend,
    tag_ids: &[TagId],
    cache: &mut HashMap<TagId, String>,
) -> Result<Vec<String>, String> {
    let mut names = Vec::with_capacity(tag_ids.len());
    for tag_id in tag_ids {
        if let Some(name) = cache.get(tag_id) {
            names.push(name.clone());
            continue;
        }
        let name = match backend.get_tag(*tag_id, DeletedRule::Exclude).await {
            Ok(tag) => tag.name,
            // A referenced tag that no longer resolves: show its id rather than
            // failing the whole listing.
            Err(tagsyd::api::ApiError::UnknownId) => tag_id.to_string(),
            Err(error) => return Err(error.to_string()),
        };
        cache.insert(*tag_id, name.clone());
        names.push(name);
    }
    Ok(names)
}

/// Materialize a set of tag ids into full [`Tag`] rows via `get_tag`, one
/// lookup per id. Ids that no longer resolve (deleted) are skipped.
async fn tags_from_ids(
    backend: &IpcClientBackend,
    tag_ids: impl IntoIterator<Item = TagId>,
) -> Result<Vec<Tag>, String> {
    let mut tags = Vec::new();
    for tag_id in tag_ids {
        match backend.get_tag(tag_id, DeletedRule::Exclude).await {
            Ok(tag) => tags.push(tag),
            Err(tagsyd::api::ApiError::UnknownId) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(tags)
}

/// Build the per-file tag-name lists shown in [`file_table`], one
/// `tags_for_file` lookup per file. Names are resolved on demand via
/// [`resolve_tag_names`], sharing `name_cache` so repeated tags cost one
/// lookup. `rule` controls whether the tag hierarchy is walked (see
/// `--include-subtags`).
async fn tags_by_file(
    backend: &IpcClientBackend,
    files: &[FileInfo],
    name_cache: &mut HashMap<TagId, String>,
    rule: SubtagRule,
) -> Result<HashMap<FileId, Vec<String>>, String> {
    let mut map = HashMap::with_capacity(files.len());

    for file in files {
        let tag_ids = backend
            .tags_for_file(file.file_id, rule)
            .await
            .map_err(|error| error.to_string())?;

        let names = resolve_tag_names(backend, &tag_ids, name_cache).await?;
        map.insert(file.file_id, names);
    }

    Ok(map)
}

/// Build the per-tag applied-tag name lists shown in [`tag_table`], one
/// `tags_for_tag` lookup per tag. The tag analogue of [`tags_by_file`]; shares
/// the same `name_cache`. `rule` controls whether the tag hierarchy is walked.
async fn tags_by_tag(
    backend: &IpcClientBackend,
    tags: &[Tag],
    name_cache: &mut HashMap<TagId, String>,
    rule: SubtagRule,
) -> Result<HashMap<TagId, Vec<String>>, String> {
    let mut map = HashMap::with_capacity(tags.len());

    for tag in tags {
        let applied_ids = backend
            .tags_for_tag(tag.id, rule)
            .await
            .map_err(|error| error.to_string())?;

        let names = resolve_tag_names(backend, &applied_ids, name_cache).await?;
        map.insert(tag.id, names);
    }

    Ok(map)
}

/// Resolve a user-supplied file id — a full id or any unambiguous short-id
/// prefix (as shown by `list-files`) — to a full [`FileId`] via the daemon.
///
/// This is the single entry point every command that accepts a file id should
/// use, so short ids work uniformly everywhere. Resolution is done daemon-side
/// against all files, so uniqueness is re-checked at use time (a prefix that
/// was unique when displayed may since have become ambiguous).
async fn resolve_file_id(backend: &IpcClientBackend, input: &str) -> Result<FileId, String> {
    backend
        .resolve_file_id(input.to_owned())
        .await
        .map_err(|error| match error {
            tagsyd::api::ApiError::UnknownId => format!("no file matches id '{input}'"),
            other => other.to_string(),
        })
}

/// Resolve a user-supplied tag id — a full id or any unambiguous short-id
/// prefix (as shown by `list-tags`) — to a full [`TagId`] via the daemon.
///
/// The tag counterpart of [`resolve_file_id`]. Every command that accepts a tag
/// id should route through this so short ids work uniformly, and so uniqueness
/// is re-checked daemon-side at use time.
async fn resolve_tag_id(backend: &IpcClientBackend, input: &str) -> Result<TagId, String> {
    backend
        .resolve_tag_id(input.to_owned())
        .await
        .map_err(|error| match error {
            tagsyd::api::ApiError::UnknownId => format!("no tag matches id '{input}'"),
            other => other.to_string(),
        })
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Path to the daemon's control socket. Defaults to the fixed
    /// `/run/tagsy/tagsy.sock`; override only for non-standard launches.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    /// Emit machine-readable JSON instead of human-friendly tables/text.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Upload a file's contents to the daemon, optionally tagging it.
    #[command(visible_alias = "u")]
    Upload {
        /// File on disk to read and upload.
        path: PathBuf,
        /// Tags to apply to the uploaded file, each a full id or any
        /// unambiguous short-id prefix of it (as shown by `list-tags`).
        #[arg(long = "tag", value_name = "TAG_ID")]
        tags: Vec<String>,
        /// Keep the local file after uploading (by default it is deleted
        /// once the upload has succeeded).
        #[arg(long = "keep")]
        keep: bool,
    },
    /// Create a tag; prints the newly-minted tag id.
    CreateTag {
        name: String,
        // Hex form (matches the Flutter app's palette, kTagColorPalette), so
        // CLI- and app-created tags render identically.
        #[arg(long, default_value = "#F44336")]
        color: String,
    },
    /// Search files with a free-form query.
    ///
    /// The query is a whitespace-separated list of chunks combined
    /// conjunctively. Each chunk may be prefixed:
    ///
    /// - `/t foo` — require the tag(s) matching `foo`
    /// - `/l foo` — logical-path substring
    /// - `!` — invert the following chunk (e.g. `! /t foo`)
    /// - no prefix — match `foo` as *either* a logical-path substring OR a tag
    ///
    /// Payloads can be written three ways: bare (`foo`), double-quoted to
    /// include whitespace (`"my file"`), or `%`-delimited to make the payload a
    /// regular expression (`%\.md$%`). Regexes are case-insensitive unless the
    /// pattern starts with `(?-i)`, need no escaping of `/`, and compose with
    /// every prefix — `/l %^photos/%`, `/t %^wip-%`, `! %\.tmp$%`.
    ///
    /// Malformed chunks are silently dropped; an invalid regex matches nothing.
    /// Examples:
    ///   `tagsy search '/t photos ! /t archived beach'`
    ///   `tagsy search '/l %^photos/\d{4}/% ! %\.tmp$%'`
    #[command(visible_alias = "s")]
    Search {
        /// The query terms; joined with spaces if given as multiple arguments.
        #[arg(trailing_var_arg = true, required = true)]
        query: Vec<String>,
        /// Also match files carrying any subtag of a `$tag`/`!tag` term,
        /// walking the hierarchy transitively.
        #[arg(long)]
        include_subtags: bool,
        /// Search soft-deleted (tombstoned) files and tags instead of live
        /// ones. Results contain *only* rows whose own tombstone is set;
        /// relationships (which tags a deleted file used to carry, etc.) are
        /// still walked live-only. The daemon's `Api::run_query` under
        /// `DeletedRule::Include` semantics — same behavior the app's "show
        /// deleted" toggle exposes.
        #[arg(long)]
        deleted: bool,
    },
    /// Edit a file in `$EDITOR`, fetching it from a peer first if it is not
    /// present locally, and writing back any changes.
    #[command(visible_alias = "e")]
    Edit {
        /// The file to edit, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
    },
    /// Download a file into the downloads directory, fetching it from a peer
    /// first if it is not present locally.
    #[command(visible_alias = "d")]
    Download {
        /// The file to download, given as a full id or any unambiguous
        /// short-id prefix of it (as shown by `list-files`).
        id: String,
    },
    /// Delete a file.
    DeleteFile {
        /// The file to delete, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
    },
    /// Restore a soft-deleted file (best-effort; fails if no source still holds
    /// its bytes).
    RestoreFile {
        /// The deleted file to restore, given as a full id or any unambiguous
        /// short-id prefix of it (as shown by `list-files --deleted`).
        id: String,
    },
    /// Delete a tag.
    DeleteTag {
        /// The tag to delete (a full id or any unambiguous short-id prefix of
        /// it, as shown by `list-tags`).
        tag_id: String,
    },
    /// Restore a soft-deleted tag.
    RestoreTag {
        /// The deleted tag to restore (a full id or any unambiguous short-id
        /// prefix of it, as shown by `list-tags --deleted`).
        tag_id: String,
    },
    /// Apply one or more tags to an existing file.
    #[command(visible_alias = "t")]
    Tag {
        /// The file to tag, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
        /// One or more tags to apply, each a full id or any unambiguous
        /// short-id prefix of it (as shown by `list-tags`).
        #[arg(required = true)]
        tag_ids: Vec<String>,
    },
    /// Remove one or more tags from a file.
    #[command(visible_alias = "ut")]
    Untag {
        /// The file to untag, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
        /// One or more tags to remove, each a full id or any unambiguous
        /// short-id prefix of it (as shown by `list-tags`).
        #[arg(required = true)]
        tag_ids: Vec<String>,
    },
    /// List the tags applied to a file.
    TagsForFile {
        /// The file to inspect, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
        /// Also include tags reached through the tag hierarchy (the tags this
        /// file's tags are subtags of), walking transitively.
        #[arg(long)]
        include_subtags: bool,
    },
    /// Rename a tag.
    RenameTag {
        /// The tag to rename (a full id or any unambiguous short-id prefix of
        /// it, as shown by `list-tags`).
        tag_id: String,
        /// The tag's new name.
        name: String,
    },
    /// Change a tag's color.
    SetTagColor {
        /// The tag to recolor (a full id or any unambiguous short-id prefix of
        /// it, as shown by `list-tags`).
        tag_id: String,
        /// The tag's new color.
        color: String,
    },
    /// Move (rename) a file to a new logical path.
    #[command(visible_alias = "mv")]
    Move {
        /// The file to move, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-files`).
        id: String,
        /// The file's new logical path.
        path: String,
    },
    /// Make a tag a subtag of one or more parent tags.
    #[command(visible_alias = "tt")]
    TagTag {
        /// The child tag, given as a full id or any unambiguous short-id prefix
        /// of it (as shown by `list-tags`).
        child: String,
        /// One or more parent tags to nest the child under, each a full id or
        /// any unambiguous short-id prefix of it.
        #[arg(required = true)]
        parents: Vec<String>,
    },
    /// Remove a tag as a subtag of one or more parent tags.
    #[command(visible_alias = "utt")]
    UntagTag {
        /// The child tag, given as a full id or any unambiguous short-id prefix
        /// of it (as shown by `list-tags`).
        child: String,
        /// One or more parent tags to detach the child from, each a full id or
        /// any unambiguous short-id prefix of it.
        #[arg(required = true)]
        parents: Vec<String>,
    },
    /// List the subtags (children) of a tag.
    Subtags {
        /// The parent tag, given as a full id or any unambiguous short-id
        /// prefix of it (as shown by `list-tags`).
        tag_id: String,
        /// Walk the hierarchy transitively (include subtags of subtags).
        #[arg(long)]
        recursive: bool,
    },
    /// List the daemon's currently-active sync operations (connecting to peers,
    /// sending/receiving files, reconciling, ...).
    #[command(visible_alias = "ops")]
    ListOperations,
    /// Purge the daemon's cached file previews, forcing them to regenerate on
    /// demand. Useful after the set of previewable file types changes (e.g. new
    /// PDF/video support). Prints how many cached previews were removed.
    PurgePreviews,
    /// Re-apply the daemon's configured tag rules to files that already exist.
    ///
    /// Tag rules normally run once, when this device first creates a file, so
    /// adding or fixing a rule leaves everything already in the catalog
    /// untouched. This command catches those files up.
    ///
    /// Only ever *adds* tags. A file that a rule no longer matches keeps the
    /// tags it has: nothing records which tag came from a rule and which from
    /// a person, so removing them could not be distinguished from deleting
    /// your own tagging.
    ///
    /// The daemon reads its configuration once at startup, so restart it
    /// before running this if you have just edited the rules.
    Retag {
        /// Report what would be tagged without changing anything.
        #[arg(long)]
        dry_run: bool,
        /// Only validate the rules — report invalid patterns and rule tags
        /// that match no known tag — without scanning or tagging any file.
        #[arg(long, conflicts_with = "dry_run")]
        check: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();

    let output_mode = if arguments.json {
        OutputMode::Json
    } else {
        OutputMode::Human
    };

    let backend = match &arguments.socket {
        Some(path) => IpcClientBackend::connect(path).await,
        None => IpcClientBackend::connect_default().await,
    };

    let backend = match backend {
        Ok(backend) => backend,
        Err(error) => {
            match output_mode {
                OutputMode::Human => {
                    eprintln!("Failed to connect to the tagsy daemon control socket: {error}");
                    eprintln!("Is the daemon running? (tagsy run <config>)");
                }
                OutputMode::Json => print_json(&json!({
                    "error": format!("failed to connect to the tagsy daemon control socket: {error}"),
                })),
            }
            return ExitCode::FAILURE;
        }
    };

    if let Err(error) = run(&backend, arguments.command, output_mode).await {
        if output_mode == OutputMode::Json {
            print_json(&json!({ "error": error }));
        } else {
            eprintln!("Error: {error}");
        }

        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

async fn run(
    backend: &IpcClientBackend,
    command: Commands,
    output_mode: OutputMode,
) -> Result<(), String> {
    match command {
        Commands::Upload { path, tags, keep } => {
            let path_name = path
                .file_name()
                .ok_or_else(|| format!("{} has no file name", path.display()))?
                .to_string_lossy()
                .to_string();

            // Resolve each `--tag` argument (full id or short prefix) via the
            // daemon, so tagging on upload accepts short ids like every other
            // tag-id command.
            let mut resolved_tags = Vec::with_capacity(tags.len());
            for tag in &tags {
                resolved_tags.push(resolve_tag_id(backend, tag).await?);
            }

            // Serve the file to the daemon as a temporary chunk provider: no
            // bytes are read into memory here. This call blocks until the daemon
            // has handed the content off to the storing peer(s).
            let file_id = backend
                .upload_file(path.clone(), path_name.clone(), resolved_tags.clone())
                .await
                .map_err(|error| error.to_string())?;

            if !keep {
                std::fs::remove_file(&path).map_err(|error| {
                    format!(
                        "uploaded as file {}, but failed to delete {}: {error}",
                        file_id.to_string(),
                        path.display()
                    )
                })?;
            }

            // Render the full entry from locally-known data rather than fetching
            // it back (the metadata write is enqueued asynchronously and would
            // race). We know the id, logical path, applied tags, and that this is
            // the first version. The content hash is computed daemon-side and is
            // not known here, so it renders empty in JSON output.
            let file = FileInfo {
                file_id,
                logical_path: tagsy_core::LogicalPath::new(path_name),
                content_hash: String::new(),
                version_number: 1,
                // The size is computed daemon-side and is not known here.
                size: 0,
                // Only one id is known locally; highlight the whole id.
                short_id_length: file_id.to_string().len(),
                // A freshly-added file is live by construction.
                deleted: false,
                // Freshly added: its only version was recorded just now. The
                // authoritative timestamps are stamped daemon-side; approximate
                // with now for this optimistic local render.
                first_recorded_at: tagsyd::clock::now_millis(),
                latest_change_at: tagsyd::clock::now_millis(),
            };

            let mut name_cache = HashMap::new();
            let mut file_tags = HashMap::new();

            let tag_names = resolve_tag_names(backend, &resolved_tags, &mut name_cache).await?;
            file_tags.insert(file_id, tag_names);

            emit_files(output_mode, std::slice::from_ref(&file), &file_tags);
        }
        Commands::CreateTag { name, color } => {
            let tag_id = backend
                .create_tag(name.clone(), color.clone())
                .await
                .map_err(|error| error.to_string())?;

            // Persistence is async (the write is enqueued), so we can't fetch the
            // row back yet without racing the pipeline. Render the full entry from
            // what we just sent instead — the id is authoritative and the
            // name/color are exactly what the daemon will persist (the CLI's
            // default color matches the daemon's empty-color default). A fresh tag
            // has no applied tags, so that column is empty.
            let tag = Tag {
                id: tag_id,
                name,
                color,
                metadata: None,
                // A freshly-created tag is live by construction.
                deleted: false,
            };
            emit_tags(output_mode, std::slice::from_ref(&tag), &HashMap::new());
        }
        Commands::Search {
            query,
            include_subtags,
            deleted,
        } => {
            let query = query.join(" ");
            // The query returns full rows for exactly the matched set (files and
            // tags), so no whole-store listing is needed to render them.
            let result = backend
                .run_query(query, subtag_rule(include_subtags), deleted_rule(deleted))
                .await
                .map_err(|error| error.to_string())?;
            let files = result.files;
            let tags = result.tags;

            let mut name_cache = HashMap::new();
            // The Tags column shows each row's own direct tags, regardless of
            // how the search matched it.
            let file_tags =
                tags_by_file(backend, &files, &mut name_cache, SubtagRule::Exclude).await?;
            let tag_tags =
                tags_by_tag(backend, &tags, &mut name_cache, SubtagRule::Exclude).await?;

            match output_mode {
                OutputMode::Human => {
                    emit_tags(output_mode, &tags, &tag_tags);
                    emit_files(output_mode, &files, &file_tags);
                }
                OutputMode::Json => {
                    let tag_rows: Vec<TagRow> = tags
                        .iter()
                        .map(|tag| {
                            TagRow::new(tag, tag_tags.get(&tag.id).cloned().unwrap_or_default())
                        })
                        .collect();
                    let file_rows: Vec<FileRow> = files
                        .iter()
                        .map(|file| {
                            FileRow::new(
                                file,
                                file_tags.get(&file.file_id).cloned().unwrap_or_default(),
                            )
                        })
                        .collect();
                    print_json(&json!({ "tags": tag_rows, "files": file_rows }));
                }
            }
        }
        Commands::Edit { id } => {
            let file_id = resolve_file_id(backend, &id).await?;
            edit_file(backend, file_id, output_mode).await?;
        }
        Commands::Download { id } => {
            let file_id = resolve_file_id(backend, &id).await?;
            download_file(backend, file_id, output_mode).await?;
        }
        Commands::DeleteFile { id } => {
            let file_id = resolve_file_id(backend, &id).await?;

            backend
                .delete_file(file_id)
                .await
                .map_err(|error| error.to_string())?;

            match output_mode {
                OutputMode::Human => println!("Deleted file {}", file_id.to_string()),
                OutputMode::Json => print_json(&json!({ "deleted": file_id })),
            }
        }
        Commands::RestoreFile { id } => {
            let file_id = resolve_file_id(backend, &id).await?;

            backend
                .restore_file(file_id)
                .await
                .map_err(|error| error.to_string())?;

            match output_mode {
                OutputMode::Human => println!("Restored file {}", file_id.to_string()),
                OutputMode::Json => print_json(&json!({ "restored": file_id })),
            }
        }
        Commands::DeleteTag { tag_id } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;

            backend
                .delete_tag(tag_id)
                .await
                .map_err(|error| error.to_string())?;

            match output_mode {
                OutputMode::Human => println!("Deleted tag {}", tag_id.to_string()),
                OutputMode::Json => print_json(&json!({ "deleted": tag_id })),
            }
        }
        Commands::RestoreTag { tag_id } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;

            backend
                .restore_tag(tag_id)
                .await
                .map_err(|error| error.to_string())?;

            match output_mode {
                OutputMode::Human => println!("Restored tag {}", tag_id.to_string()),
                OutputMode::Json => print_json(&json!({ "restored": tag_id })),
            }
        }
        Commands::Tag { id, tag_ids } => {
            let file_id = resolve_file_id(backend, &id).await?;

            let mut applied = Vec::new();
            for tag in &tag_ids {
                let tag_id = resolve_tag_id(backend, tag).await?;

                backend
                    .tag_file(tag_id, file_id)
                    .await
                    .map_err(|error| error.to_string())?;

                if output_mode == OutputMode::Human {
                    println!(
                        "Tagged file {} with tag {}",
                        file_id.to_string(),
                        tag_id.to_string()
                    );
                }

                applied.push(tag_id);
            }

            if output_mode == OutputMode::Json {
                print_json(&json!({ "file": file_id, "tagged": applied }));
            }
        }
        Commands::Untag { id, tag_ids } => {
            let file_id = resolve_file_id(backend, &id).await?;

            let mut removed = Vec::new();
            for tag in &tag_ids {
                let tag_id = resolve_tag_id(backend, tag).await?;

                backend
                    .untag_file(tag_id, file_id)
                    .await
                    .map_err(|error| error.to_string())?;

                if output_mode == OutputMode::Human {
                    println!(
                        "Removed tag {} from file {}",
                        tag_id.to_string(),
                        file_id.to_string()
                    );
                }

                removed.push(tag_id);
            }

            if output_mode == OutputMode::Json {
                print_json(&json!({ "file": file_id, "untagged": removed }));
            }
        }
        Commands::TagsForFile {
            id,
            include_subtags,
        } => {
            let file_id = resolve_file_id(backend, &id).await?;
            let tag_ids = backend
                .tags_for_file(file_id, subtag_rule(include_subtags))
                .await
                .map_err(|error| error.to_string())?;
            let tags = tags_from_ids(backend, tag_ids).await?;
            let mut name_cache = HashMap::new();
            // The Tags column shows each tag's own direct tags, regardless of
            // how the command matched them.
            let tag_tags =
                tags_by_tag(backend, &tags, &mut name_cache, SubtagRule::Exclude).await?;

            emit_tags(output_mode, &tags, &tag_tags);
        }
        Commands::RenameTag { tag_id, name } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;

            backend
                .rename_tag(tag_id, name.clone())
                .await
                .map_err(|error| error.to_string())?;

            match output_mode {
                OutputMode::Human => println!("Renamed tag {}", tag_id.to_string()),
                OutputMode::Json => print_json(&json!({ "id": tag_id, "name": name })),
            }
        }
        Commands::SetTagColor { tag_id, color } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;

            backend
                .set_tag_color(tag_id, color.clone())
                .await
                .map_err(|error| error.to_string())?;

            match output_mode {
                OutputMode::Human => println!("Recolored tag {}", tag_id.to_string()),
                OutputMode::Json => print_json(&json!({ "id": tag_id, "color": color })),
            }
        }
        Commands::Move { id, path } => {
            let file_id = resolve_file_id(backend, &id).await?;

            backend
                .move_file(file_id, path.clone())
                .await
                .map_err(|error| error.to_string())?;

            match output_mode {
                OutputMode::Human => println!("Moved file {}", file_id.to_string()),
                OutputMode::Json => print_json(&json!({ "id": file_id, "path": path })),
            }
        }
        Commands::TagTag { child, parents } => {
            let child_id = resolve_tag_id(backend, &child).await?;

            let mut applied = Vec::new();
            for parent in &parents {
                let parent_id = resolve_tag_id(backend, parent).await?;

                backend
                    .tag_tag(parent_id, child_id)
                    .await
                    .map_err(|error| error.to_string())?;

                if output_mode == OutputMode::Human {
                    println!(
                        "Tagged tag {} with {}",
                        child_id.to_string(),
                        parent_id.to_string()
                    );
                }

                applied.push(parent_id);
            }

            if output_mode == OutputMode::Json {
                print_json(&json!({ "tag": child_id, "tagged": applied }));
            }
        }
        Commands::UntagTag { child, parents } => {
            let child_id = resolve_tag_id(backend, &child).await?;

            let mut removed = Vec::new();
            for parent in &parents {
                let parent_id = resolve_tag_id(backend, parent).await?;

                backend
                    .untag_tag(parent_id, child_id)
                    .await
                    .map_err(|error| error.to_string())?;

                if output_mode == OutputMode::Human {
                    println!(
                        "Removed tag {} from {}",
                        parent_id.to_string(),
                        child_id.to_string(),
                    );
                }

                removed.push(parent_id);
            }

            if output_mode == OutputMode::Json {
                print_json(&json!({ "tag": child_id, "untagged": removed }));
            }
        }
        Commands::Subtags { tag_id, recursive } => {
            let tag_id = resolve_tag_id(backend, &tag_id).await?;
            let subtag_ids = backend
                .subtags_for_tag(tag_id, subtag_rule(recursive))
                .await
                .map_err(|error| error.to_string())?;
            let tags = tags_from_ids(backend, subtag_ids).await?;
            let mut name_cache = HashMap::new();
            // The Tags column shows each tag's own direct tags, regardless of
            // how the command matched them.
            let tag_tags =
                tags_by_tag(backend, &tags, &mut name_cache, SubtagRule::Exclude).await?;

            emit_tags(output_mode, &tags, &tag_tags);
        }
        Commands::ListOperations => {
            let operations = backend
                .list_operations()
                .await
                .map_err(|error| error.to_string())?;

            emit_operations(output_mode, &operations);
        }
        Commands::PurgePreviews => {
            let purged = backend
                .purge_previews()
                .await
                .map_err(|error| error.to_string())?;

            match output_mode {
                OutputMode::Human => println!("Purged {purged} cached previews"),
                OutputMode::Json => print_json(&json!({ "purged": purged })),
            }
        }
        Commands::Retag { dry_run, check } => {
            // Always fetch the diagnostics, even for a real run. A rule that
            // failed to compile is exactly the situation someone runs `retag`
            // to recover from, and silently retagging with it still broken
            // would look like the command simply did nothing.
            let report = backend
                .tag_rule_report()
                .await
                .map_err(|error| error.to_string())?;

            if check {
                match output_mode {
                    OutputMode::Human => print_tag_rule_report(&report),
                    OutputMode::Json => print_json(&json!({
                        "active": report.active,
                        "invalid": report.invalid,
                        "unknown_tags": report
                            .unknown_tags
                            .iter()
                            .map(|tag_id| tag_id.to_string())
                            .collect::<Vec<_>>(),
                    })),
                }
                return Ok(());
            }

            // Warnings go to stderr so they survive a `| jq` and do not
            // corrupt the JSON on stdout.
            for problem in &report.invalid {
                eprintln!("Warning: {problem}");
            }

            let summary = backend
                .retag(dry_run)
                .await
                .map_err(|error| error.to_string())?;

            match output_mode {
                OutputMode::Human => {
                    if summary.tags_applied == 0 {
                        println!(
                            "Nothing to do: {} files scanned, all already carry the tags their \
                             rules assign",
                            summary.files_scanned
                        );
                    } else if dry_run {
                        println!(
                            "Would apply {} tags across {} of {} files (dry run; nothing changed)",
                            summary.tags_applied, summary.files_changed, summary.files_scanned
                        );
                    } else {
                        println!(
                            "Applied {} tags across {} of {} files",
                            summary.tags_applied, summary.files_changed, summary.files_scanned
                        );
                    }
                }
                OutputMode::Json => print_json(&json!({
                    "dry_run": dry_run,
                    "files_scanned": summary.files_scanned,
                    "files_changed": summary.files_changed,
                    "tags_applied": summary.tags_applied,
                })),
            }
        }
    }
    Ok(())
}

/// Render tag-rule diagnostics for `retag --check`.
///
/// Both problem classes are reported as warnings rather than errors: neither
/// stops the daemon, and neither stops the *other* rules from working.
fn print_tag_rule_report(report: &tagsyd::api::TagRuleReport) {
    println!(
        "{} tag rule{} active",
        report.active,
        if report.active == 1 { "" } else { "s" }
    );

    if report.invalid.is_empty() && report.unknown_tags.is_empty() {
        println!("No problems found");
        return;
    }

    if !report.invalid.is_empty() {
        println!("\nInvalid patterns (these rules are disabled):");
        for problem in &report.invalid {
            println!("  {problem}");
        }
    }

    if !report.unknown_tags.is_empty() {
        println!("\nRules name tags that do not exist (they will never be useful):");
        for tag_id in &report.unknown_tags {
            println!("  {}", tag_id.to_string());
        }
    }
}

/// The `edit` flow — a thin driver over the daemon's stateless edit protocol.
///
/// The daemon owns the whole workflow (local-path vs. peer-fetch decision,
/// extension-preserving naming, hashing, no-op detection, upload, and temp
/// cleanup). This CLI's job is only:
///
/// 1. Ask the daemon to prepare an editable path (`begin_edit`).
/// 2. Launch `$EDITOR` on it, blocking until it exits.
/// 3. Hand the path back with `finish_edit` (uploads iff the bytes changed) on
///    success, or `cancel_edit` on editor failure.
///
/// A crash between (1) and (3) only leaks a temp file, which the daemon
/// bulk-wipes on next start.
async fn edit_file(
    backend: &IpcClientBackend,
    file_id: FileId,
    output_mode: OutputMode,
) -> Result<(), String> {
    let path = match backend.begin_edit(file_id).await {
        Ok(path) => path,
        Err(tagsyd::api::ApiError::UnknownId) => {
            return Err(format!("unknown file id: {}", file_id.to_string()));
        }
        Err(error) => return Err(error.to_string()),
    };

    // Launch the editor. On failure, tell the daemon to clean up and return
    // the editor error to the user — we do not want a stale temp to linger
    // until the next daemon restart.
    if let Err(error) = open_in_editor(&path) {
        let _ = backend.cancel_edit(path).await;
        return Err(error);
    }

    let outcome = backend
        .finish_edit(file_id, path)
        .await
        .map_err(|error| error.to_string())?;

    match output_mode {
        OutputMode::Human => {
            if outcome.changed {
                println!("Edited file {}", file_id.to_string());
            } else {
                println!("No changes");
            }
        }
        OutputMode::Json => print_json(&json!({ "id": file_id, "edited": outcome.changed })),
    }

    Ok(())
}

/// The `download` flow.
///
/// Shares its start with [`edit_file`]: locate the file's bytes — reading the
/// real file if it lives in a local sync directory, otherwise fetching them
/// from a peer — then, instead of editing, copy them into the user's downloads
/// directory.
async fn download_file(
    backend: &IpcClientBackend,
    file_id: FileId,
    output_mode: OutputMode,
) -> Result<(), String> {
    // Pull the file's metadata once (a single by-id lookup): we need its content
    // hash to fetch (if it isn't local) and its logical path to pick a sensible
    // output filename.
    let file = match backend.get_file(file_id, DeletedRule::Exclude).await {
        Ok(file) => file,
        Err(tagsyd::api::ApiError::UnknownId) => {
            return Err(format!("unknown file id: {}", file_id.to_string()));
        }
        Err(error) => return Err(error.to_string()),
    };

    // Either the file already lives in a local sync directory (copy it out,
    // leaving the real file untouched) or we fetch it, which stages a
    // CLI-owned temp we can move into place.
    let local_path = backend
        .local_path_for_file(file_id)
        .await
        .map_err(|error| error.to_string())?;

    // Name the download after the file's logical path's final component, so a
    // nested `foo/bar/name.txt` lands as `name.txt`. Fall back to the file id
    // if the logical path has no usable component.
    let logical = file.logical_path.to_string();
    let file_name = logical
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(&logical);

    let file_name = if file_name.is_empty() {
        file_id.to_string()
    } else {
        file_name.to_owned()
    };

    if let Some(path) = local_path {
        std::fs::copy(&path, &file_name).map_err(|error| {
            format!(
                "failed to copy local file {} to {file_name}: {error}",
                path.display()
            )
        })?;
    } else {
        let temp_path = backend
            .fetch_file(file_id, file.content_hash)
            .await
            .map_err(|error| error.to_string())?;

        // TODO: Share the EXDEV-only, stream-copy fallback from
        // `FileBytes::materialize_to` (tagsyd/src/file_bytes.rs) instead of
        // this ad-hoc version, which incorrectly falls back on *any* rename
        // error (e.g. EACCES/ENOSPC on the destination) rather than only on
        // cross-filesystem renames. Extract a shared helper.
        if let Err(rename_error) = std::fs::rename(&temp_path, &file_name) {
            let copied = std::fs::copy(&temp_path, &file_name);
            let _ = std::fs::remove_file(&temp_path);
            copied.map_err(|error| {
                format!(
                    "failed to move downloaded file into {file_name}: {rename_error}; copy \
                     fallback also failed: {error}"
                )
            })?;
        }
        // The daemon staged the fetched bytes in a per-request subdirectory
        // (`<fetch_temp_dir>/<uuid>/<logical_basename>`). We just moved the
        // file out of it, so the subdir is now an empty leftover. Remove it
        // (best-effort — the daemon bulk-wipes `fetch_temp_dir` on next start
        // regardless).
        if let Some(parent) = temp_path.parent() {
            let _ = std::fs::remove_dir(parent);
        }
    }

    match output_mode {
        OutputMode::Human => println!("Downloaded to {}", file_name),
        OutputMode::Json => print_json(&json!({ "id": file_id, "path": file_name })),
    }

    Ok(())
}

/// Open `path` in the user's `$EDITOR` (falling back to `vi`), blocking until
/// it exits. A non-zero editor exit is treated as an abort.
fn open_in_editor(path: &std::path::Path) -> Result<(), String> {
    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_owned());
    let status = std::process::Command::new(&editor)
        .arg(path)
        .status()
        .map_err(|error| format!("failed to launch editor '{editor}': {error}"))?;

    if !status.success() {
        return Err(format!("editor '{editor}' exited without success"));
    }

    Ok(())
}
