//! OniSync CLI client

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use owo_colors::OwoColorize;
use serde::Serialize;
use serde_json::json;
use onisync_core::{FileId, FileInfo, TagId};
use onisyncd::control::IpcClientBackend;
use onisyncd::database::{SubtagRule, Tag};
use onisyncd::transport::TransportBackend;

/// How command results are rendered to stdout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    /// Human-friendly tables and prose.
    Human,
    /// Machine-readable JSON (one value per command, pretty-printed).
    Json,
}

/// A serializable tag row, shared by every command that prints tags. Mirrors
/// the human [`tag_table`] columns: the tag's id, name, colour, and the names
/// of the tags applied to it.
#[derive(Debug, Serialize)]
struct TagRow {
    id: TagId,
    name: String,
    color: String,
    tags: Vec<String>,
}

/// A serializable file row, shared by every command that prints files. Mirrors
/// the human [`file_table`] columns plus the raw fields useful to scripts.
#[derive(Debug, Serialize)]
struct FileRow {
    id: FileId,
    path: String,
    version: i64,
    content_hash: String,
    size: u64,
    tags: Vec<String>,
}

impl TagRow {
    /// Build a row from a tag and its applied-tag names (see [`tags_by_tag`]).
    fn new(tag: &Tag, tags: Vec<String>) -> Self {
        Self {
            id: tag.id,
            name: tag.name.clone(),
            color: tag.color.clone(),
            tags,
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
fn subtag_rule(include: bool) -> SubtagRule {
    if include {
        SubtagRule::Include
    } else {
        SubtagRule::Exclude
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

/// Resolve `tag_ids` to display names, one `get_tag` per *distinct* id,
/// memoized in `cache` across calls so a tag seen on many files/tags is fetched
/// once.
///
/// This replaces the old whole-store `list_tags` map: instead of fetching every
/// tag up front (an O(all-tags) hazard), it looks up only the ids actually
/// referenced. An id that no longer resolves (deleted) falls back to its
/// stringified form.
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
        let name = match backend.get_tag(*tag_id).await {
            Ok(tag) => tag.name,
            // A referenced tag that no longer resolves: show its id rather than
            // failing the whole listing.
            Err(onisyncd::api::ApiError::NotFound) => tag_id.to_string(),
            Err(error) => return Err(error.to_string()),
        };
        cache.insert(*tag_id, name.clone());
        names.push(name);
    }
    Ok(names)
}

/// Materialize a set of tag ids into full [`Tag`] rows via `get_tag`, one
/// lookup per id. Replaces the old "list every tag, then filter to these ids"
/// pattern, which scanned the whole tag store to render a handful of rows. Ids
/// that no longer resolve (deleted) are skipped.
async fn tags_from_ids(
    backend: &IpcClientBackend,
    tag_ids: impl IntoIterator<Item = TagId>,
) -> Result<Vec<Tag>, String> {
    let mut tags = Vec::new();
    for tag_id in tag_ids {
        match backend.get_tag(tag_id).await {
            Ok(tag) => tags.push(tag),
            Err(onisyncd::api::ApiError::NotFound) => {}
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
            onisyncd::api::ApiError::NotFound => format!("no file matches id '{input}'"),
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
            onisyncd::api::ApiError::NotFound => format!("no tag matches id '{input}'"),
            other => other.to_string(),
        })
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    /// Path to the daemon's control socket. Defaults to the fixed
    /// `/run/onisync/onisync.sock`; override only for non-standard launches.
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
    /// Quote payloads to include whitespace: `/t "my tag"`. Malformed chunks
    /// are silently dropped. Example:
    ///   `onisync search '/t photos ! /t archived beach'`.
    #[command(visible_alias = "s")]
    Search {
        /// The query terms; joined with spaces if given as multiple arguments.
        #[arg(trailing_var_arg = true, required = true)]
        query: Vec<String>,
        /// Also match files carrying any subtag of a `$tag`/`!tag` term,
        /// walking the hierarchy transitively.
        #[arg(long)]
        include_subtags: bool,
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
    /// Delete a tag.
    DeleteTag {
        /// The tag to delete (a full id or any unambiguous short-id prefix of
        /// it, as shown by `list-tags`).
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
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();

    let output_mode = if arguments.json {
        OutputMode::Json
    } else {
        OutputMode::Human
    };

    // Connect the IPC-client backend to the daemon's control socket.
    let backend = match &arguments.socket {
        Some(path) => IpcClientBackend::connect(path).await,
        None => IpcClientBackend::connect_default().await,
    };

    let backend = match backend {
        Ok(backend) => backend,
        Err(error) => {
            match output_mode {
                OutputMode::Human => {
                    eprintln!("Failed to connect to the onisync daemon control socket: {error}");
                    eprintln!("Is the daemon running? (onisync run <config>)");
                }
                OutputMode::Json => print_json(&json!({
                    "error": format!("failed to connect to the onisync daemon control socket: {error}"),
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
                logical_path: onisync_core::LogicalPath::new(path_name),
                content_hash: String::new(),
                version_number: 1,
                // The size is computed daemon-side and is not known here.
                size: 0,
                // Only one id is known locally; highlight the whole id.
                short_id_length: file_id.to_string().len(),
            };
            let mut name_cache = HashMap::new();
            let tag_names = resolve_tag_names(backend, &resolved_tags, &mut name_cache).await?;
            let mut file_tags = HashMap::new();
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
            };
            emit_tags(output_mode, std::slice::from_ref(&tag), &HashMap::new());
        }
        Commands::Search {
            query,
            include_subtags,
        } => {
            let query = query.join(" ");
            // The query returns full rows for exactly the matched set (files and
            // tags), so no whole-store listing is needed to render them.
            let result = backend
                .run_query(query, subtag_rule(include_subtags))
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
            // Materialize the matched ids into full rows via `get_tag`.
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
            // Materialize the matched ids into full rows via `get_tag`.
            let tags = tags_from_ids(backend, subtag_ids).await?;
            let mut name_cache = HashMap::new();
            // The Tags column shows each tag's own direct tags, regardless of
            // how the command matched them.
            let tag_tags =
                tags_by_tag(backend, &tags, &mut name_cache, SubtagRule::Exclude).await?;

            emit_tags(output_mode, &tags, &tag_tags);
        }
    }
    Ok(())
}

/// The `edit` flow.
///
/// - If the daemon reports the file is present in a local sync directory, open
///   that real file directly in `$EDITOR`. The daemon's filesystem watcher
///   picks up the save and propagates a `FileMetadataChanged` on its own — no
///   explicit write-back, no temp file.
/// - Otherwise fetch the bytes from a peer, drop them in a temp file, open the
///   editor, and — only if the content actually changed — write the new bytes
///   back with `edit_file`.
async fn edit_file(
    backend: &IpcClientBackend,
    file_id: FileId,
    output_mode: OutputMode,
) -> Result<(), String> {
    if let Some(path) = backend
        .local_path_for_file(file_id)
        .await
        .map_err(|error| error.to_string())?
    {
        open_in_editor(&path)?;

        // The watcher propagates the on-disk save; report the same shape as the
        // fetch-and-write-back path below.
        match output_mode {
            OutputMode::Human => {}
            OutputMode::Json => print_json(&json!({ "id": file_id, "edited": true })),
        }

        return Ok(());
    }

    // Not local: we need the expected content hash to fetch. It comes from the
    // file's known metadata; if the daemon has never heard of this file there is
    // nothing to fetch. A single by-id lookup (not a full listing).
    let expected_hash = match backend.get_file(file_id).await {
        Ok(file) => file.content_hash,
        Err(onisyncd::api::ApiError::NotFound) => {
            return Err(format!("unknown file id: {}", file_id.to_string()));
        }
        Err(error) => return Err(error.to_string()),
    };

    // The daemon stages the fetched content in a temp file and hands us the
    // path with move semantics: we own it now and must consume (edit + hand
    // back) or remove it. Edit it in place, then decide by hash whether it
    // changed.
    let temp_path = backend
        .fetch_file(file_id, expected_hash.clone())
        .await
        .map_err(|error| error.to_string())?;

    let result = edit_fetched_file(backend, file_id, &temp_path, &expected_hash, output_mode).await;

    // Best-effort cleanup: `edit_file` streams the temp to the daemon but does
    // not consume it, and the no-change path never hands it off. Either way we
    // own it and remove it here.
    let _ = std::fs::remove_file(&temp_path);

    result
}

/// Edit the temp file the daemon staged for us (at `temp_path`), then push the
/// result back if it changed. `expected_hash` is the fetched content's hash;
/// comparing the post-edit hash against it detects "no change" without reading
/// either version fully into memory.
async fn edit_fetched_file(
    backend: &IpcClientBackend,
    file_id: FileId,
    temp_path: &std::path::Path,
    expected_hash: &str,
    output_mode: OutputMode,
) -> Result<(), String> {
    open_in_editor(temp_path)?;

    let (edited_hash, _edited_size) = onisyncd::control::hash_file(temp_path)
        .await
        .map_err(|error| error.to_string())?;

    if edited_hash == expected_hash {
        match output_mode {
            OutputMode::Human => println!("No changes"),
            OutputMode::Json => print_json(&json!({ "id": file_id, "edited": false })),
        }
        return Ok(());
    }

    // Serve the edited temp file to the daemon as a provider (streamed, not
    // sent as bytes). Blocks until the new content is handed off.
    backend
        .edit_file(file_id, temp_path.to_path_buf())
        .await
        .map_err(|error| error.to_string())?;

    match output_mode {
        OutputMode::Human => println!("Edited file {}", file_id.to_string()),
        OutputMode::Json => print_json(&json!({ "id": file_id, "edited": true })),
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
    let file = match backend.get_file(file_id).await {
        Ok(file) => file,
        Err(onisyncd::api::ApiError::NotFound) => {
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
        // `FileBytes::materialize_to` (onisyncd/src/file_bytes.rs) instead of
        // this ad-hoc version, which incorrectly falls back on *any* rename
        // error (e.g. EACCES/ENOSPC on the destination) rather than only on
        // cross-filesystem renames. Extract a shared helper.
        if let Err(rename_error) = std::fs::rename(&temp_path, &file_name) {
            let copied = std::fs::copy(&temp_path, &file_name);
            let _ = std::fs::remove_file(&temp_path);
            copied.map_err(|error| {
                format!("failed to move downloaded file into {file_name}: {rename_error}; copy fallback also failed: {error}")
            })?;
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
