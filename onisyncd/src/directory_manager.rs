use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use notify::{RecursiveMode, Watcher};
use onisync_core::state::{Change, ChangeOrigin};
use onisync_core::{FileId, LogicalPath, PhysicalPath, TagId};
use walkdir::WalkDir;

use crate::bus::{ContentChange, DaemonMessage, Ingest};
use crate::configuration::{Configuration, SyncType};
use crate::database::{DatabaseError, SyncDirectoryDatabase, SyncDirectoryFile};
use crate::file_bytes::FileBytes;
use crate::paths::Paths;
use crate::watcher::{DebouncedEventKind, WatchDispatcher};

/// True when every tag a sync directory requires is present in `file_tags`,
/// i.e. the file belongs in that TagBased directory. Mirrors the same-named
/// helper in `lib.rs`.
fn contains_all_tags(sync_directory_tags: &[TagId], file_tags: &[TagId]) -> bool {
    sync_directory_tags
        .iter()
        .all(|tag_id| file_tags.contains(tag_id))
}

#[derive(Debug, Clone, Copy)]
enum SyncDirectoryError {
    UnmonitoredDirectory,
    FailedToReadFile,
    MissingTrackedFile,
    FailedAddingFile,
    FailedChangingFile,
    FailedMovingFile,
    FailedRemovingFile,
}

pub enum SyncDirectoryCommand {
    CreateFile {
        file_id: FileId,
        /// Where to place the bytes on disk within the target sync directory.
        /// Already resolved from the file's logical path by the caller via
        /// `SyncType::physical_for`, so the handler stores it verbatim.
        physical_path: PhysicalPath,
        content: FileBytes,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    ChangeFile {
        file_id: FileId,
        content: FileBytes,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    MoveFile {
        file_id: FileId,
        /// The new on-disk location within the target sync directory, already
        /// resolved from the file's new logical path via
        /// `SyncType::physical_for`.
        physical_path: PhysicalPath,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    RemoveFile {
        file_id: FileId,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    /// Re-evaluate which TagBased sync directories should hold `file_id` given
    /// its *current* tag set, and reconcile placement accordingly:
    ///
    /// - a TagBased directory that now matches (`contains_all_tags`) but does
    ///   not yet hold the file gains it (the bytes are sourced from another
    ///   directory that already holds the file);
    /// - a TagBased directory that no longer matches but currently holds it
    ///   drops it.
    ///
    /// Universal directories are untouched (they have no tag filter). This is
    /// the recovery path for the tag-vs-content reconciliation race: when a
    /// peer transfer materializes a file before its `FileTagged` relationships
    /// have been applied, the file is placed only where tags already matched
    /// (e.g. Universal dirs). Applying the tags later re-runs placement so the
    /// file lands in the TagBased directories it belongs to. Idempotent: a
    /// no-op when placement is already correct.
    ///
    /// If a TagBased directory now matches the file but no local copy exists to
    /// source the bytes from, placement cannot complete locally. In that case
    /// `respond_to` receives `true` ("deferred: needs bytes") and the caller is
    /// expected to fetch the bytes over the network (by the file's latest
    /// catalog version hash) and re-drive placement via `Materialize`.
    /// Otherwise (placement resolved, or nothing to place) it receives
    /// `false`.
    ReconcileTagPlacement {
        file_id: FileId,
        /// The file's logical path, used to derive each TagBased directory's
        /// physical path via `SyncType::physical_for` when creating.
        logical_path: LogicalPath,
        /// The file's current tag set (from the main `FileDatabase`), against
        /// which each TagBased directory's tags are matched.
        file_tags: Vec<TagId>,
        /// `true` if a TagBased directory should hold the file but no local
        /// copy exists to source the bytes (the caller must fetch);
        /// `false` otherwise.
        respond_to: tokio::sync::oneshot::Sender<bool>,
    },
    /// Read the bytes for `file_id` from whichever sync directory currently
    /// holds it and respond on `respond_to`. Used by peer connection tasks to
    /// serve a pull transfer (and to answer on-demand `FetchRequest`s).
    ///
    /// Resolve `file_id` to the **absolute** on-disk path where its bytes
    /// currently live in the first sync directory that holds it, without
    /// reading the content. Responds with `None` if no sync directory has
    /// the file. Used by `onisync edit` to open a locally-present file in
    /// place (so the filesystem watcher picks up the save and propagates
    /// it).
    LocalPath {
        file_id: FileId,
        respond_to: tokio::sync::oneshot::Sender<Option<PathBuf>>,
    },
    ReadFile {
        file_id: FileId,
        respond_to: tokio::sync::oneshot::Sender<Option<(PhysicalPath, FileBytes, String)>>,
    },
}

struct RichSyncDirectory {
    path: PathBuf,
    sync_type: SyncType,
    database: SyncDirectoryDatabase,
}

/// A record that the daemon *itself* just wrote to a given on-disk path, laid
/// down at the moment of the write in `handle_command` (or during placement
/// reconciliation) and consulted in `handle_event` to decide whether an
/// incoming watcher event merely reflects that self-write — in which case it
/// must be ignored — or is a genuine user action that must be processed.
struct SelfWrite {
    /// BLAKE3 hash of the bytes the daemon materialized at this path, or `None`
    /// for a self-caused removal / pure rename (no content to match on).
    content_hash: Option<String>,
}

pub struct SyncDirectoryManager {
    sync_directories: Vec<RichSyncDirectory>,
    change_sender: tokio::sync::mpsc::UnboundedSender<DaemonMessage>,
    _dispatcher: WatchDispatcher,
    watcher_events: tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
    command_receiver: tokio::sync::mpsc::UnboundedReceiver<SyncDirectoryCommand>,
    // TODO: Make this a more robust messaging framework instead of a ref cell.
    self_writes: RefCell<HashMap<PathBuf, SelfWrite>>,
}

impl SyncDirectoryManager {
    pub async fn new(
        configuration: Configuration,
        paths: &Paths,
        change_sender: tokio::sync::mpsc::UnboundedSender<DaemonMessage>,
        command_receiver: tokio::sync::mpsc::UnboundedReceiver<SyncDirectoryCommand>,
    ) -> Self {
        let (mut dispatcher, watcher_events) = WatchDispatcher::new()
            .await
            .expect("Failed to set up debouncer");

        let sync_directories = configuration
            .sync_directories
            .iter()
            .filter_map(|sync_directory| {
                let path = sync_directory.path.clone();

                log::debug!(
                    "Setting up sync directory at {}",
                    sync_directory.path.to_string_lossy()
                );

                if let Err(error) = std::fs::create_dir_all(&path) {
                    log::error!("Failed to create sync directory: {}", error);
                    return None;
                }

                if let Err(error) = dispatcher
                    .watcher()
                    .watch(path.as_ref(), RecursiveMode::Recursive)
                {
                    log::error!("Failed to set up watcher for sync directory: {}", error);
                    return None;
                }

                let database_path = paths.sync_directory_db_path(&path);

                let database = match SyncDirectoryDatabase::initialize(database_path) {
                    Ok(database) => database,
                    Err(error) => {
                        log::error!("Failed to set up sync directory database: {:?}", error);
                        return None;
                    }
                };

                Some(RichSyncDirectory {
                    path,
                    sync_type: sync_directory.sync_type.clone(),
                    database,
                })
            })
            .collect::<Vec<_>>();

        Self {
            sync_directories,
            change_sender,
            _dispatcher: dispatcher,
            watcher_events,
            command_receiver,
            self_writes: Default::default(),
        }
    }

    /// Record that the daemon itself just wrote `path`, so the resulting
    /// watcher event(s) are recognized as self-caused and ignored.
    /// `content_hash` is the hash of the bytes materialized there (or
    /// `None` for a removal or a pure rename). See [`SelfWrite`].
    fn record_self_write(&self, path: PathBuf, content_hash: Option<String>) {
        self.self_writes
            .borrow_mut()
            .insert(path, SelfWrite { content_hash });
    }

    /// Decide whether a watcher event for `path` reflects a self-write and, if
    /// so, consume the record (first-match wins).
    ///
    /// - Ingest events (Create / move-in): any pending self-write for the path
    ///   is our own materialization; pass `observed_hash: None` to match on
    ///   presence alone.
    /// - `Modify`: pass the freshly hashed on-disk content as `observed_hash`.
    ///   The event is suppressed only when it equals the hash we materialized,
    ///   so a genuine user edit (different hash) is *not* swallowed. A pending
    ///   record whose hash does not match is left in place.
    /// - `Remove`: pass `observed_hash: None`; a presence match suppresses it.
    fn take_matching_self_write(&self, path: &Path, observed_hash: Option<&str>) -> bool {
        let mut self_writes = self.self_writes.borrow_mut();
        let Some(record) = self_writes.get(path) else {
            return false;
        };

        // For a `Modify` we only suppress when the on-disk content matches what
        // we wrote. If the caller supplies a hash and the record has one, they
        // must agree; a mismatch means a real edit landed on top of (or instead
        // of) our write, so let it through and keep the record for the event we
        // actually expected.
        if let (Some(observed), Some(recorded)) = (observed_hash, record.content_hash.as_deref())
            && observed != recorded
        {
            return false;
        }

        self_writes.remove(path);
        true
    }

    fn send_change(
        &self,
        sync_directory: &RichSyncDirectory,
        change: Change,
    ) -> Result<(), SyncDirectoryError> {
        let change_origin = ChangeOrigin::Local {
            directory_path: sync_directory.path.clone(),
        };

        // FIX: Put this into a queue for proper retry handling instead.
        let _ = self.change_sender.send(DaemonMessage::Change(
            Ingest::from_change(change),
            change_origin,
        ));

        Ok(())
    }

    /// Send a content-bearing change carrying [`FileBytes`] (which may still
    /// live on disk) onto the ingest bus as an [`Ingest::Content`].
    fn send_content_change(
        &self,
        sync_directory: &RichSyncDirectory,
        content_change: ContentChange,
    ) -> Result<(), SyncDirectoryError> {
        let change_origin = ChangeOrigin::Local {
            directory_path: sync_directory.path.clone(),
        };

        // FIX: Put this into a queue for proper retry handling instead.
        let _ = self.change_sender.send(DaemonMessage::Change(
            Ingest::Content(content_change),
            change_origin,
        ));

        Ok(())
    }

    fn add_file(
        &self,
        sync_directory: &RichSyncDirectory,
        path: impl AsRef<Path>,
        content: FileBytes,
        content_hash: String,
        size: u64,
        tags: Vec<TagId>,
    ) -> Result<(), SyncDirectoryError> {
        let file_id = FileId::new();

        // Ingestion boundary: this file's on-disk relative path within the sync
        // directory *is* its logical identity. `add_file` is only used by
        // TagBased directories (Universal uses `upload_file`), so the physical
        // and logical paths coincide here; we still derive them explicitly.
        let logical_path = PhysicalPath::new(path.as_ref().to_string_lossy()).into_logical();
        let physical_path = sync_directory
            .sync_type
            .physical_for(&logical_path, file_id);

        sync_directory
            .database
            .add_file(file_id, &physical_path)
            .map_err(|_| SyncDirectoryError::FailedAddingFile)?;

        // TagBased ingestion leaves the file in place: the content is a copy of
        // the on-disk source (`get_file_content` returns `FileToCopy`).
        self.send_content_change(
            sync_directory,
            ContentChange::FileAdded {
                file_id,
                logical_path,
                content,
                content_hash,
                size,
                tags,
            },
        )
    }

    fn upload_file(
        &self,
        sync_directory: &RichSyncDirectory,
        path: impl AsRef<Path>,
        content: FileBytes,
        content_hash: String,
        size: u64,
        tags: Vec<TagId>,
    ) -> Result<(), SyncDirectoryError> {
        let file_id = FileId::new();

        // Ingestion boundary: the on-disk relative path becomes the file's
        // logical identity. (For a Universal directory that relative path is
        // itself the file's `file_id`, so an uploaded Universal file's logical
        // path is its id until a real name is supplied elsewhere.)
        let logical_path = PhysicalPath::new(path.as_ref().to_string_lossy()).into_logical();

        let full_path = sync_directory.path.join(path.as_ref());

        // A Universal upload removes the source from this directory: the bytes
        // move into their content-addressed location. So the content is handed
        // downstream as a *move*, and the consumer (or the fan-out in
        // `handle_changes`) is responsible for relocating/removing the source
        // rather than us deleting it eagerly here.
        //
        // The move out of this directory still produces a `Remove` watcher
        // event we must ignore; record the self-write up front (no content hash:
        // a removal has no bytes to match on).
        self.record_self_write(full_path.clone(), None);

        log::info!(
            "File {} was uploaded; its bytes will be moved out of this directory",
            full_path.to_string_lossy()
        );

        if let Err(error) = self.send_content_change(
            sync_directory,
            ContentChange::FileAdded {
                file_id,
                logical_path,
                content: content.into_move(),
                content_hash,
                size,
                tags,
            },
        ) {
            log::error!(
                "Failed to add file {}: {:?}",
                path.as_ref().to_string_lossy(),
                error
            );
            return Err(error);
        }

        Ok(())
    }

    fn update_file_content(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
        content: FileBytes,
        content_hash: String,
        size: u64,
    ) -> Result<(), SyncDirectoryError> {
        self.send_content_change(
            sync_directory,
            ContentChange::FileChanged {
                file_id,
                content,
                content_hash,
                size,
            },
        )
    }

    /// Handle a file being moved/renamed *within* a sync directory on this
    /// device. The new on-disk relative path is an ingestion boundary: it
    /// defines the file's new logical identity. We update our own physical
    /// record and announce the new logical path to peers via `FileMoved`.
    ///
    /// Only meaningful for `TagBased` directories (a Universal directory has no
    /// user-visible on-disk names to move); callers guarantee that.
    fn move_file_within_directory(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
        new_relative_path: impl AsRef<Path>,
    ) -> Result<(), SyncDirectoryError> {
        let logical_path =
            PhysicalPath::new(new_relative_path.as_ref().to_string_lossy()).into_logical();
        let physical_path = sync_directory
            .sync_type
            .physical_for(&logical_path, file_id);

        sync_directory
            .database
            .update_file_physical_path(file_id, &physical_path)
            .map_err(|_| SyncDirectoryError::FailedChangingFile)?;

        self.send_change(
            sync_directory,
            Change::FileMoved {
                file_id,
                logical_path,
            },
        )
    }

    fn remove_file_by_id(
        &self,
        sync_directory: &RichSyncDirectory,
        file_id: FileId,
    ) -> Result<(), SyncDirectoryError> {
        sync_directory
            .database
            .remove_file_by_id(file_id)
            .map_err(|_| SyncDirectoryError::FailedRemovingFile)?;

        self.send_change(
            sync_directory,
            Change::FileDeleted {
                file_id,
                deleted_at: crate::database::now_millis(),
            },
        )
    }

    fn get_all_files(
        &self,
        sync_directory: &RichSyncDirectory,
    ) -> Result<Vec<SyncDirectoryFile>, SyncDirectoryError> {
        sync_directory
            .database
            .get_all_files()
            .map_err(|_| SyncDirectoryError::MissingTrackedFile)
    }

    fn get_all_files_at(
        &self,
        sync_directory: &RichSyncDirectory,
        physical_path: impl AsRef<Path>,
    ) -> Result<Vec<SyncDirectoryFile>, SyncDirectoryError> {
        let physical_path = PhysicalPath::new(physical_path.as_ref().to_string_lossy());
        sync_directory
            .database
            .get_all_files_at(&physical_path)
            .map_err(|_| SyncDirectoryError::MissingTrackedFile)
    }

    /// Describe the content at `path` for ingestion without buffering it into
    /// memory: returns a [`FileBytes::FileToCopy`] referencing the file, its
    /// BLAKE3 hash (computed by streaming the file, so a large file is never
    /// held in memory at once), and its size in bytes.
    ///
    /// The size is read here, at hash time, since the file is already being
    /// opened/streamed and its exact byte length is known.
    ///
    /// `FileToCopy` is the safe default (the source is left in place).
    /// Producers whose ingestion should *consume* the source (e.g. a
    /// Universal upload) convert it to [`FileBytes::FileToMove`] via
    /// [`FileBytes::into_move`].
    async fn get_file_content(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(FileBytes, String, u64), SyncDirectoryError> {
        let content = FileBytes::FileToCopy(path.as_ref().to_path_buf());
        let content_hash = content
            .hash()
            .await
            .map_err(|_| SyncDirectoryError::FailedToReadFile)?;
        let size = content
            .byte_len()
            .await
            .map_err(|_| SyncDirectoryError::FailedToReadFile)?;
        Ok((content, content_hash, size))
    }

    fn sync_directory_for_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<&RichSyncDirectory, SyncDirectoryError> {
        self.sync_directories
            .iter()
            .find(|sync_directory| path.as_ref().starts_with(&sync_directory.path))
            .ok_or(SyncDirectoryError::UnmonitoredDirectory)
    }

    fn get_file_id(
        &self,
        sync_directory: &RichSyncDirectory,
        physical_path: impl AsRef<Path>,
    ) -> Result<FileId, SyncDirectoryError> {
        let physical_path = PhysicalPath::new(physical_path.as_ref().to_string_lossy());
        sync_directory
            .database
            .get_file_id(&physical_path)
            .map_err(|_| SyncDirectoryError::MissingTrackedFile)
    }

    async fn intial_sync_universal(
        &self,
        sync_directory: &RichSyncDirectory,
        files: Vec<SyncDirectoryFile>,
        last_known_hashes: &HashMap<FileId, String>,
    ) {
        for sync_file in files {
            let full_path = sync_directory.path.join(sync_file.file_id.to_string());

            log::debug!("Checking file {}", full_path.to_string_lossy());

            if !full_path.exists() {
                log::info!(
                    "File {} was deleted without monitoring. Syncing deletion",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.remove_file_by_id(sync_directory, sync_file.file_id) {
                    log::error!(
                        "Failed to remove file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }

                continue;
            }

            let (content, content_hash, size) = match self.get_file_content(&full_path).await {
                Ok(triple) => triple,
                Err(error) => {
                    log::error!("Failed to read file content: {:?}", error);
                    continue;
                }
            };

            let last_known_hash = last_known_hashes.get(&sync_file.file_id);
            if last_known_hash.map(String::as_str) != Some(content_hash.as_str()) {
                log::info!(
                    "File {} was changed without monitoring. Syncing change",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.update_file_content(
                    sync_directory,
                    sync_file.file_id,
                    content,
                    content_hash,
                    size,
                ) {
                    log::error!(
                        "Failed to update file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }
            }
        }

        for entry in WalkDir::new(&sync_directory.path)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
        {
            let Ok(relative_path) = entry.path().strip_prefix(&sync_directory.path) else {
                panic!("Walkdir returned a path outside of the sync directory");
            };

            // FIX: Maybe this should not use to_string_lossy but rather to utf8 since a
            // valid uuid will always ve valid utf8?
            if let Some(file_id) = FileId::from_string(&relative_path.to_string_lossy()) {
                match sync_directory.database.get_file(file_id) {
                    // File is already tracked.
                    Ok(_) => {
                        log::debug!(
                            "File {} is already tracked",
                            relative_path.to_string_lossy()
                        );
                        continue;
                    }
                    Err(DatabaseError::MissingFile) => {
                        // Fall through.
                    }
                    Err(error) => {
                        panic!("Database error: {:?}", error);
                    }
                }
            }

            log::info!(
                "File {} was added without monitoring. Uploading file",
                entry.path().to_string_lossy()
            );

            let (content, content_hash, size) = match self.get_file_content(entry.path()).await {
                Ok(triple) => triple,
                Err(error) => {
                    log::error!("Failed to read added file: {:?}", error);
                    continue;
                }
            };

            self.upload_file(
                sync_directory,
                relative_path,
                content,
                content_hash,
                size,
                Vec::new(),
            )
            .unwrap();
        }
    }

    async fn intial_sync_tagged(
        &self,
        sync_directory: &RichSyncDirectory,
        files: Vec<SyncDirectoryFile>,
        tags: &[TagId],
        last_known_hashes: &HashMap<FileId, String>,
    ) {
        for sync_file in files {
            let full_path = sync_directory.path.join(sync_file.physical_path.as_str());

            log::debug!("Checking file {}", full_path.to_string_lossy());

            if !full_path.exists() {
                log::info!(
                    "File {} was deleted without monitoring. Syncing deletion",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.remove_file_by_id(sync_directory, sync_file.file_id) {
                    log::error!(
                        "Failed to remove file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }

                continue;
            }

            let (content, content_hash, size) = match self.get_file_content(&full_path).await {
                Ok(triple) => triple,
                Err(error) => {
                    log::error!("Failed to read file content: {:?}", error);
                    continue;
                }
            };

            let last_known_hash = last_known_hashes.get(&sync_file.file_id);
            if last_known_hash.map(String::as_str) != Some(content_hash.as_str()) {
                log::info!(
                    "File {} was changed without monitoring. Syncing change",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.update_file_content(
                    sync_directory,
                    sync_file.file_id,
                    content,
                    content_hash,
                    size,
                ) {
                    log::error!(
                        "Failed to update file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }
            }
        }

        for entry in WalkDir::new(&sync_directory.path)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
        {
            let Ok(relative_path) = entry.path().strip_prefix(&sync_directory.path) else {
                log::error!("Walkdir returned a path outside of the sync directory");
                continue;
            };

            match sync_directory
                .database
                .get_file_id(&PhysicalPath::new(relative_path.to_string_lossy()))
            {
                // File is already tracked.
                Ok(_) => {
                    log::debug!(
                        "File {} is already tracked",
                        relative_path.to_string_lossy()
                    );
                }
                Err(DatabaseError::MissingFile) => {
                    log::info!(
                        "File {} was added without monitoring. Syncing addition",
                        entry.path().to_string_lossy()
                    );

                    let (content, content_hash, size) =
                        match self.get_file_content(entry.path()).await {
                            Ok(triple) => triple,
                            Err(error) => {
                                log::error!("Failed to read added file: {:?}", error);
                                continue;
                            }
                        };

                    if let Err(error) = self.add_file(
                        sync_directory,
                        relative_path,
                        content,
                        content_hash,
                        size,
                        tags.to_vec(),
                    ) {
                        log::error!(
                            "Failed to add file {}: {:?}",
                            relative_path.to_string_lossy(),
                            error
                        );
                    }
                }
                Err(error) => {
                    panic!("Database error: {:?}", error);
                }
            }
        }
    }

    async fn run_initial_sync(&mut self, last_known_hashes: &HashMap<FileId, String>) {
        for sync_directory in &self.sync_directories {
            log::debug!(
                "Checking for missed updates at {}",
                sync_directory.path.to_string_lossy()
            );

            let files = match self.get_all_files(sync_directory) {
                Ok(files) => files,
                Err(error) => {
                    log::error!("Failed to get list of tracked files: {:?}", error);
                    continue;
                }
            };

            match &sync_directory.sync_type {
                SyncType::Universal { .. } => {
                    self.intial_sync_universal(sync_directory, files, last_known_hashes)
                        .await
                }
                SyncType::TagBased { tags } => {
                    self.intial_sync_tagged(sync_directory, files, tags, last_known_hashes)
                        .await
                }
            }
        }
    }

    fn try_remove_empty_directory(&self, directory_path: impl AsRef<Path>) {
        if let Ok(mut read_dir) = directory_path.as_ref().read_dir()
            && read_dir.next().is_none()
        {
            log::info!(
                "Removing empty directory {}",
                directory_path.as_ref().to_string_lossy()
            );

            std::fs::remove_dir(directory_path).expect("failed to remove empty directory");
        }
    }

    /// Re-run tag-based placement for `file_id` against its current
    /// `file_tags`. See [`SyncDirectoryCommand::ReconcileTagPlacement`] for
    /// the rationale.
    ///
    /// For each TagBased directory: if it should hold the file (its tags are a
    /// subset of `file_tags`) but does not, create it there sourcing the bytes
    /// from any directory that already holds the file; if it holds the file but
    /// should not, remove it. Universal directories are skipped. Idempotent.
    ///
    /// Returns `true` if a TagBased directory should hold the file but no local
    /// copy exists to source the bytes from — i.e. placement was *deferred* and
    /// the caller must fetch the bytes over the network. `false` otherwise.
    async fn reconcile_tag_placement(
        &self,
        file_id: FileId,
        logical_path: &LogicalPath,
        file_tags: &[TagId],
    ) -> Result<bool, SyncDirectoryError> {
        // Source path lazily: we only need it if some directory must gain the
        // file. Find the on-disk path in the first directory that already holds
        // it. We keep the *path* (not `FileBytes`) so each destination can build
        // its own `FileToCopy`, which leaves the source in place for the next.
        let mut source_path: Option<PathBuf> = None;
        let mut deferred = false;

        for sync_directory in &self.sync_directories {
            let SyncType::TagBased {
                tags: sync_directory_tags,
            } = &sync_directory.sync_type
            else {
                // Universal directories have no tag filter; their membership
                // never changes on a tag update.
                continue;
            };

            let should_hold = contains_all_tags(sync_directory_tags, file_tags);
            let currently_holds = sync_directory.database.get_file(file_id).is_ok();

            match (should_hold, currently_holds) {
                (true, false) => {
                    // Newly matching: place the file here. Source the bytes from
                    // a directory that already holds it; if none does, report the
                    // deferral so the caller fetches the bytes over the network.
                    if source_path.is_none() {
                        source_path = self.first_holding_path(file_id);
                    }

                    let Some(source_path) = &source_path else {
                        log::debug!(
                            "ReconcileTagPlacement: no source copy of {} yet; deferring placement into {} (caller will fetch)",
                            file_id.to_string(),
                            sync_directory.path.to_string_lossy()
                        );
                        deferred = true;
                        continue;
                    };

                    let physical_path =
                        sync_directory.sync_type.physical_for(logical_path, file_id);
                    let file_path = sync_directory.path.join(physical_path.as_str());

                    log::info!(
                        "ReconcileTagPlacement: adding {} to {}",
                        file_id.to_string(),
                        file_path.to_string_lossy()
                    );

                    // Re-materialize from the source. `FileToCopy` leaves the
                    // source in place, so multiple destinations can share it.
                    std::fs::create_dir_all(
                        file_path
                            .parent()
                            .ok_or(SyncDirectoryError::FailedAddingFile)?,
                    )
                    .map_err(|_| SyncDirectoryError::FailedAddingFile)?;

                    let content = FileBytes::FileToCopy(source_path.clone());
                    // `FileToCopy` leaves the source in place, so hashing does
                    // not consume it; do it before materializing so the write
                    // is recognized as self-caused.
                    let content_hash = content
                        .hash()
                        .await
                        .map_err(|_| SyncDirectoryError::FailedAddingFile)?;
                    content.materialize_to(&file_path).await.map_err(|error| {
                        log::error!(
                            "ReconcileTagPlacement: failed to materialize {}: {error}",
                            file_path.display()
                        );
                        SyncDirectoryError::FailedAddingFile
                    })?;

                    // TODO: Handle naming collisions. Should append some suffix perhaps (?)
                    sync_directory
                        .database
                        .add_file(file_id, &physical_path)
                        .map_err(|_| SyncDirectoryError::FailedAddingFile)?;

                    self.record_self_write(file_path, Some(content_hash));
                }
                (false, true) => {
                    // No longer matching: drop the file from this directory.
                    let file = sync_directory
                        .database
                        .get_file(file_id)
                        .map_err(|_| SyncDirectoryError::FailedRemovingFile)?;
                    let file_path = sync_directory.path.join(file.physical_path.as_str());

                    log::info!(
                        "ReconcileTagPlacement: removing {} from {}",
                        file_id.to_string(),
                        file_path.to_string_lossy()
                    );

                    std::fs::remove_file(&file_path)
                        .map_err(|_| SyncDirectoryError::FailedRemovingFile)?;

                    if let Some(directory) = PathBuf::from(file.physical_path.as_str()).parent()
                        && !directory.as_os_str().is_empty()
                    {
                        self.try_remove_empty_directory(sync_directory.path.join(directory));
                    }

                    sync_directory
                        .database
                        .remove_file_by_id(file_id)
                        .map_err(|_| SyncDirectoryError::FailedRemovingFile)?;

                    self.record_self_write(file_path, None);
                }
                // Already in the desired state: nothing to do.
                (true, true) | (false, false) => {}
            }
        }

        Ok(deferred)
    }

    /// The on-disk path of `file_id`'s bytes in the first sync directory that
    /// holds it (and where the file actually exists on disk), or `None` if no
    /// directory has it. Used as the copy source when re-placing the file.
    fn first_holding_path(&self, file_id: FileId) -> Option<PathBuf> {
        for sync_directory in &self.sync_directories {
            let Ok(file) = sync_directory.database.get_file(file_id) else {
                continue;
            };

            let path = match &sync_directory.sync_type {
                SyncType::Universal { .. } => sync_directory.path.join(file_id.to_string()),
                SyncType::TagBased { .. } => sync_directory.path.join(file.physical_path.as_str()),
            };

            if path.exists() {
                return Some(path);
            }
        }

        None
    }

    async fn handle_command(
        &mut self,
        command: SyncDirectoryCommand,
    ) -> Result<(), SyncDirectoryError> {
        match command {
            SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                // Idempotency: several placement triggers can legitimately fire
                // for the same file (each `FileTagged` relationship reconcile,
                // plus the connect-time placement sweep), each potentially
                // fetching the bytes and emitting a `Materialize` -> `CreateFile`.
                // If this directory already tracks the file at the same physical
                // path, the bytes are already correctly placed; treat the repeat
                // as a no-op success rather than letting the duplicate insert hit
                // the per-directory DB primary key (which surfaced as
                // `FailedAddingFile` and dropped the file).
                if let Ok(existing) = sync_directory.database.get_file(file_id)
                    && existing.physical_path == physical_path
                {
                    log::debug!(
                        "CreateFile: {} already present in {} at {}; skipping (idempotent)",
                        file_id.to_string(),
                        sync_directory.path.to_string_lossy(),
                        physical_path.as_str()
                    );
                    return Ok(());
                }

                // `physical_path` was already resolved from the file's logical
                // path via `SyncType::physical_for` by the caller, so it is the
                // correct on-disk name for this directory type (the file_id for
                // Universal, the logical path for TagBased). Store the bytes and
                // record the physical path verbatim.
                let file_path = sync_directory.path.join(physical_path.as_str());

                log::info!("Adding file at {}", file_path.to_string_lossy());

                // FIX: Don't unwrap.
                std::fs::create_dir_all(file_path.parent().unwrap())
                    .expect("failed to create file subdirectory");

                // Hash the bytes before materializing (which consumes `content`)
                // so we can recognize the watcher event this write produces as
                // self-caused. The watcher may surface it as a `Create` or, when
                // the materialize is a rename into place, as a `Move`-in — the
                // path-keyed record matches either.
                let content_hash = content.hash().await.map_err(|error| {
                    log::error!(
                        "CreateFile: failed to HASH content for {} at {} (source {:?}): {error}",
                        file_id.to_string(),
                        file_path.display(),
                        content.path(),
                    );
                    SyncDirectoryError::FailedAddingFile
                })?;

                content.materialize_to(&file_path).await.map_err(|error| {
                    log::error!(
                        "CreateFile: failed to MATERIALIZE {} at {}: {error}",
                        file_id.to_string(),
                        file_path.display()
                    );
                    SyncDirectoryError::FailedAddingFile
                })?;

                sync_directory
                    .database
                    .add_file(file_id, &physical_path)
                    .map_err(|error| {
                        log::error!(
                            "CreateFile: failed to INSERT DB row for {} at {}: {:?}",
                            file_id.to_string(),
                            physical_path.as_str(),
                            error
                        );
                        SyncDirectoryError::FailedAddingFile
                    })?;

                self.record_self_write(file_path, Some(content_hash));
            }
            SyncDirectoryCommand::ChangeFile {
                file_id,
                content,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                let physical_path = match &sync_directory.sync_type {
                    SyncType::Universal { .. } => PhysicalPath::new(file_id.to_string()),
                    SyncType::TagBased { .. } => {
                        sync_directory
                            .database
                            .get_file(file_id)
                            .map_err(|_| SyncDirectoryError::FailedChangingFile)?
                            .physical_path
                    }
                };
                let file_path = sync_directory.path.join(physical_path.as_str());

                log::info!("Modifying file at {}", file_path.to_string_lossy());

                // Hash before materializing so the resulting `Modify` event can
                // be matched by content: a user edit landing on the same path
                // would hash differently and must *not* be suppressed.
                let content_hash = content
                    .hash()
                    .await
                    .map_err(|_| SyncDirectoryError::FailedChangingFile)?;

                content.materialize_to(&file_path).await.map_err(|error| {
                    log::error!(
                        "Failed to materialize changed file at {}: {error}",
                        file_path.display()
                    );
                    SyncDirectoryError::FailedChangingFile
                })?;

                self.record_self_write(file_path, Some(content_hash));
            }
            SyncDirectoryCommand::MoveFile {
                file_id,
                physical_path,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                match &sync_directory.sync_type {
                    SyncType::Universal { .. } => {
                        // Universal directories store files under their `file_id`
                        // on disk, so a logical rename never moves any bytes: the
                        // resolved `physical_path` is still the `file_id` and this
                        // is a no-op DB write kept for symmetry.
                        sync_directory
                            .database
                            .update_file_physical_path(file_id, &physical_path)
                            .map_err(|_| SyncDirectoryError::FailedMovingFile)?;
                    }
                    SyncType::TagBased { .. } => {
                        let file = sync_directory
                            .database
                            .get_file(file_id)
                            .map_err(|_| SyncDirectoryError::FailedMovingFile)?;

                        let old_file_path = sync_directory.path.join(file.physical_path.as_str());
                        let new_file_path = sync_directory.path.join(physical_path.as_str());

                        log::info!(
                            "Moving file from {} to {}",
                            old_file_path.to_string_lossy(),
                            new_file_path.to_string_lossy()
                        );

                        sync_directory
                            .database
                            .update_file_physical_path(file_id, &physical_path)
                            .map_err(|_| SyncDirectoryError::FailedMovingFile)?;

                        // TODO: Don't unwrap
                        std::fs::create_dir_all(new_file_path.parent().unwrap())
                            .expect("failed to create new directory");
                        std::fs::rename(&old_file_path, &new_file_path)
                            .expect("failed to move file");

                        // If the moved file was in a directory that is now empty, we want to remove the
                        // directory as well.
                        if let Some(directory) = PathBuf::from(file.physical_path.as_str()).parent()
                            && !directory.as_os_str().is_empty()
                        {
                            self.try_remove_empty_directory(old_file_path.parent().unwrap());
                        }

                        // The rename is self-caused. Depending on how the OS and
                        // debouncer report it we may see a combined `Move`, or a
                        // `Remove` at the old path plus a `Create`/`Move`-in at
                        // the new one. Record both endpoints (no content hash: a
                        // rename does not change bytes) so any of those shapes is
                        // recognized and ignored.
                        self.record_self_write(old_file_path, None);
                        self.record_self_write(new_file_path, None);
                    }
                };
            }
            SyncDirectoryCommand::RemoveFile {
                file_id,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;
                let file = sync_directory
                    .database
                    .get_file(file_id)
                    .map_err(|_| SyncDirectoryError::FailedRemovingFile)?;

                // Recovery vault: a Universal directory with `keep_deleted_files`
                // retains its physical copy (and its per-directory DB row) on
                // delete, so an accidental deletion can be undone here. The file
                // is still gone from the catalog and every other directory; this
                // one just doesn't drop its bytes.
                if let SyncType::Universal {
                    keep_deleted_files: true,
                } = &sync_directory.sync_type
                {
                    log::info!(
                        "Keeping deleted file {} in {} (keep_deleted_files)",
                        file_id.to_string(),
                        sync_directory.path.to_string_lossy()
                    );
                    return Ok(());
                }

                log::info!(
                    "Removing file {} from {}",
                    file.physical_path,
                    sync_directory.path.to_string_lossy()
                );

                let file_path = match &sync_directory.sync_type {
                    SyncType::Universal { .. } => sync_directory.path.join(file_id.to_string()),
                    SyncType::TagBased { .. } => {
                        sync_directory.path.join(file.physical_path.as_str())
                    }
                };

                // Tolerate the file already being gone: a same-content duplicate
                // can leave two file_ids pointing at the same physical path, so a
                // second `RemoveFile` for that path finds nothing on disk. That is
                // not an error — we still want to clean up this file_id's DB row
                // below. Any other IO error is logged but must not crash the sole
                // sync-directory thread.
                match std::fs::remove_file(&file_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        log::debug!(
                            "RemoveFile: {} already gone; cleaning up DB row only",
                            file_path.to_string_lossy()
                        );
                    }
                    Err(error) => {
                        log::error!(
                            "RemoveFile: failed to remove {}: {error}",
                            file_path.to_string_lossy()
                        );
                        return Err(SyncDirectoryError::FailedRemovingFile);
                    }
                }

                // If the removed file was in a directory that is now empty, we want to remove
                // the directory as well.
                if let SyncType::TagBased { .. } = &sync_directory.sync_type
                    && let Some(directory) = PathBuf::from(file.physical_path.as_str()).parent()
                    && !directory.as_os_str().is_empty()
                {
                    let directory_path = sync_directory.path.join(directory);
                    self.try_remove_empty_directory(directory_path);
                }

                sync_directory
                    .database
                    .remove_file_by_id(file_id)
                    .map_err(|_| SyncDirectoryError::FailedRemovingFile)?;

                self.record_self_write(file_path, None);
            }
            SyncDirectoryCommand::ReconcileTagPlacement {
                file_id,
                logical_path,
                file_tags,
                respond_to,
            } => {
                let deferred = self
                    .reconcile_tag_placement(file_id, &logical_path, &file_tags)
                    .await?;
                // Best-effort: the caller may have dropped the receiver.
                let _ = respond_to.send(deferred);
            }
            SyncDirectoryCommand::LocalPath {
                file_id,
                respond_to,
            } => {
                // Resolve the absolute on-disk path of the first sync directory
                // that has `file_id`, without reading the bytes. We do not
                // verify the file exists on disk here; the DB row is treated as
                // authoritative (mirrors how `ReadFile` trusts it before the
                // read). The caller opens the path directly.
                let mut response: Option<PathBuf> = None;
                for sync_directory in &self.sync_directories {
                    let file = match sync_directory.database.get_file(file_id) {
                        Ok(file) => file,
                        Err(_) => continue,
                    };
                    let absolute_path = sync_directory.path.join(file.physical_path.as_str());
                    response = Some(absolute_path);
                    break;
                }
                let _ = respond_to.send(response);
            }
            SyncDirectoryCommand::ReadFile {
                file_id,
                respond_to,
            } => {
                // Walk our sync directories looking for the first one that
                // claims to have `file_id` in its database. For TagBased
                // directories the on-disk path is the recorded relative path;
                // for Universal directories the file is stored under its
                // `file_id`. If a database row points at a missing file on
                // disk we log and continue to the next directory.
                let mut response: Option<(PhysicalPath, FileBytes, String)> = None;
                for sync_directory in &self.sync_directories {
                    let file = match sync_directory.database.get_file(file_id) {
                        Ok(file) => file,
                        Err(_) => continue,
                    };
                    // `physical_path`/`absolute_path` describe where the bytes
                    // live in *this* sync directory. For Universal directories
                    // the file is stored under its `file_id`; for TagBased it is
                    // stored under its recorded relative path. This is the
                    // *physical* path only: the caller substitutes the logical
                    // (human-readable) name from the main database before sending
                    // to a peer, since the per-directory DB may only hold the
                    // physical name (the `file_id` for Universal).
                    //
                    // We return the bytes as a `FileToCopy` referencing the
                    // on-disk file rather than reading them here: the caller
                    // buffers them into a wire `Change` only when actually
                    // answering a peer, so an unfulfilled request never reads
                    // the file.
                    let physical_path = file.physical_path.clone();
                    let absolute_path = sync_directory.path.join(physical_path.as_str());
                    let (content, content_hash, _size) =
                        match self.get_file_content(&absolute_path).await {
                            Ok(triple) => triple,
                            Err(error) => {
                                log::warn!(
                                    "ReadFile: {} reported {} but read failed: {:?}",
                                    sync_directory.path.to_string_lossy(),
                                    absolute_path.to_string_lossy(),
                                    error
                                );
                                continue;
                            }
                        };
                    response = Some((physical_path, content, content_hash));
                    break;
                }
                let _ = respond_to.send(response);
            }
        }

        Ok(())
    }

    async fn handle_event(&self, event: DebouncedEventKind) -> Result<(), SyncDirectoryError> {
        match event {
            DebouncedEventKind::Create { file_name } => {
                // A Create for a path the daemon just wrote is our own
                // operation (most often a peer-received file placed into a
                // Universal directory under its `file_id`).
                if self.take_matching_self_write(&file_name, None) {
                    log::debug!(
                        "Ignoring Create for {} (our own operation)",
                        file_name.to_string_lossy()
                    );
                    return Ok(());
                }

                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();

                let (content, content_hash, size) = self.get_file_content(&file_name).await?;

                match &sync_directory.sync_type {
                    SyncType::Universal { .. } => {
                        self.upload_file(
                            sync_directory,
                            sync_relative_path,
                            content,
                            content_hash,
                            size,
                            Vec::new(),
                        )?;
                    }
                    SyncType::TagBased { tags } => {
                        self.add_file(
                            sync_directory,
                            sync_relative_path,
                            content,
                            content_hash,
                            size,
                            tags.to_vec(),
                        )?;
                    }
                }
            }
            DebouncedEventKind::Move { from, to } => {
                let Some(any_path) = from.as_ref().or(to.as_ref()) else {
                    log::warn!("Received a Move event with neither from nor to; ignoring");
                    return Ok(());
                };
                let sync_directory = self.sync_directory_for_path(any_path)?;

                if let Some(from) = &from
                    && let Some(to) = &to
                {
                    // Move within the directory.

                    if let SyncType::Universal { .. } = sync_directory.sync_type {
                        // A Universal directory stores files under their `file_id`
                        // on disk; a rename *within* it has no logical meaning and
                        // must not propagate. This event is normally one we caused
                        // ourselves — materializing a received/uploaded file moves
                        // it into place under its `file_id` (a rename the watcher
                        // reports as a Move) — and should have been skipped. If a
                        // user manually renamed a UUID file it likewise carries no
                        // logical meaning. Either way: ignore it, never crash.
                        // (A *logical* rename arrives as a `FileMoved` change and
                        // is handled in `handle_command`.)
                        log::debug!(
                            "Ignoring intra-Universal move {} -> {} (no logical meaning)",
                            from.to_string_lossy(),
                            to.to_string_lossy()
                        );
                        return Ok(());
                    };

                    // A rename the daemon performed itself (`MoveFile`) records
                    // both endpoints as self-writes. The debouncer may deliver
                    // it as this combined `Move`; consume the records and ignore
                    // it so we do not re-announce our own move. (If it instead
                    // arrives split as a Remove + Create/Move-in, those arms
                    // consume the same records.)
                    let from_self = self.take_matching_self_write(from, None);
                    let to_self = self.take_matching_self_write(to, None);
                    if from_self || to_self {
                        log::debug!(
                            "Ignoring intra-directory move {} -> {} (our own operation)",
                            from.to_string_lossy(),
                            to.to_string_lossy()
                        );
                        return Ok(());
                    }

                    let relative_from = from.strip_prefix(&sync_directory.path).unwrap();
                    let Ok(relative_to) = to.strip_prefix(&sync_directory.path) else {
                        // TODO: Handle a move *out* to a different sync directory
                        // as a delete-here + add-there. For now, ignore rather
                        // than crash.
                        log::warn!(
                            "Ignoring move of {} out to another location (cross-directory moves not yet handled)",
                            from.to_string_lossy()
                        );
                        return Ok(());
                    };

                    if let Ok(file_id) = self.get_file_id(sync_directory, relative_from) {
                        self.move_file_within_directory(sync_directory, file_id, relative_to)?;
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            let path = PathBuf::from(sync_file.physical_path.as_str());
                            let relative_path = path.strip_prefix(relative_from).unwrap();
                            let new_path = relative_to.join(relative_path);

                            self.move_file_within_directory(
                                sync_directory,
                                sync_file.file_id,
                                new_path,
                            )?;
                        }
                    }
                } else if let Some(from) = from {
                    // File was moved outside of the synced directory.

                    let relative_from = from.strip_prefix(&sync_directory.path).unwrap();

                    if let Ok(file_id) = self.get_file_id(sync_directory, relative_from) {
                        self.remove_file_by_id(sync_directory, file_id)?;
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            self.remove_file_by_id(sync_directory, sync_file.file_id)?;
                        }
                    }
                } else if let Some(to) = to {
                    // File was moved here from outside of the synced directory.
                    //
                    // This is also how the watcher reports our *own* placement:
                    // materializing a peer-received file renames it in from the
                    // daemon temp dir, arriving as `Move { from: None, to }`.

                    if to.is_file() {
                        if self.take_matching_self_write(&to, None) {
                            log::debug!(
                                "Ignoring move-in of {} (our own operation)",
                                to.to_string_lossy()
                            );
                            return Ok(());
                        }

                        let sync_relative_path = to.strip_prefix(&sync_directory.path).unwrap();

                        let (content, content_hash, size) = self.get_file_content(&to).await?;

                        match &sync_directory.sync_type {
                            SyncType::Universal { .. } => {
                                self.upload_file(
                                    sync_directory,
                                    sync_relative_path,
                                    content,
                                    content_hash,
                                    size,
                                    Vec::new(),
                                )?;
                            }
                            SyncType::TagBased { tags } => {
                                self.add_file(
                                    sync_directory,
                                    sync_relative_path,
                                    content,
                                    content_hash,
                                    size,
                                    tags.to_vec(),
                                )?;
                            }
                        }
                    } else if to.is_dir() {
                        for entry in WalkDir::new(&to)
                            .into_iter()
                            .filter_map(|entry| entry.ok())
                            .filter(|entry| entry.file_type().is_file())
                        {
                            if self.take_matching_self_write(entry.path(), None) {
                                log::debug!(
                                    "Ignoring move-in of {} (our own operation)",
                                    entry.path().to_string_lossy()
                                );
                                continue;
                            }

                            let sync_relative_path =
                                entry.path().strip_prefix(&sync_directory.path).unwrap();

                            let (content, content_hash, size) =
                                self.get_file_content(entry.path()).await?;

                            match &sync_directory.sync_type {
                                SyncType::Universal { .. } => {
                                    self.upload_file(
                                        sync_directory,
                                        sync_relative_path,
                                        content,
                                        content_hash,
                                        size,
                                        Vec::new(),
                                    )?;
                                }
                                SyncType::TagBased { tags } => {
                                    self.add_file(
                                        sync_directory,
                                        sync_relative_path,
                                        content,
                                        content_hash,
                                        size,
                                        tags.to_vec(),
                                    )?;
                                }
                            }
                        }
                    } else {
                        log::warn!(
                            "A file that is not a regular file or a directory was detected. This is unsupported at the moment"
                        );
                    }
                } else {
                    log::error!("Received an empty move. This should never happen");
                }
            }
            DebouncedEventKind::Modify { file_name } => {
                let (content, content_hash, size) = self.get_file_content(&file_name).await?;

                // Suppress only if the on-disk content matches what the daemon
                // just wrote here.
                if self.take_matching_self_write(&file_name, Some(&content_hash)) {
                    log::debug!(
                        "Ignoring Modify of {} (our own operation)",
                        file_name.to_string_lossy()
                    );
                    return Ok(());
                }

                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();
                let file_id = self.get_file_id(sync_directory, sync_relative_path)?;

                self.update_file_content(sync_directory, file_id, content, content_hash, size)?;
            }
            DebouncedEventKind::Remove { file_name } => {
                // A removal the daemon caused itself (delete, move-out, or the
                // source side of a rename) has no content to match on, so a
                // presence match consumes the record and ignores the event.
                if self.take_matching_self_write(&file_name, None) {
                    log::debug!(
                        "Ignoring Remove of {} (our own operation)",
                        file_name.to_string_lossy()
                    );
                    return Ok(());
                }

                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let sync_relative_path = file_name.strip_prefix(&sync_directory.path).unwrap();
                let file_id = self.get_file_id(sync_directory, sync_relative_path)?;

                self.remove_file_by_id(sync_directory, file_id)?;
            }
        }

        Ok(())
    }

    /// Run the directory manager.
    ///
    /// `last_known_hashes` is the last-known content hash per `FileId` as
    /// observed by previous runs of the daemon, loaded once from the main
    /// DB's `file_versions` table at startup. Used exclusively during
    /// `run_initial_sync` to decide whether an on-disk file changed while the
    /// daemon was offline; it is dropped once the initial sync finishes.
    /// Shutdown-safety invariant: this loop is cancelled abruptly on shutdown
    /// (`handle_sync_directories` drops the manager when its
    /// `CancellationToken` fires, which runs `WatchDispatcher::drop` to
    /// stop the watcher). Because cancellation can only interrupt this
    /// future at an `.await` point, and the only `.await`s in this loop are
    /// the two `recv()` calls in the `select!` below, a shutdown can only
    /// ever land *between* whole events — never midway through
    /// `handle_command`/`handle_event`. Those handlers are fully
    /// synchronous today, so each one runs to completion atomically.
    ///
    /// DANGER: do not introduce an `.await` inside `handle_command`,
    /// `handle_event`, or anything they call. Doing so creates a new
    /// cancellation point mid-handler: a shutdown could then abandon a
    /// sync-directory DB write or a partial file mirror halfway through,
    /// leaving on-disk state inconsistent. If any of that work must become
    /// async, first make shutdown cooperative here (observe cancellation at the
    /// top of the loop and return normally) instead of relying on abrupt drop.
    pub async fn run(&mut self, last_known_hashes: HashMap<FileId, String>) {
        self.run_initial_sync(&last_known_hashes).await;

        log::info!("Directories are fully synced");

        loop {
            tokio::select! {
                command = self.command_receiver.recv() => {
                    let Some(command) = command else {
                        // TODO: Maybe this is an error?
                        break;
                    };

                    if let Err(error) = self.handle_command(command).await {
                        log::error!("Failed to handle command: {:?}", error);
                    }
                },
                watcher_event = self.watcher_events.recv() => {
                    let Some(event) = watcher_event else {
                        // TODO: Maybe this is an error?
                        break;
                    };

                    log::debug!("Received event: {:?}", event);

                    if let Err(error) = self.handle_event(event).await {
                        log::error!("Failed to handle event: {:?}", error);
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::configuration::{Configuration, SyncDirectory};

    /// A unique temp directory for a test, created eagerly.
    fn temp_dir(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "onisync-dirmgr-test-{}-{}-{}",
            label,
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    /// Build a `SyncDirectoryManager` with a single Universal sync directory at
    /// `sync_dir`, returning the manager (the change receiver is discarded; the
    /// tests only exercise `handle_command`, which does not emit changes).
    async fn universal_manager(data_dir: &Path, sync_dir: &Path) -> SyncDirectoryManager {
        universal_manager_with(data_dir, sync_dir, false).await
    }

    /// As [`universal_manager`] but with an explicit `keep_deleted_files` flag
    /// on the Universal directory.
    async fn universal_manager_with(
        data_dir: &Path,
        sync_dir: &Path,
        keep_deleted_files: bool,
    ) -> SyncDirectoryManager {
        let configuration = Configuration {
            sync_directories: vec![SyncDirectory {
                path: sync_dir.to_path_buf(),
                sync_type: SyncType::Universal { keep_deleted_files },
            }],
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
        };
        let paths = Paths::new(data_dir, data_dir.join("identity"));
        let (change_sender, _change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        SyncDirectoryManager::new(configuration, &paths, change_sender, command_receiver).await
    }

    /// `CreateFile` carrying a `FileToMove` renames the source into the file's
    /// content-addressed (`file_id`) location and removes the source. This is
    /// the Universal-upload materialization path.
    #[tokio::test]
    async fn create_file_with_move_relocates_source() {
        let data_dir = temp_dir("move-data");
        let sync_dir = temp_dir("move-sync");
        let mut manager = universal_manager(&data_dir, &sync_dir).await;

        // A source file sitting in the sync directory under a human name (as if
        // just dropped in for upload).
        let source = sync_dir.join("photo.jpg");
        std::fs::write(&source, b"image-bytes").unwrap();

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::FileToMove(source.clone()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert!(
            !source.exists(),
            "FileToMove must consume the source at {}",
            source.display()
        );
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"image-bytes",
            "bytes must land at the content-addressed destination"
        );
    }

    /// `CreateFile` carrying a `FileToCopy` writes the destination and leaves
    /// the source untouched (the tag-based / keep-source path).
    #[tokio::test]
    async fn create_file_with_copy_preserves_source() {
        let data_dir = temp_dir("copy-data");
        let sync_dir = temp_dir("copy-sync");
        let mut manager = universal_manager(&data_dir, &sync_dir).await;

        // Source lives outside the sync directory (a copy target must not be
        // removed regardless of where it is).
        let external = temp_dir("copy-external");
        let source = external.join("doc.txt");
        std::fs::write(&source, b"doc-bytes").unwrap();

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::FileToCopy(source.clone()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert!(
            source.exists(),
            "FileToCopy must leave the source in place at {}",
            source.display()
        );
        assert_eq!(std::fs::read(&destination).unwrap(), b"doc-bytes");
    }

    /// A repeated `CreateFile` for a file the directory already holds at the
    /// same physical path is a no-op success, not a `FailedAddingFile` error.
    /// Several placement triggers (per-`FileTagged` reconciles + the connect
    /// sweep) can legitimately fetch and re-place the same file; the sink must
    /// tolerate the duplicate rather than dropping the file on a PK violation.
    #[tokio::test]
    async fn create_file_is_idempotent_for_same_file() {
        let data_dir = temp_dir("idem-data");
        let sync_dir = temp_dir("idem-sync");
        let mut manager = universal_manager(&data_dir, &sync_dir).await;

        let external = temp_dir("idem-external");
        let source = external.join("doc.txt");
        std::fs::write(&source, b"doc-bytes").unwrap();

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        let create = |content| SyncDirectoryCommand::CreateFile {
            file_id,
            physical_path: physical_path.clone(),
            content,
            sync_directory_path: sync_dir.clone(),
        };

        manager
            .handle_command(create(FileBytes::FileToCopy(source.clone())))
            .await
            .expect("first CreateFile must succeed");

        // Second CreateFile for the same file_id + physical path: must be a
        // no-op success, not an error.
        manager
            .handle_command(create(FileBytes::FileToCopy(source.clone())))
            .await
            .expect("repeated CreateFile must be an idempotent no-op, not an error");

        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"doc-bytes",
            "the file must still be present after the repeated create"
        );
    }

    /// `ChangeFile` carrying a `FileToCopy` overwrites the existing bytes at
    /// the file's on-disk location.
    #[tokio::test]
    async fn change_file_overwrites_destination() {
        let data_dir = temp_dir("change-data");
        let sync_dir = temp_dir("change-sync");
        let mut manager = universal_manager(&data_dir, &sync_dir).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        // Seed an initial version so there is something to overwrite.
        std::fs::write(&destination, b"old-bytes").unwrap();

        let external = temp_dir("change-external");
        let source = external.join("new.bin");
        std::fs::write(&source, b"new-bytes").unwrap();

        manager
            .handle_command(SyncDirectoryCommand::ChangeFile {
                file_id,
                content: FileBytes::FileToCopy(source.clone()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert_eq!(std::fs::read(&destination).unwrap(), b"new-bytes");
        assert!(source.exists(), "FileToCopy must leave the source in place");
    }

    /// Regression: materializing a peer-received file into a Universal
    /// directory arrives at the watcher as `Move { from: None, to }` (a
    /// rename in from the daemon temp dir). It must NOT be re-ingested as a
    /// new upload — doing so minted a duplicate file whose logical path was
    /// the real file's `file_id` and looped. A move-in of an
    /// already-tracked path is ignored.
    #[tokio::test]
    async fn move_in_of_tracked_file_is_not_reingested() {
        let data_dir = temp_dir("reingest-data");
        let sync_dir = temp_dir("reingest-sync");

        // Keep the change receiver so we can assert nothing new is emitted.
        let configuration = Configuration {
            sync_directories: vec![SyncDirectory {
                path: sync_dir.to_path_buf(),
                sync_type: SyncType::Universal {
                    keep_deleted_files: false,
                },
            }],
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
        };
        let paths = Paths::new(&data_dir, data_dir.join("identity"));
        let (change_sender, mut change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut manager =
            SyncDirectoryManager::new(configuration, &paths, change_sender, command_receiver).await;

        // Materialize a received file: writes it under its file_id and tracks it.
        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());
        let external = temp_dir("reingest-external");
        let source = external.join("incoming.bin");
        std::fs::write(&source, b"received-bytes").unwrap();
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::FileToCopy(source),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        // Now simulate the watcher's move-in event for that same file.
        manager
            .handle_event(DebouncedEventKind::Move {
                from: None,
                to: Some(destination.clone()),
            })
            .await
            .unwrap();

        // No change must have been emitted (no re-ingestion / new FileAdded).
        assert!(
            change_receiver.try_recv().is_err(),
            "move-in of an already-tracked file must not emit a change (no re-ingestion)"
        );
        // The sync-directory DB must still hold exactly the one file.
        assert!(
            manager.sync_directories[0]
                .database
                .get_file(file_id)
                .is_ok(),
            "the original file must remain tracked"
        );
    }

    /// Build a single-Universal-directory manager, returning the manager and
    /// its change receiver so a test can assert on emitted changes.
    async fn universal_manager_with_receiver(
        data_dir: &Path,
        sync_dir: &Path,
    ) -> (
        SyncDirectoryManager,
        tokio::sync::mpsc::UnboundedReceiver<DaemonMessage>,
    ) {
        let configuration = Configuration {
            sync_directories: vec![SyncDirectory {
                path: sync_dir.to_path_buf(),
                sync_type: SyncType::Universal {
                    keep_deleted_files: false,
                },
            }],
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
        };
        let paths = Paths::new(data_dir, data_dir.join("identity"));
        let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        let manager =
            SyncDirectoryManager::new(configuration, &paths, change_sender, command_receiver).await;
        (manager, change_receiver)
    }

    /// A `Modify` whose on-disk content matches the bytes the daemon just wrote
    /// (via `ChangeFile`) is recognized as our own write and suppressed — no
    /// change is re-emitted. A path-only guard cannot distinguish this from a
    /// real edit; the hash comparison is what makes the distinction reliable.
    #[tokio::test]
    async fn self_caused_modify_is_suppressed() {
        let data_dir = temp_dir("selfmod-data");
        let sync_dir = temp_dir("selfmod-sync");
        let (mut manager, mut change_receiver) =
            universal_manager_with_receiver(&data_dir, &sync_dir).await;

        // Track a file, then drain the FileAdded emitted by the create.
        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"v1".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        // Daemon changes the content itself; this records a self-write for the
        // new bytes and writes them to disk. `ChangeFile` legitimately emits a
        // FileChanged for its own edit — drain everything so far.
        manager
            .handle_command(SyncDirectoryCommand::ChangeFile {
                file_id,
                content: FileBytes::InMemory(b"v2".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        while change_receiver.try_recv().is_ok() {}

        // The watcher now reports the Modify for that same write. It must be
        // suppressed: on-disk content ("v2") matches the recorded hash.
        assert_eq!(std::fs::read(&destination).unwrap(), b"v2");
        manager
            .handle_event(DebouncedEventKind::Modify {
                file_name: destination.clone(),
            })
            .await
            .unwrap();

        assert!(
            change_receiver.try_recv().is_err(),
            "a self-caused Modify must not re-emit a change"
        );
    }

    /// A `Modify` whose on-disk content differs from what the daemon last wrote
    /// is a genuine user edit and must be propagated — the self-write record
    /// for a stale hash must not swallow it.
    #[tokio::test]
    async fn user_edit_after_self_write_is_propagated() {
        let data_dir = temp_dir("useredit-data");
        let sync_dir = temp_dir("useredit-sync");
        let (mut manager, mut change_receiver) =
            universal_manager_with_receiver(&data_dir, &sync_dir).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"v1".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        while change_receiver.try_recv().is_ok() {}

        // The user edits the file to different content than the daemon last
        // materialized. The self-write record (hash of "v1") is still pending
        // from the create, but the on-disk hash now differs.
        std::fs::write(&destination, b"user-edited").unwrap();
        manager
            .handle_event(DebouncedEventKind::Modify {
                file_name: destination.clone(),
            })
            .await
            .unwrap();

        assert!(
            change_receiver.try_recv().is_ok(),
            "a user edit with different content must be propagated"
        );
    }

    /// Self-write suppression is keyed by path, not by the predicted event
    /// variant: a materialize the watcher surfaces as a plain `Create` (rather
    /// than the `Move`-in of the other regression test) is still recognized and
    /// ignored.
    #[tokio::test]
    async fn self_caused_create_variant_is_suppressed() {
        let data_dir = temp_dir("selfcreate-data");
        let sync_dir = temp_dir("selfcreate-sync");
        let (mut manager, mut change_receiver) =
            universal_manager_with_receiver(&data_dir, &sync_dir).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"bytes".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        while change_receiver.try_recv().is_ok() {}

        manager
            .handle_event(DebouncedEventKind::Create {
                file_name: destination,
            })
            .await
            .unwrap();

        assert!(
            change_receiver.try_recv().is_err(),
            "a self-caused Create must not re-emit a change (no re-ingestion)"
        );
    }

    /// Build a manager with a Universal directory (index 0) and a TagBased
    /// directory (index 1) requiring `tags`.
    async fn mixed_manager(
        data_dir: &Path,
        universal_dir: &Path,
        tagged_dir: &Path,
        tags: Vec<TagId>,
    ) -> SyncDirectoryManager {
        let configuration = Configuration {
            sync_directories: vec![
                SyncDirectory {
                    path: universal_dir.to_path_buf(),
                    sync_type: SyncType::Universal {
                        keep_deleted_files: false,
                    },
                },
                SyncDirectory {
                    path: tagged_dir.to_path_buf(),
                    sync_type: SyncType::TagBased { tags },
                },
            ],
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
        };
        let paths = Paths::new(&data_dir, data_dir.join("identity"));
        let (change_sender, _change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        SyncDirectoryManager::new(configuration, &paths, change_sender, command_receiver).await
    }

    /// Regression: when one materialized file fans out to multiple sync
    /// directories, the fan-out gives earlier targets a `FileToCopy` and the
    /// last target a `FileToMove` (no eager source delete). Both destinations
    /// must end up with the bytes, and the move must consume the shared source.
    /// The previous code eagerly deleted the move source right after enqueuing
    /// the (asynchronously processed) copies, so the copies raced a deleted
    /// source and failed with `FailedAddingFile`, dropping the file everywhere.
    #[tokio::test]
    async fn fan_out_copy_then_move_places_into_all_directories() {
        let data_dir = temp_dir("fanout-data");
        let universal_dir = temp_dir("fanout-universal");
        let tagged_dir = temp_dir("fanout-tagged");
        let tag = TagId::new();
        let mut manager = mixed_manager(&data_dir, &universal_dir, &tagged_dir, vec![tag]).await;

        let file_id = FileId::new();
        // A single shared source, as a completed transfer's temp file would be.
        let external = temp_dir("fanout-external");
        let source = external.join("transfer-temp");
        std::fs::write(&source, b"shared-bytes").unwrap();

        // Mirror the fan-out command sequence for two targets: the earlier
        // target copies (source preserved), the last target moves (source
        // consumed). Commands are processed in order.
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(file_id.to_string()),
                content: FileBytes::FileToCopy(source.clone()),
                sync_directory_path: universal_dir.clone(),
            })
            .await
            .expect("copy target must succeed while the source still exists");

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new("photo.jpg"),
                content: FileBytes::FileToMove(source.clone()),
                sync_directory_path: tagged_dir.clone(),
            })
            .await
            .expect("move target must succeed and consume the source");

        assert_eq!(
            std::fs::read(universal_dir.join(file_id.to_string())).unwrap(),
            b"shared-bytes",
            "the copy target must have the bytes"
        );
        assert_eq!(
            std::fs::read(tagged_dir.join("photo.jpg")).unwrap(),
            b"shared-bytes",
            "the move target must have the bytes"
        );
        assert!(
            !source.exists(),
            "the trailing move must consume the shared source"
        );
    }

    /// Regression for the tag-vs-content reconciliation race
    /// (STREAMING_FOLLOWUPS §1.3): a peer transfer materialized a file
    /// before its tags were applied, so it landed only in the Universal
    /// directory (which has no tag filter). When the tags arrive,
    /// `ReconcileTagPlacement` must place the file into the now-matching
    /// TagBased directory, sourcing the bytes from the Universal copy.
    #[tokio::test]
    async fn reconcile_places_file_into_newly_matching_tag_directory() {
        let data_dir = temp_dir("reconcile-add-data");
        let universal_dir = temp_dir("reconcile-add-universal");
        let tagged_dir = temp_dir("reconcile-add-tagged");
        let tag = TagId::new();
        let mut manager = mixed_manager(&data_dir, &universal_dir, &tagged_dir, vec![tag]).await;

        // Simulate the race: the file was materialized into the Universal dir
        // only (tags not yet known), stored under its file_id.
        let file_id = FileId::new();
        let logical_path = LogicalPath::new("photo.jpg");
        let external = temp_dir("reconcile-add-external");
        let source = external.join("incoming.bin");
        std::fs::write(&source, b"received-bytes").unwrap();
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(file_id.to_string()),
                content: FileBytes::FileToCopy(source),
                sync_directory_path: universal_dir.clone(),
            })
            .await
            .unwrap();

        // The TagBased dir does not hold it yet.
        assert!(
            manager.sync_directories[1]
                .database
                .get_file(file_id)
                .is_err()
        );

        // Tags arrive: the file now matches the TagBased directory.
        let (respond_to, deferred) = tokio::sync::oneshot::channel();
        manager
            .handle_command(SyncDirectoryCommand::ReconcileTagPlacement {
                file_id,
                logical_path: logical_path.clone(),
                file_tags: vec![tag],
                respond_to,
            })
            .await
            .unwrap();
        assert!(
            !deferred.await.unwrap(),
            "a local source copy exists, so placement must complete (not defer)"
        );

        // The file now lives in the TagBased dir under its logical path, with
        // the correct bytes, and remains in the Universal dir.
        let tagged_destination = tagged_dir.join(logical_path.as_str());
        assert_eq!(
            std::fs::read(&tagged_destination).unwrap(),
            b"received-bytes",
            "file must be placed into the newly-matching TagBased directory"
        );
        assert!(
            manager.sync_directories[1]
                .database
                .get_file(file_id)
                .is_ok(),
            "TagBased dir DB must track the newly-placed file"
        );
        assert!(
            universal_dir.join(file_id.to_string()).exists(),
            "Universal copy must remain in place (FileToCopy source)"
        );
    }

    /// Symmetric with the add case: a file that loses a TagBased directory's
    /// tags must be dropped from it, while the Universal copy is untouched.
    #[tokio::test]
    async fn reconcile_removes_file_from_no_longer_matching_tag_directory() {
        let data_dir = temp_dir("reconcile-remove-data");
        let universal_dir = temp_dir("reconcile-remove-universal");
        let tagged_dir = temp_dir("reconcile-remove-tagged");
        let tag = TagId::new();
        let mut manager = mixed_manager(&data_dir, &universal_dir, &tagged_dir, vec![tag]).await;

        let file_id = FileId::new();
        let logical_path = LogicalPath::new("photo.jpg");
        let external = temp_dir("reconcile-remove-external");
        let source = external.join("incoming.bin");
        std::fs::write(&source, b"received-bytes").unwrap();

        // The file is present in both directories.
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(file_id.to_string()),
                content: FileBytes::FileToCopy(source.clone()),
                sync_directory_path: universal_dir.clone(),
            })
            .await
            .unwrap();
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(logical_path.as_str()),
                content: FileBytes::FileToCopy(source),
                sync_directory_path: tagged_dir.clone(),
            })
            .await
            .unwrap();

        let tagged_destination = tagged_dir.join(logical_path.as_str());
        assert!(tagged_destination.exists());

        // The file is untagged: it no longer matches the TagBased directory.
        manager
            .handle_command(SyncDirectoryCommand::ReconcileTagPlacement {
                file_id,
                logical_path,
                file_tags: Vec::new(),
                respond_to: {
                    let (respond_to, _) = tokio::sync::oneshot::channel();
                    respond_to
                },
            })
            .await
            .unwrap();

        assert!(
            !tagged_destination.exists(),
            "file must be removed from the TagBased directory it no longer matches"
        );
        assert!(
            manager.sync_directories[1]
                .database
                .get_file(file_id)
                .is_err(),
            "TagBased dir DB must no longer track the file"
        );
        assert!(
            universal_dir.join(file_id.to_string()).exists(),
            "Universal copy must be untouched"
        );
    }

    /// When no directory yet holds the file, a reconcile that would add it
    /// reports the deferral (so the caller fetches the bytes over the network)
    /// and creates nothing locally.
    #[tokio::test]
    async fn reconcile_defers_when_no_source_copy_exists() {
        let data_dir = temp_dir("reconcile-defer-data");
        let universal_dir = temp_dir("reconcile-defer-universal");
        let tagged_dir = temp_dir("reconcile-defer-tagged");
        let tag = TagId::new();
        let mut manager = mixed_manager(&data_dir, &universal_dir, &tagged_dir, vec![tag]).await;

        let file_id = FileId::new();
        let logical_path = LogicalPath::new("photo.jpg");

        let (respond_to, deferred) = tokio::sync::oneshot::channel();
        manager
            .handle_command(SyncDirectoryCommand::ReconcileTagPlacement {
                file_id,
                logical_path: logical_path.clone(),
                file_tags: vec![tag],
                respond_to,
            })
            .await
            .unwrap();

        assert!(
            deferred.await.unwrap(),
            "with no source copy, placement must report deferred so the caller fetches"
        );
        assert!(
            !tagged_dir.join(logical_path.as_str()).exists(),
            "with no source copy, no file is created locally (bytes fetched by caller)"
        );
        assert!(
            manager.sync_directories[1]
                .database
                .get_file(file_id)
                .is_err(),
            "TagBased dir DB must not track a file that was never materialized"
        );
    }

    /// Create then `RemoveFile` a file in a Universal directory with
    /// `keep_deleted_files = true`: the bytes and the per-directory DB row must
    /// survive so the file can be recovered.
    #[tokio::test]
    async fn keep_deleted_files_retains_bytes_on_remove() {
        let data_dir = temp_dir("keep-data");
        let sync_dir = temp_dir("keep-sync");
        let mut manager = universal_manager_with(&data_dir, &sync_dir, true).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"precious".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        assert!(destination.exists());

        manager
            .handle_command(SyncDirectoryCommand::RemoveFile {
                file_id,
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert!(
            destination.exists(),
            "keep_deleted_files must retain the physical copy on delete"
        );
        assert_eq!(std::fs::read(&destination).unwrap(), b"precious");
        assert!(
            manager.sync_directories[0]
                .database
                .get_file(file_id)
                .is_ok(),
            "keep_deleted_files must retain the per-directory DB row for recovery"
        );
    }

    /// The default (`keep_deleted_files = false`) still deletes the bytes on
    /// `RemoveFile`.
    #[tokio::test]
    async fn default_universal_deletes_bytes_on_remove() {
        let data_dir = temp_dir("del-data");
        let sync_dir = temp_dir("del-sync");
        let mut manager = universal_manager_with(&data_dir, &sync_dir, false).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"disposable".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        assert!(destination.exists());

        manager
            .handle_command(SyncDirectoryCommand::RemoveFile {
                file_id,
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert!(
            !destination.exists(),
            "default Universal must delete the physical copy on remove"
        );
        assert!(
            manager.sync_directories[0]
                .database
                .get_file(file_id)
                .is_err(),
            "default Universal must drop the per-directory DB row"
        );
    }
}
