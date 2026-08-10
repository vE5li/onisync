//! The per-sync-directory index: a two-column `(file_id, physical_path)` map
//! from a file's catalog identity to where its bytes actually sit inside one
//! sync directory.
//!
//! Deliberately tiny and deliberately *not* a `CatalogStore`. It answers only
//! "where is this file on disk here?" and its inverse, which is all the
//! filesystem watcher needs to turn a path event back into a `FileId`. Each
//! sync directory owns its own database file.

use std::path::Path;

use tagsy_core::{FileId, PhysicalPath};
use rusqlite::Connection;

use super::schema;
use super::types::DatabaseError;

#[derive(Debug, Clone)]
pub struct SyncDirectoryFile {
    pub file_id: FileId,
    /// The file's physical path on disk, relative to this sync directory's
    /// root. For a `TagBased` directory this equals the file's logical path;
    /// for a `Universal` directory it is the file's `file_id` (files are stored
    /// under their id on disk). This also serves as the reverse index for
    /// filesystem events (path -> file_id), so it must always reflect the
    /// actual on-disk name. It is NOT the value to advertise to peers or
    /// show to users; for that use `files_v2.logical_path` from the
    /// [`CatalogStore`](super::CatalogStore).
    pub physical_path: PhysicalPath,
}

#[derive(Debug)]
pub struct DirectoryIndex {
    connection: Connection,
}

impl DirectoryIndex {
    pub fn initialize(database_path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let connection =
            Connection::open(database_path).map_err(|_| DatabaseError::UnableToOpenOrCreate)?;

        // Run migrations here.

        schema::create_directory_files_v1(&connection)?;

        Ok(Self { connection })
    }

    /// Add a new file.
    ///
    /// Note: content hashes are stored in `CatalogStore`'s `file_versions_v1`,
    /// not here. After calling this you typically also want to call
    /// `CatalogStore::record_version`.
    pub fn add_file(
        &self,
        file_id: FileId,
        physical_path: &PhysicalPath,
    ) -> Result<(), DatabaseError> {
        self.connection.execute(
            "INSERT INTO files_v1 (id, physical_path) VALUES (?1, ?2)",
            (file_id, physical_path),
        )?;

        Ok(())
    }

    pub fn update_file_physical_path(
        &self,
        file_id: FileId,
        physical_path: &PhysicalPath,
    ) -> Result<(), DatabaseError> {
        self.connection.execute(
            "UPDATE files_v1 SET physical_path = ?2 WHERE id = ?1",
            (file_id, physical_path),
        )?;

        Ok(())
    }

    pub fn remove_file_by_id(&self, file_id: FileId) -> Result<(), DatabaseError> {
        self.connection
            .execute("DELETE FROM files_v1 WHERE id = ?1", [file_id])?;

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
            .prepare("SELECT physical_path FROM files_v1 WHERE id = ?1")?;

        let file = statement
            .query_map([file_id], |row| {
                Ok(SyncDirectoryFile {
                    file_id,
                    physical_path: row.get(0)?,
                })
            })?
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
            .prepare("SELECT 1 FROM files_v1 WHERE physical_path = ?1 AND id != ?2")?;

        let in_use = statement
            .query_map((physical_path, except_file_id), |_row| Ok(()))?
            .next()
            .is_some();

        Ok(in_use)
    }

    pub fn get_file_id(&self, physical_path: &PhysicalPath) -> Result<FileId, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM files_v1 WHERE physical_path = ?1")?;

        let id = statement
            .query_map([physical_path], |row| row.get(0))?
            .map(|preview| preview.unwrap())
            .next()
            .ok_or(DatabaseError::MissingFile)?;

        Ok(id)
    }

    pub fn get_all_files(&self) -> Result<Vec<SyncDirectoryFile>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, physical_path FROM files_v1")?;

        Ok(statement
            .query_map([], |row| {
                Ok(SyncDirectoryFile {
                    file_id: row.get(0)?,
                    physical_path: row.get(1)?,
                })
            })?
            .map(|file| file.unwrap())
            .collect())
    }

    pub fn get_all_files_at(
        &self,
        physical_path: &PhysicalPath,
    ) -> Result<Vec<SyncDirectoryFile>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT id, physical_path FROM files_v1 WHERE physical_path LIKE ?1")?;

        let matcher = format!("{}%", physical_path.as_str());

        Ok(statement
            .query_map([matcher], |row| {
                Ok(SyncDirectoryFile {
                    file_id: row.get(0)?,
                    physical_path: row.get(1)?,
                })
            })?
            .map(|file| file.unwrap())
            .collect())
    }
}
