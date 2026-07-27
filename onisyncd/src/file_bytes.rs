//! Daemon-internal representation of a file's content in transit.
//!
//! Historically file content was carried everywhere as an owned, fully-buffered
//! `Vec<u8>` (see the wire types in `onisync-core`). That does not scale to
//! large files: every ingestion read the whole file into memory before it could
//! be placed into a sync directory.
//!
//! [`FileBytes`] lets an internal producer describe *where* a file's content
//! lives and *how* the consumer is allowed to obtain it, without eagerly
//! reading it into memory:
//!
//! - [`FileBytes::InMemory`] — the bytes are already in memory (e.g. a small
//!   programmatic upload through the API). Nothing to read from disk.
//! - [`FileBytes::FileToCopy`] — the bytes live at a path the producer still
//!   owns. The consumer must *copy* (or stream-read) from it and must never
//!   remove it. Safe to hand to any number of consumers.
//! - [`FileBytes::FileToMove`] — the bytes live at a path whose lifetime the
//!   producer relinquishes to the consumer. The consumer may `rename` it into
//!   place, which is destructive and can therefore be honored by *exactly one*
//!   consumer.
//!
//! This type is deliberately **daemon-only** and is never serialized: the
//! `FileToMove`/`FileToCopy` variants carry machine-local paths that are
//! meaningless to a peer. Content bound for a peer crosses the wire boundary as
//! a `Vec<u8>` via [`FileBytes::into_vec`] (see the peer-forward seam in
//! `handle_changes`); a streaming peer transport is a separate, later step.
//!
//! ## Ownership / cleanup
//!
//! `FileToMove`/`FileToCopy` reference files their *producer* owns; dropping a
//! `FileBytes` without consuming it does **not** delete anything. Most
//! producers point these variants at real files (a watched file, a CLI upload
//! source) rather than throwaway daemon temporaries, so a leak-on-drop is not a
//! concern for them.
//!
//! The on-demand fetch path *does* create daemon-owned temp files (a completed
//! peer transfer, and the staging done by `Api::fetch_file`); those live under
//! the fetch temp dir (`Paths::fetch_temp_dir`) and are cleaned up in bulk on
//! daemon start (`Paths::clean_fetch_temp_dir`) rather than by a per-value drop
//! guard, since their consumer (the co-located CLI / UI) takes over ownership
//! with move semantics.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Size of the buffer used when streaming a file for hashing or copying.
const STREAM_CHUNK: usize = 64 * 1024;

/// Where a file's content lives and how a consumer may obtain it.
///
/// See the module documentation for the semantics of each variant. This type
/// intentionally does not implement `Clone`: cloning a `FileToMove` would
/// silently create a second "mover" for a single source, which can only be
/// honored once. The one place that fans a single ingested file out to several
/// sync directories (`handle_changes`) constructs each command's variant
/// explicitly instead, so it can guarantee at most one `FileToMove`.
#[derive(Debug)]
pub enum FileBytes {
    /// Content already resident in memory.
    InMemory(Vec<u8>),
    /// Content at a producer-owned path; the consumer must copy, never remove.
    FileToCopy(PathBuf),
    /// Content whose lifetime is handed to the consumer; may be renamed into
    /// place. Honorable by exactly one consumer.
    FileToMove(PathBuf),
}

/// An error while reading, hashing, materializing, or buffering [`FileBytes`].
///
/// A cross-filesystem (`EXDEV`) rename during `materialize_to` is *not* an
/// error variant: it is handled transparently by a stream-copy-then-delete
/// fallback (see [`FileBytes::materialize_to`]), so it surfaces only as an `Io`
/// error if that fallback itself fails.
#[derive(Debug)]
pub enum FileBytesError {
    /// An I/O error occurred against `path` (or an in-memory buffer when
    /// `path` is `None`).
    Io {
        path: Option<PathBuf>,
        source: std::io::Error,
    },
}

impl std::fmt::Display for FileBytesError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileBytesError::Io {
                path: Some(path),
                source,
            } => write!(formatter, "I/O error for {}: {source}", path.display()),
            FileBytesError::Io { path: None, source } => {
                write!(formatter, "I/O error: {source}")
            }
        }
    }
}

impl std::error::Error for FileBytesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FileBytesError::Io { source, .. } => Some(source),
        }
    }
}

impl FileBytes {
    /// The on-disk path backing this content, if any. `InMemory` has none.
    pub fn path(&self) -> Option<&Path> {
        match self {
            FileBytes::InMemory(_) => None,
            FileBytes::FileToCopy(path) | FileBytes::FileToMove(path) => Some(path),
        }
    }

    /// Reinterpret a file-backed content as a *move* (the source should not
    /// survive ingestion). A `FileToCopy` becomes a `FileToMove` for the same
    /// path; a `FileToMove` is returned unchanged. `InMemory` has no source to
    /// move and is returned unchanged.
    pub fn into_move(self) -> FileBytes {
        match self {
            FileBytes::FileToCopy(path) | FileBytes::FileToMove(path) => {
                FileBytes::FileToMove(path)
            }
            in_memory @ FileBytes::InMemory(_) => in_memory,
        }
    }

    /// The total byte length of this content.
    pub async fn byte_len(&self) -> Result<u64, FileBytesError> {
        match self {
            FileBytes::InMemory(bytes) => Ok(bytes.len() as u64),
            FileBytes::FileToCopy(path) | FileBytes::FileToMove(path) => {
                let metadata =
                    tokio::fs::metadata(path)
                        .await
                        .map_err(|source| FileBytesError::Io {
                            path: Some(path.clone()),
                            source,
                        })?;
                Ok(metadata.len())
            }
        }
    }

    /// Read up to `max_len` bytes starting at `offset`, returning the bytes and
    /// whether this chunk reaches the end of the content.
    ///
    /// Used by the transfer *sender* to answer a chunk request without holding
    /// the whole file in memory (the file-backed variants seek + read a bounded
    /// window). An `offset` at or past the end yields an empty final chunk.
    pub async fn read_chunk_at(
        &self,
        offset: u64,
        max_len: usize,
    ) -> Result<(Vec<u8>, bool), FileBytesError> {
        match self {
            FileBytes::InMemory(bytes) => {
                let total = bytes.len() as u64;
                let start = offset.min(total) as usize;
                let end = (start + max_len).min(bytes.len());
                let chunk = bytes[start..end].to_vec();
                let last = end as u64 >= total;
                Ok((chunk, last))
            }
            FileBytes::FileToCopy(path) | FileBytes::FileToMove(path) => {
                use tokio::io::AsyncSeekExt;
                let total = self.byte_len().await?;
                let mut file =
                    tokio::fs::File::open(path)
                        .await
                        .map_err(|source| FileBytesError::Io {
                            path: Some(path.clone()),
                            source,
                        })?;
                file.seek(std::io::SeekFrom::Start(offset))
                    .await
                    .map_err(|source| FileBytesError::Io {
                        path: Some(path.clone()),
                        source,
                    })?;
                let mut buffer = vec![0u8; max_len];
                let mut filled = 0;
                // `read` may return fewer bytes than requested; loop until the
                // buffer is full or EOF so each chunk is a predictable size.
                while filled < max_len {
                    let read = file.read(&mut buffer[filled..]).await.map_err(|source| {
                        FileBytesError::Io {
                            path: Some(path.clone()),
                            source,
                        }
                    })?;
                    if read == 0 {
                        break;
                    }
                    filled += read;
                }
                buffer.truncate(filled);
                let last = offset + filled as u64 >= total;
                Ok((buffer, last))
            }
        }
    }

    /// Compute the BLAKE3 hex digest of this content, streaming from disk for
    /// the file-backed variants so the whole file is never held in memory.
    ///
    /// Returns the 64-char lowercase hex string used in `file_versions`.
    pub async fn hash(&self) -> Result<String, FileBytesError> {
        match self {
            FileBytes::InMemory(bytes) => Ok(blake3::hash(bytes).to_hex().to_string()),
            FileBytes::FileToCopy(path) | FileBytes::FileToMove(path) => {
                let mut file =
                    tokio::fs::File::open(path)
                        .await
                        .map_err(|source| FileBytesError::Io {
                            path: Some(path.clone()),
                            source,
                        })?;
                let mut hasher = blake3::Hasher::new();
                let mut buffer = vec![0u8; STREAM_CHUNK];
                loop {
                    let read =
                        file.read(&mut buffer)
                            .await
                            .map_err(|source| FileBytesError::Io {
                                path: Some(path.clone()),
                                source,
                            })?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                }
                Ok(hasher.finalize().to_hex().to_string())
            }
        }
    }

    /// Place this content at `dest`, consuming `self`.
    ///
    /// - `InMemory` writes the buffer to `dest`.
    /// - `FileToCopy` streams the source into `dest`, leaving the source in
    ///   place (the producer still owns it).
    /// - `FileToMove` renames the source to `dest` (single destructive
    ///   consumer). If the rename crosses filesystems (`EXDEV`) — common,
    ///   because sync directories are user-configured paths that may live on a
    ///   different mount than the daemon's temp dir — it falls back to a
    ///   stream-copy followed by removing the source, preserving move
    ///   semantics.
    ///
    /// The parent directory of `dest` must already exist.
    pub async fn materialize_to(self, dest: &Path) -> Result<(), FileBytesError> {
        match self {
            FileBytes::InMemory(bytes) => {
                let mut file =
                    tokio::fs::File::create(dest)
                        .await
                        .map_err(|source| FileBytesError::Io {
                            path: Some(dest.to_path_buf()),
                            source,
                        })?;
                file.write_all(&bytes)
                    .await
                    .map_err(|source| FileBytesError::Io {
                        path: Some(dest.to_path_buf()),
                        source,
                    })?;
                file.flush().await.map_err(|source| FileBytesError::Io {
                    path: Some(dest.to_path_buf()),
                    source,
                })?;
                Ok(())
            }
            FileBytes::FileToCopy(source) => stream_copy(&source, dest).await,
            FileBytes::FileToMove(source) => match tokio::fs::rename(&source, dest).await {
                Ok(()) => Ok(()),
                Err(error) if is_cross_device(&error) => {
                    // Cross-filesystem rename: copy then delete the source so
                    // the move still consumes it. `dest` and the temp source
                    // routinely live on different mounts.
                    stream_copy(&source, dest).await?;
                    if let Err(error) = tokio::fs::remove_file(&source).await {
                        // The bytes are safely at `dest`; a leftover source is a
                        // leak, not a correctness problem. Log and continue.
                        log::warn!(
                            "Cross-device move copied {} -> {} but failed to remove source: {error}",
                            source.display(),
                            dest.display()
                        );
                    }
                    Ok(())
                }
                Err(source_error) => Err(FileBytesError::Io {
                    path: Some(source),
                    source: source_error,
                }),
            },
        }
    }
}

/// Stream-copy `source` into `dest` without buffering the whole file.
async fn stream_copy(source: &Path, dest: &Path) -> Result<(), FileBytesError> {
    let mut reader = tokio::fs::File::open(source)
        .await
        .map_err(|error| FileBytesError::Io {
            path: Some(source.to_path_buf()),
            source: error,
        })?;
    let mut writer = tokio::fs::File::create(dest)
        .await
        .map_err(|error| FileBytesError::Io {
            path: Some(dest.to_path_buf()),
            source: error,
        })?;

    tokio::io::copy(&mut reader, &mut writer)
        .await
        .map_err(|error| FileBytesError::Io {
            path: Some(dest.to_path_buf()),
            source: error,
        })?;
    writer.flush().await.map_err(|error| FileBytesError::Io {
        path: Some(dest.to_path_buf()),
        source: error,
    })?;
    Ok(())
}

/// Whether an I/O error is a cross-filesystem (`EXDEV`) rename failure.
fn is_cross_device(error: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc_exdev())
    }
    #[cfg(not(unix))]
    {
        // On non-unix targets there is no stable EXDEV constant here; fall back
        // to treating it as a generic I/O error (never a cross-device rename).
        let _ = error;
        false
    }
}

/// `EXDEV` errno. Defined inline to avoid pulling in the `libc` crate for a
/// single constant; it is 18 on Linux and the BSDs/macOS.
#[cfg(unix)]
const fn libc_exdev() -> i32 {
    18
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "onisync-filebytes-test-{}-{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    #[tokio::test]
    async fn in_memory_hash_matches_blake3() {
        let bytes = b"hello world".to_vec();
        let file_bytes = FileBytes::InMemory(bytes.clone());
        assert_eq!(
            file_bytes.hash().await.unwrap(),
            blake3::hash(&bytes).to_hex().to_string()
        );
    }

    #[tokio::test]
    async fn file_hash_matches_in_memory_hash() {
        let dir = temp_dir();
        let source = dir.join("source.bin");
        // Larger than STREAM_CHUNK to exercise the read loop.
        let bytes: Vec<u8> = (0..(STREAM_CHUNK * 3 + 7)).map(|i| i as u8).collect();
        std::fs::write(&source, &bytes).unwrap();

        let file_hash = FileBytes::FileToCopy(source).hash().await.unwrap();
        let expected = FileBytes::InMemory(bytes).hash().await.unwrap();
        assert_eq!(file_hash, expected);
    }

    #[tokio::test]
    async fn materialize_in_memory_writes_bytes() {
        let dir = temp_dir();
        let dest = dir.join("dest.bin");
        FileBytes::InMemory(b"payload".to_vec())
            .materialize_to(&dest)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"payload");
    }

    #[tokio::test]
    async fn materialize_copy_preserves_source() {
        let dir = temp_dir();
        let source = dir.join("source.bin");
        let dest = dir.join("dest.bin");
        std::fs::write(&source, b"copy me").unwrap();

        FileBytes::FileToCopy(source.clone())
            .materialize_to(&dest)
            .await
            .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"copy me");
        assert!(source.exists(), "FileToCopy must leave the source in place");
    }

    #[tokio::test]
    async fn materialize_move_consumes_source() {
        let dir = temp_dir();
        let source = dir.join("source.bin");
        let dest = dir.join("dest.bin");
        std::fs::write(&source, b"move me").unwrap();

        FileBytes::FileToMove(source.clone())
            .materialize_to(&dest)
            .await
            .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"move me");
        assert!(!source.exists(), "FileToMove must remove the source");
    }
}
