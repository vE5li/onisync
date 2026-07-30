//! Content-addressed chunk transfer over peer links.
//!
//! There is one byte-movement mechanism. A chunk is treated as **pure content,
//! not a message**: its identity *is* `(file_id, content_hash, offset)`, and
//! because chunking is deterministic (offset a multiple of [`CHUNK_SIZE`], the
//! canonical length `min(CHUNK_SIZE, size - offset)` derived from the version's
//! authoritative size), that key denotes one exact, bit-identical byte range on
//! every peer whose copy hashes to `content_hash`. Nothing else correlates a
//! request to its reply — no transfer session, no per-request cookie, no
//! open/close handshake.
//!
//! This module provides:
//!
//! - [`receive`] — the single receiver driver. Given a `(file_id,
//!   content_hash, expected_size)` and a way to send [`Sync::ChunkRequest`]s and
//!   await [`ChunkReply`]s, it keeps a window of requests in flight, streams
//!   replies into a temp file with incremental BLAKE3, and verifies the whole
//!   file at the end. Where each chunk's *first* request goes is a routing
//!   policy supplied by the caller, not part of the driver.
//! - [`answer_chunk_request`] — the stateless holder side. It answers a single
//!   `ChunkRequest` from a [`ChunkSource`] after verifying (via a
//!   [`VerifiedHashCache`]) that the source's content matches `content_hash`.
//! - [`ChunkSource`] / [`ProviderSource`] — where servable bytes live.
//!
//! Integrity is **end-to-end**: only the origin receiver verifies the
//! accumulated hash against `content_hash`. Relays (see [`fetch`]) hold no
//! bytes and verify nothing.
//!
//! [`Sync::ChunkRequest`]: onisync_core::state::Sync::ChunkRequest
//! [`fetch`]: crate::fetch

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::file_bytes::FileBytes;

/// A sink for byte-transfer progress.
///
/// The receiver reports the running total of bytes written (and the known
/// total) through this so a caller — the peer session — can surface a live
/// [`Operation`](crate::operations) with a progress bar. It is a thin boxed
/// callback rather than a hard dependency on the operations module, so the
/// driver stays unit-testable in isolation. Reporting is best-effort and never
/// affects transfer correctness.
pub type ProgressSink = Box<dyn Fn(u64, Option<u64>) + Send + Sync>;

/// Bytes per chunk. This is part of the **wire contract**: it defines chunk
/// boundaries and the canonical chunk length every node derives from the
/// version's size, so changing it is a protocol-breaking change and must go
/// with a `PROTOCOL_VERSION` bump.
pub const CHUNK_SIZE: usize = 64 * 1024;

/// How many chunk requests the receiver keeps in flight at once. A larger
/// window hides per-chunk round-trip latency. Kept small so a relayed transfer
/// bounds in-flight bytes per hop to `WINDOW * CHUNK_SIZE`.
pub const WINDOW: u64 = 8;

/// A reply to one of the receiver's outstanding `ChunkRequest`s, demuxed by the
/// peer session for a specific in-flight receive. The `file_id` / `content_hash`
/// are fixed for the whole receive, so only the `offset` (and, for `Data`, the
/// bytes) are carried here — the reply is matched to a pending request by
/// `offset`.
#[derive(Debug)]
pub enum ChunkReply {
    /// The canonical bytes at `offset`.
    Data { offset: u64, bytes: Vec<u8> },
    /// This direction cannot serve `offset` (missing content or the file
    /// changed). A miss from *all* directions fails the receive.
    Miss { offset: u64 },
}

/// A `ChunkRequest` the receiver wants sent. The peer session routes it toward
/// a holder (per its routing policy) and wraps it as
/// [`Sync::ChunkRequest`](onisync_core::state::Sync::ChunkRequest).
#[derive(Debug)]
pub struct ChunkRequest {
    pub offset: u64,
}

/// Why a receive failed.
#[derive(Debug)]
pub enum TransferError {
    /// A chunk was missed from every reachable direction (the version is
    /// superseded, or the only holder is unreachable). No retry helps.
    ChunkUnavailable { offset: u64 },
    /// A *connected* peer accepted the request but went silent for
    /// [`HOP_TIMEOUT`](crate::fetch::HOP_TIMEOUT): no chunk was written within
    /// the per-chunk liveness window. The one guard against hanging forever.
    LivenessTimeout,
    /// The reassembled content did not hash to the expected value.
    HashMismatch { expected: String, actual: String },
    /// A local I/O error writing the temp file.
    Io(std::io::Error),
    /// The inbound reply channel closed before the receive completed (the link
    /// dropped).
    ChannelClosed,
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferError::ChunkUnavailable { offset } => {
                write!(formatter, "chunk at offset {offset} unavailable from any peer")
            }
            TransferError::LivenessTimeout => {
                write!(formatter, "transfer stalled (liveness timeout)")
            }
            TransferError::HashMismatch { expected, actual } => write!(
                formatter,
                "content hash mismatch: expected {expected}, got {actual}"
            ),
            TransferError::Io(error) => write!(formatter, "transfer I/O error: {error}"),
            TransferError::ChannelClosed => write!(formatter, "transfer channel closed early"),
        }
    }
}

impl std::error::Error for TransferError {}

/// The outcome of a receive, delivered once it finishes.
pub enum ReceiveOutcome {
    /// The bytes arrived and hashed correctly; here is the temp file.
    Complete(FileBytes),
    /// The receive failed (unavailable / liveness timeout / hash mismatch /
    /// I/O / link drop).
    Failed(TransferError),
}

/// Drive a **content-addressed receive** to completion.
///
/// - Keeps up to [`WINDOW`] `ChunkRequest`s in flight, each emitted on
///   `requests` (the peer session routes them toward a holder).
/// - Streams `ChunkReply::Data` into a temp file at `temp_path` in offset
///   order (buffering out-of-order replies), hashing incrementally.
/// - Terminates and verifies when `expected_size` bytes have been written; a
///   zero-length file is one request at `offset = 0` returning empty bytes.
/// - A `ChunkReply::Miss` for any offset, a closed reply channel, or the
///   per-chunk liveness timeout fails the receive immediately (no retry, no
///   re-flood; recovery is external).
///
/// On success the temp file *is* the content, returned as a
/// [`FileBytes::FileToMove`] so the caller can rename it into place. On any
/// error the temp file is removed.
pub async fn receive(
    content_hash: String,
    expected_size: u64,
    temp_path: PathBuf,
    requests: UnboundedSender<ChunkRequest>,
    mut replies: UnboundedReceiver<ChunkReply>,
    progress: Option<ProgressSink>,
) -> Result<FileBytes, TransferError> {
    let result = receive_inner(
        &content_hash,
        expected_size,
        &temp_path,
        &requests,
        &mut replies,
        progress.as_ref(),
    )
    .await;

    if result.is_err() {
        let _ = tokio::fs::remove_file(&temp_path).await;
    }

    result.map(|()| FileBytes::FileToMove(temp_path))
}

async fn receive_inner(
    content_hash: &str,
    expected_size: u64,
    temp_path: &Path,
    requests: &UnboundedSender<ChunkRequest>,
    replies: &mut UnboundedReceiver<ChunkReply>,
    progress: Option<&ProgressSink>,
) -> Result<(), TransferError> {
    let mut file = tokio::fs::File::create(temp_path)
        .await
        .map_err(TransferError::Io)?;
    let mut hasher = blake3::Hasher::new();

    // A zero-length file still needs exactly one request (offset 0) to receive
    // the empty chunk; otherwise never request an offset at or beyond the
    // authoritative size.
    let request_ceiling = expected_size.max(1);
    let may_request = |offset: u64| offset < request_ceiling;

    let mut next_request_offset: u64 = 0;
    let mut in_flight: u64 = 0;
    let mut write_offset: u64 = 0;
    let mut pending: std::collections::BTreeMap<u64, Vec<u8>> = Default::default();

    // Prime the window, capped so we never request past EOF.
    while in_flight < WINDOW && may_request(next_request_offset) {
        requests
            .send(ChunkRequest {
                offset: next_request_offset,
            })
            .map_err(|_| TransferError::ChannelClosed)?;
        next_request_offset += CHUNK_SIZE as u64;
        in_flight += 1;
    }

    loop {
        // Per-chunk liveness timeout: reset on each successful write (below).
        // A connected-but-silent peer trips this rather than hanging forever.
        let message = match tokio::time::timeout(crate::fetch::HOP_TIMEOUT, replies.recv()).await {
            Ok(Some(message)) => message,
            Ok(None) => return Err(TransferError::ChannelClosed),
            Err(_) => return Err(TransferError::LivenessTimeout),
        };

        match message {
            ChunkReply::Data { offset, bytes } => {
                in_flight = in_flight.saturating_sub(1);
                // Duplicate for an already-written offset: drop it (races are
                // free — bytes for a key are bit-identical).
                if offset < write_offset {
                    continue;
                }
                pending.entry(offset).or_insert(bytes);

                // Flush any contiguous chunks starting at write_offset.
                let mut wrote_any = false;
                while let Some(chunk) = pending.remove(&write_offset) {
                    hasher.update(&chunk);
                    file.write_all(&chunk).await.map_err(TransferError::Io)?;
                    write_offset += chunk.len() as u64;
                    wrote_any = true;
                    if let Some(report) = progress {
                        report(write_offset, Some(expected_size));
                    }
                }
                let _ = wrote_any;

                // Completion: we have written the whole file.
                if write_offset >= expected_size {
                    file.flush().await.map_err(TransferError::Io)?;
                    let actual = hasher.finalize().to_hex().to_string();
                    if actual == content_hash {
                        return Ok(());
                    }
                    return Err(TransferError::HashMismatch {
                        expected: content_hash.to_owned(),
                        actual,
                    });
                }

                // Refill the window, capped so we never request past EOF.
                while in_flight < WINDOW && may_request(next_request_offset) {
                    requests
                        .send(ChunkRequest {
                            offset: next_request_offset,
                        })
                        .map_err(|_| TransferError::ChannelClosed)?;
                    next_request_offset += CHUNK_SIZE as u64;
                    in_flight += 1;
                }
            }
            ChunkReply::Miss { offset } => {
                // A miss for an already-written offset (a late duplicate) is
                // harmless: ignore it. Otherwise the chunk is unavailable from
                // every direction the peer session tried, so the receive fails.
                if offset < write_offset {
                    continue;
                }
                return Err(TransferError::ChunkUnavailable { offset });
            }
        }
    }
}

/// A source of file bytes a holder reads chunks from.
///
/// Dyn-compatible (boxed future) so the provider registry can hold an
/// `Arc<dyn ChunkSource>` — a local on-disk [`FileBytes`] or a remote provider
/// such as the CLI over the control socket — behind one type.
pub type ChunkFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(Vec<u8>, bool), String>> + Send + 'a>,
>;

pub trait ChunkSource: Send + Sync {
    /// Read up to `max_len` bytes at `offset`, returning the bytes and whether
    /// this chunk reaches the end of the content.
    fn read_chunk_at(&self, offset: u64, max_len: usize) -> ChunkFuture<'_>;
}

impl ChunkSource for std::sync::Arc<dyn ChunkSource> {
    fn read_chunk_at(&self, offset: u64, max_len: usize) -> ChunkFuture<'_> {
        (**self).read_chunk_at(offset, max_len)
    }
}

impl ChunkSource for FileBytes {
    fn read_chunk_at(&self, offset: u64, max_len: usize) -> ChunkFuture<'_> {
        Box::pin(async move {
            FileBytes::read_chunk_at(self, offset, max_len)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

/// The one-shot a provider chunk reply is delivered on: `(bytes, is_last)` or
/// an error string.
pub type ProviderChunkReply = tokio::sync::oneshot::Sender<Result<(Vec<u8>, bool), String>>;

/// One chunk request routed to a remote provider (the CLI over the control
/// socket): the requested `offset` and the one-shot to deliver the reply on.
pub type ProviderChunkRequest = (u64, ProviderChunkReply);

/// A [`ChunkSource`] backed by a remote provider reached over a request channel
/// (e.g. the CLI over the control connection). Each `read_chunk_at` sends a
/// [`ProviderChunkRequest`] and awaits the reply, so the whole file is never
/// buffered daemon-side.
///
/// When it observes the final chunk (`last == true`) it fires `on_complete`
/// once, so the daemon can signal the client to release the file after the
/// bytes have been served.
#[derive(Clone)]
pub struct ProviderSource {
    requests: UnboundedSender<ProviderChunkRequest>,
    on_complete: UnboundedSender<()>,
}

impl ProviderSource {
    pub fn new(
        requests: UnboundedSender<ProviderChunkRequest>,
        on_complete: UnboundedSender<()>,
    ) -> Self {
        Self {
            requests,
            on_complete,
        }
    }
}

impl ChunkSource for ProviderSource {
    fn read_chunk_at(&self, offset: u64, _max_len: usize) -> ChunkFuture<'_> {
        Box::pin(async move {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            self.requests
                .send((offset, reply_tx))
                .map_err(|_| "provider gone".to_owned())?;
            let (bytes, last) = reply_rx
                .await
                .map_err(|_| "provider dropped before replying".to_owned())??;
            if last {
                let _ = self.on_complete.send(());
            }
            Ok((bytes, last))
        })
    }
}

/// A per-holder cache of verified content hashes, keyed by the on-disk path
/// backing a file: `path -> (mtime, size, hash)`. Lets a holder answer repeated
/// `ChunkRequest`s for the same file without re-hashing it every time, while
/// still invalidating on any mtime/size change (a file edited mid-serve stops
/// matching, so the holder answers `ChunkMiss`).
///
/// Cheap to clone (an `Arc<Mutex<..>>`); every peer session shares one.
#[derive(Clone, Default)]
pub struct VerifiedHashCache {
    inner: std::sync::Arc<Mutex<HashMap<PathBuf, VerifiedEntry>>>,
}

#[derive(Clone)]
struct VerifiedEntry {
    mtime: Option<SystemTime>,
    size: u64,
    hash: String,
}

impl VerifiedHashCache {
    pub fn new() -> Self {
        Self::default()
    }

    fn get(&self, path: &Path, mtime: Option<SystemTime>, size: u64) -> Option<String> {
        let map = self.inner.lock().unwrap();
        map.get(path).and_then(|entry| {
            (entry.mtime == mtime && entry.size == size).then(|| entry.hash.clone())
        })
    }

    fn put(&self, path: PathBuf, mtime: Option<SystemTime>, size: u64, hash: String) {
        self.inner
            .lock()
            .unwrap()
            .insert(path, VerifiedEntry { mtime, size, hash });
    }
}

/// The result of answering a `ChunkRequest`: either the canonical bytes at the
/// requested offset, or a miss (this holder cannot serve the key).
pub enum ChunkAnswer {
    Data(Vec<u8>),
    Miss,
}

/// Answer a single content-addressed `ChunkRequest` for `content_hash` from
/// `source`, serving the canonical chunk at `offset`.
///
/// `pre_verified` says the caller has already established that `source`'s bytes
/// hash to `content_hash` — true for a provider (looked up by its
/// `(file_id, content_hash)` registration key) and for a sync-directory file
/// whose `ReadFile` already returned a matching hash. When `pre_verified`, no
/// hashing is done here.
///
/// **Providers must be `pre_verified`.** Re-hashing a [`ProviderSource`] reads
/// the whole file *through the provider* and, on reaching the end, fires the
/// provider's `on_complete` — which the daemon interprets as "the transfer is
/// done, release the file". Hashing it here would therefore release the file
/// after the first chunk and make every later chunk unavailable. Providers are
/// trusted by their registration key instead.
///
/// When `!pre_verified` (e.g. a sync-directory file we want to (re)confirm),
/// verification is cached by `cache` keyed on the source's on-disk path +
/// mtime/size, so it is paid once per unchanged file; a source with no on-disk
/// path is hashed each call.
///
/// `offset` MUST be `CHUNK_SIZE`-aligned; a misaligned request is a
/// [`ChunkAnswer::Miss`]. An out-of-range offset yields an empty
/// [`ChunkAnswer::Data`] for a matching source (harmless — the receiver
/// terminates on size), consistent with `read_chunk_at`.
pub async fn answer_chunk_request<S: ChunkSource>(
    source: &S,
    source_path: Option<&Path>,
    cache: &VerifiedHashCache,
    content_hash: &str,
    offset: u64,
    pre_verified: bool,
) -> ChunkAnswer {
    // Malformed request: offsets must land on chunk boundaries.
    if !offset.is_multiple_of(CHUNK_SIZE as u64) {
        return ChunkAnswer::Miss;
    }

    if !pre_verified {
        // Verify the source matches `content_hash`, using the cache when
        // possible. Never applied to a provider (see the doc note above).
        let verified = match source_path {
            Some(path) => {
                let (mtime, size) = match tokio::fs::metadata(path).await {
                    Ok(metadata) => (metadata.modified().ok(), metadata.len()),
                    Err(_) => return ChunkAnswer::Miss,
                };
                match cache.get(path, mtime, size) {
                    Some(cached) => cached == content_hash,
                    None => {
                        let hash = match hash_source(source).await {
                            Some(hash) => hash,
                            None => return ChunkAnswer::Miss,
                        };
                        cache.put(path.to_path_buf(), mtime, size, hash.clone());
                        hash == content_hash
                    }
                }
            }
            None => match hash_source(source).await {
                Some(hash) => hash == content_hash,
                None => return ChunkAnswer::Miss,
            },
        };

        if !verified {
            return ChunkAnswer::Miss;
        }
    }

    match source.read_chunk_at(offset, CHUNK_SIZE).await {
        Ok((bytes, _last)) => ChunkAnswer::Data(bytes),
        Err(_) => ChunkAnswer::Miss,
    }
}

/// Stream-hash a [`ChunkSource`] by reading it in `CHUNK_SIZE` windows until the
/// end. Returns `None` on a read error.
async fn hash_source<S: ChunkSource>(source: &S) -> Option<String> {
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0u64;
    loop {
        let (bytes, last) = source.read_chunk_at(offset, CHUNK_SIZE).await.ok()?;
        hasher.update(&bytes);
        offset += bytes.len() as u64;
        if last || bytes.is_empty() {
            break;
        }
    }
    Some(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn temp_path(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "onisync-transfer-test-{}-{}-{}",
            label,
            std::process::id(),
            unique
        ))
    }

    /// A stateless chunk-answering stub: serves canonical chunks of `bytes`.
    /// Drives a [`receive`] against it, answering each request from `bytes`.
    async fn drive_receive_from(bytes: Vec<u8>) -> Result<Vec<u8>, TransferError> {
        let content_hash = blake3::hash(&bytes).to_hex().to_string();
        let size = bytes.len() as u64;
        let dest = temp_path("dest");

        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

        let serve_bytes = bytes.clone();
        let server = tokio::spawn(async move {
            while let Some(ChunkRequest { offset }) = req_rx.recv().await {
                let start = (offset as usize).min(serve_bytes.len());
                let end = (start + CHUNK_SIZE).min(serve_bytes.len());
                let chunk = serve_bytes[start..end].to_vec();
                if reply_tx
                    .send(ChunkReply::Data { offset, bytes: chunk })
                    .is_err()
                {
                    break;
                }
            }
        });

        let received = receive(content_hash, size, dest.clone(), req_tx, reply_rx, None).await;
        let _ = server.await;

        let result = received.map(|file_bytes| {
            let path = file_bytes.path().unwrap().to_path_buf();
            std::fs::read(&path).unwrap()
        });
        let _ = std::fs::remove_file(&dest);
        result
    }

    #[tokio::test]
    async fn receive_small() {
        let bytes = b"hello transfer".to_vec();
        assert_eq!(drive_receive_from(bytes.clone()).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn receive_empty() {
        assert!(drive_receive_from(Vec::new()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn receive_multi_chunk() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE * 5 + 123)).map(|i| i as u8).collect();
        assert_eq!(drive_receive_from(bytes.clone()).await.unwrap(), bytes);
    }

    /// A duplicate `Data` for an already-written offset is ignored, and the
    /// receive still completes with the correct hash.
    #[tokio::test]
    async fn duplicate_data_ignored() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE + 10)).map(|i| i as u8).collect();
        let content_hash = blake3::hash(&bytes).to_hex().to_string();
        let size = bytes.len() as u64;
        let dest = temp_path("dup");

        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

        let serve_bytes = bytes.clone();
        tokio::spawn(async move {
            while let Some(ChunkRequest { offset }) = req_rx.recv().await {
                let start = (offset as usize).min(serve_bytes.len());
                let end = (start + CHUNK_SIZE).min(serve_bytes.len());
                let chunk = serve_bytes[start..end].to_vec();
                // Answer twice for offset 0 to exercise the dedup path.
                let _ = reply_tx.send(ChunkReply::Data {
                    offset,
                    bytes: chunk.clone(),
                });
                if offset == 0 {
                    let _ = reply_tx.send(ChunkReply::Data { offset, bytes: chunk });
                }
            }
        });

        let received = receive(content_hash, size, dest.clone(), req_tx, reply_rx, None)
            .await
            .map(|fb| std::fs::read(fb.path().unwrap()).unwrap());
        let _ = std::fs::remove_file(&dest);
        assert_eq!(received.unwrap(), bytes);
    }

    /// A total miss mid-stream fails the receive immediately (no retry).
    #[tokio::test]
    async fn total_miss_fails() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE * 2)).map(|i| i as u8).collect();
        let content_hash = blake3::hash(&bytes).to_hex().to_string();
        let size = bytes.len() as u64;
        let dest = temp_path("miss");

        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

        let serve_bytes = bytes.clone();
        tokio::spawn(async move {
            while let Some(ChunkRequest { offset }) = req_rx.recv().await {
                if offset == 0 {
                    let end = CHUNK_SIZE.min(serve_bytes.len());
                    let _ = reply_tx.send(ChunkReply::Data {
                        offset,
                        bytes: serve_bytes[..end].to_vec(),
                    });
                } else {
                    let _ = reply_tx.send(ChunkReply::Miss { offset });
                }
            }
        });

        let received = receive(content_hash, size, dest.clone(), req_tx, reply_rx, None).await;
        assert!(matches!(
            received,
            Err(TransferError::ChunkUnavailable { .. })
        ));
        assert!(!dest.exists());
    }

    /// A second holder answering a chunk the first missed lets the receive
    /// complete — multi-source on the first request, not a re-flood. Here the
    /// stub answers every offset (simulating the peer session having routed the
    /// missed offset elsewhere and delivered its `Data`).
    #[tokio::test]
    async fn hash_mismatch_rejected() {
        let bytes = b"real bytes".to_vec();
        let wrong_hash = blake3::hash(b"different").to_hex().to_string();
        let size = bytes.len() as u64;
        let dest = temp_path("mismatch");

        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

        let serve_bytes = bytes.clone();
        tokio::spawn(async move {
            while let Some(ChunkRequest { offset }) = req_rx.recv().await {
                let start = (offset as usize).min(serve_bytes.len());
                let end = (start + CHUNK_SIZE).min(serve_bytes.len());
                let _ = reply_tx.send(ChunkReply::Data {
                    offset,
                    bytes: serve_bytes[start..end].to_vec(),
                });
            }
        });

        let received = receive(wrong_hash, size, dest.clone(), req_tx, reply_rx, None).await;
        assert!(matches!(received, Err(TransferError::HashMismatch { .. })));
        assert!(!dest.exists());
    }

    /// A per-chunk no-progress stall trips the liveness timeout and fails: a
    /// *connected* peer accepted the request but never answers, and both the
    /// request and reply channels stay open (only the liveness guard can fire).
    #[tokio::test(start_paused = true)]
    async fn liveness_timeout_fails() {
        let size = (CHUNK_SIZE * 2) as u64;
        let content_hash = blake3::hash(&vec![0u8; size as usize]).to_hex().to_string();
        let dest = temp_path("stall");

        let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();
        // Keep both channels open but never answer, so neither a closed channel
        // nor a miss can occur — only the liveness timeout.
        let _held_req = req_rx;
        let _held_reply = reply_tx;

        let received = receive(content_hash, size, dest.clone(), req_tx, reply_rx, None).await;
        assert!(matches!(received, Err(TransferError::LivenessTimeout)));
        assert!(!dest.exists());
    }

    /// The serve side verifies against `content_hash` and serves the canonical
    /// chunk; a misaligned offset is a miss; a wrong hash is a miss.
    #[tokio::test]
    async fn answer_serves_and_verifies() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE + 5)).map(|i| i as u8).collect();
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let source = FileBytes::InMemory(bytes.clone());
        let cache = VerifiedHashCache::new();

        // Aligned, correct hash: serves the first chunk.
        match answer_chunk_request(&source, None, &cache, &hash, 0, false).await {
            ChunkAnswer::Data(chunk) => assert_eq!(chunk, bytes[..CHUNK_SIZE]),
            ChunkAnswer::Miss => panic!("expected data"),
        }
        // Misaligned offset: miss (even when pre_verified).
        assert!(matches!(
            answer_chunk_request(&source, None, &cache, &hash, 1, true).await,
            ChunkAnswer::Miss
        ));
        // Wrong hash: miss.
        let wrong = blake3::hash(b"nope").to_hex().to_string();
        assert!(matches!(
            answer_chunk_request(&source, None, &cache, &wrong, 0, false).await,
            ChunkAnswer::Miss
        ));
    }

    /// A pre-verified source is served without any hashing — the regression
    /// guard for the CLI-upload bug: re-hashing a `ProviderSource` streams the
    /// whole file and fires its `on_complete` at EOF, which released the file
    /// after the first chunk and made later chunks unavailable. With
    /// `pre_verified`, `on_complete` never fires from serving, and each chunk is
    /// served independently.
    #[tokio::test]
    async fn provider_pre_verified_serves_all_chunks_without_completing() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE * 3 + 9)).map(|i| i as u8).collect();
        let hash = blake3::hash(&bytes).to_hex().to_string();

        // Wire a fake provider client that answers chunk requests from `bytes`.
        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ProviderChunkRequest>();
        let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let provider = ProviderSource::new(req_tx, done_tx);

        let serve_bytes = bytes.clone();
        tokio::spawn(async move {
            while let Some((offset, reply)) = req_rx.recv().await {
                let start = (offset as usize).min(serve_bytes.len());
                let end = (start + CHUNK_SIZE).min(serve_bytes.len());
                let last = end >= serve_bytes.len();
                let _ = reply.send(Ok((serve_bytes[start..end].to_vec(), last)));
            }
        });

        let cache = VerifiedHashCache::new();
        // Serve every chunk pre-verified (as the daemon does for a registered
        // provider); each must return the right bytes.
        let mut offset = 0u64;
        while offset < bytes.len() as u64 {
            match answer_chunk_request(&provider, None, &cache, &hash, offset, true).await {
                ChunkAnswer::Data(chunk) => {
                    let start = offset as usize;
                    let end = (start + CHUNK_SIZE).min(bytes.len());
                    assert_eq!(chunk, bytes[start..end], "chunk at {offset} mismatched");
                }
                ChunkAnswer::Miss => panic!("chunk at {offset} unexpectedly missed"),
            }
            offset += CHUNK_SIZE as u64;
        }

        // `on_complete` fires exactly once — when the *final* chunk is served
        // (its provider reply carried `last = true`) — not during any earlier
        // verification. Crucially it did not fire before the last chunk, so no
        // chunk was ever unavailable.
        assert!(done_rx.try_recv().is_ok(), "expected one on_complete at EOF");
        assert!(done_rx.try_recv().is_err(), "on_complete must fire only once");
    }

    /// The verified-hash cache invalidates on mtime/size change: a file edited
    /// after being cached stops matching its old hash.
    #[tokio::test]
    async fn verified_cache_invalidates_on_change() {
        let dir = std::env::temp_dir().join(format!(
            "onisync-cache-test-{}-{}",
            std::process::id(),
            temp_path("x").file_name().unwrap().to_string_lossy()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.bin");
        std::fs::write(&path, b"first").unwrap();

        let cache = VerifiedHashCache::new();
        let first_hash = blake3::hash(b"first").to_hex().to_string();
        let source = FileBytes::FileToCopy(path.clone());

        // First serve populates the cache.
        assert!(matches!(
            answer_chunk_request(&source, Some(&path), &cache, &first_hash, 0, false).await,
            ChunkAnswer::Data(_)
        ));

        // Edit the file: the old hash must no longer verify (cache invalidated
        // by mtime/size), and the new hash must serve.
        // Sleep briefly so mtime advances on coarse-grained filesystems, then
        // change the size too (invalidates regardless of mtime resolution).
        std::fs::write(&path, b"second content longer").unwrap();
        assert!(matches!(
            answer_chunk_request(&source, Some(&path), &cache, &first_hash, 0, false).await,
            ChunkAnswer::Miss
        ));
        let second_hash = blake3::hash(b"second content longer").to_hex().to_string();
        assert!(matches!(
            answer_chunk_request(&source, Some(&path), &cache, &second_hash, 0, false).await,
            ChunkAnswer::Data(_)
        ));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
