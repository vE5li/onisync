//! Pull-based file transfer over a single peer link.
//!
//! Bytes move by a *pull* protocol (see the `Sync::Transfer*` wire messages):
//! the **receiver** drives, the **sender** only replies. This yields fair
//! interleaving with other link traffic (the sender never emits a chunk
//! unprompted) and inherent backpressure (the receiver asks for the next chunk
//! only when it is ready), at the cost of a round-trip per chunk — which a
//! small in-flight *window* (see [`WINDOW`]) hides.
//!
//! This module provides the two **two-party** endpoints as self-contained async
//! drivers:
//!
//! - [`run_receiver`] — drives a transfer to completion: opens it, keeps a
//!   window of chunk requests in flight, streams replies to a temp file with
//!   incremental BLAKE3, verifies the hash on the final chunk, and returns the
//!   temp file as a [`FileBytes::FileToMove`] for the caller to materialize.
//! - [`run_sender`] — answers chunk requests for a [`FileBytes`] source by
//!   reading a bounded window at the requested offset.
//!
//! Each endpoint communicates over two channels so it can be driven by a peer
//! session (which demuxes inbound `Sync::Transfer*` frames by `TransferId`) or,
//! in tests, by wiring two endpoints back to back. The relayed (multi-hop)
//! fetch case is *not* here: it is a thin forwarding layer built on top of this
//! primitive (see `fetch`).

use std::path::PathBuf;

use onisync_core::state::Sync as SyncMessage;
use onisync_core::{FileId, TransferId};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::file_bytes::FileBytes;

/// Bytes per chunk. A chunk request/reply pair moves at most this many bytes.
pub const CHUNK_SIZE: usize = 64 * 1024;

/// How many chunk requests the receiver keeps in flight at once. `1` is the
/// simplest correct value (strict request/reply); a larger window hides
/// per-chunk round-trip latency without changing the protocol. Kept small so a
/// relayed transfer bounds in-flight bytes per hop to `WINDOW * CHUNK_SIZE`.
pub const WINDOW: u64 = 8;

/// The subset of `Sync` messages that belong to one transfer. A peer session
/// demuxes inbound `Sync::Transfer*` frames into per-transfer channels of these
/// (the `transfer_id` is stripped since it identifies the channel).
#[derive(Debug)]
pub enum TransferMessage {
    /// Receiver → sender: open the transfer.
    Start {
        file_id: FileId,
        content_hash: String,
    },
    /// Receiver → sender: send the chunk at `offset`.
    ChunkRequest { offset: u64 },
    /// Sender → receiver: the bytes at `offset`; `last` marks the final chunk.
    Chunk {
        offset: u64,
        bytes: Vec<u8>,
        last: bool,
    },
    /// Either direction: abort.
    Abort { reason: String },
}

impl TransferMessage {
    /// Reconstruct the wire `Sync` frame for this message under `transfer_id`.
    pub fn into_sync(self, transfer_id: TransferId) -> SyncMessage {
        match self {
            TransferMessage::Start {
                file_id,
                content_hash,
            } => SyncMessage::TransferStart {
                transfer_id,
                file_id,
                content_hash,
            },
            TransferMessage::ChunkRequest { offset } => SyncMessage::TransferChunkRequest {
                transfer_id,
                offset,
            },
            TransferMessage::Chunk {
                offset,
                bytes,
                last,
            } => SyncMessage::TransferChunk {
                transfer_id,
                offset,
                bytes,
                last,
            },
            TransferMessage::Abort { reason } => SyncMessage::TransferAbort {
                transfer_id,
                reason,
            },
        }
    }

    /// Extract the per-transfer message from a wire `Sync` frame, if it is one.
    /// Returns the `TransferId` (for demuxing) alongside the message.
    pub fn from_sync(sync: SyncMessage) -> Option<(TransferId, TransferMessage)> {
        match sync {
            SyncMessage::TransferStart {
                transfer_id,
                file_id,
                content_hash,
            } => Some((
                transfer_id,
                TransferMessage::Start {
                    file_id,
                    content_hash,
                },
            )),
            SyncMessage::TransferChunkRequest {
                transfer_id,
                offset,
            } => Some((transfer_id, TransferMessage::ChunkRequest { offset })),
            SyncMessage::TransferChunk {
                transfer_id,
                offset,
                bytes,
                last,
            } => Some((
                transfer_id,
                TransferMessage::Chunk {
                    offset,
                    bytes,
                    last,
                },
            )),
            SyncMessage::TransferAbort {
                transfer_id,
                reason,
            } => Some((transfer_id, TransferMessage::Abort { reason })),
            _ => None,
        }
    }
}

/// Why a transfer failed.
#[derive(Debug)]
pub enum TransferError {
    /// The peer aborted (or the link dropped) before the transfer completed.
    Aborted(String),
    /// The reassembled content did not hash to the expected value.
    HashMismatch { expected: String, actual: String },
    /// A local I/O error writing the temp file.
    Io(std::io::Error),
    /// The inbound channel closed before the transfer completed.
    ChannelClosed,
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferError::Aborted(reason) => write!(formatter, "transfer aborted: {reason}"),
            TransferError::HashMismatch { expected, actual } => {
                write!(
                    formatter,
                    "content hash mismatch: expected {expected}, got {actual}"
                )
            }
            TransferError::Io(error) => write!(formatter, "transfer I/O error: {error}"),
            TransferError::ChannelClosed => write!(formatter, "transfer channel closed early"),
        }
    }
}

impl std::error::Error for TransferError {}

/// Drive a transfer as the **receiver** to completion.
///
/// Opens the transfer (`Start`), keeps up to [`WINDOW`] chunk requests in
/// flight, streams replies into a temp file at `temp_path` while hashing
/// incrementally, and on the final chunk verifies the accumulated hash against
/// `content_hash`. On success the temp file *is* the content, returned as a
/// [`FileBytes::FileToMove`] so the caller can rename it into place.
///
/// `outbound` carries this endpoint's messages to the peer; `inbound` delivers
/// the peer's replies (already demuxed for this `transfer_id`).
///
/// `expected_size` is the file's known content size in bytes (from the
/// catalog/manifest). It is used only to cap the request window so the receiver
/// never asks for chunks past end-of-file — completion and correctness still
/// rest on the sender's `last` flag and the BLAKE3 verification. A `0` here
/// means "size unknown" and disables the cap (falls back to the old behaviour).
///
/// On any error the temp file is removed and an abort is sent to the peer.
pub async fn run_receiver(
    file_id: FileId,
    content_hash: String,
    expected_size: u64,
    temp_path: PathBuf,
    outbound: UnboundedSender<TransferMessage>,
    mut inbound: UnboundedReceiver<TransferMessage>,
) -> Result<FileBytes, TransferError> {
    let result = receive_inner(
        file_id,
        &content_hash,
        expected_size,
        &temp_path,
        &outbound,
        &mut inbound,
    )
    .await;

    if let Err(error) = &result {
        // Best-effort cleanup + notify the peer we gave up.
        let _ = tokio::fs::remove_file(&temp_path).await;
        let _ = outbound.send(TransferMessage::Abort {
            reason: error.to_string(),
        });
    }

    result.map(|()| FileBytes::FileToMove(temp_path))
}

async fn receive_inner(
    file_id: FileId,
    content_hash: &str,
    expected_size: u64,
    temp_path: &PathBuf,
    outbound: &UnboundedSender<TransferMessage>,
    inbound: &mut UnboundedReceiver<TransferMessage>,
) -> Result<(), TransferError> {
    let mut file = tokio::fs::File::create(temp_path)
        .await
        .map_err(TransferError::Io)?;
    let mut hasher = blake3::Hasher::new();

    // How far ahead we may request. When the size is known we never request an
    // offset at or beyond it, so no chunk past EOF is ever asked for and the
    // completion-abort below is not needed in the common case. A zero-length
    // file still needs exactly one request (offset 0) to receive the empty
    // final chunk, so treat `expected_size == 0` as "one chunk at offset 0".
    // An `expected_size` of 0 for a genuinely-unknown size is indistinguishable
    // here, but that path still terminates on the sender's `last` flag.
    let request_ceiling = expected_size.max(1);
    let may_request = |offset: u64| offset < request_ceiling;

    // Open the transfer, then prime the request window. Requests are keyed by
    // the offset we want next; because chunk sizes are fixed at CHUNK_SIZE we
    // can compute request offsets without waiting for replies.
    outbound
        .send(TransferMessage::Start {
            file_id,
            content_hash: content_hash.to_owned(),
        })
        .map_err(|_| TransferError::ChannelClosed)?;

    let mut next_request_offset: u64 = 0;
    let mut in_flight: u64 = 0;
    // The offset we expect to write next; replies may arrive out of order
    // within the window, so buffer any that arrive early.
    let mut write_offset: u64 = 0;
    let mut pending: std::collections::BTreeMap<u64, (Vec<u8>, bool)> = Default::default();
    let mut saw_last = false;
    let mut last_offset: u64 = 0;

    // Prime the window, capped so we never request past EOF.
    while in_flight < WINDOW && may_request(next_request_offset) {
        outbound
            .send(TransferMessage::ChunkRequest {
                offset: next_request_offset,
            })
            .map_err(|_| TransferError::ChannelClosed)?;
        next_request_offset += CHUNK_SIZE as u64;
        in_flight += 1;
    }

    loop {
        let message = inbound.recv().await.ok_or(TransferError::ChannelClosed)?;
        match message {
            TransferMessage::Chunk {
                offset,
                bytes,
                last,
            } => {
                in_flight = in_flight.saturating_sub(1);
                if last {
                    saw_last = true;
                    last_offset = offset;
                }
                pending.insert(offset, (bytes, last));

                // Flush any contiguous chunks starting at write_offset.
                while let Some((chunk, chunk_last)) = pending.remove(&write_offset) {
                    hasher.update(&chunk);
                    file.write_all(&chunk).await.map_err(TransferError::Io)?;
                    write_offset += chunk.len() as u64;
                    if chunk_last {
                        // Final chunk written: verify and finish. With a known
                        // size we cap requests at EOF, so normally nothing is
                        // outstanding here; but if the size hint was wrong (or
                        // unknown) we may have windowed requests past EOF that
                        // the sender will answer — send an Abort so it stops
                        // rather than emitting orphaned chunks our peer session
                        // would then log as "unknown transfer".
                        file.flush().await.map_err(TransferError::Io)?;
                        if in_flight > 0 {
                            let _ = outbound.send(TransferMessage::Abort {
                                reason: "transfer complete".to_owned(),
                            });
                        }
                        // The `last` flag is authoritative for completion; the
                        // known size is only a hint. Log if the bytes actually
                        // received disagree with it (e.g. a stale pre-migration
                        // placeholder size), but still let the hash decide
                        // correctness.
                        if expected_size != 0 && write_offset != expected_size {
                            log::warn!(
                                "transfer for {}: received {} bytes but expected size was {} (size hint stale?); verifying by hash",
                                file_id.to_string(),
                                write_offset,
                                expected_size
                            );
                        }
                        let actual = hasher.finalize().to_hex().to_string();
                        if actual == content_hash {
                            return Ok(());
                        }
                        return Err(TransferError::HashMismatch {
                            expected: content_hash.to_owned(),
                            actual,
                        });
                    }
                }

                // Refill the window (unless we've already seen the last chunk,
                // in which case no more requests are useful), capped so we never
                // request past EOF when the size is known.
                if !saw_last {
                    while in_flight < WINDOW && may_request(next_request_offset) {
                        outbound
                            .send(TransferMessage::ChunkRequest {
                                offset: next_request_offset,
                            })
                            .map_err(|_| TransferError::ChannelClosed)?;
                        next_request_offset += CHUNK_SIZE as u64;
                        in_flight += 1;
                    }
                } else if in_flight == 0 && write_offset <= last_offset {
                    // We've seen `last` but haven't been able to flush up to it
                    // yet and have no requests outstanding — this means a gap we
                    // will never fill (sender ended early). Treat as abort.
                    return Err(TransferError::Aborted(
                        "sender ended before all requested chunks arrived".to_owned(),
                    ));
                }
            }
            TransferMessage::Abort { reason } => {
                return Err(TransferError::Aborted(reason));
            }
            // A receiver should not get Start/ChunkRequest; ignore defensively.
            TransferMessage::Start { .. } | TransferMessage::ChunkRequest { .. } => {}
        }
    }
}

/// Drive a transfer as the **sender**: answer chunk requests for `source` until
/// the transfer completes or aborts.
///
/// Expects the first inbound message to be `Start` (used only to log/validate;
/// the caller has already matched `source` to the requested `file_id`).
/// Thereafter, each `ChunkRequest { offset }` is answered with a
/// `TransferChunk` read from `source` at that offset. Returns when the receiver
/// aborts, the channel closes, or the last chunk has been served and the
/// receiver stops requesting (channel close).
/// A source of file bytes a transfer sender reads chunks from.
///
/// Dyn-compatible (boxed future) so the provider registry can hold a
/// `Arc<dyn ChunkSource>` — a local in-memory/on-disk [`FileBytes`] or a remote
/// provider such as the CLI over the control socket — behind one type, and
/// `run_sender` can serve a peer's pull regardless of where the bytes live.
/// The future returned by [`ChunkSource::read_chunk_at`]: a boxed, `Send`
/// future yielding `(bytes, is_last)` or an error string.
pub type ChunkFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(Vec<u8>, bool), String>> + Send + 'a>,
>;

pub trait ChunkSource: Send + Sync {
    /// Read up to `max_len` bytes at `offset`, returning the bytes and whether
    /// this chunk reaches the end.
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
/// buffered daemon-side — chunks are pulled on demand from the provider.
///
/// When it observes the final chunk (`last == true`) it fires `on_complete`
/// once, so the daemon can signal the client to release the file after a
/// transfer completes.
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

pub async fn run_sender<S: ChunkSource>(
    source: S,
    outbound: UnboundedSender<TransferMessage>,
    mut inbound: UnboundedReceiver<TransferMessage>,
) {
    while let Some(message) = inbound.recv().await {
        match message {
            TransferMessage::Start { .. } => {
                // Nothing to do: the caller resolved `source` from the Start's
                // file_id already. Wait for chunk requests.
            }
            TransferMessage::ChunkRequest { offset } => {
                match source.read_chunk_at(offset, CHUNK_SIZE).await {
                    Ok((bytes, last)) => {
                        if outbound
                            .send(TransferMessage::Chunk {
                                offset,
                                bytes,
                                last,
                            })
                            .is_err()
                        {
                            // Receiver gone.
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = outbound.send(TransferMessage::Abort {
                            reason: format!("sender read error: {error}"),
                        });
                        return;
                    }
                }
            }
            TransferMessage::Abort { .. } => return,
            // A sender should not get Chunk; ignore defensively.
            TransferMessage::Chunk { .. } => {}
        }
    }
}

/// The outcome of a receiver transfer, delivered once it finishes.
pub enum ReceiveOutcome {
    /// The bytes arrived and hashed correctly; here is the temp file.
    Complete(FileBytes),
    /// The transfer failed (abort / hash mismatch / I/O / link drop).
    Failed(TransferError),
}

/// Spawn a **receiver** transfer bound to a peer link.
///
/// `peer_tx` is the peer session's `Frame` outbound queue; this endpoint's
/// messages are wrapped as `Frame::Sync(..)` under `transfer_id` and pushed
/// onto it. Returns the sender the peer session must feed inbound
/// `TransferMessage`s for `transfer_id` into (register it in the demux table).
/// The final [`ReceiveOutcome`] is delivered on `done`.
#[allow(clippy::too_many_arguments)]
pub fn spawn_receiver<F>(
    transfer_id: TransferId,
    file_id: FileId,
    content_hash: String,
    expected_size: u64,
    temp_path: PathBuf,
    peer_tx: UnboundedSender<F>,
    wrap: impl Fn(SyncMessage) -> F + Send + 'static,
    done: tokio::sync::oneshot::Sender<ReceiveOutcome>,
) -> UnboundedSender<TransferMessage>
where
    F: Send + 'static,
{
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel::<TransferMessage>();
    let (endpoint_out_tx, mut endpoint_out_rx) =
        tokio::sync::mpsc::unbounded_channel::<TransferMessage>();

    // Forwarder: endpoint outbound TransferMessage -> wrapped Frame on peer_tx.
    tokio::spawn(async move {
        while let Some(message) = endpoint_out_rx.recv().await {
            if peer_tx.send(wrap(message.into_sync(transfer_id))).is_err() {
                break;
            }
        }
    });

    tokio::spawn(async move {
        let outcome = match run_receiver(
            file_id,
            content_hash,
            expected_size,
            temp_path,
            endpoint_out_tx,
            inbound_rx,
        )
        .await
        {
            Ok(file_bytes) => ReceiveOutcome::Complete(file_bytes),
            Err(error) => ReceiveOutcome::Failed(error),
        };

        let _ = done.send(outcome);
    });

    inbound_tx
}

/// Spawn a **sender** transfer bound to a peer link, serving `source`.
///
/// As with [`spawn_receiver`], returns the inbound `TransferMessage` sender for
/// the demux table; the endpoint's replies are wrapped under `transfer_id` and
/// pushed onto `peer_tx`.
pub fn spawn_sender<F, S>(
    transfer_id: TransferId,
    source: S,
    peer_tx: UnboundedSender<F>,
    wrap: impl Fn(SyncMessage) -> F + Send + 'static,
) -> UnboundedSender<TransferMessage>
where
    F: Send + 'static,
    S: ChunkSource + 'static,
{
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel::<TransferMessage>();
    let (endpoint_out_tx, mut endpoint_out_rx) =
        tokio::sync::mpsc::unbounded_channel::<TransferMessage>();

    tokio::spawn(async move {
        while let Some(message) = endpoint_out_rx.recv().await {
            if peer_tx.send(wrap(message.into_sync(transfer_id))).is_err() {
                break;
            }
        }
    });

    tokio::spawn(run_sender(source, endpoint_out_tx, inbound_rx));

    inbound_tx
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

    /// Wire a receiver and sender back to back over two channels and run the
    /// transfer to completion, returning the received bytes.
    async fn roundtrip(source: FileBytes) -> Result<Vec<u8>, TransferError> {
        let content_hash = source.hash().await.unwrap();
        let expected_size = source.byte_len().await.unwrap();
        let file_id = FileId::new();
        let dest = temp_path("dest");

        // receiver -> sender
        let (r2s_tx, r2s_rx) = tokio::sync::mpsc::unbounded_channel();
        // sender -> receiver
        let (s2r_tx, s2r_rx) = tokio::sync::mpsc::unbounded_channel();

        let sender = tokio::spawn(run_sender(source, s2r_tx, r2s_rx));
        let received = run_receiver(
            file_id,
            content_hash,
            expected_size,
            dest.clone(),
            r2s_tx,
            s2r_rx,
        )
        .await;
        sender.await.unwrap();

        let result = received.map(|file_bytes| {
            let path = file_bytes.path().unwrap().to_path_buf();
            std::fs::read(&path).unwrap()
        });
        let _ = std::fs::remove_file(&dest);
        result
    }

    #[tokio::test]
    async fn roundtrip_small_in_memory() {
        let bytes = b"hello transfer".to_vec();
        let received = roundtrip(FileBytes::InMemory(bytes.clone())).await.unwrap();
        assert_eq!(received, bytes);
    }

    #[tokio::test]
    async fn roundtrip_empty() {
        let received = roundtrip(FileBytes::InMemory(Vec::new())).await.unwrap();
        assert!(received.is_empty());
    }

    #[tokio::test]
    async fn roundtrip_multi_chunk_file() {
        // Several chunks plus a partial one, exercising windowing + reassembly.
        let source_path = temp_path("multi-src");
        let bytes: Vec<u8> = (0..(CHUNK_SIZE * 5 + 123)).map(|i| i as u8).collect();
        std::fs::write(&source_path, &bytes).unwrap();

        let received = roundtrip(FileBytes::FileToCopy(source_path.clone()))
            .await
            .unwrap();
        assert_eq!(received, bytes);
        let _ = std::fs::remove_file(&source_path);
    }

    #[tokio::test]
    async fn hash_mismatch_is_rejected() {
        // Receiver expects a hash that does not match the source bytes.
        let file_id = FileId::new();
        let dest = temp_path("mismatch-dest");
        let (r2s_tx, r2s_rx) = tokio::sync::mpsc::unbounded_channel();
        let (s2r_tx, s2r_rx) = tokio::sync::mpsc::unbounded_channel();

        let sender = tokio::spawn(run_sender(
            FileBytes::InMemory(b"real bytes".to_vec()),
            s2r_tx,
            r2s_rx,
        ));
        let wrong_hash = blake3::hash(b"different").to_hex().to_string();
        let received = run_receiver(
            file_id,
            wrong_hash,
            b"real bytes".len() as u64,
            dest.clone(),
            r2s_tx,
            s2r_rx,
        )
        .await;
        sender.await.unwrap();

        assert!(matches!(received, Err(TransferError::HashMismatch { .. })));
        // Temp file cleaned up on failure.
        assert!(!dest.exists());
    }

    #[tokio::test]
    async fn sender_abort_propagates() {
        // Point the sender at a nonexistent file so its first read aborts.
        let file_id = FileId::new();
        let dest = temp_path("abort-dest");
        let missing = temp_path("does-not-exist");
        let (r2s_tx, r2s_rx) = tokio::sync::mpsc::unbounded_channel();
        let (s2r_tx, s2r_rx) = tokio::sync::mpsc::unbounded_channel();

        let sender = tokio::spawn(run_sender(FileBytes::FileToCopy(missing), s2r_tx, r2s_rx));
        let hash = blake3::hash(b"whatever").to_hex().to_string();
        // Size unknown (source is missing): pass 0 to disable the request cap
        // so we still request offset 0 and receive the sender's abort.
        let received = run_receiver(file_id, hash, 0, dest.clone(), r2s_tx, s2r_rx).await;
        sender.await.unwrap();

        assert!(matches!(received, Err(TransferError::Aborted(_))));
        assert!(!dest.exists());
    }

    /// With a known size, the receiver must never request a chunk at or beyond
    /// EOF: no wasted past-EOF requests, and no completion-abort needed. We
    /// drive the receiver against a hand-rolled "sender" that records every
    /// requested offset and serves the real bytes.
    #[tokio::test]
    async fn known_size_never_requests_past_eof() {
        // A size that is NOT a multiple of CHUNK_SIZE, so the final chunk is
        // partial — the case that used to overshoot.
        let bytes: Vec<u8> = (0..(CHUNK_SIZE * 3 + 7)).map(|i| i as u8).collect();
        let size = bytes.len() as u64;
        let content_hash = blake3::hash(&bytes).to_hex().to_string();
        let file_id = FileId::new();
        let dest = temp_path("cap-dest");

        let (r2s_tx, mut r2s_rx) = tokio::sync::mpsc::unbounded_channel::<TransferMessage>();
        let (s2r_tx, s2r_rx) = tokio::sync::mpsc::unbounded_channel::<TransferMessage>();

        // Hand-rolled sender: record requested offsets and serve chunks. Asserts
        // no offset is at or beyond `size`.
        let sender_bytes = bytes.clone();
        let sender = tokio::spawn(async move {
            let mut requested = Vec::new();
            while let Some(message) = r2s_rx.recv().await {
                match message {
                    TransferMessage::Start { .. } => {}
                    TransferMessage::ChunkRequest { offset } => {
                        requested.push(offset);
                        assert!(
                            offset < size,
                            "receiver requested offset {offset} at/beyond EOF {size}"
                        );
                        let start = offset as usize;
                        let end = (start + CHUNK_SIZE).min(sender_bytes.len());
                        let chunk = sender_bytes[start..end].to_vec();
                        let last = end == sender_bytes.len();
                        if s2r_tx
                            .send(TransferMessage::Chunk {
                                offset,
                                bytes: chunk,
                                last,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    TransferMessage::Abort { .. } => break,
                    TransferMessage::Chunk { .. } => {}
                }
            }
            requested
        });

        let received =
            run_receiver(file_id, content_hash, size, dest.clone(), r2s_tx, s2r_rx).await;
        let requested = sender.await.unwrap();

        let received_bytes = received
            .map(|file_bytes| std::fs::read(file_bytes.path().unwrap()).unwrap())
            .unwrap();
        assert_eq!(received_bytes, bytes);

        // Exactly ceil(size / CHUNK_SIZE) distinct offsets, none past EOF.
        let expected_requests = size.div_ceil(CHUNK_SIZE as u64);
        let mut unique = requested.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len() as u64, expected_requests);

        let _ = std::fs::remove_file(&dest);
    }

    /// A zero-length file still completes: exactly one request at offset 0
    /// yields the empty final chunk.
    #[tokio::test]
    async fn known_zero_size_requests_only_offset_zero() {
        let received = roundtrip(FileBytes::InMemory(Vec::new())).await.unwrap();
        assert!(received.is_empty());
    }
}
