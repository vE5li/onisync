//! On-demand recursive file fetch across the live peer tree.
//!
//! Used by `onisync edit <uuid>` when a file's bytes are not present
//! locally. A [`Sync::FetchRequest`](onisync_core::state::Sync::FetchRequest) is
//! flooded across the tree of *live* peer connections, which is assumed acyclic
//! (there is no seen-set / TTL — a node simply never forwards a request back to
//! the peer it arrived from). Modelled as a "call stack across machines":
//!
//! - Each node, on receiving a `FetchRequest` it cannot satisfy locally,
//!   forwards it to all its neighbours **except the one it came from** and
//!   records a [`PendingFetch`] so it knows where to return the answer.
//! - The **first** `FetchFound` whose hash matches wins and unwinds back toward
//!   the origin immediately; the pending entry is removed and any later replies
//!   for the same `request_id` are dropped.
//! - A node returns `FetchMissing` to its parent only once **every** child it
//!   forwarded to has returned `FetchMissing` or timed out.
//!
//! Every hop arms its own timeout so a single dead branch cannot stall the
//! chain; the originating CLI also applies an overall deadline.
//!
//! The pending table is shared (`Arc<Mutex<..>>`) between `handle_changes`
//! (which seeds *local-origin* requests coming off the ingest bus) and the peer
//! session tasks (which seed *relayed* requests and deliver replies). This is
//! the only cross-session coordination the feature introduces; the control
//! layer never touches it — it only enqueues a `DaemonMessage::Fetch` and
//! awaits the `oneshot`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use onisync_core::state::{Frame, Sync as SyncMessage};
use onisync_core::{FileId, RequestId};
use tokio::sync::{Mutex, RwLock, oneshot};

use crate::bus::FetchError;
use crate::configuration::RuntimeConfiguration;
use crate::file_bytes::FileBytes;
use crate::transfer::ChunkSource;

/// How long any single hop waits for its children before concluding the subtree
/// does not have the content. The originating CLI applies its own (larger)
/// overall deadline on top of this.
pub const HOP_TIMEOUT: Duration = Duration::from_secs(8);

/// Where the answer to a pending fetch must be delivered.
enum ReplyTarget {
    /// This request originated locally (from the ingest bus / CLI). Resolve the
    /// waiting caller directly.
    Local(oneshot::Sender<Result<FileBytes, FetchError>>),
    /// This request was relayed from a peer; the answer must be sent back to
    /// that peer (identified by public key).
    Peer(String),
}

/// A fetch this node is currently awaiting answers for.
struct PendingFetch {
    file_id: FileId,
    expected_hash: String,
    reply_to: ReplyTarget,
    /// Public keys of the peers we forwarded the request to and have not yet
    /// heard a terminal reply from. When this drains to empty (and no child
    /// answered `Found`), we report `Missing` to `reply_to`.
    children_outstanding: HashSet<String>,
}

/// What the caller (the peer session, which owns the transfer machinery) should
/// do after the fetch engine processes an inbound `FetchFound`.
///
/// `FetchFound` is content-less: the bytes are pulled over a transfer. The
/// engine resolves the routing but cannot open transfers itself (inbound
/// transfer frames are demuxed by the peer session), so it returns this action
/// for the session to execute.
pub enum FoundAction {
    /// Open a receiver transfer against `from_peer` for
    /// `file_id`/`expected_hash` and, once the bytes arrive (a
    /// hash-verified temp file), do `then`.
    Receive {
        from_peer: String,
        file_id: FileId,
        expected_hash: String,
        /// The file's size in bytes as announced in the `FetchFound` (0 if the
        /// answering node could not determine it). Used to cap the pull's
        /// request window at EOF.
        expected_size: u64,
        then: FoundThen,
    },
}

/// What to do with the bytes once a fetch transfer completes.
pub enum FoundThen {
    /// Deliver to the local waiter (CLI/ingest bus).
    DeliverLocal(oneshot::Sender<Result<FileBytes, FetchError>>),
    /// This node is a relay: cache the received bytes so a subsequent
    /// `TransferStart` from the parent can serve them, then forward the
    /// (content-less) `FetchFound` up to `parent_peer`.
    RelayUp {
        parent_peer: String,
        request_id: RequestId,
        file_id: FileId,
        content_hash: String,
        /// The size carried by the `FetchFound` we are relaying upward.
        size: u64,
    },
}

/// The fetch subsystem: the shared table of in-flight fetches (keyed by
/// `request_id`) plus the peer runtime it routes frames through.
///
/// Cheap to clone (both fields are `Arc`s); every peer session and
/// `handle_changes` holds a clone so they can seed and resolve requests against
/// the same table. Owning `runtime_configuration` here (rather than passing it
/// into each call) lets the engine operations be plain methods.
#[derive(Clone)]
pub struct PendingFetches {
    inner: Arc<Mutex<HashMap<RequestId, PendingFetch>>>,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    /// Files a relay node has received for an in-flight fetch and is holding to
    /// serve upward. Keyed by `(file_id, content_hash)`. Populated when a relay
    /// finishes receiving; consulted by the `TransferStart` sender path when
    /// the file is not in any sync directory; evicted once served.
    fetch_cache: Arc<Mutex<HashMap<(FileId, String), FileBytes>>>,
    /// Content a local client (the CLI) is temporarily providing on demand,
    /// keyed by `(file_id, content_hash)`. A `TransferStart` the sync
    /// directories cannot satisfy is served by streaming chunks from the
    /// registered [`ProviderSource`] (which round-trips to the client). Entries
    /// are registered while an upload/edit is in flight and removed when the
    /// client releases the file.
    providers: Arc<Mutex<ProviderRegistry>>,
}

/// Temporary chunk providers keyed by `(file_id, content_hash)`.
type ProviderRegistry = HashMap<(FileId, String), Arc<dyn ChunkSource>>;

/// Reference to a peer's outbound frame queue plus its public key.
struct PeerOutbound {
    public_key: String,
    sender: tokio::sync::mpsc::UnboundedSender<Frame>,
}

impl PendingFetches {
    pub fn new(runtime_configuration: Arc<RwLock<RuntimeConfiguration>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            runtime_configuration,
            fetch_cache: Arc::new(Mutex::new(HashMap::new())),
            providers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a temporary chunk provider (the CLI) for
    /// `file_id`/`content_hash`. A `TransferStart` for this file that no
    /// sync directory can serve will stream chunks from `source`.
    pub async fn register_provider(
        &self,
        file_id: FileId,
        content_hash: String,
        source: Arc<dyn ChunkSource>,
    ) {
        self.providers
            .lock()
            .await
            .insert((file_id, content_hash), source);
    }

    /// Remove a temporary provider (the client released the file).
    pub async fn unregister_provider(&self, file_id: FileId, content_hash: &str) {
        self.providers
            .lock()
            .await
            .remove(&(file_id, content_hash.to_owned()));
    }

    /// Look up a registered provider for `file_id`/`content_hash`. Returns a
    /// clone (the `ProviderSource` is a cheap channel handle) so the transfer
    /// sender can stream from it without holding the registry lock.
    pub async fn provider_for(
        &self,
        file_id: FileId,
        content_hash: &str,
    ) -> Option<Arc<dyn ChunkSource>> {
        self.providers
            .lock()
            .await
            .get(&(file_id, content_hash.to_owned()))
            .cloned()
    }

    /// Register a relay-held file so a subsequent `TransferStart` from the
    /// parent can serve it (see [`Self::take_fetch_cached`]).
    pub async fn cache_relay_file(
        &self,
        file_id: FileId,
        content_hash: String,
        content: FileBytes,
    ) {
        self.fetch_cache
            .lock()
            .await
            .insert((file_id, content_hash), content);
    }

    /// Take (removing) a relay-held file matching `file_id`/`content_hash`, if
    /// present. Eviction-on-serve: the entry is consumed so no stale bytes
    /// linger. Returns the temp `FileBytes` for the sender to stream from.
    pub async fn take_fetch_cached(
        &self,
        file_id: FileId,
        content_hash: &str,
    ) -> Option<FileBytes> {
        self.fetch_cache
            .lock()
            .await
            .remove(&(file_id, content_hash.to_owned()))
    }

    /// Snapshot every connected peer's outbound sender, optionally excluding
    /// one public key (the peer a relayed request came from — never echo it
    /// back).
    async fn connected_peers(&self, exclude: Option<&str>) -> Vec<PeerOutbound> {
        let runtime = self.runtime_configuration.read().await;
        runtime
            .peers
            .iter()
            .filter(|(public_key, _)| exclude != Some(public_key.as_str()))
            .filter_map(|(public_key, runtime_peer)| {
                runtime_peer.outbound.as_ref().map(|sender| PeerOutbound {
                    public_key: public_key.clone(),
                    sender: sender.clone(),
                })
            })
            .collect()
    }

    /// Resolve a single peer's outbound sender by public key.
    async fn peer_outbound(
        &self,
        public_key: &str,
    ) -> Option<tokio::sync::mpsc::UnboundedSender<Frame>> {
        let runtime = self.runtime_configuration.read().await;
        runtime
            .peers
            .get(public_key)
            .and_then(|runtime_peer| runtime_peer.outbound.clone())
    }

    /// Begin a fetch that originated locally (from the ingest bus / CLI).
    pub async fn start_local_fetch(
        &self,
        file_id: FileId,
        expected_hash: String,
        respond_to: oneshot::Sender<Result<FileBytes, FetchError>>,
    ) {
        let peers = self.connected_peers(None).await;
        if peers.is_empty() {
            log::debug!(
                "start_local_fetch: {} (hash {}): no connected peers; reporting NotAvailable",
                file_id.to_string(),
                expected_hash,
            );
            let _ = respond_to.send(Err(FetchError::NotAvailable));
            return;
        }

        let request_id = RequestId::new();
        let children: HashSet<String> = peers.iter().map(|peer| peer.public_key.clone()).collect();

        log::debug!(
            "start_local_fetch: {} (hash {}) as request {request_id}; flooding to {} peer(s): {:?}",
            file_id.to_string(),
            expected_hash,
            children.len(),
            children,
        );

        {
            let mut table = self.inner.lock().await;
            table.insert(
                request_id,
                PendingFetch {
                    file_id,
                    expected_hash: expected_hash.clone(),
                    reply_to: ReplyTarget::Local(respond_to),
                    children_outstanding: children,
                },
            );
        }

        let request = Frame::Sync(SyncMessage::FetchRequest {
            request_id,
            file_id,
            expected_hash,
        });
        for peer in &peers {
            let _ = peer.sender.send(request.clone());
        }

        self.arm_hop_timeout(request_id);
    }

    /// Handle an inbound `FetchRequest` relayed from `from_public_key`.
    ///
    /// If `local_size` is `Some(size)` we hold the file: answer a content-less
    /// `FetchFound` (carrying `size` so the puller can cap its request window)
    /// straight back to the sender, which then pulls the bytes from us over a
    /// transfer, served from our sync directories by the standard
    /// `TransferStart` path. Otherwise forward to all peers except the sender
    /// and register a pending entry keyed by the request's own `request_id`.
    /// With no other peers to try, answer `FetchMissing` immediately.
    pub async fn handle_incoming_request(
        &self,
        from_public_key: &str,
        request_id: RequestId,
        file_id: FileId,
        expected_hash: String,
        local_size: Option<u64>,
    ) {
        if let Some(size) = local_size {
            log::debug!(
                "handle_incoming_request: request {request_id} for {} from {}: have it locally ({size} bytes); answering FetchFound",
                file_id.to_string(),
                from_public_key,
            );
            let content_hash = expected_hash.clone();
            if let Some(sender) = self.peer_outbound(from_public_key).await {
                let _ = sender.send(Frame::Sync(SyncMessage::FetchFound {
                    request_id,
                    file_id,
                    content_hash,
                    size,
                }));
            }
            return;
        }

        let peers = self.connected_peers(Some(from_public_key)).await;
        if peers.is_empty() {
            log::debug!(
                "handle_incoming_request: request {request_id} for {} from {}: not local and no other peers to relay to; answering \
                 FetchMissing",
                file_id.to_string(),
                from_public_key,
            );
            if let Some(sender) = self.peer_outbound(from_public_key).await {
                let _ = sender.send(Frame::Sync(SyncMessage::FetchMissing { request_id }));
            }
            return;
        }

        let children: HashSet<String> = peers.iter().map(|peer| peer.public_key.clone()).collect();
        log::debug!(
            "handle_incoming_request: request {request_id} for {} from {}: not local; relaying to {} other peer(s): {:?}",
            file_id.to_string(),
            from_public_key,
            children.len(),
            children,
        );
        {
            let mut table = self.inner.lock().await;
            // A duplicate request_id (would only happen on a cyclic graph, which
            // we assume away) is ignored: keep the first registration.
            table.entry(request_id).or_insert(PendingFetch {
                file_id,
                expected_hash: expected_hash.clone(),
                reply_to: ReplyTarget::Peer(from_public_key.to_owned()),
                children_outstanding: children,
            });
        }

        let request = Frame::Sync(SyncMessage::FetchRequest {
            request_id,
            file_id,
            expected_hash,
        });
        for peer in &peers {
            let _ = peer.sender.send(request.clone());
        }

        self.arm_hop_timeout(request_id);
    }

    /// Handle an inbound content-less `FetchFound` from `from_public_key`.
    ///
    /// First-wins: resolve and remove the pending entry. Returns a
    /// [`FoundAction`] telling the caller to open a receiver transfer against
    /// the child that answered (`from_public_key`) and, on completion, either
    /// deliver the bytes locally or relay upward. Late duplicates (no entry)
    /// and hash mismatches return `None`.
    pub async fn handle_incoming_found(
        &self,
        from_public_key: &str,
        request_id: RequestId,
        file_id: FileId,
        content_hash: String,
        size: u64,
    ) -> Option<FoundAction> {
        let entry = {
            let mut table = self.inner.lock().await;
            table.remove(&request_id)
        };
        let Some(entry) = entry else {
            log::debug!(
                "handle_incoming_found: FetchFound for {} from {} but no pending entry (late duplicate or already resolved); ignoring",
                file_id.to_string(),
                from_public_key,
            );
            return None;
        };

        // Defensive: only accept an answer matching what this request expected.
        if content_hash != entry.expected_hash {
            log::warn!(
                "FetchFound for {} announced hash {} but {} was expected; dropping",
                request_id,
                content_hash,
                entry.expected_hash
            );
            return None;
        }

        log::debug!(
            "handle_incoming_found: request {request_id} for {} answered by {}; opening receiver transfer",
            file_id.to_string(),
            from_public_key,
        );

        let then = match entry.reply_to {
            ReplyTarget::Local(sender) => FoundThen::DeliverLocal(sender),
            ReplyTarget::Peer(parent_public_key) => FoundThen::RelayUp {
                parent_peer: parent_public_key,
                request_id,
                file_id,
                content_hash: content_hash.clone(),
                size,
            },
        };

        Some(FoundAction::Receive {
            from_peer: from_public_key.to_owned(),
            file_id,
            expected_hash: content_hash,
            expected_size: size,
            then,
        })
    }

    /// Handle an inbound `FetchMissing` from `from_public_key`. Removes that
    /// child from the pending entry; if it was the last outstanding child,
    /// reports `Missing` upward.
    pub async fn handle_incoming_missing(&self, from_public_key: &str, request_id: RequestId) {
        let resolved = {
            let mut table = self.inner.lock().await;
            match table.get_mut(&request_id) {
                Some(entry) => {
                    entry.children_outstanding.remove(from_public_key);
                    log::debug!(
                        "handle_incoming_missing: {} reported missing for request {request_id}; {} child(ren) still outstanding",
                        from_public_key,
                        entry.children_outstanding.len(),
                    );
                    if entry.children_outstanding.is_empty() {
                        table.remove(&request_id)
                    } else {
                        None
                    }
                }
                None => {
                    log::debug!(
                        "handle_incoming_missing: {} reported missing for request {request_id} but no pending entry (already resolved); \
                         ignoring",
                        from_public_key,
                    );
                    None
                }
            }
        };

        if let Some(entry) = resolved {
            self.report_missing(request_id, entry).await;
        }
    }

    /// Deliver a negative answer to a pending entry's `reply_to`.
    async fn report_missing(&self, request_id: RequestId, entry: PendingFetch) {
        log::debug!(
            "Reporting fetch {} for file {} as missing",
            request_id,
            entry.file_id.to_string()
        );
        match entry.reply_to {
            ReplyTarget::Local(sender) => {
                let _ = sender.send(Err(FetchError::NotAvailable));
            }
            ReplyTarget::Peer(parent_public_key) => {
                if let Some(sender) = self.peer_outbound(&parent_public_key).await {
                    let _ = sender.send(Frame::Sync(SyncMessage::FetchMissing { request_id }));
                }
            }
        }
    }

    /// Spawn a task that, after [`HOP_TIMEOUT`], forces resolution of
    /// `request_id` as missing if it is still pending (a child never answered).
    fn arm_hop_timeout(&self, request_id: RequestId) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(HOP_TIMEOUT).await;
            let entry = {
                let mut table = this.inner.lock().await;
                table.remove(&request_id)
            };
            if let Some(entry) = entry {
                log::debug!(
                    "Fetch {} timed out at a hop; reporting missing upward",
                    request_id
                );
                this.report_missing(request_id, entry).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::{Configuration, RuntimeConfiguration};

    fn engine() -> PendingFetches {
        let configuration = Configuration {
            sync_directories: Vec::new(),
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
        };
        let runtime = Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));
        PendingFetches::new(runtime)
    }

    #[tokio::test]
    async fn fetch_cache_take_evicts() {
        let engine = engine();
        let file_id = FileId::new();
        engine
            .cache_relay_file(
                file_id,
                "hash".to_owned(),
                FileBytes::InMemory(b"x".to_vec()),
            )
            .await;

        // First take returns the file; second take finds nothing (evicted).
        assert!(engine.take_fetch_cached(file_id, "hash").await.is_some());
        assert!(engine.take_fetch_cached(file_id, "hash").await.is_none());
    }

    #[tokio::test]
    async fn fetch_cache_take_requires_matching_hash() {
        let engine = engine();
        let file_id = FileId::new();
        engine
            .cache_relay_file(
                file_id,
                "right".to_owned(),
                FileBytes::InMemory(b"x".to_vec()),
            )
            .await;
        assert!(engine.take_fetch_cached(file_id, "wrong").await.is_none());
        // The mismatched take does not evict the real entry.
        assert!(engine.take_fetch_cached(file_id, "right").await.is_some());
    }

    #[tokio::test]
    async fn found_for_unknown_request_is_none() {
        let engine = engine();
        let action = engine
            .handle_incoming_found(
                "peer",
                RequestId::new(),
                FileId::new(),
                "hash".to_owned(),
                0,
            )
            .await;
        assert!(action.is_none());
    }
}
