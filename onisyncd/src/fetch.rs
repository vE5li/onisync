//! Content-keyed chunk relay across the live peer tree.
//!
//! This is the routing layer for the one byte-movement mechanism (see
//! [`transfer`](crate::transfer)). A chunk's identity *is* `(file_id,
//! content_hash, offset)`; nothing else correlates a request to its reply. The
//! relay maintains a **content-keyed waiter table**: for each in-flight key it
//! records which downstream links (peer sessions or local receivers) are
//! waiting, and which upstream neighbours it forwarded the request to.
//!
//! - On a `ChunkRequest` from a downstream link for a key we do not already
//!   have an entry for, forward it to every neighbour except the sender and
//!   record them in `upstream_outstanding`; if an entry already exists, just
//!   add the link to `downstream` (request coalescing — one upstream fetch
//!   fanned to all waiters).
//! - On `ChunkData` from an upstream, fan it to *every* downstream waiter and
//!   drop the entry (first-writer-wins; later duplicates find no entry).
//! - On `ChunkMiss` from an upstream, remove it from `upstream_outstanding`;
//!   when that empties, fan `ChunkMiss` to all downstream waiters and drop.
//! - On link drop or TTL expiry, prune / fan `ChunkMiss` accordingly.
//!
//! A relay holds **no byte buffers** — only the waiter table, whose size is
//! bounded by the number of distinct in-flight chunk keys. Integrity is
//! end-to-end; the relay verifies nothing.
//!
//! Local receivers (files this node is pulling) are modelled as `Local`
//! downstream waiters, so multi-source and coalescing fall out for free: the
//! receiver's `ChunkRequest`s go through the same table as relayed ones.
//!
//! The provider registry (temporary local chunk sources, e.g. the CLI serving
//! an in-flight upload) also lives here, since providers are "things that can
//! answer a `ChunkRequest`".

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use onisync_core::FileId;
use onisync_core::state::{Frame, Sync as SyncMessage};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

use crate::configuration::RuntimeConfiguration;
use crate::transfer::{ChunkReply, ChunkSource};

/// How long a relay waiter entry lives before it is presumed dead, and how long
/// the receiver's per-chunk liveness guard waits with no progress. One tunable
/// across the relay layer and the receiver.
pub const HOP_TIMEOUT: Duration = Duration::from_secs(8);

/// The content key identifying one canonical chunk across all peers.
type ChunkKey = (FileId, String, u64);

/// A link waiting for a chunk: either a peer we must forward the reply to, or a
/// local receiver whose reply channel we deliver on.
enum Waiter {
    /// A neighbour peer (by public key) that sent us a `ChunkRequest`; the
    /// reply is sent back to it as a wire frame.
    Peer(String),
    /// A local receive driver; the reply is delivered on its channel, matched
    /// by offset within the receive.
    Local(tokio::sync::mpsc::UnboundedSender<ChunkReply>),
}

/// One outstanding chunk key we are relaying / receiving.
struct WaiterEntry {
    downstream: Vec<Waiter>,
    /// Neighbours we forwarded the request to and have not yet heard a terminal
    /// reply from. When this drains to empty (all missed), we fan `ChunkMiss`
    /// downstream.
    upstream_outstanding: HashSet<String>,
    /// TTL; armed when the entry is created and not refreshed by coalescing
    /// joiners (so a stream of joiners can't keep a dead upstream alive).
    deadline: Instant,
}

/// A registered temporary chunk provider (e.g. the CLI serving an upload).
type ProviderRegistry = HashMap<(FileId, String), Arc<dyn ChunkSource>>;

/// The content-keyed relay: the shared waiter table plus the peer runtime it
/// routes frames through and the provider registry.
///
/// Cheap to clone (every field is an `Arc`); every peer session holds a clone
/// so requests forwarded on one session and replies arriving on another share
/// one table.
#[derive(Clone)]
pub struct PendingFetches {
    inner: Arc<Mutex<HashMap<ChunkKey, WaiterEntry>>>,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    providers: Arc<Mutex<ProviderRegistry>>,
}

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
            providers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // ---- Provider registry ------------------------------------------------

    /// Register a temporary chunk provider (the CLI) for
    /// `file_id`/`content_hash`. A `ChunkRequest` for this file that no sync
    /// directory can serve will stream chunks from `source`.
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

    /// Look up a registered provider for `file_id`/`content_hash`.
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

    // ---- Peer plumbing ----------------------------------------------------

    /// Snapshot every connected peer's outbound sender, optionally excluding
    /// one public key (the peer a request came from — never echo it back).
    async fn connected_peers(&self, exclude: Option<&str>) -> Vec<PeerOutbound> {
        self.runtime_configuration
            .read()
            .await
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
        self.runtime_configuration
            .read()
            .await
            .peers
            .get(public_key)
            .and_then(|runtime_peer| runtime_peer.outbound.clone())
    }

    // ---- Local receiver requests ------------------------------------------

    /// Route a **local receiver's** `ChunkRequest` for `(file_id, content_hash,
    /// offset)`, delivering the eventual reply on `reply_tx`.
    ///
    /// `toward` is the routing policy: the neighbour most likely to hold the
    /// content (the announcing origin / last-good direction). When `Some`, the
    /// request is directed there; when `None`, it floods to all connected
    /// neighbours. Coalesces onto an existing entry for the same key.
    ///
    /// If there are no connected peers to ask, the reply is immediately a
    /// `ChunkMiss` (the receive then fails, as intended).
    pub async fn request_chunk_local(
        &self,
        file_id: FileId,
        content_hash: String,
        offset: u64,
        toward: Option<&str>,
        reply_tx: tokio::sync::mpsc::UnboundedSender<ChunkReply>,
    ) {
        let key = (file_id, content_hash.clone(), offset);

        // Choose the upstream neighbours to forward to.
        let targets: Vec<PeerOutbound> = match toward {
            Some(public_key) => match self.peer_outbound(public_key).await {
                Some(sender) => vec![PeerOutbound {
                    public_key: public_key.to_owned(),
                    sender,
                }],
                // The preferred direction is gone; fall back to flooding.
                None => self.connected_peers(None).await,
            },
            None => self.connected_peers(None).await,
        };

        let short = short_hash(&content_hash);
        if targets.is_empty() {
            log::debug!(
                "relay[{short}]: local request offset={offset}: no connected peers; local miss"
            );
            let _ = reply_tx.send(ChunkReply::Miss { offset });
            return;
        }

        let mut newly_created = false;
        {
            let mut table = self.inner.lock().await;
            match table.get_mut(&key) {
                Some(entry) => {
                    entry.downstream.push(Waiter::Local(reply_tx));
                    log::trace!(
                        "relay[{short}]: local request offset={offset} coalesced onto existing \
                         entry ({} downstream)",
                        entry.downstream.len()
                    );
                }
                None => {
                    let upstream_outstanding: HashSet<String> =
                        targets.iter().map(|peer| peer.public_key.clone()).collect();
                    log::debug!(
                        "relay[{short}]: local request offset={offset}: new entry, forwarding to \
                         {} upstream {:?}",
                        upstream_outstanding.len(),
                        upstream_outstanding
                    );
                    table.insert(key.clone(), WaiterEntry {
                        downstream: vec![Waiter::Local(reply_tx)],
                        upstream_outstanding,
                        deadline: Instant::now() + HOP_TIMEOUT,
                    });
                    newly_created = true;
                }
            }
        }

        if newly_created {
            let request = Frame::Sync(SyncMessage::ChunkRequest {
                file_id,
                content_hash,
                offset,
            });
            for peer in &targets {
                let _ = peer.sender.send(request.clone());
            }
            self.arm_ttl(key);
        }
    }

    // ---- Inbound frame handling (relay) -----------------------------------

    /// Handle an inbound `ChunkRequest` from `from_public_key` that this node
    /// could not serve locally (the caller already checked its sync directories
    /// / providers). Coalesce onto an existing entry, or forward to all
    /// neighbours except the sender. With no other neighbours, answer
    /// `ChunkMiss` straight back.
    pub async fn relay_chunk_request(
        &self,
        from_public_key: &str,
        file_id: FileId,
        content_hash: String,
        offset: u64,
    ) {
        let key = (file_id, content_hash.clone(), offset);
        let short = short_hash(&content_hash);

        let peers = self.connected_peers(Some(from_public_key)).await;
        if peers.is_empty() {
            log::debug!(
                "relay[{short}]: request offset={offset} from {from_public_key}: no other \
                 neighbours; miss back"
            );
            if let Some(sender) = self.peer_outbound(from_public_key).await {
                let _ = sender.send(Frame::Sync(SyncMessage::ChunkMiss {
                    file_id,
                    content_hash,
                    offset,
                }));
            }
            return;
        }

        let mut newly_created = false;
        {
            let mut table = self.inner.lock().await;
            match table.get_mut(&key) {
                Some(entry) => {
                    entry
                        .downstream
                        .push(Waiter::Peer(from_public_key.to_owned()));
                    log::trace!(
                        "relay[{short}]: request offset={offset} from {from_public_key} coalesced \
                         ({} downstream)",
                        entry.downstream.len()
                    );
                }
                None => {
                    let upstream_outstanding: HashSet<String> =
                        peers.iter().map(|peer| peer.public_key.clone()).collect();
                    log::debug!(
                        "relay[{short}]: request offset={offset} from {from_public_key}: \
                         forwarding to {} upstream {:?}",
                        upstream_outstanding.len(),
                        upstream_outstanding
                    );
                    table.insert(key.clone(), WaiterEntry {
                        downstream: vec![Waiter::Peer(from_public_key.to_owned())],
                        upstream_outstanding,
                        deadline: Instant::now() + HOP_TIMEOUT,
                    });
                    newly_created = true;
                }
            }
        }

        if newly_created {
            let request = Frame::Sync(SyncMessage::ChunkRequest {
                file_id,
                content_hash,
                offset,
            });
            for peer in &peers {
                let _ = peer.sender.send(request.clone());
            }
            self.arm_ttl(key);
        }
    }

    /// Handle an inbound `ChunkData` from an upstream. Fan the bytes to every
    /// downstream waiter and drop the entry (first-writer-wins). Late
    /// duplicates find no entry and are dropped.
    pub async fn handle_chunk_data(
        &self,
        file_id: FileId,
        content_hash: String,
        offset: u64,
        bytes: Vec<u8>,
    ) {
        let key = (file_id, content_hash.clone(), offset);
        let short = short_hash(&content_hash);
        let entry = {
            let mut table = self.inner.lock().await;
            table.remove(&key)
        };
        let Some(entry) = entry else {
            log::trace!(
                "relay[{short}]: data offset={offset} ({} bytes) but no waiter entry \
                 (late/duplicate); dropping",
                bytes.len()
            );
            return;
        };

        let mut local = 0usize;
        let mut peers = 0usize;
        for waiter in entry.downstream {
            match waiter {
                Waiter::Local(reply) => {
                    local += 1;
                    let _ = reply.send(ChunkReply::Data {
                        offset,
                        bytes: bytes.clone(),
                    });
                }
                Waiter::Peer(public_key) => {
                    peers += 1;
                    if let Some(sender) = self.peer_outbound(&public_key).await {
                        let _ = sender.send(Frame::Sync(SyncMessage::ChunkData {
                            file_id,
                            content_hash: content_hash.clone(),
                            offset,
                            bytes: bytes.clone(),
                        }));
                    }
                }
            }
        }
        log::debug!(
            "relay[{short}]: data offset={offset} ({} bytes) fanned to {local} local + {peers} \
             peer waiter(s)",
            bytes.len()
        );
    }

    /// Handle an inbound `ChunkMiss` from `from_public_key`. Remove it from the
    /// entry's `upstream_outstanding`; if that empties (all upstreams missed),
    /// fan `ChunkMiss` to all downstream waiters and drop the entry.
    pub async fn handle_chunk_miss(
        &self,
        from_public_key: &str,
        file_id: FileId,
        content_hash: String,
        offset: u64,
    ) {
        let key = (file_id, content_hash.clone(), offset);
        let short = short_hash(&content_hash);
        let exhausted = {
            let mut table = self.inner.lock().await;
            match table.get_mut(&key) {
                Some(entry) => {
                    entry.upstream_outstanding.remove(from_public_key);
                    if entry.upstream_outstanding.is_empty() {
                        log::debug!(
                            "relay[{short}]: miss offset={offset} from {from_public_key}: all \
                             upstreams exhausted; fanning miss down"
                        );
                        table.remove(&key)
                    } else {
                        log::trace!(
                            "relay[{short}]: miss offset={offset} from {from_public_key}: {} \
                             upstream(s) still outstanding",
                            entry.upstream_outstanding.len()
                        );
                        None
                    }
                }
                None => {
                    log::trace!(
                        "relay[{short}]: miss offset={offset} from {from_public_key} but no \
                         waiter entry; ignoring"
                    );
                    None
                }
            }
        };

        if let Some(entry) = exhausted {
            self.fan_miss(file_id, &content_hash, offset, entry).await;
        }
    }

    /// Prune a dropped link from every entry's `downstream` and
    /// `upstream_outstanding`, applying the same emptying rules (an entry whose
    /// upstreams all vanished fans `ChunkMiss` down; an entry with no remaining
    /// downstream is dropped).
    pub async fn prune_link(&self, public_key: &str) {
        let mut exhausted: Vec<(ChunkKey, WaiterEntry)> = Vec::new();
        {
            let mut table = self.inner.lock().await;
            let mut to_remove: Vec<ChunkKey> = Vec::new();
            for (key, entry) in table.iter_mut() {
                entry.downstream.retain(|waiter| match waiter {
                    Waiter::Peer(peer) => peer != public_key,
                    Waiter::Local(reply) => !reply.is_closed(),
                });
                entry.upstream_outstanding.remove(public_key);
                if entry.downstream.is_empty() || entry.upstream_outstanding.is_empty() {
                    to_remove.push(key.clone());
                }
            }
            for key in to_remove {
                if let Some(entry) = table.remove(&key) {
                    exhausted.push((key, entry));
                }
            }
        }
        if !exhausted.is_empty() {
            log::debug!(
                "relay: link {public_key} dropped; failing {} affected waiter entry/entries",
                exhausted.len()
            );
        }
        for ((file_id, content_hash, offset), entry) in exhausted {
            self.fan_miss(file_id, &content_hash, offset, entry).await;
        }
    }

    /// Deliver `ChunkMiss` to every downstream waiter of a dropped entry.
    async fn fan_miss(&self, file_id: FileId, content_hash: &str, offset: u64, entry: WaiterEntry) {
        for waiter in entry.downstream {
            match waiter {
                Waiter::Local(reply) => {
                    let _ = reply.send(ChunkReply::Miss { offset });
                }
                Waiter::Peer(public_key) => {
                    if let Some(sender) = self.peer_outbound(&public_key).await {
                        let _ = sender.send(Frame::Sync(SyncMessage::ChunkMiss {
                            file_id,
                            content_hash: content_hash.to_owned(),
                            offset,
                        }));
                    }
                }
            }
        }
    }

    /// Spawn a task that, after [`HOP_TIMEOUT`], drops `key` if it is still
    /// pending with the same deadline and fans `ChunkMiss` to its downstream
    /// waiters. The TTL is not refreshed by coalescing joiners.
    fn arm_ttl(&self, key: ChunkKey) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(HOP_TIMEOUT).await;
            let now = Instant::now();
            let entry = {
                let mut table = this.inner.lock().await;
                match table.get(&key) {
                    // Only expire if the deadline has actually passed (a fresh
                    // entry reusing the key after a drop would have a later
                    // deadline; leave it alone).
                    Some(entry) if entry.deadline <= now => table.remove(&key),
                    _ => None,
                }
            };
            if let Some(entry) = entry {
                let (file_id, content_hash, offset) = key;
                log::debug!(
                    "relay[{}]: TTL expired for offset={offset}; fanning miss to {} downstream \
                     waiter(s)",
                    short_hash(&content_hash),
                    entry.downstream.len()
                );
                this.fan_miss(file_id, &content_hash, offset, entry).await;
            }
        });
    }
}

/// A short, log-friendly prefix of a hex content hash (first 8 chars).
fn short_hash(hash: &str) -> &str {
    hash.get(..8).unwrap_or(hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::{Configuration, RuntimeConfiguration, RuntimePeer};

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

    /// Build an engine with `count` fake connected peers named "peer0".. and
    /// return the engine plus each peer's public key and its inbound frame
    /// receiver (what that peer would see arriving on the wire).
    async fn engine_with_peers(
        count: usize,
    ) -> (
        PendingFetches,
        Vec<(String, tokio::sync::mpsc::UnboundedReceiver<Frame>)>,
    ) {
        let engine = engine();
        let mut peers = Vec::new();
        {
            let mut runtime = engine.runtime_configuration.write().await;
            for i in 0..count {
                let public_key = format!("peer{i}");
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
                let mut runtime_peer = RuntimePeer::new();
                runtime_peer.outbound = Some(tx);
                runtime.peers.insert(public_key.clone(), runtime_peer);
                peers.push((public_key, rx));
            }
        }
        (engine, peers)
    }

    /// With no connected peers, a local request immediately misses.
    #[tokio::test]
    async fn local_request_no_peers_misses() {
        let engine = engine();
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(FileId::new(), "hash".to_owned(), 0, None, reply_tx)
            .await;
        match reply_rx.recv().await {
            Some(ChunkReply::Miss { offset }) => assert_eq!(offset, 0),
            other => panic!("expected miss, got {other:?}"),
        }
    }

    /// Provider register / lookup / unregister round-trips.
    #[tokio::test]
    async fn provider_registry_roundtrip() {
        let engine = engine();
        let file_id = FileId::new();
        let source: Arc<dyn ChunkSource> =
            Arc::new(crate::file_bytes::FileBytes::InMemory(b"x".to_vec()));
        engine
            .register_provider(file_id, "hash".to_owned(), source)
            .await;
        assert!(engine.provider_for(file_id, "hash").await.is_some());
        engine.unregister_provider(file_id, "hash").await;
        assert!(engine.provider_for(file_id, "hash").await.is_none());
    }

    /// Coalescing: two downstream waiters for the same key cause exactly one
    /// upstream fetch, and a single `ChunkData` fans out to both.
    #[tokio::test]
    async fn coalesces_and_fans_out() {
        let (engine, mut peers) = engine_with_peers(1).await;
        let upstream = peers[0].0.clone();
        let file_id = FileId::new();

        // Two local receivers request the same (file, hash, offset).
        let (reply_a_tx, mut reply_a_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reply_b_tx, mut reply_b_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, Some(&upstream), reply_a_tx)
            .await;
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, Some(&upstream), reply_b_tx)
            .await;

        // Exactly one ChunkRequest forwarded upstream (coalesced).
        let upstream_rx = &mut peers[0].1;
        assert!(matches!(
            upstream_rx.recv().await,
            Some(Frame::Sync(SyncMessage::ChunkRequest { offset: 0, .. }))
        ));
        assert!(
            upstream_rx.try_recv().is_err(),
            "second request not coalesced"
        );

        // The upstream answers once; both downstreams get the bytes.
        engine
            .handle_chunk_data(file_id, "h".to_owned(), 0, b"payload".to_vec())
            .await;
        match reply_a_rx.recv().await {
            Some(ChunkReply::Data { bytes, .. }) => assert_eq!(bytes, b"payload"),
            other => panic!("A: expected data, got {other:?}"),
        }
        match reply_b_rx.recv().await {
            Some(ChunkReply::Data { bytes, .. }) => assert_eq!(bytes, b"payload"),
            other => panic!("B: expected data, got {other:?}"),
        }
    }

    /// Exhaustion: a `ChunkMiss` from every upstream fans `ChunkMiss` down.
    #[tokio::test]
    async fn exhaustion_fans_miss() {
        let (engine, mut peers) = engine_with_peers(2).await;
        let file_id = FileId::new();

        // A relayed request from a *third* peer floods to peer0 and peer1.
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, None, reply_tx)
            .await;
        // Both upstreams saw the request.
        assert!(peers[0].1.recv().await.is_some());
        assert!(peers[1].1.recv().await.is_some());

        // First miss: not yet exhausted, no downstream reply.
        engine
            .handle_chunk_miss("peer0", file_id, "h".to_owned(), 0)
            .await;
        assert!(reply_rx.try_recv().is_err());

        // Second (last) miss: exhausted, fan ChunkMiss to the local waiter.
        engine
            .handle_chunk_miss("peer1", file_id, "h".to_owned(), 0)
            .await;
        assert!(matches!(
            reply_rx.recv().await,
            Some(ChunkReply::Miss { offset: 0 })
        ));
    }

    /// Link-drop pruning: dropping the only upstream fails the downstream
    /// waiter (rather than hanging until the TTL).
    #[tokio::test]
    async fn link_drop_prunes_and_fails() {
        let (engine, mut peers) = engine_with_peers(1).await;
        let upstream = peers[0].0.clone();
        let file_id = FileId::new();

        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, Some(&upstream), reply_tx)
            .await;
        assert!(peers[0].1.recv().await.is_some());

        engine.prune_link(&upstream).await;
        assert!(matches!(
            reply_rx.recv().await,
            Some(ChunkReply::Miss { offset: 0 })
        ));
    }

    /// TTL expiry fans `ChunkMiss` to downstream waiters when no upstream ever
    /// answers.
    #[tokio::test(start_paused = true)]
    async fn ttl_expiry_fans_miss() {
        let (engine, mut peers) = engine_with_peers(1).await;
        let upstream = peers[0].0.clone();
        let file_id = FileId::new();

        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, Some(&upstream), reply_tx)
            .await;
        assert!(peers[0].1.recv().await.is_some());

        // Advance past the TTL; the armed task expires the entry.
        tokio::time::advance(HOP_TIMEOUT + Duration::from_millis(1)).await;
        // Let the spawned TTL task run.
        tokio::task::yield_now().await;
        match reply_rx.recv().await {
            Some(ChunkReply::Miss { offset: 0 }) => {}
            other => panic!("expected miss from TTL, got {other:?}"),
        }
    }

    /// A relay holds no byte buffers: the waiter table only tracks link
    /// handles, never bytes. (`WaiterEntry` has no byte field — this test
    /// documents that invariant by asserting a relayed request forwards
    /// without buffering.)
    #[tokio::test]
    async fn relay_holds_no_bytes() {
        let (engine, mut peers) = engine_with_peers(2).await;
        let file_id = FileId::new();

        // peer0 asks us; we don't hold it, so we relay to peer1.
        engine
            .relay_chunk_request("peer0", file_id, "h".to_owned(), 0)
            .await;
        // peer1 (the only non-sender neighbour) got the forwarded request.
        assert!(matches!(
            peers[1].1.recv().await,
            Some(Frame::Sync(SyncMessage::ChunkRequest { offset: 0, .. }))
        ));
        // peer0 (the sender) is never echoed to.
        assert!(peers[0].1.try_recv().is_err());

        // When peer1 answers, the bytes are forwarded straight to peer0 and the
        // entry is dropped — nothing is cached.
        engine
            .handle_chunk_data(file_id, "h".to_owned(), 0, b"bytes".to_vec())
            .await;
        assert!(matches!(
            peers[0].1.recv().await,
            Some(Frame::Sync(SyncMessage::ChunkData { .. }))
        ));
        assert!(engine.inner.lock().await.is_empty());
    }
}
