//! Content-keyed **preview** relay across the live peer tree.
//!
//! The preview analogue of [`fetch`](crate::fetch), but far simpler: a preview
//! is one small blob, not a windowed byte stream, so there is no offset, no
//! chunking, and no integrity re-hashing. A preview's request identity is
//! `(file_id, content_hash)`; a holder either produces a preview of that exact
//! content ([`Sync::PreviewData`]) or misses ([`Sync::PreviewMiss`]).
//!
//! The waiter-table mechanics mirror the chunk relay one-to-one:
//!
//! - On a `PreviewRequest` from a downstream link for a key we have no entry
//!   for, forward it to every neighbour except the sender and record them in
//!   `upstream_outstanding`; if an entry exists, coalesce onto it.
//! - On `PreviewData` from an upstream, fan it to every downstream waiter and
//!   drop the entry (first-responder-wins; previews of the same content need
//!   not be byte-identical, so later duplicates are simply discarded).
//! - On `PreviewMiss`, remove that upstream from `upstream_outstanding`; when
//!   it empties, fan `PreviewMiss` down and drop.
//! - On link drop or TTL expiry, prune / fan `PreviewMiss` accordingly.
//!
//! A missed preview is *not* an error to the caller: a local request that
//! exhausts every direction resolves to [`Preview::None`] (see
//! `PreviewReply::Miss` handling at the call site).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tagsy_core::state::{Frame, Sync as SyncMessage};
use tagsy_core::{FileId, Preview};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

use crate::configuration::RuntimeConfiguration;
use crate::transfer::HOP_TIMEOUT;

/// The content key identifying one canonical preview across all peers.
type PreviewKey = (FileId, String);

/// The outcome delivered to a local preview requester.
#[derive(Debug, Clone)]
pub enum PreviewReply {
    /// A peer produced a preview of the requested content.
    Data(Preview),
    /// No reachable peer could serve a preview of this content.
    Miss,
}

/// A link waiting for a preview: a neighbour peer we must forward the reply to,
/// or a local requester whose reply channel we deliver on.
enum Waiter {
    Peer(String),
    Local(tokio::sync::oneshot::Sender<PreviewReply>),
}

/// One outstanding preview key we are relaying / awaiting.
struct WaiterEntry {
    downstream: Vec<Waiter>,
    upstream_outstanding: HashSet<String>,
    deadline: Instant,
}

/// Reference to a peer's outbound frame queue plus its public key.
struct PeerOutbound {
    public_key: String,
    sender: tokio::sync::mpsc::UnboundedSender<Frame>,
}

/// The content-keyed preview relay: the shared waiter table plus the peer
/// runtime it routes frames through. Cheap to clone (every field is an `Arc`);
/// every peer session holds a clone so a request forwarded on one session and a
/// reply arriving on another share one table.
#[derive(Clone)]
pub struct PendingPreviews {
    inner: Arc<Mutex<HashMap<PreviewKey, WaiterEntry>>>,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
}

impl PendingPreviews {
    pub fn new(runtime_configuration: Arc<RwLock<RuntimeConfiguration>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            runtime_configuration,
        }
    }

    // ---- Peer plumbing ----------------------------------------------------

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

    // ---- Local requests ---------------------------------------------------

    /// Route a **local** preview request for `(file_id, content_hash)`,
    /// delivering the eventual reply on `reply_tx`.
    ///
    /// Floods to every connected neighbour (unlike chunks, there is no
    /// "announcing origin" direction to prefer). Coalesces onto an existing
    /// entry for the same key. With no connected peers, replies `Miss`
    /// immediately.
    pub async fn request_preview_local(
        &self,
        file_id: FileId,
        content_hash: String,
        reply_tx: tokio::sync::oneshot::Sender<PreviewReply>,
    ) {
        let key = (file_id, content_hash.clone());
        let peers = self.connected_peers(None).await;

        if peers.is_empty() {
            let _ = reply_tx.send(PreviewReply::Miss);
            return;
        }

        let mut newly_created = false;
        {
            let mut table = self.inner.lock().await;
            match table.get_mut(&key) {
                Some(entry) => entry.downstream.push(Waiter::Local(reply_tx)),
                None => {
                    let upstream_outstanding: HashSet<String> =
                        peers.iter().map(|peer| peer.public_key.clone()).collect();
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
            let request = Frame::Sync(SyncMessage::PreviewRequest {
                file_id,
                content_hash,
            });
            for peer in &peers {
                let _ = peer.sender.send(request.clone());
            }
            self.arm_ttl(key);
        }
    }

    // ---- Inbound frame handling (relay) -----------------------------------

    /// Handle an inbound `PreviewRequest` from `from_public_key` that this node
    /// could not serve locally (the caller already checked local presence).
    /// Coalesce onto an existing entry, or forward to all neighbours except the
    /// sender. With no other neighbours, answer `PreviewMiss` straight back.
    pub async fn relay_preview_request(
        &self,
        from_public_key: &str,
        file_id: FileId,
        content_hash: String,
    ) {
        let key = (file_id, content_hash.clone());
        let peers = self.connected_peers(Some(from_public_key)).await;

        if peers.is_empty() {
            if let Some(sender) = self.peer_outbound(from_public_key).await {
                let _ = sender.send(Frame::Sync(SyncMessage::PreviewMiss {
                    file_id,
                    content_hash,
                }));
            }
            return;
        }

        let mut newly_created = false;
        {
            let mut table = self.inner.lock().await;
            match table.get_mut(&key) {
                Some(entry) => entry
                    .downstream
                    .push(Waiter::Peer(from_public_key.to_owned())),
                None => {
                    let upstream_outstanding: HashSet<String> =
                        peers.iter().map(|peer| peer.public_key.clone()).collect();
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
            let request = Frame::Sync(SyncMessage::PreviewRequest {
                file_id,
                content_hash,
            });
            for peer in &peers {
                let _ = peer.sender.send(request.clone());
            }
            self.arm_ttl(key);
        }
    }

    /// Handle an inbound `PreviewData` from an upstream. Fan it to every
    /// downstream waiter and drop the entry (first-responder-wins). Late
    /// duplicates find no entry and are dropped.
    pub async fn handle_preview_data(
        &self,
        file_id: FileId,
        content_hash: String,
        preview: Preview,
    ) {
        let key = (file_id, content_hash.clone());
        let entry = {
            let mut table = self.inner.lock().await;
            table.remove(&key)
        };
        let Some(entry) = entry else {
            return;
        };

        for waiter in entry.downstream {
            match waiter {
                Waiter::Local(reply) => {
                    let _ = reply.send(PreviewReply::Data(preview.clone()));
                }
                Waiter::Peer(public_key) => {
                    if let Some(sender) = self.peer_outbound(&public_key).await {
                        let _ = sender.send(Frame::Sync(SyncMessage::PreviewData {
                            file_id,
                            content_hash: content_hash.clone(),
                            preview: preview.clone(),
                        }));
                    }
                }
            }
        }
    }

    /// Handle an inbound `PreviewMiss` from `from_public_key`. Remove it from
    /// the entry's `upstream_outstanding`; if that empties, fan `PreviewMiss`
    /// to all downstream waiters and drop the entry.
    pub async fn handle_preview_miss(
        &self,
        from_public_key: &str,
        file_id: FileId,
        content_hash: String,
    ) {
        let key = (file_id, content_hash.clone());
        let exhausted = {
            let mut table = self.inner.lock().await;
            match table.get_mut(&key) {
                Some(entry) => {
                    entry.upstream_outstanding.remove(from_public_key);
                    if entry.upstream_outstanding.is_empty() {
                        table.remove(&key)
                    } else {
                        None
                    }
                }
                None => None,
            }
        };

        if let Some(entry) = exhausted {
            self.fan_miss(file_id, &content_hash, entry).await;
        }
    }

    /// Prune a dropped link from every entry, applying the same emptying rules
    /// as the chunk relay (an entry whose upstreams all vanished, or that has
    /// no remaining downstream, fans `PreviewMiss` down and is dropped).
    pub async fn prune_link(&self, public_key: &str) {
        let mut exhausted: Vec<(PreviewKey, WaiterEntry)> = Vec::new();
        {
            let mut table = self.inner.lock().await;
            let mut to_remove: Vec<PreviewKey> = Vec::new();
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
        for ((file_id, content_hash), entry) in exhausted {
            self.fan_miss(file_id, &content_hash, entry).await;
        }
    }

    /// Deliver `PreviewMiss` to every downstream waiter of a dropped entry.
    async fn fan_miss(&self, file_id: FileId, content_hash: &str, entry: WaiterEntry) {
        for waiter in entry.downstream {
            match waiter {
                Waiter::Local(reply) => {
                    let _ = reply.send(PreviewReply::Miss);
                }
                Waiter::Peer(public_key) => {
                    if let Some(sender) = self.peer_outbound(&public_key).await {
                        let _ = sender.send(Frame::Sync(SyncMessage::PreviewMiss {
                            file_id,
                            content_hash: content_hash.to_owned(),
                        }));
                    }
                }
            }
        }
    }

    /// After [`HOP_TIMEOUT`], drop `key` if it is still pending with the same
    /// deadline and fan `PreviewMiss` to its downstream waiters. The TTL is not
    /// refreshed by coalescing joiners.
    fn arm_ttl(&self, key: PreviewKey) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(HOP_TIMEOUT).await;
            let now = Instant::now();
            let entry = {
                let mut table = this.inner.lock().await;
                match table.get(&key) {
                    Some(entry) if entry.deadline <= now => table.remove(&key),
                    _ => None,
                }
            };
            if let Some(entry) = entry {
                let (file_id, content_hash) = key;
                this.fan_miss(file_id, &content_hash, entry).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::{Configuration, RuntimePeer};

    fn engine() -> PendingPreviews {
        let configuration = Configuration {
            sync_directories: Vec::new(),
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
            preview_generation_policy: crate::configuration::PreviewGenerationPolicy::Lazy,
            editor_rules: Vec::new(),
            tag_rules: Vec::new(),
        };
        let runtime = Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));
        PendingPreviews::new(runtime)
    }

    async fn engine_with_peers(
        count: usize,
    ) -> (
        PendingPreviews,
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

    #[tokio::test]
    async fn local_request_no_peers_misses() {
        let engine = engine();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        engine
            .request_preview_local(FileId::new(), "hash".to_owned(), reply_tx)
            .await;
        assert!(matches!(reply_rx.await, Ok(PreviewReply::Miss)));
    }

    #[tokio::test]
    async fn coalesces_and_fans_out() {
        let (engine, mut peers) = engine_with_peers(1).await;
        let file_id = FileId::new();

        let (reply_a_tx, reply_a_rx) = tokio::sync::oneshot::channel();
        let (reply_b_tx, reply_b_rx) = tokio::sync::oneshot::channel();
        engine
            .request_preview_local(file_id, "h".to_owned(), reply_a_tx)
            .await;
        engine
            .request_preview_local(file_id, "h".to_owned(), reply_b_tx)
            .await;

        // Exactly one PreviewRequest forwarded upstream (coalesced).
        let upstream_rx = &mut peers[0].1;
        assert!(matches!(
            upstream_rx.recv().await,
            Some(Frame::Sync(SyncMessage::PreviewRequest { .. }))
        ));
        assert!(upstream_rx.try_recv().is_err(), "second not coalesced");

        engine
            .handle_preview_data(file_id, "h".to_owned(), Preview::Text("hi".to_owned()))
            .await;
        assert!(matches!(
            reply_a_rx.await,
            Ok(PreviewReply::Data(Preview::Text(text))) if text == "hi"
        ));
        assert!(matches!(
            reply_b_rx.await,
            Ok(PreviewReply::Data(Preview::Text(text))) if text == "hi"
        ));
    }

    #[tokio::test]
    async fn exhaustion_fans_miss() {
        let (engine, mut peers) = engine_with_peers(2).await;
        let file_id = FileId::new();

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        engine
            .request_preview_local(file_id, "h".to_owned(), reply_tx)
            .await;
        assert!(peers[0].1.recv().await.is_some());
        assert!(peers[1].1.recv().await.is_some());

        engine
            .handle_preview_miss("peer0", file_id, "h".to_owned())
            .await;
        engine
            .handle_preview_miss("peer1", file_id, "h".to_owned())
            .await;
        assert!(matches!(reply_rx.await, Ok(PreviewReply::Miss)));
    }

    #[tokio::test]
    async fn relay_forwards_and_drops() {
        let (engine, mut peers) = engine_with_peers(2).await;
        let file_id = FileId::new();

        engine
            .relay_preview_request("peer0", file_id, "h".to_owned())
            .await;
        assert!(matches!(
            peers[1].1.recv().await,
            Some(Frame::Sync(SyncMessage::PreviewRequest { .. }))
        ));
        assert!(peers[0].1.try_recv().is_err());

        engine
            .handle_preview_data(file_id, "h".to_owned(), Preview::None)
            .await;
        assert!(matches!(
            peers[0].1.recv().await,
            Some(Frame::Sync(SyncMessage::PreviewData { .. }))
        ));
        assert!(engine.inner.lock().await.is_empty());
    }
}
