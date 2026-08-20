//! Core tagsy runtime as a library.
//!
//! The runtime is callable as a library function ([`run`]): the desktop binary
//! (`main.rs`) is a thin CLI wrapper, and other frontends (e.g. an Android
//! native library) can link this crate and call [`run`] directly without a
//! `main()`.
//!
//! All business logic (peer sync, the DB pipeline, change handling) lives
//! here behind [`run`]. Frontends supply:
//!
//! - a [`Configuration`](configuration::Configuration),
//! - a [`RunPaths`] describing where the data directory and identity key live,
//! - a [`ShutdownSignal`] used to stop the runtime cleanly.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tagsy_core::state::{Change, ChangeOrigin, Frame, Sync as SyncMessage};
use tagsy_core::{FileId, LogicalPath, TagId};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_util::sync::CancellationToken;

use crate::bus::{ContentChange, DaemonMessage, Ingest};
use crate::catalog::placement::{self, Placement};
#[cfg(feature = "preview-generation")]
use crate::catalog::previews::preview_extension_for;
use crate::catalog::previews::{
    PREVIEW_GENERATION_COMPILED, maybe_eager_preview, resolve_preview, try_serve_generated_preview,
};
use crate::configuration::{CompiledTagRules, Configuration, Peer, RuntimeConfiguration, SyncType};
use crate::directory_manager::{SyncDirectoryCommand, SyncDirectoryManager};
use crate::fetch::PendingFetches;
use crate::file_bytes::FileBytes;
use crate::identity::{HandshakeMessage, Identity};
use crate::paths::Paths;
use crate::peer::plan::{
    MissingContent, PeerDeletion, PeerMove, PeerRestore, SyncPlan, build_local_manifest,
    plan_file_sync,
};
use crate::peer::plan_tags::{build_local_tag_manifest, build_tag_request_response, plan_tag_sync};
use crate::preview_fetch::PendingPreviews;
use crate::store::CatalogStore;
use crate::transfer::{ChunkAnswer, ChunkReply, ChunkRequest, ReceiveOutcome, VerifiedHashCache};

pub mod api;
pub mod bus;
pub mod catalog;
pub mod clock;
pub mod configuration;
pub mod control;
pub mod directory_manager;
pub mod fetch;
pub mod file_bytes;
pub mod identity;
pub mod operations;
pub mod paths;
pub mod peer;
#[cfg(feature = "preview-generation")]
pub mod preview;
pub mod preview_fetch;
pub mod store;
pub mod transfer;
pub mod transport;
pub mod watcher;

/// What a peer-session receive materializes on completion: the received bytes
/// are written into our sync directories and the version recorded, placing per
/// `placement`. Carried alongside the receive's outcome so the session's
/// completion handler can dispatch.
///
/// (On-demand fetches — `tagsy edit`, deferred placement — do not go through
/// a peer session; they drive a receive directly via [`fetch_via_relay`] and
/// return the temp file to their waiter.)
struct ReceiverPurpose {
    file_id: FileId,
    content_hash: String,
    origin: ChangeOrigin,
    placement: bus::MaterializePlacement,
}

/// The shared routing handles every peer-connection task needs: the runtime
/// peer table, the pending on-demand fetches, and the two senders into the
/// change bus and sync-directory manager.
///
/// Bundled into one `Clone` struct so `handle_connection`, `connect_to_peer`,
/// and `run_peer_session` can pass a single context around instead of the same
/// four arguments each (which also keeps them under clippy's argument-count
/// lint). All four fields are cheap to clone (`Arc`s / channel senders).
#[derive(Clone)]
struct PeerContext {
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    pending_fetches: PendingFetches,
    /// Content-keyed preview relay, sibling to `pending_fetches`. Every peer
    /// session holds a clone so a `PreviewRequest` forwarded on one link and
    /// its `PreviewData`/`PreviewMiss` arriving on another share one waiter
    /// table.
    pending_previews: PendingPreviews,
    change_sender: UnboundedSender<DaemonMessage>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
    /// Whether this device may *generate* previews locally (its policy permits
    /// it and the `preview-generation` feature is compiled in). When `false`, a
    /// peer `PreviewRequest` is answered only from our preview cache, never by
    /// decoding local bytes.
    can_generate_previews: bool,
    /// Node-wide cache of verified content hashes (`path -> (mtime, size,
    /// hash)`), so a holder answering repeated `ChunkRequest`s for the same
    /// unchanged file hashes it once. Shared across all peer sessions.
    verified_hashes: VerifiedHashCache,
    /// Live sync-operation registry, so peer sessions can surface what they are
    /// doing (serving/receiving files, reconciling, fetching) to the UI.
    operations: crate::operations::Operations,
}

/// Cooperative shutdown handle for [`run`].
///
/// A thin wrapper around a [`CancellationToken`]. The caller holds the
/// [`ShutdownSignal`] and calls [`ShutdownSignal::shutdown`] (e.g. from a
/// Ctrl-C handler, a systemd stop, or the Android service `onDestroy`); the
/// running [`run`] future observes the cancellation, stops accepting new work,
/// drains its tasks, and returns cleanly.
#[derive(Debug, Clone, Default)]
pub struct ShutdownSignal {
    token: CancellationToken,
}

impl ShutdownSignal {
    /// Create a fresh, un-triggered shutdown signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request shutdown. Idempotent; safe to call from any task/thread.
    pub fn shutdown(&self) {
        self.token.cancel();
    }

    /// Has shutdown been requested yet?
    pub fn is_shutdown(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Access the underlying token (e.g. to derive child tokens for tasks).
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }
}

/// Errors that can abort startup of [`run`].
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The identity key could not be loaded from `identity_file`.
    #[error("failed to load identity key at {}: {source}", path.display())]
    Identity {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Opening the main database failed.
    #[error("failed to open main database: {0}")]
    Database(#[source] store::DatabaseError),
    /// Binding the peer-sync listener failed.
    #[error("failed to bind peer listener to {address}: {source}")]
    Bind {
        address: String,
        #[source]
        source: std::io::Error,
    },
}

/// Enqueue a `Change::TagAdded` for every tag declared in the configuration, so
/// their definitions are guaranteed to exist before any tagging/reconciliation
/// runs. Called from [`run`] before `handle_changes` starts draining the bus.
/// Best-effort per tag: an empty name is skipped (the DB rejects it anyway) and
/// a closed channel is logged.
///
/// `modified_at` stamped on config-declared tag definitions. Deliberately the
/// lowest possible value so a declaration acts as a last-writer-wins *floor*:
/// `add_tag`'s guard (`excluded.modified_at > tags.modified_at`) means any real
/// edit — always stamped with a positive wall-clock `now_millis()` — wins, and
/// a re-declared tag on the next boot never clobbers a rename/recolor made in
/// between. See [`TagDeclaration`](configuration::TagDeclaration).
fn enqueue_declared_tags(
    change_sender: &UnboundedSender<DaemonMessage>,
    configuration: &Configuration,
) {
    const DECLARED_TAG_MODIFIED_AT: i64 = i64::MIN;

    for tag in &configuration.tags {
        if tag.name.trim().is_empty() {
            log::warn!(
                "Skipping config tag declaration {} with empty name",
                tag.id.to_string()
            );
            continue;
        }

        // Normalize an empty color to the same default the API uses, so a
        // declared tag renders consistently with a UI-created one.
        let color = if tag.color.trim().is_empty() {
            "#F44336".to_owned()
        } else {
            tag.color.clone()
        };

        let change = Change::TagAdded {
            tag_id: tag.id,
            tag_name: tag.name.clone(),
            color,
            metadata: None,
            modified_at: DECLARED_TAG_MODIFIED_AT,
        };
        let change_origin = ChangeOrigin::Local {
            directory_path: std::path::PathBuf::new(),
        };

        if let Err(error) = change_sender.send(DaemonMessage::change(change, change_origin)) {
            log::error!(
                "Failed to enqueue declared tag {} ({}): {error}",
                tag.name,
                tag.id.to_string()
            );
        }
    }
}

/// Start the tagsy sync engine, returning a UI-facing [`Api`](api::Api)
/// handle alongside the runtime driver future.
///
/// This is the former body of the `Run` CLI subcommand, lifted into a library
/// function so it can be driven by any frontend. It performs all fallible
/// startup (loading the identity, opening the main DB, binding the peer
/// listener) up front and returns:
///
/// - an [`Api`](api::Api) the caller can use immediately to serve the UI
///   (reads, writes, event subscription), and
/// - a driver future that runs the accept loop / idle-until-shutdown and then
///   drains the spawned tasks. The caller must poll it to completion (e.g.
///   `tokio::spawn` it, or `.await` it) for the runtime to make progress; it
///   returns once `shutdown` is triggered.
///
/// Every frontend (desktop binary, Android in-process backend, host harness)
/// uses this: the ones that do not need the [`Api`](api::Api) simply await the
/// driver and drop the handle.
pub async fn run(
    configuration: Configuration,
    paths: Paths,
    shutdown: ShutdownSignal,
) -> Result<
    (
        api::Api,
        impl std::future::Future<Output = Result<(), RunError>>,
    ),
    RunError,
> {
    let runtime_configuration = Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));

    // Reconcile the preview-generation policy against what this binary can
    // actually do. A policy that wants to generate (`Lazy`/`Eager`) needs the
    // `preview-generation` feature compiled in; if it is not, we cannot honor
    // the policy, so we log an error and fall back to behaving as `Never`
    // (cache + serve + relay only). This is a soft fallback, not a fatal error:
    // the daemon still runs and still participates in the preview network.
    let policy = configuration.preview_generation_policy;
    let can_generate_previews = if policy.generates() && !PREVIEW_GENERATION_COMPILED {
        log::error!(
            "preview_generation_policy is {:?} but this build was compiled without the \
             `preview-generation` feature; falling back to no local generation (Never). This \
             device will only cache and serve previews obtained from peers.",
            policy
        );
        false
    } else {
        policy.generates() && PREVIEW_GENERATION_COMPILED
    };
    log::info!(
        "Preview generation policy: {:?} (local generation {})",
        policy,
        if can_generate_previews {
            "enabled"
        } else {
            "disabled"
        }
    );

    // Compile the tag rules once. Shared (not cloned) because a `Regex` is
    // expensive to build and both consumers only ever read: `handle_changes`
    // matches every newly-created file against them, and `Api` needs the same
    // set to re-apply them on demand (`retag`).
    //
    // A rule that fails to compile is dropped from the matcher set but retained
    // as an error, and never prevents startup — see `CompiledTagRules`.
    let tag_rules = Arc::new(CompiledTagRules::compile(&configuration.tag_rules));
    for error in tag_rules.errors() {
        log::error!("{error}; this rule is disabled, all others still apply");
    }

    // Shared content-keyed chunk relay. Every peer session and `handle_changes`
    // holds a clone: requests forwarded on one session and replies arriving on
    // another share one waiter table, so multi-source pulls and relay coalescing
    // work across links. Also owns the temporary-provider registry (CLI
    // uploads). Cheap to clone (Arcs).
    let pending_fetches = crate::fetch::PendingFetches::new(runtime_configuration.clone());

    // Sibling of `pending_fetches` for previews: a content-keyed waiter table
    // shared by every peer session and `handle_changes`, so a preview requested
    // on one link and answered on another resolve together. Cheap to clone.
    let pending_previews =
        crate::preview_fetch::PendingPreviews::new(runtime_configuration.clone());

    let identity = Identity::load(paths.identity_path()).map_err(|source| RunError::Identity {
        path: paths.identity_path().to_path_buf(),
        source,
    })?;
    let identity = Arc::new(identity);

    let main_db_path = paths.main_db_path();

    // Open the main DB. It will be owned by `handle_changes` (the only task
    // that mutates it). Before handing it off, snapshot the latest content
    // hash per file so `SyncDirectoryManager` can detect files that changed on
    // disk while we were offline without ever touching the main DB itself.
    let database = CatalogStore::initialize(&main_db_path).map_err(RunError::Database)?;

    let last_known_hashes = database
        .latest_content_hashes()
        .map_err(RunError::Database)?;

    let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();

    // Guarantee the config-declared tag definitions exist before anything else.
    // These are enqueued now, while `handle_changes` has not yet started
    // draining the bus, so they are the *first* changes it applies — before any
    // peer connects and before any `FileTagged`/reconciliation runs. That way a
    // `SyncType::TagBased` directory referencing a declared id always resolves.
    enqueue_declared_tags(&change_sender, &configuration);

    // Broadcast of applied changes for the UI-facing API event stream.
    // `handle_changes` publishes every change it applies here;
    // API subscribers receive them best-effort. Capacity bounds how far a slow
    // subscriber may lag before it observes `Lagged` (mapped to `Resynced` by
    // the transport). Sized generously; the UI is expected to keep up.
    let (event_sender, _event_receiver) = tokio::sync::broadcast::channel(1024);

    // Live sync-operation registry, shared by the UI-facing API (to snapshot /
    // subscribe) and every peer session (to report work in progress).
    let operations = crate::operations::Operations::new();

    let fetch_temp_dir = paths.fetch_temp_dir();
    if let Err(error) = paths.clean_fetch_temp_dir().await {
        log::warn!(
            "Failed to prepare fetch temp dir {}: {error}",
            fetch_temp_dir.display()
        );
    }

    // The UI-facing API handle. Reads open their own read-only DB handle on
    // `main_db_path`; writes go onto `change_sender`; events come from
    // `event_sender`.
    let api = api::Api::new(
        main_db_path.clone(),
        change_sender.clone(),
        command_sender.clone(),
        event_sender.clone(),
        pending_fetches.clone(),
        fetch_temp_dir,
        operations.clone(),
        configuration.editor_rules.clone(),
        tag_rules.clone(),
    );

    // The sync-directory manager is inherently single-threaded: it holds
    // `RefCell`s that are `!Send`, and it now `.await`s file I/O (streaming
    // materialization) while borrowing them. Rather than force `Send` on all of
    // that, run it on a dedicated OS thread with a current-thread runtime +
    // `LocalSet`. A oneshot lets the shutdown path below join it like the other
    // tasks.
    let (sync_directories_done_tx, sync_directories_handle) = tokio::sync::oneshot::channel();
    let sync_directories_thread = {
        let configuration = configuration.clone();
        let paths = paths.clone();
        let change_sender = change_sender.clone();
        let shutdown_child = shutdown.token().child_token();

        std::thread::Builder::new()
            .name("tagsy-sync-directories".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build sync-directory runtime");

                let local = tokio::task::LocalSet::new();
                local.block_on(
                    &runtime,
                    handle_sync_directories(
                        configuration,
                        paths,
                        last_known_hashes,
                        change_sender,
                        command_receiver,
                        shutdown_child,
                    ),
                );

                let _ = sync_directories_done_tx.send(());
            })
            .expect("failed to spawn sync-directory thread")
    };

    let changes_handle = tokio::spawn(handle_changes(
        configuration.clone(),
        tag_rules.clone(),
        runtime_configuration.clone(),
        pending_fetches.clone(),
        pending_previews.clone(),
        database,
        change_receiver,
        change_sender.clone(),
        command_sender.clone(),
        event_sender,
        operations.clone(),
        shutdown.token().child_token(),
    ));

    // The routing handles every peer-connection task needs. Built once and
    // cloned per spawned task (below, and in the accept loop inside `driver`).
    let peer_context = PeerContext {
        runtime_configuration: runtime_configuration.clone(),
        pending_fetches: pending_fetches.clone(),
        pending_previews: pending_previews.clone(),
        change_sender: change_sender.clone(),
        command_sender: command_sender.clone(),
        can_generate_previews,
        verified_hashes: VerifiedHashCache::new(),
        operations: operations.clone(),
    };

    let mut peer_handles = Vec::new();
    for peer in &configuration.peers {
        if peer.address.is_some() {
            peer_handles.push(tokio::spawn(connect_to_peer(
                identity.clone(),
                peer.clone(),
                main_db_path.clone(),
                peer_context.clone(),
                shutdown.token().child_token(),
            )));
        }
    }

    // Bind the peer-sync listener up front (if configured) so bind failures
    // surface to the caller before we hand back the `Api`, rather than inside
    // the driver future.
    let listener = if let Some(listen_port) = configuration.listen_port {
        let bind_address = format!("0.0.0.0:{listen_port}");
        let listener = TcpListener::bind(&bind_address)
            .await
            .map_err(|source| RunError::Bind {
                address: bind_address.clone(),
                source,
            })?;
        log::info!("Listening for peer connections on {bind_address}");
        Some(listener)
    } else {
        log::info!("No listen_port configured; not accepting inbound peer connections");
        None
    };

    // The driver future: runs the accept loop (or idles until shutdown), then
    // cancels and drains all spawned tasks. The caller polls it to completion.
    let driver = async move {
        if let Some(listener) = listener {
            loop {
                tokio::select! {
                    _ = shutdown.token().cancelled() => {
                        log::info!("Shutdown requested; stopping peer listener");
                        break;
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, address)) => {
                                tokio::spawn(handle_connection(
                                    configuration.clone(),
                                    identity.clone(),
                                    main_db_path.clone(),
                                    peer_context.clone(),
                                    stream,
                                    address,
                                    shutdown.token().child_token(),
                                ));
                            }
                            Err(error) => {
                                log::warn!("Peer listener accept error: {error}");
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            // Keep the runtime alive so the spawned tasks can run, until shutdown.
            shutdown.token().cancelled().await;
            log::info!("Shutdown requested; stopping runtime");
        }

        // Ensure the long-lived tasks observe cancellation, then drain them.
        shutdown.shutdown();

        // Dropping the senders lets the receiving tasks fall out of their loops
        // once their channels are empty.
        drop(change_sender);
        drop(command_sender);

        let _ = sync_directories_handle.await;
        // Join the dedicated OS thread now that its runtime has finished.
        let _ = sync_directories_thread.join();
        let _ = changes_handle.await;
        for handle in peer_handles {
            let _ = handle.await;
        }

        log::info!("tagsy runtime stopped cleanly");
        Ok(())
    };

    Ok((api, driver))
}

async fn handle_connection(
    configuration: Configuration,
    identity: Arc<Identity>,
    main_db_path: PathBuf,
    context: PeerContext,
    raw_stream: TcpStream,
    address: SocketAddr,
    shutdown: CancellationToken,
) {
    log::debug!("Incoming TCP connection from: {:?}", address);

    let Ok(websoccket_stream) = tokio_tungstenite::accept_async(raw_stream).await else {
        log::error!("Error during the websocket handshake occurred");
        return;
    };

    log::debug!("WebSocket connection established: {:?}", address);

    let (mut outgoing, mut incoming) = websoccket_stream.split();

    // Read the peer's handshake first (they initiated the TCP connection).
    let peer_public_key = match read_handshake(&mut incoming, &configuration, &identity).await {
        HandshakeResult::Accepted(public_key) => public_key,
        HandshakeResult::Rejected => return,
    };

    // Respond: sign the peer's public key to prove we own our private key.
    let response = match identity.sign_handshake(&peer_public_key) {
        Ok(response) => response,
        Err(error) => {
            log::warn!("Failed to build handshake response for {address}: {error}");
            return;
        }
    };

    if let Err(error) = outgoing
        .send(Message::text(serde_json::to_string(&response).unwrap()))
        .await
    {
        log::warn!("Failed to send handshake to {address}: {error}");
        return;
    }

    let peer_name = configuration.peer_name(&peer_public_key).to_owned();

    log::info!("Inbound peer at {address} identified as {peer_name} ({peer_public_key})");

    run_peer_session(
        &peer_public_key,
        &peer_name,
        &main_db_path,
        outgoing,
        incoming,
        operations::Direction::Inbound,
        context,
        &shutdown,
    )
    .await;

    log::info!("Inbound connection from {peer_name} closed");
}

/// Maintain an outbound WebSocket connection to a single peer.
///
/// On each successful connection, a fresh `(peer_tx, peer_rx)` channel is
/// created. `peer_tx` is stored in
/// `RuntimeConfiguration.peers[public_key].outbound` so that `forward_to_peers`
/// can send `Change`s to this peer. When the connection drops, `outbound` is
/// reset to `None` and the task sleeps before retrying.
async fn connect_to_peer(
    identity: Arc<Identity>,
    peer: Peer,
    main_db_path: PathBuf,
    context: PeerContext,
    shutdown: CancellationToken,
) {
    // TODO: Make this configurable.
    const RETRY_INTERVAL: Duration = Duration::from_secs(5);

    let Some((ip, port)) = peer.address else {
        // Caller should have filtered these out, but be defensive.
        return;
    };
    let url = format!("ws://{ip}:{port}");

    loop {
        if shutdown.is_cancelled() {
            return;
        }

        log::debug!("Attempting outbound connection to {} ({url})", peer.name);
        // Surface the connection attempt as a live operation. It resolves when
        // we hand off to `run_peer_session` (completed) or the attempt fails
        // (the handle is dropped -> aborted).
        let connecting = context
            .operations
            .begin(operations::OperationKind::connecting_to_peer(
                peer.name.clone(),
                url.clone(),
            ));
        let connect = tokio::select! {
            _ = shutdown.cancelled() => return,
            connect = tokio_tungstenite::connect_async(&url) => connect,
        };
        match connect {
            Ok((ws_stream, _response)) => {
                log::info!("Outbound connection established to {} ({url})", peer.name);

                let (mut outgoing, mut incoming) = ws_stream.split();

                // Build our handshake: sign the peer's public key to prove our identity.
                let handshake = match identity.sign_handshake(&peer.public_key) {
                    Ok(handshake) => handshake,
                    Err(error) => {
                        log::error!("Cannot build handshake for peer {}: {error}", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };

                // Send our handshake first.
                if let Err(error) = outgoing
                    .send(Message::text(serde_json::to_string(&handshake).unwrap()))
                    .await
                {
                    log::warn!("Failed to send handshake to {}: {error}", peer.name);
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }

                // Read their response.
                let received = match incoming.next().await {
                    Some(Ok(message)) => message.to_string(),
                    Some(Err(error)) => {
                        log::warn!("Handshake read error from {}: {error}", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                    None => {
                        log::warn!("Peer {} closed before sending handshake", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };
                let response: HandshakeMessage = match serde_json::from_str(&received) {
                    Ok(response) => response,
                    Err(error) => {
                        log::warn!("Invalid handshake JSON from {}: {error}", peer.name);
                        tokio::time::sleep(RETRY_INTERVAL).await;
                        continue;
                    }
                };

                // Verify their public key matches what we expect.
                if response.public_key != peer.public_key {
                    log::warn!(
                        "Peer {} announced public_key {:?}, expected {:?}; dropping connection",
                        peer.name,
                        response.public_key,
                        peer.public_key
                    );
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }

                // Verify their signature proves ownership of that public key.
                if let Err(error) = identity.verify_handshake(&response) {
                    log::warn!(
                        "Peer {} handshake verification failed ({error}); dropping connection",
                        peer.name
                    );
                    tokio::time::sleep(RETRY_INTERVAL).await;
                    continue;
                }

                // Connected: the attempt operation is done. The session's own
                // `PeerConnected` operation now represents the live link.
                connecting.complete();

                run_peer_session(
                    &peer.public_key,
                    &peer.name,
                    &main_db_path,
                    outgoing,
                    incoming,
                    operations::Direction::Outbound,
                    context.clone(),
                    &shutdown,
                )
                .await;

                log::info!("Outbound connection to {} dropped", peer.name);
            }
            Err(error) => {
                log::debug!("Outbound connection to {url} failed: {error}");
            }
        }

        if shutdown.is_cancelled() {
            return;
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(RETRY_INTERVAL) => {}
        }
    }
}

enum HandshakeResult {
    Accepted(String),
    Rejected,
}

async fn read_handshake(
    incoming: &mut SplitStream<WebSocketStream<TcpStream>>,
    configuration: &Configuration,
    identity: &Identity,
) -> HandshakeResult {
    let Some(first) = incoming.next().await else {
        log::warn!("Peer closed before sending handshake");
        return HandshakeResult::Rejected;
    };
    let first = match first {
        Ok(message) => message.to_string(),
        Err(error) => {
            log::warn!("Handshake read error: {error}");
            return HandshakeResult::Rejected;
        }
    };
    let message: HandshakeMessage = match serde_json::from_str(&first) {
        Ok(message) => message,
        Err(error) => {
            log::warn!("Invalid handshake JSON: {error}");
            return HandshakeResult::Rejected;
        }
    };

    if !configuration
        .peers
        .iter()
        .any(|peer| peer.public_key == message.public_key)
    {
        log::warn!(
            "Rejecting connection: unknown public_key {:?}",
            message.public_key
        );
        return HandshakeResult::Rejected;
    }

    // Verify the peer's signature proves ownership of that public key.
    match identity.verify_handshake(&message) {
        Ok(peer_public_key) => HandshakeResult::Accepted(peer_public_key),
        Err(error) => {
            log::warn!("Peer handshake verification failed ({error}); rejecting connection");
            HandshakeResult::Rejected
        }
    }
}

/// Drive a fully-handshaken WebSocket connection until it closes.
///
/// Shared between inbound (`handle_connection`) and outbound
/// (`connect_to_peer`) paths because the post-handshake behavior is identical:
/// build and send our manifest, register an outbound channel, then loop over
/// outbound `Frame`s and inbound WebSocket frames.
///
/// Opens its own read-only handle on the main DB. The DB is shared with
/// `handle_changes` and with other connection tasks; SQLite serializes these
/// accesses at the file level. Writes still only happen from `handle_changes`.
#[allow(clippy::too_many_arguments)]
async fn run_peer_session<S>(
    peer_public_key: &str,
    peer_name: &str,
    main_db_path: &std::path::Path,
    mut outgoing: SplitSink<WebSocketStream<S>, Message>,
    mut incoming: SplitStream<WebSocketStream<S>>,
    direction: operations::Direction,
    context: PeerContext,
    shutdown: &CancellationToken,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let PeerContext {
        runtime_configuration,
        pending_fetches,
        pending_previews,
        change_sender,
        command_sender,
        can_generate_previews,
        verified_hashes,
        operations,
    } = context;

    // The steady-state "connected to this peer" operation. Held for the life of
    // the session; dropped (its terminal `Aborted`/`Completed`) when the
    // session ends. We `complete` it on a clean close below.
    let _peer_connected = operations.begin(operations::OperationKind::peer_connected(
        peer_name,
        peer_public_key,
        direction,
    ));

    // CatalogStore wraps a rusqlite Connection which is Send but not Sync.
    // We must never hold `&CatalogStore` across an `.await` in this task,
    // otherwise tokio::spawn rejects the future as non-Send. All sync helpers
    // below take `&CatalogStore` synchronously and return owned data; this
    // function does the awaits separately.
    //
    // The database path is supplied by the caller. This connection is
    // READ-ONLY: the session makes reconciliation decisions from it but routes
    // every write through `change_sender` to `handle_changes`, the sole
    // main-database writer.
    let database = match CatalogStore::initialize(main_db_path) {
        Ok(database) => database,
        Err(error) => {
            log::error!("Peer {peer_name}: failed to open main DB for session: {error:?}");
            return;
        }
    };

    let (peer_tx, mut peer_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
    // Sentinel clone retained for the lifetime of the session: we use
    // `same_channel` against the slot in `RuntimePeer.outbound` to know whether
    // the sender currently parked there is still ours (vs. one a sibling
    // session installed after we registered).
    let our_sender = peer_tx.clone();

    // Completed receives report their outcome here; the select loop drains it to
    // materialize the bytes. There is no per-transfer demux table any more:
    // inbound `ChunkData`/`ChunkMiss` are routed by the content-keyed relay
    // (`pending_fetches`), not by a session-scoped id.
    let (receiver_done_tx, mut receiver_done_rx) =
        tokio::sync::mpsc::unbounded_channel::<(ReceiverPurpose, ReceiveOutcome)>();

    // Temp directory for in-flight received files. Kept per-session under the
    // system temp dir; a completed receive's temp file is then materialized
    // (moved) into the sync directories.
    let transfer_temp_dir = std::env::temp_dir().join(format!(
        "tagsy-transfer-{}-{}",
        std::process::id(),
        peer_public_key
    ));

    // Start a content-addressed receive of `file_id`/`content_hash` from *this*
    // peer (the announcing origin / the peer we're reconciling with), tagged
    // with `purpose` (what to do with the bytes once received). The receive
    // sources each chunk through the content-keyed relay, directing the first
    // request toward this peer; a later chunk central has caught up on is served
    // by it like any other holder — no session to renegotiate. The outcome is
    // forwarded onto `receiver_done_rx`.
    let start_pull = {
        let pending_fetches = pending_fetches.clone();
        let receiver_done_tx = receiver_done_tx.clone();
        let transfer_temp_dir = transfer_temp_dir.clone();
        let operations = operations.clone();
        let peer_name = peer_name.to_owned();
        let peer_public_key = peer_public_key.to_owned();

        move |file_id: FileId,
              content_hash: String,
              expected_size: u64,
              purpose: ReceiverPurpose| {
            let temp_path = transfer_temp_dir.join(uuid::Uuid::new_v4().to_string());

            // Surface this pull as a live "receiving file" operation with byte
            // progress. The handle lives on the bridge task below and reaches a
            // terminal state from the receive outcome.
            let receiving = operations.begin(operations::OperationKind::receiving_file(
                file_id, &peer_name,
            ));
            let progress = {
                let operations = operations.clone();
                let id = receiving.id();
                Box::new(move |done: u64, total: Option<u64>| {
                    operations.report_progress(id, done, total);
                }) as transfer::ProgressSink
            };

            let outcome_rx = spawn_content_receive(
                &pending_fetches,
                file_id,
                content_hash,
                expected_size,
                temp_path,
                Some(peer_public_key.clone()),
                Some(progress),
            );

            let done_tx = receiver_done_tx.clone();
            tokio::spawn(async move {
                if let Ok(outcome) = outcome_rx.await {
                    match &outcome {
                        ReceiveOutcome::Complete(_) => receiving.complete(),
                        ReceiveOutcome::Failed(error) => receiving.fail(error.to_string()),
                    }
                    let _ = done_tx.send((purpose, outcome));
                }
                // If `outcome_rx` closed without a value, `receiving` drops
                // here and the operation is marked aborted.
            });
        }
    };

    if let Err(error) = tokio::fs::create_dir_all(&transfer_temp_dir).await {
        log::warn!(
            "Failed to create transfer temp dir for {peer_name}: {error}; transfers to this peer \
             will fail"
        );
    }

    // Register our outbound sender so `forward_to_peers` can route live
    // changes through this connection.
    //
    // The slot can hold one of three things:
    // - `None`: free, install our sender, we own it.
    // - `Some(dead)`: a previous session's sender whose receiver has been dropped
    //   (this happens because the cleanup at the end of a session cannot detect "I
    //   am the dropped receiver"; `is_closed` returns false while we still hold our
    //   own receiver). We replace it transparently.
    // - `Some(live)`: a sibling session is actively running for this peer (e.g.
    //   both sides dialed each other at the same time). Fall back to inbound-only
    //   so we don't double-send.
    // Command channel for `handle_changes` to trigger byte pulls on this link.
    let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel::<bus::PeerCommand>();

    let owns_outbound = {
        let mut runtime = runtime_configuration.write().await;

        match runtime.peers.get_mut(peer_public_key) {
            Some(runtime_peer) => {
                let slot_is_dead = runtime_peer
                    .outbound
                    .as_ref()
                    .map(|sender| sender.is_closed())
                    .unwrap_or(true);

                if slot_is_dead {
                    runtime_peer.outbound = Some(peer_tx);
                    runtime_peer.commands = Some(command_tx);
                    true
                } else {
                    log::debug!(
                        "Peer {peer_name} already has an outbound sender; inbound-only mode for \
                         this connection"
                    );
                    false
                }
            }
            None => {
                log::error!(
                    "Peer {peer_name} missing from RuntimeConfiguration; dropping connection"
                );
                return;
            }
        }
    };

    // Announce our *tag* manifest first thing post-handshake, before the file
    // manifest. Ordering is deliberate and matters for placement efficiency:
    //
    // - Frames travel over one ordered link, so the peer handles our `TagManifest`
    //   before our `Manifest`.
    // - Handling `TagManifest` enqueues the `FileTagged`/`FileUntagged`
    //   relationships onto the change bus; handling `Manifest` starts file pull
    //   *transfers* whose `Materialize` is only enqueued once the bytes finish
    //   arriving (many round-trips later).
    // - `handle_changes` is a single FIFO consumer, so relationships enqueued first
    //   are applied before any later `Materialize`.
    //
    // Net effect: when a peer brings both new tags and new files, the tags are
    // in place by the time files materialize, so each file lands in its
    // matching TagBased directories on the *first* placement — avoiding the
    // re-placement copy that `ReconcileTagPlacement` would otherwise perform
    // (STREAMING_FOLLOWUPS §1.3). That fix still guarantees *correctness*
    // regardless of order; this ordering is purely the efficiency win.
    //
    // Relationship rows carry no FK on the tag definition (`entries` table), so
    // applying `FileTagged` before the corresponding `TagAdded` definition
    // (which may still be in flight via `TagRequest`) is safe.
    match build_local_tag_manifest(&database) {
        Ok((definitions, relationships)) => {
            let frame = Frame::Sync(SyncMessage::TagManifest {
                definitions,
                relationships,
            });
            if let Err(error) = send_frame(&mut outgoing, &frame).await {
                log::warn!("Failed to send initial tag manifest to {peer_name}: {error}");
                clear_outbound_if_owned(
                    &runtime_configuration,
                    peer_public_key,
                    owns_outbound,
                    &our_sender,
                )
                .await;
                return;
            }
            log::debug!("Sent initial tag manifest to {peer_name}");
        }
        Err(error) => {
            log::error!("Peer {peer_name}: failed to build initial tag manifest: {error:?}");
        }
    }

    // Send our file manifest right after the tag manifest (see the ordering
    // rationale above). The peer compares it against their own history and
    // requests anything they need.
    match build_local_manifest(&database) {
        Ok(manifest) => {
            let frame = Frame::Sync(SyncMessage::Manifest { entries: manifest });
            if let Err(error) = send_frame(&mut outgoing, &frame).await {
                log::warn!("Failed to send initial manifest to {peer_name}: {error}");
                clear_outbound_if_owned(
                    &runtime_configuration,
                    peer_public_key,
                    owns_outbound,
                    &our_sender,
                )
                .await;
                return;
            }
            log::debug!("Sent initial manifest to {peer_name}");
        }
        Err(error) => {
            log::error!("Peer {peer_name}: failed to build initial manifest: {error:?}");
            // Continue without manifest; the peer's manifest still drives
            // anything they need to receive from us.
        }
    }

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::info!("Shutdown requested; closing session with {peer_name}");
                break;
            }
            outbound = peer_rx.recv() => {
                let Some(frame) = outbound else {
                    // Sender dropped (cleared during teardown or replaced).
                    break;
                };
                if let Err(error) = send_frame(&mut outgoing, &frame).await {
                    log::warn!("Outbound send to {peer_name} failed: {error}");
                    break;
                }
            }
            command = command_rx.recv() => {
                let Some(command) = command else {
                    // All command senders dropped (peer removed from runtime).
                    continue;
                };
                match command {
                    bus::PeerCommand::StartReceive {
                        file_id,
                        content_hash,
                        expected_size,
                        placement,
                    } => {
                        // `handle_changes` recorded a live change this peer
                        // announced and wants its bytes. Pull them over this
                        // link; materialize (and record the version) on
                        // completion.
                        let purpose = ReceiverPurpose {
                            file_id,
                            content_hash: content_hash.clone(),
                            origin: ChangeOrigin::Peer {
                                public_key: peer_public_key.to_owned(),
                            },
                            placement,
                        };
                        start_pull(file_id, content_hash, expected_size, purpose);
                    }
                }
            }
            completed = receiver_done_rx.recv() => {
                let Some((purpose, outcome)) = completed else {
                    // The done channel is never fully dropped while the session
                    // lives (we hold a sender clone), so `None` only at teardown.
                    continue;
                };
                let content = match outcome {
                    ReceiveOutcome::Complete(content) => content,
                    ReceiveOutcome::Failed(error) => {
                        log::warn!("Receive from {peer_name} failed: {error}");
                        continue;
                    }
                };
                let ReceiverPurpose {
                    file_id,
                    content_hash,
                    origin,
                    placement,
                } = purpose;
                log::debug!(
                    "Receive from {peer_name} completed for {}; materializing",
                    file_id.to_string()
                );
                if let Err(error) = change_sender.send(DaemonMessage::Materialize {
                    file_id,
                    content,
                    content_hash,
                    origin,
                    placement,
                }) {
                    log::error!(
                        "change_sender closed; cannot materialize receive for {}: {error}",
                        file_id.to_string()
                    );
                    break;
                }
            }
            inbound = incoming.next() => {
                let Some(message) = inbound else {
                    log::info!("Peer {peer_name} closed the connection");
                    break;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        log::warn!("Read error from {peer_name}: {error}");
                        break;
                    }
                };
                // Ignore WebSocket control frames (ping/pong/close); only
                // data frames carry a `Frame`. Peer `Frame`s are MessagePack
                // (see `send_frame`).
                let payload = match &message {
                    Message::Binary(bytes) => bytes.as_ref(),
                    Message::Text(text) => text.as_bytes(),
                    _ => continue,
                };
                let frame: Frame = match rmp_serde::from_slice(payload) {
                    Ok(frame) => frame,
                    Err(error) => {
                        log::error!(
                            "Failed to deserialize inbound Frame from {peer_name}: {error}"
                        );
                        continue;
                    }
                };
                match frame {
                    Frame::Change(change) => {
                        if let Err(error) = change_sender.send(DaemonMessage::Change(
                            Ingest::from_change(change),
                            ChangeOrigin::Peer {
                                public_key: peer_public_key.to_owned(),
                            },
                        )) {
                            log::error!(
                                "change_sender closed; cannot dispatch inbound Change: {error}"
                            );
                            break;
                        }
                    }
                    Frame::Sync(SyncMessage::Manifest { entries }) => {
                        // Confirm the peer is registered (so the content
                        // receives below have a live link to drive) before running the
                        // synchronous reconciliation. Doing the DB work outside
                        // of any held `RwLockReadGuard` keeps this future `Send`
                        // (CatalogStore isn't Sync).
                        let peer_registered = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone())
                            .is_some();

                        if !peer_registered {
                            log::warn!(
                                "No outbound channel registered for {peer_name}; \
                                 cannot reconcile manifest"
                            );
                            continue;
                        }

                        // Capture the announced file_ids before `plan_file_sync`
                        // consumes `entries`; used for the placement sweep below.
                        let announced_file_ids: Vec<FileId> =
                            entries.iter().map(|entry| entry.file_id).collect();

                        let reconciling = operations.begin(
                            operations::OperationKind::reconciling_manifest(peer_name),
                        );
                        let SyncPlan {
                            pulls,
                            deletions,
                            restores,
                            moves,
                        } = plan_file_sync(peer_name, entries, &database);
                        reconciling.complete();

                        // Apply peer deletions that won last-writer-wins by
                        // enqueuing them through the sole DB writer.
                        for PeerDeletion {
                            file_id,
                            deleted_at,
                        } in deletions
                        {
                            if let Err(error) =
                                change_sender.send(DaemonMessage::Change(
                                    Ingest::from_change(Change::FileDeleted {
                                        file_id,
                                        deleted_at,
                                    }),
                                    ChangeOrigin::Peer {
                                        public_key: peer_public_key.to_owned(),
                                    },
                                ))
                            {
                                log::error!(
                                    "Reconciliation: failed to enqueue delete for {} \
                                     announced by {peer_name}: {error}",
                                    file_id.to_string()
                                );
                            }
                        }

                        // Apply peer restores that won last-writer-wins by
                        // enqueuing them as `Change::FileRestored` through the
                        // sole DB writer. This reuses the live-restore handler
                        // (three-way LWW guard, tombstone clear, byte pull,
                        // forward) — the offline-restore catch-up.
                        for PeerRestore {
                            file_id,
                            restored_at,
                            content_hash,
                            size,
                        } in restores
                        {
                            if let Err(error) =
                                change_sender.send(DaemonMessage::Change(
                                    Ingest::from_change(Change::FileRestored {
                                        file_id,
                                        content_hash,
                                        size,
                                        restored_at,
                                    }),
                                    ChangeOrigin::Peer {
                                        public_key: peer_public_key.to_owned(),
                                    },
                                ))
                            {
                                log::error!(
                                    "Reconciliation: failed to enqueue restore for {} \
                                     announced by {peer_name}: {error}",
                                    file_id.to_string()
                                );
                            }
                        }

                        // Apply peer moves that won last-writer-wins by enqueuing
                        // them as `Change::FileMoved` through the sole DB writer.
                        // This reuses the live-move handler, which re-applies the
                        // LWW guard, repositions the bytes in matching sync
                        // directories, and forwards. This is the offline-move
                        // catch-up: a rename made while we were disconnected.
                        for PeerMove {
                            file_id,
                            logical_path,
                            modified_at,
                        } in moves
                        {
                            if let Err(error) =
                                change_sender.send(DaemonMessage::Change(
                                    Ingest::from_change(Change::FileMoved {
                                        file_id,
                                        logical_path,
                                        modified_at,
                                    }),
                                    ChangeOrigin::Peer {
                                        public_key: peer_public_key.to_owned(),
                                    },
                                ))
                            {
                                log::error!(
                                    "Reconciliation: failed to enqueue move for {} \
                                     announced by {peer_name}: {error}",
                                    file_id.to_string()
                                );
                            }
                        }

                        // Files we are pulling as a result of catalog reconciliation
                        // (below); excluded from the placement sweep so we do not
                        // double-fetch them.
                        let pulling: HashSet<FileId> =
                            pulls.iter().map(|pull| pull.file_id).collect();
                        // Start a content-addressed receive for each wanted
                        // file, directing chunk requests toward this peer; it
                        // serves the canonical chunks it holds and any other
                        // holder can serve the rest via the relay. `placement`
                        // is `Create` for files we've never seen (using the
                        // manifest's `logical_path`) and `Change` for files we
                        // already know — see `plan_file_sync`.
                        for MissingContent {
                            file_id,
                            content_hash,
                            size,
                            logical_path_modified_at,
                            placement,
                        } in pulls
                        {
                            // Resolve the file's logical identity for the catalog
                            // write: from the placement for a `Create` (the file
                            // is new to us), or from the DB for a `Change` (we
                            // already know it). A missing logical path for a
                            // `Change` should not happen, but if it does we skip.
                            let logical_path = match &placement {
                                bus::MaterializePlacement::Create { logical_path, .. } => {
                                    logical_path.clone()
                                }
                                bus::MaterializePlacement::Change => {
                                    match database.logical_path_for_file_id(file_id) {
                                        Ok(logical_path) => logical_path,
                                        Err(error) => {
                                            log::error!(
                                                "Reconciliation: no logical path for known \
                                                 file {} ({error:?}); skipping",
                                                file_id.to_string()
                                            );
                                            continue;
                                        }
                                    }
                                }
                            };

                            // Hand the catalog write (files row + version) to
                            // `handle_changes`, the sole main-DB writer, rather
                            // than writing on this session's own connection. The
                            // byte pull below is a transfer, not a DB write, so it
                            // stays here. `file_versions` is byte-independent, so
                            // cataloging happens whether or not the pull completes.
                            if let Err(error) = change_sender.send(DaemonMessage::CatalogFile {
                                file_id,
                                logical_path,
                                logical_path_modified_at,
                                content_hash: content_hash.clone(),
                                size: size as u64,
                                origin: ChangeOrigin::Peer {
                                    public_key: peer_public_key.to_owned(),
                                },
                            }) {
                                log::error!(
                                    "Reconciliation: failed to enqueue catalog write for {} \
                                     announced by {peer_name}: {error}",
                                    file_id.to_string()
                                );
                                continue;
                            }
                            let purpose = ReceiverPurpose {
                                file_id,
                                content_hash: content_hash.clone(),
                                origin: ChangeOrigin::Peer {
                                    public_key: peer_public_key.to_owned(),
                                },
                                placement,
                            };
                            start_pull(file_id, content_hash, size as u64, purpose);
                        }

                        // Placement sweep: for every announced file whose catalog
                        // version already matched (so it was NOT in `wanted` and no
                        // pull was started), ask `handle_changes` to re-run tag
                        // placement. If a local TagBased sync directory now wants
                        // the file but we do not hold the bytes, that fetches them
                        // on demand — the connect-time counterpart to the live
                        // `FileTagged` recovery path. Files we are already pulling
                        // are skipped to avoid double-fetching.
                        //
                        // We deliberately hand this to `handle_changes` via the bus
                        // rather than fetching here: the fetch floods
                        // `ChunkRequest`s and awaits `ChunkData` replies that
                        // arrive as inbound frames on *this* session's select loop.
                        // Awaiting inline would block that loop and deadlock the
                        // fetch. Note: tag relationships from the peer's
                        // `TagManifest` apply asynchronously, so files not yet
                        // tagged are covered later by the live `FileTagged`
                        // handler; this sweep proactively covers files already
                        // tagged locally (e.g. from a prior session).
                        for file_id in announced_file_ids {
                            if pulling.contains(&file_id) {
                                continue;
                            }
                            if let Err(error) = change_sender
                                .send(DaemonMessage::ReconcilePlacement { file_id })
                            {
                                log::error!(
                                    "Reconciliation: failed to enqueue placement sweep \
                                     for {}: {error}",
                                    file_id.to_string()
                                );
                            }
                        }
                    }
                    // A peer asks us for the canonical chunk at `offset` of
                    // `file_id`/`content_hash`. If a local source (a sync
                    // directory or a temporary provider) verifies against
                    // `content_hash`, answer `ChunkData` directly; otherwise
                    // relay the request to our other neighbours (the relay fans
                    // the eventual reply back). A relay holds no bytes.
                    Frame::Sync(SyncMessage::ChunkRequest {
                        file_id,
                        content_hash,
                        offset,
                    }) => {
                        let short = content_hash.get(..8).unwrap_or(&content_hash);
                        log::debug!(
                            "peer[{peer_name}] <- ChunkRequest {} [{short}] offset={offset}",
                            file_id.to_string()
                        );
                        let answer = answer_local_chunk(
                            &command_sender,
                            &pending_fetches,
                            &verified_hashes,
                            file_id,
                            &content_hash,
                            offset,
                        )
                        .await;

                        match answer {
                            Some(ChunkAnswer::Data(bytes)) => {
                                log::debug!(
                                    "peer[{peer_name}] -> ChunkData [{short}] offset={offset} ({} bytes) served locally",
                                    bytes.len()
                                );
                                let _ = our_sender.send(Frame::Sync(SyncMessage::ChunkData {
                                    file_id,
                                    content_hash,
                                    offset,
                                    bytes,
                                }));
                            }
                            // We hold the content but it does not verify (an
                            // impossible-for-a-consistent-catalog case) — treat
                            // as absent and relay so another holder can serve it.
                            Some(ChunkAnswer::Miss) | None => {
                                log::debug!(
                                    "peer[{peer_name}]: [{short}] offset={offset} not served locally; relaying"
                                );
                                pending_fetches
                                    .relay_chunk_request(
                                        peer_public_key,
                                        file_id,
                                        content_hash,
                                        offset,
                                    )
                                    .await;
                            }
                        }
                    }
                    // Reply bytes arriving from an upstream: fan to every
                    // downstream waiter (local receives and relayed peers) for
                    // this key via the content-keyed table.
                    Frame::Sync(SyncMessage::ChunkData {
                        file_id,
                        content_hash,
                        offset,
                        bytes,
                    }) => {
                        log::debug!(
                            "peer[{peer_name}] <- ChunkData {} [{}] offset={offset} ({} bytes)",
                            file_id.to_string(),
                            content_hash.get(..8).unwrap_or(&content_hash),
                            bytes.len()
                        );
                        pending_fetches
                            .handle_chunk_data(file_id, content_hash, offset, bytes)
                            .await;
                    }
                    Frame::Sync(SyncMessage::ChunkMiss {
                        file_id,
                        content_hash,
                        offset,
                    }) => {
                        log::debug!(
                            "peer[{peer_name}] <- ChunkMiss {} [{}] offset={offset}",
                            file_id.to_string(),
                            content_hash.get(..8).unwrap_or(&content_hash)
                        );
                        pending_fetches
                            .handle_chunk_miss(peer_public_key, file_id, content_hash, offset)
                            .await;
                    }
                    Frame::Sync(SyncMessage::TagManifest {
                        definitions,
                        relationships,
                    }) => {
                        // Relationships carry their whole state (including the
                        // soft-delete flag), so apply them directly via the bus
                        // — last-writer-wins is enforced in the DB layer. For
                        // definitions, request the full payload of any the peer
                        // has newer than (or that are unknown to) us.
                        let outbound = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone());

                        let Some(outbound) = outbound else {
                            log::warn!(
                                "Peer {peer_name} is not connected; \
                                 not responding to TagManifest"
                            );
                            continue;
                        };

                        let reconciling_tags = operations
                            .begin(operations::OperationKind::reconciling_tags(peer_name));

                        plan_tag_sync(
                            peer_name,
                            peer_public_key,
                            definitions,
                            relationships,
                            &database,
                            &outbound,
                            &change_sender,
                        );
                        reconciling_tags.complete();
                    }
                    Frame::Sync(SyncMessage::TagRequest { tag_id }) => {
                        // Answer with the full tag definition as a
                        // `Change::TagAdded`. `TagNotFound` if we no longer hold
                        // the tag.
                        let outbound = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone());

                        let Some(outbound) = outbound else {
                            log::warn!(
                                "Peer {peer_name} is not connected; \
                                 not responding to TagRequest"
                            );
                            continue;
                        };

                        let frame = build_tag_request_response(peer_name, tag_id, &database);

                        if let Err(error) = outbound.send(frame) {
                            log::warn!(
                                "Failed to enqueue tag Sync response for {peer_name}: {error}"
                            );
                        }
                    }
                    Frame::Sync(SyncMessage::TagNotFound { tag_id }) => {
                        log::warn!(
                            "Peer {peer_name} reported TagNotFound for tag {}",
                            tag_id.to_string()
                        );
                    }
                    Frame::Sync(SyncMessage::PreviewRequest {
                        file_id,
                        content_hash,
                    }) => {
                        // Answer a peer's preview request in three tiers:
                        //   1. Cache hit — serve it. Always available (a DB read,
                        //      no generation support needed), so even a `Never`
                        //      device serves previews it fetched earlier.
                        //   2. Cache miss + we can generate + bytes are local —
                        //      generate, serve (and it gets cached when we, or
                        //      the requester, next resolve it).
                        //   3. Otherwise — relay across the tree.
                        // We answer `PreviewData` even for `Preview::None`: a
                        // holder that has the bytes but they are un-previewable
                        // is authoritative, so downstream caches that negative
                        // result rather than re-asking.
                        let short = content_hash.get(..8).unwrap_or(&content_hash);

                        // Tier 1: our cache. `preview_for` is a read on this
                        // session's read-only DB handle.
                        let cached = match database.preview_for(file_id, &content_hash) {
                            Ok(cached) => cached,
                            Err(error) => {
                                log::debug!(
                                    "peer[{peer_name}]: [{short}] preview cache lookup failed: \
                                     {error:?}; treating as miss"
                                );
                                None
                            }
                        };

                        if let Some(preview) = cached {
                            log::debug!(
                                "peer[{peer_name}]: served cached PreviewData {} [{short}]",
                                file_id.to_string()
                            );
                            let _ = our_sender.send(Frame::Sync(SyncMessage::PreviewData {
                                file_id,
                                content_hash,
                                preview,
                            }));
                            continue;
                        }

                        // Tier 2: generate from local bytes, if this device may
                        // generate and holds the content. Delegated to a
                        // `cfg`-gated helper so the feature-specific machinery
                        // (and its use of `can_generate_previews` /
                        // `command_sender`) lives outside this match arm and
                        // compiles away entirely without the feature.
                        //
                        // The extension (a type-detection hint) is looked up
                        // here from the session's DB handle; only meaningful
                        // when we can generate.
                        #[cfg(feature = "preview-generation")]
                        let extension = if can_generate_previews {
                            preview_extension_for(&database, file_id)
                        } else {
                            None
                        };
                        #[cfg(not(feature = "preview-generation"))]
                        let extension: Option<String> = None;
                        let served = try_serve_generated_preview(
                            &our_sender,
                            &command_sender,
                            can_generate_previews,
                            peer_name,
                            file_id,
                            &content_hash,
                            extension,
                        )
                        .await;

                        // Tier 3: relay to other neighbours.
                        if !served {
                            log::debug!(
                                "peer[{peer_name}]: [{short}] preview not served locally; relaying"
                            );
                            pending_previews
                                .relay_preview_request(peer_public_key, file_id, content_hash)
                                .await;
                        }
                    }
                    Frame::Sync(SyncMessage::PreviewData {
                        file_id,
                        content_hash,
                        preview,
                    }) => {
                        pending_previews
                            .handle_preview_data(file_id, content_hash, preview)
                            .await;
                    }
                    Frame::Sync(SyncMessage::PreviewMiss {
                        file_id,
                        content_hash,
                    }) => {
                        pending_previews
                            .handle_preview_miss(peer_public_key, file_id, content_hash)
                            .await;
                    }
                }
            }
        }
    }

    clear_outbound_if_owned(
        &runtime_configuration,
        peer_public_key,
        owns_outbound,
        &our_sender,
    )
    .await;

    // This link is gone: prune it from the relay's waiter table so any chunk
    // key that was only reachable through it fails its downstream waiters
    // (rather than hanging until the TTL) and any request it was waiting on is
    // forgotten.
    pending_fetches.prune_link(peer_public_key).await;
    // Same for the preview relay: any preview key only reachable through this
    // link fails its downstream waiters (resolving them to `None`) rather than
    // hanging until the TTL.
    pending_previews.prune_link(peer_public_key).await;
}

async fn send_frame<S>(
    outgoing: &mut SplitSink<WebSocketStream<S>, Message>,
    frame: &Frame,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // Peer `Frame`s are encoded as MessagePack and sent as binary WebSocket
    // frames. This avoids serde_json's `Vec<u8>` -> array-of-integers blowup
    // (~4x on the wire), which dominated the payload for file transfers.
    let bytes = rmp_serde::to_vec_named(frame).map_err(|e| format!("serialize: {e}"))?;
    outgoing
        .send(Message::binary(bytes))
        .await
        .map_err(|e| format!("send: {e}"))
}

async fn clear_outbound_if_owned(
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    peer_public_key: &str,
    owns_outbound: bool,
    our_sender: &UnboundedSender<Frame>,
) {
    if !owns_outbound {
        return;
    }
    let mut runtime = runtime_configuration.write().await;
    if let Some(runtime_peer) = runtime.peers.get_mut(peer_public_key)
        && let Some(current) = runtime_peer.outbound.as_ref()
        && current.same_channel(our_sender)
    {
        // The slot still holds the sender we installed (no sibling session
        // replaced it). Drop it so the next session sees a free slot.
        //
        // We deliberately do not check `is_closed()` here: that check is
        // unreliable while we (the receiver's owner) are still alive, and
        // pointless once we're not. Identity via `same_channel` is the only
        // reliable test.
        runtime_peer.outbound = None;
        // The command channel is installed and cleared in lockstep with
        // `outbound` (same owner), so clear it here too.
        runtime_peer.commands = None;
    }
}

/// Read `file_id`'s bytes from local sync directories, but only return them if
/// they hash to `expected_hash`. Used by the on-demand fetch path to satisfy a
/// `DaemonMessage::Fetch` locally before flooding chunk requests to peers.
///
/// Returns `Some(bytes)` on a hash match, `None` if the file is absent locally
/// or its local content does not match the requested hash (in which case the
/// request should be served from peers).
pub(crate) async fn read_local_if_hash_matches(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    file_id: FileId,
    expected_hash: &str,
) -> Option<FileBytes> {
    let (respond_to, response) = tokio::sync::oneshot::channel();

    if command_sender
        .send(SyncDirectoryCommand::ReadFile {
            file_id,
            respond_to,
        })
        .is_err()
    {
        log::error!("command_sender closed; cannot read local bytes for fetch");
        return None;
    }

    match response.await {
        Ok(Some((_physical_path, file_bytes, content_hash))) if content_hash == expected_hash => {
            Some(file_bytes)
        }
        Ok(_) => None,
        Err(error) => {
            log::error!(
                "Directory manager dropped ReadFile responder for {}: {error}",
                file_id.to_string()
            );
            None
        }
    }
}

/// Answer a content-addressed `ChunkRequest` from a **local** source, if this
/// node holds `file_id`/`content_hash`.
///
/// Resolves a source in priority order — a matching file in our sync
/// directories, then a temporary provider (a local client serving on demand,
/// e.g. the CLI uploading) — and serves the canonical chunk at `offset` via
/// [`transfer::answer_chunk_request`].
///
/// The sync-directory case resolves only the file's *path* (via `LocalPath`,
/// which does **not** read or hash the bytes) and lets
/// [`transfer::answer_chunk_request`] verify against `content_hash` through the
/// `verified_hashes` cache — so the file is hashed **once** and every
/// subsequent chunk request is a cache hit plus a bounded seek/read. This keeps
/// serving a large file O(size) rather than O(size²/chunk): the previous
/// `ReadFile`-per-chunk path re-hashed the whole file on *every* request (the
/// cause of large-file download timeouts). A provider is looked up by its
/// `(file_id, content_hash)` registration key, which *is* its verification, and
/// is served **pre-verified** (re-hashing a provider would fire its
/// `on_complete` mid-serve and release the file — see
/// [`transfer::answer_chunk_request`]).
///
/// Returns `Some(ChunkAnswer::Data)` when we served bytes, `Some(Miss)` when we
/// hold the file but it does not match or the offset is malformed, and `None`
/// when no local source holds the file at all (the caller then relays the
/// request to other neighbours).
async fn answer_local_chunk(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    pending_fetches: &PendingFetches,
    verified_hashes: &VerifiedHashCache,
    file_id: FileId,
    content_hash: &str,
    offset: u64,
) -> Option<ChunkAnswer> {
    // 1. A file in our sync directories. Resolve just the path (no hashing);
    // `answer_chunk_request` verifies against `content_hash` via the cache
    // (hashing once, then serving from the cache on subsequent chunks).
    let (respond_to, response) = tokio::sync::oneshot::channel();
    if command_sender
        .send(SyncDirectoryCommand::LocalPath {
            file_id,
            respond_to,
        })
        .is_ok()
        && let Ok(Some(path)) = response.await
    {
        let source = FileBytes::FileToCopy(path.clone());
        return Some(
            transfer::answer_chunk_request(
                &source,
                Some(&path),
                verified_hashes,
                content_hash,
                offset,
                /* pre_verified */ false,
            )
            .await,
        );
    }

    // 2. A temporary provider (CLI upload/edit in flight), trusted by its
    // registration key — served pre-verified so we never re-hash it (which
    // would release the file after the first chunk).
    if let Some(provider) = pending_fetches.provider_for(file_id, content_hash).await {
        return Some(
            transfer::answer_chunk_request(
                &provider,
                None,
                verified_hashes,
                content_hash,
                offset,
                true,
            )
            .await,
        );
    }

    None
}

/// Spawn a **content-addressed receive** of `(file_id, content_hash)` into a
/// fresh temp file, sourcing each chunk through the content-keyed relay.
///
/// Each chunk the receiver wants is routed toward `toward` (the announcing
/// origin / peer we're reconciling with) when `Some`, else flooded across all
/// neighbours. Inbound `ChunkData`/`ChunkMiss` frames are delivered to the
/// receive by the relay's waiter table (which the per-chunk request registered
/// as a `Local` waiter), so no session-scoped demux is needed. The final
/// [`ReceiveOutcome`] is delivered on the returned oneshot.
///
/// This is the single receive entry point shared by live-sync/reconcile pulls
/// (driven by a peer session) and on-demand fetches / deferred placement
/// (driven inside `handle_changes`).
fn spawn_content_receive(
    pending_fetches: &PendingFetches,
    file_id: FileId,
    content_hash: String,
    expected_size: u64,
    temp_path: PathBuf,
    toward: Option<String>,
    progress: Option<transfer::ProgressSink>,
) -> tokio::sync::oneshot::Receiver<ReceiveOutcome> {
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();

    log::debug!(
        "spawn_content_receive: {} [{}] size={expected_size} toward={}",
        file_id.to_string(),
        content_hash.get(..8).unwrap_or(&content_hash),
        toward.as_deref().unwrap_or("<flood>")
    );

    // The receive driver emits `ChunkRequest`s on `req` and awaits `ChunkReply`s
    // on `reply`. The bridge below routes each request through the relay,
    // passing a clone of the reply sender so the relay's waiter table can fan
    // the eventual `ChunkData`/`ChunkMiss` back to this receive.
    let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
    let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

    let pending_fetches_bridge = pending_fetches.clone();
    let content_hash_bridge = content_hash.clone();
    tokio::spawn(async move {
        while let Some(ChunkRequest { offset }) = req_rx.recv().await {
            pending_fetches_bridge
                .request_chunk_local(
                    file_id,
                    content_hash_bridge.clone(),
                    offset,
                    toward.as_deref(),
                    reply_tx.clone(),
                )
                .await;
        }
    });

    tokio::spawn(async move {
        let outcome = match transfer::receive(
            content_hash,
            expected_size,
            temp_path,
            req_tx,
            reply_rx,
            progress,
        )
        .await
        {
            Ok(file_bytes) => ReceiveOutcome::Complete(file_bytes),
            Err(error) => ReceiveOutcome::Failed(error),
        };
        let _ = outcome_tx.send(outcome);
    });

    outcome_rx
}

/// Ask the peer that announced a change (`change_origin`) to serve us its
/// bytes: send a `StartReceive` command to that peer's live session, which owns
/// the receive machinery. No-op if the change is local-origin or the peer has
/// no live session (reconciliation will pick it up on the next connect).
async fn request_pull_from_origin(
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    change_origin: &ChangeOrigin,
    file_id: FileId,
    content_hash: String,
    expected_size: u64,
    placement: bus::MaterializePlacement,
) {
    let ChangeOrigin::Peer { public_key } = change_origin else {
        // Local-origin content already has its bytes; nothing to pull.
        return;
    };
    let commands = runtime_configuration
        .read()
        .await
        .peers
        .get(public_key)
        .and_then(|runtime_peer| runtime_peer.commands.clone());

    match commands {
        Some(commands) => {
            if commands
                .send(bus::PeerCommand::StartReceive {
                    file_id,
                    content_hash,
                    expected_size,
                    placement,
                })
                .is_err()
            {
                log::warn!(
                    "Peer {public_key} command channel closed; cannot pull {}; reconciliation \
                     will retry on reconnect",
                    file_id.to_string()
                );
            }
        }
        None => {
            log::debug!(
                "Announcing peer {public_key} has no live session; deferring pull of {} to \
                 reconciliation",
                file_id.to_string()
            );
        }
    }
}

async fn handle_sync_directories(
    configuration: Configuration,
    paths: Paths,
    last_known_hashes: HashMap<FileId, String>,
    change_sender: UnboundedSender<DaemonMessage>,
    command_receiver: UnboundedReceiver<SyncDirectoryCommand>,
    shutdown: CancellationToken,
) {
    let mut manager =
        SyncDirectoryManager::new(configuration, &paths, change_sender, command_receiver).await;

    tokio::select! {
        _ = shutdown.cancelled() => {
            log::info!("Shutdown requested; stopping sync directory manager");
        }
        _ = manager.run(last_known_hashes) => {}
    }
}

/// On-demand fetch of `(file_id, content_hash)` through the content-keyed
/// relay, flooding across the live peer tree (no preferred direction). Returns
/// the completed temp file, or an error if no reachable holder could serve it.
///
/// Used by `tagsy edit`, deferred TagBased placement, and the restore
/// availability probe. Unlike a live-sync pull (which directs the first request
/// toward the announcing origin), the direction here is unknown, so the first
/// request for each chunk floods; whichever direction answers establishes the
/// route for subsequent chunks.
async fn fetch_via_relay(
    pending_fetches: &PendingFetches,
    file_id: FileId,
    content_hash: String,
    expected_size: u64,
    progress: Option<transfer::ProgressSink>,
) -> Result<FileBytes, bus::FetchError> {
    let temp_dir = std::env::temp_dir().join(format!("tagsy-fetch-{}", std::process::id()));
    if let Err(error) = tokio::fs::create_dir_all(&temp_dir).await {
        log::warn!("Failed to create fetch temp dir: {error}");
    }
    let temp_path = temp_dir.join(uuid::Uuid::new_v4().to_string());
    let short = content_hash.get(..8).unwrap_or(&content_hash).to_owned();
    log::debug!(
        "fetch_via_relay: start {} [{short}] size={expected_size} (flood)",
        file_id.to_string()
    );
    let started = std::time::Instant::now();

    let outcome_rx = spawn_content_receive(
        pending_fetches,
        file_id,
        content_hash,
        expected_size,
        temp_path,
        None,
        progress,
    );

    match outcome_rx.await {
        Ok(ReceiveOutcome::Complete(content)) => {
            log::debug!(
                "fetch_via_relay: {} [{short}] complete in {:?}",
                file_id.to_string(),
                started.elapsed()
            );
            Ok(content)
        }
        Ok(ReceiveOutcome::Failed(error)) => {
            log::warn!(
                "fetch_via_relay: {} [{short}] failed in {:?}: {error}",
                file_id.to_string(),
                started.elapsed()
            );
            Err(bus::FetchError::NotAvailable)
        }
        Err(_) => {
            log::warn!(
                "fetch_via_relay: {} [{short}] receive task dropped",
                file_id.to_string()
            );
            Err(bus::FetchError::NotAvailable)
        }
    }
}

/// Availability probe: does *anyone* in the live peer tree still hold the bytes
/// for `(file_id, content_hash)`? Rather than a separate discovery message this
/// is a single offset-0 `ChunkRequest` routed through the relay (flooding),
/// whose returned bytes are discarded. Any `ChunkData` proves availability;
/// exhaustion (`ChunkMiss` from all directions) or the TTL proves absence.
///
/// Used by restore before clearing a tombstone, so we never announce a restore
/// whose bytes cannot be recovered.
async fn probe_availability(
    pending_fetches: &PendingFetches,
    file_id: FileId,
    content_hash: String,
) -> bool {
    let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();
    // Direction unknown: flood. The relay registers this as a `Local` waiter for
    // offset 0 and forwards to all neighbours.
    pending_fetches
        .request_chunk_local(file_id, content_hash, 0, None, reply_tx)
        .await;

    matches!(reply_rx.recv().await, Some(ChunkReply::Data { .. }))
}

// This is the single change-handling pipeline task; its parameters are the
// distinct long-lived handles it owns (a change receiver, database, event
// sender) plus the routing handles it shares. They don't form a reusable
// cluster the way `PeerContext` does, so they're kept as plain arguments
// rather than bundled into a single-use struct.
//
// EVENT PUBLISHING (KNOWN-SUBOPTIMAL — see the TODO below)
// --------------------------------------------------------
// Applied changes are published to UI-facing API subscribers over
// `event_sender`. The intended single publish site is the fall-through at the
// very bottom of the message loop, but most arms of the loop `continue` before
// reaching it, so each one that must notify the UI has to emit for itself.
// The emit sites are therefore hand-maintained and easy to forget:
//
//   1. bottom-of-loop fall-through   — every `Ingest::Meta` change (this is how
//      a device learns about peer edits)
//   2. `Change::FileRestored` arm    — `continue`s
//   3. `DaemonMessage::AnnounceProvided` arm — `continue`s; this is the local
//      client upload/edit path (`Api::upload_file` / `Api::edit_file`)
//   4. `DaemonMessage::Materialize` arm — `continue`s; "bytes are now on disk"
//   5. `dispatch_and_forward`        — reached from `Ingest::Content`, which
//      `continue`s; sync-directory watcher edits
//
// Arms that deliberately do NOT publish (they change no user-visible catalog
// state): `Fetch`, `GetPreview`, `ApplyPreview`, `PurgePreviews`,
// `ReconcilePlacement`, `CatalogFile`.
//
// TODO: Make publishing structural instead of hand-maintained. Every missing
// emit shows up as a UI that silently serves stale data until the user
// navigates away and back, and the bug is invisible at the call site. Options,
// roughly in increasing order of effort:
//
//   - Restructure the loop body so the publish is unconditional — e.g. extract
//     it into a function returning `Option<Change>` (or a small
//     `Published`/`Silent` enum) that the caller publishes, so `continue`
//     cannot skip it and a new arm has to state its intent explicitly.
//   - Give the event bus its own type instead of reusing the wire `Change`.
//     `Materialize` currently has to re-send the metadata change it already
//     announced because there is no way to say "bytes landed"; a dedicated
//     `ApiEvent`-shaped bus would model catalog-change vs. byte-arrival vs.
//     placement separately and drop the duplicate.
//   - Flatten `ApiEvent` into a real DTO across the bridge. It crosses into
//     Dart as an opaque handle with no accessors (`tagsy-bridge/src/api.rs`),
//     so every screen full-reloads on every change anywhere in the store and
//     cannot filter by `file_id` — mirror what `OperationUpdateDto` already
//     does for the operations stream.
#[allow(clippy::too_many_arguments)]
async fn handle_changes(
    configuration: Configuration,
    tag_rules: Arc<CompiledTagRules>,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    pending_fetches: PendingFetches,
    pending_previews: PendingPreviews,
    mut database: CatalogStore,
    mut change_receiver: tokio::sync::mpsc::UnboundedReceiver<DaemonMessage>,
    change_sender: UnboundedSender<DaemonMessage>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
    event_sender: tokio::sync::broadcast::Sender<Change>,
    operations: crate::operations::Operations,
    shutdown: CancellationToken,
) {
    /// Origin tag stored in `file_versions.origin` for locally-observed
    /// versions. Peer-originated versions will use the originating peer's
    /// public key here instead.
    const LOCAL_ORIGIN: &str = "local";

    /// Resolve the `origin` string to store in `file_versions.origin` for a
    /// `Change` we just received.
    fn version_origin(change_origin: &ChangeOrigin) -> &str {
        match change_origin {
            ChangeOrigin::Local { .. } => LOCAL_ORIGIN,
            ChangeOrigin::Peer { public_key } => public_key.as_str(),
        }
    }

    async fn forward_to_peers(
        configuration: &Configuration,
        runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
        change: &Change,
        change_origin: &ChangeOrigin,
    ) {
        // TODO: Apply per-peer SyncType filtering once it's tracked (step 8).
        let runtime = runtime_configuration.read().await;
        for peer in &configuration.peers {
            if let ChangeOrigin::Peer { public_key } = &change_origin
                && public_key == &peer.public_key
            {
                // Nothing to do, the change originates from this peer.
                continue;
            }

            let Some(runtime_peer) = runtime.peers.get(&peer.public_key) else {
                log::warn!(
                    "Peer {} ({}) missing from RuntimeConfiguration",
                    peer.name,
                    peer.public_key
                );
                continue;
            };

            let Some(outbound) = runtime_peer.outbound.as_ref() else {
                // TODO: Buffer or rely on reconciliation (step 6) when peer reconnects.
                log::debug!("Peer {} not connected; dropping outbound Change", peer.name);
                continue;
            };

            if let Err(error) = outbound.send(Frame::Change(change.clone())) {
                log::warn!("Failed to enqueue Change for peer {}: {error}", peer.name);
            }
        }
    }

    /// Merge the tags [`CompiledTagRules`] assigns to `logical_path` into
    /// `tags`, skipping any the caller already supplied.
    ///
    /// Deduplication is not strictly required for correctness — `tag_file` is
    /// an idempotent last-writer-wins upsert — but a duplicate would be
    /// announced twice to every peer and would show up twice in the outgoing
    /// change's tag list, so it is cheaper to drop it here.
    fn apply_tag_rules(
        tag_rules: &CompiledTagRules,
        logical_path: &LogicalPath,
        tags: &mut Vec<TagId>,
    ) {
        if tag_rules.is_empty() {
            return;
        }

        for tag_id in tag_rules.tags_for(logical_path) {
            if tags.contains(&tag_id) {
                continue;
            }
            log::debug!(
                "Tag rule matched {}: applying tag {}",
                logical_path,
                tag_id.to_string()
            );
            tags.push(tag_id);
        }
    }

    /// Handle a [`ContentChange`] (`FileAdded`/`FileChanged` carrying
    /// [`FileBytes`]): persist the version, dispatch bytes to matching sync
    /// directories, and forward a wire `Change` to peers.
    #[allow(clippy::too_many_arguments)]
    async fn handle_content_change(
        configuration: &Configuration,
        tag_rules: &CompiledTagRules,
        runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
        database: &mut CatalogStore,
        command_sender: &UnboundedSender<SyncDirectoryCommand>,
        change_sender: &UnboundedSender<DaemonMessage>,
        event_sender: &tokio::sync::broadcast::Sender<Change>,
        content_change: ContentChange,
        change_origin: ChangeOrigin,
    ) {
        match content_change {
            ContentChange::FileAdded {
                file_id,
                logical_path,
                content,
                content_hash,
                size,
                mut tags,
            } => {
                // Reconciliation and live edits can both deliver a `FileAdded`
                // for a `file_id` we already know. Branch on existence to stay
                // idempotent (see the historical notes preserved below).
                let already_exists = database.file_exists(file_id).unwrap_or_else(|error| {
                    log::error!(
                        "file_exists check failed for {}: {:?}; assuming new",
                        file_id.to_string(),
                        error
                    );
                    false
                });

                if !already_exists {
                    // Seed the path's LWW clock with our wall clock now: this is
                    // a genuinely local creation, so "now" is the true origin
                    // time. We stamp the same value onto the outgoing
                    // `FileMetadataAdded` (via `WireKind::Added`) so every peer
                    // seeds an identical clock and a later move orders against
                    // the real creation time, not each peer's receive time.
                    let logical_path_modified_at = clock::now_millis();
                    if let Err(error) =
                        database.add_file(file_id, &logical_path, logical_path_modified_at)
                    {
                        // Do not panic: a single bad inbound change must not
                        // take down the sole DB writer.
                        log::error!(
                            "Failed to add file {} ({}): {:?}; skipping change",
                            file_id.to_string(),
                            logical_path,
                            error
                        );
                        return;
                    }

                    if let Err(error) = database.record_version(
                        file_id,
                        &content_hash,
                        version_origin(&change_origin),
                        size as i64,
                    ) {
                        log::error!(
                            "Failed to record initial version for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                    }

                    // Apply the carried tags for a *locally*-ingested file.
                    //
                    // A local `FileAdded` from a TagBased directory carries that
                    // directory's required tags: they are the ground truth for
                    // this new file and must become real file-tag relationships,
                    // both persisted here and propagated to peers as
                    // `FileTagged` changes (so the relationship reconciles the
                    // same way an API `tag_file` would). Peer-originated adds are
                    // NOT tagged here — their relationships arrive via their own
                    // tag manifest / `FileTagged` changes, so applying `tags`
                    // again would restamp `modified_at` and clobber LWW state.
                    //
                    // `tags` is also used below for local dispatch filtering.
                    if let ChangeOrigin::Local { .. } = &change_origin {
                        // Creation-time tag rules, merged *before* the tagging
                        // loop and before `tags` is used for dispatch
                        // filtering (`contains_all_tags`) and for the outgoing
                        // `WireKind::Added`. Order matters: applying them later
                        // would tag the file locally but leave it out of the
                        // `TagBased` sync directories that the new tag makes it
                        // belong to, so the same file would be placed
                        // differently depending on whether its tag came from a
                        // rule or from the caller.
                        //
                        // Inside the `Local` guard because rules run only on
                        // the device that creates a file; an inbound peer file
                        // already carries the tags its origin's rules assigned.
                        apply_tag_rules(tag_rules, &logical_path, &mut tags);

                        for tag_id in &tags {
                            let modified_at = clock::now_millis();
                            if let Err(error) = database.tag_file(*tag_id, file_id, modified_at) {
                                log::error!(
                                    "Failed to tag locally-added file {} with {}: {:?}",
                                    file_id.to_string(),
                                    tag_id.to_string(),
                                    error
                                );
                                continue;
                            }

                            forward_to_peers(
                                configuration,
                                runtime_configuration,
                                &Change::FileTagged {
                                    file_id,
                                    tag_id: *tag_id,
                                    metadata: None,
                                    modified_at,
                                },
                                &change_origin,
                            )
                            .await;
                        }
                    }

                    let mut targets = Vec::new();
                    for sync_directory in &configuration.sync_directories {
                        if let ChangeOrigin::Local { directory_path } = &change_origin
                            && directory_path == &sync_directory.path
                            && let SyncType::TagBased { .. } = &sync_directory.sync_type
                        {
                            continue;
                        };

                        if let SyncType::TagBased {
                            tags: sync_directory_tags,
                        } = &sync_directory.sync_type
                            && !placement::contains_all_tags(sync_directory_tags, &tags)
                        {
                            continue;
                        }

                        let physical_path = sync_directory
                            .sync_type
                            .physical_for(&logical_path, file_id);
                        targets.push(Placement::Create {
                            file_id,
                            physical_path,
                            sync_directory_path: sync_directory.path.clone(),
                        });
                    }

                    dispatch_and_forward(
                        configuration,
                        runtime_configuration,
                        command_sender,
                        event_sender,
                        targets,
                        content,
                        &change_origin,
                        WireKind::Added {
                            file_id,
                            logical_path,
                            logical_path_modified_at,
                            content_hash,
                            size,
                            tags,
                        },
                    )
                    .await;
                    // Bytes just landed in our sync directories: warm the
                    // preview cache now on an eager-preview device.
                    maybe_eager_preview(configuration, change_sender, file_id);
                } else {
                    // Known file: decide by whether this is already the version
                    // we currently hold (latest). Matching an *older* historical
                    // hash is a legitimate revert and must be promoted to a new
                    // version — not ignored. (Materialization echoes are already
                    // suppressed upstream by the directory manager's
                    // already-tracked / skip-queue guards, so this need only
                    // guard against a true no-op re-announcement of the current
                    // content.)
                    let current_hash = database
                        .latest_version(file_id)
                        .unwrap_or_else(|error| {
                            log::error!(
                                "latest_version failed for known file {}: {:?}; treating as no-op",
                                file_id.to_string(),
                                error
                            );
                            None
                        })
                        .map(|version| version.content_hash);

                    if current_hash.as_deref() == Some(content_hash.as_str()) {
                        log::debug!(
                            "Ignoring no-op FileAdded for {} (already the current version)",
                            file_id.to_string()
                        );
                        return;
                    }

                    log::debug!(
                        "Promoting FileAdded for known file {} to FileChanged (new content_hash)",
                        file_id.to_string()
                    );
                    if let Err(error) = database.record_version(
                        file_id,
                        &content_hash,
                        version_origin(&change_origin),
                        size as i64,
                    ) {
                        log::error!(
                            "Failed to record version for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                    }
                    // A new local version supersedes any tombstone (restore
                    // after delete). No-op if not tombstoned.
                    if let Err(error) = database.restore_file(file_id) {
                        log::error!(
                            "Failed to clear tombstone for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                    }

                    let local_file_tags = database
                        .tag_ids_for_file(file_id, store::SubtagRule::Exclude)
                        .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                        .unwrap_or_else(|error| {
                            log::error!(
                                "Failed to read local tags for {}: {:?}",
                                file_id.to_string(),
                                error
                            );
                            Vec::new()
                        });

                    let targets = placement::placements_for(
                        configuration,
                        &change_origin,
                        file_id,
                        &local_file_tags,
                    );
                    dispatch_and_forward(
                        configuration,
                        runtime_configuration,
                        command_sender,
                        event_sender,
                        targets,
                        content,
                        &change_origin,
                        WireKind::Changed {
                            file_id,
                            content_hash,
                            size,
                        },
                    )
                    .await;
                    maybe_eager_preview(configuration, change_sender, file_id);
                }
            }
            ContentChange::FileChanged {
                file_id,
                content,
                content_hash,
                size,
            } => {
                let file_tags = match database.tag_ids_for_file(file_id, store::SubtagRule::Exclude)
                {
                    Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                    Err(error) => {
                        log::error!(
                            "FileChanged: failed to get tags for {}: {:?}; skipping",
                            file_id.to_string(),
                            error
                        );
                        return;
                    }
                };

                if let Err(error) = database.record_version(
                    file_id,
                    &content_hash,
                    version_origin(&change_origin),
                    size as i64,
                ) {
                    log::error!(
                        "Failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }
                // A new local version supersedes any tombstone (restore after
                // delete). No-op if not tombstoned.
                if let Err(error) = database.restore_file(file_id) {
                    log::error!(
                        "Failed to clear tombstone for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }

                let targets =
                    placement::placements_for(configuration, &change_origin, file_id, &file_tags);
                dispatch_and_forward(
                    configuration,
                    runtime_configuration,
                    command_sender,
                    event_sender,
                    targets,
                    content,
                    &change_origin,
                    WireKind::Changed {
                        file_id,
                        content_hash,
                        size,
                    },
                )
                .await;
                maybe_eager_preview(configuration, change_sender, file_id);
            }
        }
    }

    /// The metadata-only wire `Change` to announce to peers for a local content
    /// ingestion. `Change` no longer carries bytes; peers pull them separately.
    enum WireKind {
        Added {
            file_id: FileId,
            logical_path: LogicalPath,
            logical_path_modified_at: i64,
            content_hash: String,
            size: u64,
            tags: Vec<TagId>,
        },
        Changed {
            file_id: FileId,
            content_hash: String,
            size: u64,
        },
    }

    impl WireKind {
        fn into_change(self) -> Change {
            match self {
                WireKind::Added {
                    file_id,
                    logical_path,
                    logical_path_modified_at,
                    content_hash,
                    size,
                    tags,
                } => Change::FileMetadataAdded {
                    file_id,
                    logical_path,
                    logical_path_modified_at,
                    content_hash,
                    size,
                    tags,
                },
                WireKind::Changed {
                    file_id,
                    content_hash,
                    size,
                } => Change::FileMetadataChanged {
                    file_id,
                    content_hash,
                    size,
                },
            }
        }
    }

    /// Dispatch a local content ingestion to matching sync directories
    /// (streaming the bytes to disk) and announce a metadata-only wire `Change`
    /// to peers.
    ///
    /// The bytes are never buffered here for peers: `Change` is metadata-only,
    /// so a peer that wants the content pulls it over a separate transfer. This
    /// keeps large local ingests entirely off the heap regardless of how many
    /// peers are connected.
    ///
    /// Also publishes the change to UI subscribers. See `EVENT PUBLISHING` on
    /// [`handle_changes`]: this arm `continue`s and so never reaches the shared
    /// publish at the bottom of the loop, so it must emit for itself.
    async fn dispatch_and_forward(
        configuration: &Configuration,
        runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
        command_sender: &UnboundedSender<SyncDirectoryCommand>,
        event_sender: &tokio::sync::broadcast::Sender<Change>,
        targets: Vec<Placement>,
        content: FileBytes,
        change_origin: &ChangeOrigin,
        wire: WireKind,
    ) {
        placement::place_content(command_sender, targets, content).await;
        let change = wire.into_change();
        forward_to_peers(configuration, runtime_configuration, &change, change_origin).await;
        let _ = event_sender.send(change);
    }

    log::info!("handle_changes task started; awaiting changes");

    loop {
        let message = tokio::select! {
            _ = shutdown.cancelled() => {
                log::info!("Shutdown requested; stopping change handler");
                break;
            }
            received = change_receiver.recv() => {
                match received {
                    Some(item) => item,
                    None => {
                        log::warn!(
                            "handle_changes: change_receiver returned None \
                             (all senders dropped); exiting"
                        );
                        break;
                    }
                }
            }
        };

        // Route the two bus message kinds. A `Fetch` is an on-demand request
        // for a file's bytes (from `tagsy edit`): satisfy it locally if we
        // hold matching content, otherwise drive a content-addressed receive
        // that floods `ChunkRequest`s to peers. A `Change` falls through to the
        // DB-writer pipeline below.
        let ingest = match message {
            DaemonMessage::Change(ingest, change_origin) => (ingest, change_origin),
            DaemonMessage::Fetch {
                file_id,
                expected_hash,
                respond_to,
            } => {
                if let Some(file_bytes) =
                    read_local_if_hash_matches(&command_sender, file_id, &expected_hash).await
                {
                    let _ = respond_to.send(Ok(file_bytes));
                    return;
                }

                // Resolve the version's authoritative size (needed to bound the
                // receive) from the catalog. The catalog is byte-independent, so
                // this is known for any file we know about even if its bytes are
                // not local. Without a matching version we cannot fetch by hash.
                let expected_size = match database.latest_version(file_id) {
                    Ok(Some(version)) if version.content_hash == expected_hash => {
                        version.size as u64
                    }
                    _ => {
                        let _ = respond_to.send(Err(bus::FetchError::NotAvailable));
                        return;
                    }
                };

                // Surface this on-demand fetch as a live operation, then drive
                // the content-addressed receive off-loop (flooding across the
                // peer tree) so the single-threaded consumer is not blocked.
                let fetching = operations.begin(operations::OperationKind::fetching(file_id));
                let pending_fetches_fetch = pending_fetches.clone();
                tokio::spawn(async move {
                    let result = fetch_via_relay(
                        &pending_fetches_fetch,
                        file_id,
                        expected_hash,
                        expected_size,
                        None,
                    )
                    .await;
                    match &result {
                        Ok(_) => fetching.complete(),
                        Err(error) => fetching.fail(error.to_string()),
                    }
                    let _ = respond_to.send(result);
                });

                continue;
            }
            DaemonMessage::Restore {
                file_id,
                respond_to,
            } => {
                // User-initiated restore of a soft-deleted file. Read the file's
                // latest known version *while it is still tombstoned* (its
                // version history is retained on soft delete). The catalog is
                // not mutated here — only once the bytes are confirmed
                // recoverable (see `ApplyRestore`).
                let deletion_state =
                    database
                        .file_deletion_state(file_id)
                        .unwrap_or_else(|error| {
                            log::error!(
                                "Restore: file_deletion_state failed for {}: {:?}; treating as \
                                 unknown",
                                file_id.to_string(),
                                error
                            );
                            None
                        });

                let is_deleted = matches!(deletion_state, Some(state) if state.deleted);
                if !is_deleted {
                    // Not tombstoned (or unknown): nothing to restore.
                    let _ = respond_to.send(Err(bus::RestoreError::NotDeleted));
                    continue;
                }

                let latest = database.latest_version(file_id).unwrap_or_else(|error| {
                    log::error!(
                        "Restore: latest_version failed for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                    None
                });

                // A tombstoned file always has a version in practice (one is
                // recorded at creation before it could ever be deleted). Guard
                // defensively: without a version there is no hash to restore by.
                let Some(latest) = latest else {
                    log::error!(
                        "Restore: {} is tombstoned but has no recorded version; cannot restore",
                        file_id.to_string()
                    );
                    let _ = respond_to.send(Err(bus::RestoreError::NotAvailable));
                    continue;
                };

                let content_hash = latest.content_hash;
                let size = latest.size as u64;
                // Stamp the restore now: recorded as the restored version's
                // `observed_at` so it beats any lingering peer `deleted_at`.
                let restored_at = clock::now_millis();

                // Run the availability probe off-loop so the (potentially slow)
                // peer round-trip never blocks the sole DB writer. It checks the
                // local `keep_deleted_files` vault first, then floods a probe to
                // peers. On success it re-enters via `ApplyRestore` (handled on
                // this loop); on failure it replies `Err` directly.
                let command_sender_probe = command_sender.clone();
                let pending_fetches_probe = pending_fetches.clone();
                let change_sender_probe = change_sender.clone();
                tokio::spawn(async move {
                    // Local vault (or any local copy) first: cheap and avoids a
                    // network round-trip when we kept the bytes ourselves.
                    let locally_available =
                        read_local_if_hash_matches(&command_sender_probe, file_id, &content_hash)
                            .await
                            .is_some();

                    let available = if locally_available {
                        true
                    } else {
                        probe_availability(&pending_fetches_probe, file_id, content_hash.clone())
                            .await
                    };

                    if !available {
                        log::debug!(
                            "Restore: {} has no recoverable bytes (vault/peers); failing restore",
                            file_id.to_string()
                        );
                        let _ = respond_to.send(Err(bus::RestoreError::NotAvailable));
                        return;
                    }

                    // Bytes are recoverable: hand the catalog mutation back to
                    // the DB-writer loop.
                    if change_sender_probe
                        .send(DaemonMessage::ApplyRestore {
                            file_id,
                            content_hash,
                            size,
                            restored_at,
                            respond_to,
                        })
                        .is_err()
                    {
                        log::error!(
                            "Restore: change channel closed; cannot apply restore for {}",
                            file_id.to_string()
                        );
                        // `respond_to` moved into the failed send; the waiter
                        // times out, which is the shutting-down case.
                    }
                });

                continue;
            }
            DaemonMessage::ApplyRestore {
                file_id,
                content_hash,
                size,
                restored_at,
                respond_to,
            } => {
                // The probe confirmed the bytes are recoverable. Apply the
                // restore on the DB-writer loop: set the `restored_at` clock and
                // clear the tombstone (no fabricated version — the three-way LWW
                // handles it), forward the un-delete to peers, then drive
                // placement so the bytes are pulled ONLY into directories that
                // want them.
                match database.apply_restore(file_id, restored_at) {
                    Ok(true) => {}
                    Ok(false) => {
                        // A delete newer than our restore stamp still wins. This
                        // should not happen for a user-initiated restore stamped
                        // "now", but stay defensive rather than lie about success.
                        log::warn!(
                            "ApplyRestore: {} still tombstoned after restore (a newer delete \
                             wins); failing",
                            file_id.to_string()
                        );
                        let _ = respond_to.send(Err(bus::RestoreError::NotAvailable));
                        continue;
                    }
                    Err(error) => {
                        log::error!(
                            "ApplyRestore: failed to apply restore for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                        let _ = respond_to.send(Err(bus::RestoreError::NotAvailable));
                        continue;
                    }
                }

                let change = Change::FileRestored {
                    file_id,
                    content_hash,
                    size,
                    restored_at,
                };
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &ChangeOrigin::Local {
                        directory_path: std::path::PathBuf::new(),
                    },
                )
                .await;

                // Re-drive placement: pull the bytes into any sync directory
                // that should hold the now-live file (Universal dirs, matching
                // TagBased dirs), sourcing them from the vault or a peer. If no
                // local directory wants the file, nothing is pulled.
                if let Some(deferred) =
                    placement::plan_placement(&command_sender, &database, file_id)
                {
                    let pending_fetches = pending_fetches.clone();
                    let change_sender = change_sender.clone();
                    let operations = operations.clone();
                    tokio::spawn(async move {
                        placement::fetch_and_place_deferred(
                            &pending_fetches,
                            &change_sender,
                            &operations,
                            deferred,
                        )
                        .await;
                    });
                }

                // Publish to UI-facing API subscribers so the deleted-files view
                // refreshes (the file is now live). This arm `continue`s and so
                // bypasses the shared publish at the bottom of the loop; emit
                // here, mirroring it. See `EVENT PUBLISHING` on `handle_changes`.
                let _ = event_sender.send(change);

                let _ = respond_to.send(Ok(()));
                continue;
            }
            DaemonMessage::GetPreview {
                file_id,
                respond_to,
            } => {
                // Overall stopwatch for this preview request, threaded through to
                // the `ApplyPreview` re-entry so we can log the full
                // request→reply latency in one place.
                let request_start = std::time::Instant::now();

                // Resolve the file's current content hash. An unknown file (no
                // recorded version) has nothing to key a preview by.
                let content_hash = match database.latest_version(file_id) {
                    Ok(Some(version)) => version.content_hash,
                    Ok(None) => {
                        let _ = respond_to.send(Err(bus::PreviewError::UnknownFile));
                        continue;
                    }
                    Err(error) => {
                        log::error!(
                            "GetPreview: latest_version failed for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                        let _ = respond_to.send(Err(bus::PreviewError::UnknownFile));
                        continue;
                    }
                };

                // Cache hit (including a cached `Preview::None`): answer now.
                match database.preview_for(file_id, &content_hash) {
                    Ok(Some(preview)) => {
                        log::debug!(
                            "GetPreview: {} served from cache in {:?}",
                            file_id.to_string(),
                            request_start.elapsed()
                        );
                        let _ = respond_to.send(Ok(preview));
                        continue;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        log::error!(
                            "GetPreview: preview_for failed for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                        // Fall through and try to (re)generate.
                    }
                }

                log::debug!(
                    "GetPreview: {} cache miss; resolving off-loop (hash resolution + cache \
                     lookup took {:?})",
                    file_id.to_string(),
                    request_start.elapsed()
                );

                // Cache miss: resolve the preview off the writer loop, then
                // re-enter via `ApplyPreview` to cache it and reply. This
                // mirrors `Fetch`→`Materialize`: generation (`spawn_blocking`)
                // and any peer round-trip must never block the sole DB writer.
                //
                // `can_generate` gates *local* generation: only a device whose
                // policy generates (and whose build has the feature) decodes
                // locally; otherwise `resolve_preview` goes straight to peers.
                let can_generate = PREVIEW_GENERATION_COMPILED
                    && configuration.preview_generation_policy.generates();
                // The file's extension is a type-detection hint for local
                // generation; look it up here while we hold the DB handle (the
                // spawned task has none). Only meaningful when we can generate.
                #[cfg(feature = "preview-generation")]
                let extension = if can_generate {
                    preview_extension_for(&database, file_id)
                } else {
                    None
                };
                #[cfg(not(feature = "preview-generation"))]
                let extension: Option<String> = None;
                let command_sender_preview = command_sender.clone();
                let pending_previews_preview = pending_previews.clone();
                let change_sender_preview = change_sender.clone();
                tokio::spawn(async move {
                    let resolve_start = std::time::Instant::now();
                    let result = resolve_preview(
                        &command_sender_preview,
                        &pending_previews_preview,
                        file_id,
                        &content_hash,
                        can_generate,
                        extension,
                    )
                    .await;
                    log::debug!(
                        "GetPreview: {} resolve_preview took {:?} (ok={}, total since request: \
                         {:?})",
                        file_id.to_string(),
                        resolve_start.elapsed(),
                        result.is_ok(),
                        request_start.elapsed()
                    );

                    if change_sender_preview
                        .send(DaemonMessage::ApplyPreview {
                            file_id,
                            content_hash,
                            result,
                            respond_to,
                        })
                        .is_err()
                    {
                        log::error!(
                            "GetPreview: change channel closed; cannot apply preview for {}",
                            file_id.to_string()
                        );
                        // `respond_to` moved into the failed send; the waiter
                        // observes the shutting-down case via timeout.
                    }
                });

                continue;
            }
            DaemonMessage::ApplyPreview {
                file_id,
                content_hash,
                result,
                respond_to,
            } => {
                // Cache the resolved preview on the writer loop, then reply.
                // Only an authoritative `Ok(preview)` (including a cacheable
                // `Preview::None`) is written; a transient `Err` (e.g.
                // `Unavailable` — local generation produced nothing and no peer
                // served one) is forwarded to the caller unchanged and left
                // *out* of the cache, so the next request re-attempts.
                //
                // Caching is best-effort: a DB error still returns the preview
                // to the caller (they just don't get the cache benefit next
                // time).
                if let Ok(preview) = &result {
                    let cache_write_start = std::time::Instant::now();
                    if let Err(error) = database.record_preview(file_id, &content_hash, preview) {
                        log::error!(
                            "ApplyPreview: record_preview failed for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                    }
                    log::debug!(
                        "ApplyPreview: {} cache write took {:?}; replying to caller",
                        file_id.to_string(),
                        cache_write_start.elapsed()
                    );
                } else {
                    log::debug!(
                        "ApplyPreview: {} resolved transiently unavailable; not caching, replying \
                         to caller",
                        file_id.to_string()
                    );
                }
                let _ = respond_to.send(result);
                continue;
            }
            DaemonMessage::PurgePreviews { respond_to } => {
                // Operator-initiated wipe of the whole preview cache, handled on
                // the sole DB writer. Previews are hash-keyed and regenerated on
                // demand, so this only forces re-evaluation on the next request.
                let result = database.purge_previews();
                match &result {
                    Ok(purged) => log::info!("PurgePreviews: purged {purged} cached previews"),
                    Err(error) => log::error!("PurgePreviews: failed to purge previews: {error:?}"),
                }
                let _ = respond_to.send(result);
                continue;
            }
            DaemonMessage::ReconcilePlacement { file_id } => {
                // Connect-time placement sweep, handed off from a peer session so
                // the fetch runs here (not on the session's frame loop). If a
                // TagBased sync directory wants this file but we lack its bytes,
                // fetch them on demand and place them.
                //
                // The synchronous DB step (`plan_placement`) runs on this loop,
                // but the follow-up (`fetch_and_place_deferred`) must NOT be
                // awaited here: it blocks for the whole network fetch, and
                // it finishes by enqueueing a `DaemonMessage::Materialize` onto
                // *this* loop's own channel. Awaiting it therefore stalls the
                // single-threaded consumer so the `Materialize` it produces can
                // never be dequeued — the file is fetched, "materialized", but
                // never placed, and the next reconcile re-fetches it forever.
                // Spawn it instead (it holds only owned, Send data by design) so
                // the loop stays free to process the resulting `Materialize`.
                if let Some(deferred) =
                    placement::plan_placement(&command_sender, &database, file_id)
                {
                    let pending_fetches = pending_fetches.clone();
                    let change_sender = change_sender.clone();
                    let operations = operations.clone();
                    tokio::spawn(async move {
                        placement::fetch_and_place_deferred(
                            &pending_fetches,
                            &change_sender,
                            &operations,
                            deferred,
                        )
                        .await;
                    });
                }
                continue;
            }
            DaemonMessage::CatalogFile {
                file_id,
                logical_path,
                logical_path_modified_at,
                content_hash,
                size,
                origin,
            } => {
                // A peer session's `Manifest` reconciliation decided to catalog
                // this file/version. We are the sole main-DB writer, so the
                // write happens here. Insert the `files` row if new, then append
                // the version (byte-independent catalog; the bytes are pulled
                // separately on the session link). Seed the path clock from the
                // manifest entry's originating stamp (not our receive time).
                let is_new = !database.file_exists(file_id).unwrap_or(false);
                if is_new
                    && let Err(error) =
                        database.add_file(file_id, &logical_path, logical_path_modified_at)
                {
                    log::error!(
                        "CatalogFile: failed to add file {} ({}): {:?}; skipping version record",
                        file_id.to_string(),
                        logical_path,
                        error
                    );
                    continue;
                }

                if let Err(error) = database.record_version(
                    file_id,
                    &content_hash,
                    version_origin(&origin),
                    size as i64,
                ) {
                    log::error!(
                        "CatalogFile: failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }
                // Cataloging a version means the peer holds content newer than
                // (or equal to) any local tombstone — clear it so a
                // previously-deleted file becomes live again (restore after
                // delete). No-op when the file was not tombstoned.
                if let Err(error) = database.restore_file(file_id) {
                    log::error!(
                        "CatalogFile: failed to clear tombstone for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }

                // Announce this reconcile-derived version onward so it
                // propagates transitively across the peer tree. Without this a
                // change learned via `Manifest` reconciliation would dead-end
                // here: a hub (e.g. `central`) that catches an offline-created
                // file up from one peer via reconcile would never relay it to
                // its other continuously-connected peers, which only ever hear
                // live `FileMetadata{Added,Changed}` — never this catalog write.
                // We reconcile pairwise, but not every pair of peers reconciles
                // directly, so transitive forwarding is required for
                // convergence. Mirror the live handlers: a brand-new file is a
                // `FileMetadataAdded` (tags empty — they reconcile separately via
                // `TagManifest`, exactly as this reconcile's own `Create`
                // placement left them); a new version of a known file is a
                // `FileMetadataChanged`. The `content_hash`/`origin` carry the
                // three-way LWW clocks unchanged so downstream reconciliation is
                // unaffected.
                let change = if is_new {
                    Change::FileMetadataAdded {
                        file_id,
                        logical_path,
                        logical_path_modified_at,
                        content_hash,
                        size,
                        tags: Vec::new(),
                    }
                } else {
                    Change::FileMetadataChanged {
                        file_id,
                        content_hash,
                        size,
                    }
                };
                forward_to_peers(&configuration, &runtime_configuration, &change, &origin).await;
                continue;
            }
            DaemonMessage::Materialize {
                file_id,
                content,
                content_hash,
                origin,
                placement,
            } => {
                // Bytes arrived over a peer transfer. The version was already
                // recorded into the catalog when the triggering announcement was
                // handled (`FileMetadataAdded`/`Changed` or `Manifest`
                // reconcile), so we do NOT record it here — `Materialize` is now
                // purely about placing the bytes into matching sync directories.
                // Forwarding to peers likewise already happened at announce time.
                log::debug!(
                    "Materializing received content for {} ({})",
                    file_id.to_string(),
                    content_hash
                );

                // Build the local placement targets for the arrived bytes.
                let targets = match placement {
                    bus::MaterializePlacement::Create { logical_path, tags } => {
                        // New file: create it in every matching sync directory,
                        // deriving each directory's physical path from the
                        // logical path.
                        //
                        // Tag-filter using the *union* of the carried tags and
                        // the file's current DB tags. The carried tags cover a
                        // live `FileMetadataAdded` (whose `FileTagged`
                        // relationships may not be applied yet); the DB tags
                        // cover a `Manifest` reconcile pull, which carries empty
                        // tags because it cannot know them at pull time — but by
                        // the time this `Materialize` runs, the `TagManifest`'s
                        // `FileTagged` changes have been applied (they are
                        // enqueued before the pull's transfer completes), so the
                        // DB has them. Without this, reconcile-pulled files
                        // matched no TagBased directory and were dropped.
                        let db_tags = database
                            .tag_ids_for_file(file_id, store::SubtagRule::Exclude)
                            .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                            .unwrap_or_else(|error| {
                                log::error!(
                                    "Materialize: failed to read tags for {}: {:?}; using carried \
                                     tags only",
                                    file_id.to_string(),
                                    error
                                );
                                Vec::new()
                            });
                        let effective_tags = placement::effective_placement_tags(&tags, &db_tags);

                        let mut targets = Vec::new();
                        for sync_directory in &configuration.sync_directories {
                            if let SyncType::TagBased {
                                tags: sync_directory_tags,
                            } = &sync_directory.sync_type
                                && !placement::contains_all_tags(
                                    sync_directory_tags,
                                    &effective_tags,
                                )
                            {
                                continue;
                            }
                            let physical_path = sync_directory
                                .sync_type
                                .physical_for(&logical_path, file_id);
                            targets.push(Placement::Create {
                                file_id,
                                physical_path,
                                sync_directory_path: sync_directory.path.clone(),
                            });
                        }
                        targets
                    }
                    bus::MaterializePlacement::Change => {
                        // Existing file: overwrite it in the sync directories
                        // that already hold it (tag-filtered by current tags).
                        let file_tags = database
                            .tag_ids_for_file(file_id, store::SubtagRule::Exclude)
                            .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                            .unwrap_or_else(|error| {
                                log::error!(
                                    "Materialize: failed to read tags for {}: {:?}",
                                    file_id.to_string(),
                                    error
                                );
                                Vec::new()
                            });
                        // Peer-origin: no origin directory to skip. Sentinel
                        // empty path never matches a real sync directory.
                        let sentinel = ChangeOrigin::Local {
                            directory_path: std::path::PathBuf::new(),
                        };
                        placement::placements_for(&configuration, &sentinel, file_id, &file_tags)
                    }
                };
                placement::place_content(&command_sender, targets, content).await;
                // No `forward_to_peers` here: the announcement was already
                // forwarded when it was first handled (announce time). `origin`
                // is unused now that we neither record nor re-announce here.
                let _ = origin;
                // Bytes for this version are now on disk locally: on an
                // eager-preview device, warm the preview cache now so a later
                // peer `PreviewRequest` is a cache hit rather than a decode.
                maybe_eager_preview(&configuration, &change_sender, file_id);

                // Publish to UI-facing API subscribers. The catalog already
                // published at announce time, but that fires *before* the bytes
                // exist locally, so anything keyed on local presence (a file
                // detail view switching from the remote thumbnail to the
                // full-fidelity on-disk preview, a tag-triggered fetch landing)
                // would stay stale until the view is reopened. This is the
                // "bytes are now on disk" edge.
                //
                // Synthetic, local-only: the event bus is typed as `Change`, so
                // we re-send the metadata change we already announced rather
                // than modelling byte arrival properly. It is never forwarded to
                // peers, so the duplicate cannot escape this device. See
                // `EVENT PUBLISHING` on `handle_changes`.
                let size = database
                    .latest_version(file_id)
                    .unwrap_or_else(|error| {
                        log::error!(
                            "Materialize: latest_version failed for {}: {:?}; reporting size 0",
                            file_id.to_string(),
                            error
                        );
                        None
                    })
                    .map(|version| version.size.max(0) as u64)
                    .unwrap_or(0);
                let _ = event_sender.send(Change::FileMetadataChanged {
                    file_id,
                    content_hash,
                    size,
                });
                continue;
            }
            DaemonMessage::AnnounceProvided {
                file_id,
                logical_path,
                content_hash,
                size,
                mut tags,
            } => {
                // A local client (CLI) uploaded/edited a file it serves on
                // demand. Record it locally and announce metadata-only to peers;
                // peers pull the bytes from the registered provider. No local
                // sync-directory placement: a CLI upload targets peers (files
                // already in a sync directory are synced without the CLI).
                let change = match logical_path {
                    Some(logical_path) => {
                        // Genuinely local (CLI) creation: "now" is the true
                        // origin time. Stamp the same value onto the outgoing
                        // announcement so peers seed an identical path clock.
                        let logical_path_modified_at = clock::now_millis();
                        if let Err(error) =
                            database.add_file(file_id, &logical_path, logical_path_modified_at)
                        {
                            log::error!(
                                "AnnounceProvided: failed to add file {} ({}): {:?}",
                                file_id.to_string(),
                                logical_path,
                                error
                            );
                            continue;
                        }
                        // Creation-time tag rules. This is one of exactly two
                        // places a file is *created by this device* (the other
                        // is the local `ContentChange::FileAdded` branch in
                        // `handle_content_change`), and therefore one of
                        // exactly two places rules may run. An
                        // `AnnounceProvided` is always local — a peer's
                        // announcement arrives as `Change::FileMetadataAdded`
                        // and is handled further down, deliberately without
                        // rules.
                        //
                        // Merged before the tagging loop and before the change
                        // is built, so rule tags are persisted locally and
                        // carried to peers exactly like caller-supplied ones.
                        apply_tag_rules(&tag_rules, &logical_path, &mut tags);

                        // Persist the upload's tags into the local catalog. The
                        // outgoing `FileMetadataAdded` carries them to peers, but
                        // the local DB is only updated here — without this a
                        // locally-uploaded file would appear untagged on this
                        // device (its tags only materializing on peers, or on a
                        // later byte-pull placement). Stamp them with the same
                        // creation clock as the file so LWW orders consistently.
                        for tag_id in &tags {
                            if let Err(error) =
                                database.tag_file(*tag_id, file_id, logical_path_modified_at)
                            {
                                log::error!(
                                    "AnnounceProvided: failed to tag file {} with {}: {:?}",
                                    file_id.to_string(),
                                    tag_id.to_string(),
                                    error
                                );
                            }
                        }
                        Change::FileMetadataAdded {
                            file_id,
                            logical_path,
                            logical_path_modified_at,
                            content_hash: content_hash.clone(),
                            size,
                            tags,
                        }
                    }
                    None => Change::FileMetadataChanged {
                        file_id,
                        content_hash: content_hash.clone(),
                        size,
                    },
                };
                let origin = ChangeOrigin::Local {
                    directory_path: std::path::PathBuf::new(),
                };
                if let Err(error) = database.record_version(
                    file_id,
                    &content_hash,
                    version_origin(&origin),
                    size as i64,
                ) {
                    log::error!(
                        "AnnounceProvided: failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }
                forward_to_peers(&configuration, &runtime_configuration, &change, &origin).await;
                // Publish to UI-facing API subscribers so an open file view
                // picks up the new version on the device that *made* the edit.
                // Peers learn of it through the forwarded `Change` above, which
                // they ingest as `Ingest::Meta` and publish from the shared site
                // at the bottom of the loop; without this the originating device
                // is the only one that never refreshes. See `EVENT PUBLISHING`
                // on `handle_changes`.
                let _ = event_sender.send(change);
                continue;
            }
        };

        // Content-bearing ingestions (`ContentChange::FileAdded`/`FileChanged`)
        // carry a `FileBytes` that may still live on disk; they are handled
        // separately so the bytes are streamed into sync directories and only
        // buffered into a wire `Change` at the peer-forward boundary. Every
        // other change is pure metadata and flows through the wire-`Change`
        // match below.
        let (change, change_origin) = match ingest {
            (Ingest::Content(content_change), change_origin) => {
                handle_content_change(
                    &configuration,
                    &tag_rules,
                    &runtime_configuration,
                    &mut database,
                    &command_sender,
                    &change_sender,
                    &event_sender,
                    content_change,
                    change_origin,
                )
                .await;
                continue;
            }
            (Ingest::Meta(change), change_origin) => (change, change_origin),
        };

        match &change {
            // A metadata-only `FileMetadataAdded` announcement — always from a
            // peer (local ingestion carries bytes and arrives as
            // `Ingest::Content`). Record the file + version into the catalog and
            // forward onward; the bytes are pulled separately (and may never be
            // pulled at all if no local sync directory wants them).
            Change::FileMetadataAdded {
                file_id,
                logical_path,
                logical_path_modified_at,
                content_hash,
                size,
                tags,
            } => {
                // Metadata-only announcement from a peer. `file_versions` is the
                // byte-independent *catalog* of versions we know exist in the
                // network — NOT a record of bytes we hold (that is the
                // per-sync-directory databases). So we record the version here,
                // on announcement, regardless of whether we ever pull the bytes.
                let already_exists = database.file_exists(*file_id).unwrap_or_else(|error| {
                    log::error!(
                        "file_exists check failed for {}: {:?}; assuming new",
                        file_id.to_string(),
                        error
                    );
                    false
                });

                if !already_exists {
                    // Seed the path clock from the *originating* device's stamp
                    // carried on the announcement (not our receive time), so a
                    // later `FileMoved` orders against the true creation time.
                    if let Err(error) =
                        database.add_file(*file_id, logical_path, *logical_path_modified_at)
                    {
                        log::error!(
                            "Failed to add file {} ({}): {:?}; skipping change",
                            file_id.to_string(),
                            logical_path,
                            error
                        );
                        continue;
                    }
                    // Persist the tags carried on the announcement into our
                    // catalog. Downstream this same list also drives placement
                    // (`MaterializePlacement::Create`), but placement only
                    // *filters* sync directories — it never writes the
                    // relationships. Without this write a peer would know the
                    // file but show it untagged, since the upload path carries
                    // tags on the creation change rather than as separate
                    // `FileTagged` messages. Stamp with the file's creation
                    // clock so LWW orders identically on every device.
                    for tag_id in tags {
                        if let Err(error) =
                            database.tag_file(*tag_id, *file_id, *logical_path_modified_at)
                        {
                            log::error!(
                                "FileMetadataAdded: failed to tag file {} with {}: {:?}",
                                file_id.to_string(),
                                tag_id.to_string(),
                                error
                            );
                        }
                    }
                } else {
                    // Skip only if this is the version we already hold as latest
                    // in the catalog, not merely present somewhere in history: a
                    // revert to an older hash is a genuine new version and must
                    // be appended (and its bytes re-pulled where wanted).
                    let current_hash = database
                        .latest_version(*file_id)
                        .ok()
                        .flatten()
                        .map(|version| version.content_hash);
                    if current_hash.as_deref() == Some(content_hash.as_str()) {
                        log::debug!(
                            "Ignoring no-op FileMetadataAdded for {} (already the current version)",
                            file_id.to_string()
                        );
                        // Still forward so the announcement propagates the tree.
                        forward_to_peers(
                            &configuration,
                            &runtime_configuration,
                            &change,
                            &change_origin,
                        )
                        .await;
                        continue;
                    }
                }

                // Record the version into the catalog now, on announcement.
                if let Err(error) = database.record_version(
                    *file_id,
                    content_hash,
                    version_origin(&change_origin),
                    *size as i64,
                ) {
                    log::error!(
                        "FileMetadataAdded: failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }
                // A newer version supersedes any local tombstone (restore after
                // delete). No-op if not tombstoned.
                if let Err(error) = database.restore_file(*file_id) {
                    log::error!(
                        "FileMetadataAdded: failed to clear tombstone for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }

                // Forward the announcement to our other peers immediately so the
                // catalog propagates across the whole tree, independent of
                // whether we pull the bytes. A downstream peer that then sends a
                // `ChunkRequest` against us before (or without) us holding the
                // bytes gets a `ChunkMiss` (we relay it onward), so it fetches
                // from another holder — this is the fix for the central-relay
                // race the design targets.
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;

                // Trigger a byte pull from the announcing peer to place the file
                // into any matching local sync directory. If none matches, the
                // pull still runs today but the bytes are dropped at placement;
                // that is optimized separately.
                request_pull_from_origin(
                    &runtime_configuration,
                    &change_origin,
                    *file_id,
                    content_hash.clone(),
                    *size,
                    bus::MaterializePlacement::Create {
                        logical_path: logical_path.clone(),
                        tags: tags.clone(),
                    },
                )
                .await;
            }
            // A metadata-only `FileMetadataChanged` announcement — always from a
            // peer. Record the new version into the catalog and forward it; pull
            // the bytes where a local sync directory wants them.
            Change::FileMetadataChanged {
                file_id,
                content_hash,
                size,
            } => {
                // Skip only if this hash is already our latest catalog version.
                // It is NOT enough for the hash to appear somewhere in history: a
                // revert back to an older hash (present in history but not the
                // latest) is a genuine new version we must append (and re-pull
                // the bytes for where wanted). A whole-history check here
                // previously kept the wrong bytes on disk and hung `edit`.
                let current_hash = database
                    .latest_version(*file_id)
                    .ok()
                    .flatten()
                    .map(|version| version.content_hash);
                if current_hash.as_deref() == Some(content_hash.as_str()) {
                    log::debug!(
                        "Ignoring no-op FileMetadataChanged for {} (already the current version)",
                        file_id.to_string()
                    );
                    // Already our latest catalog version. Announce onward so the
                    // change still propagates the tree.
                    forward_to_peers(
                        &configuration,
                        &runtime_configuration,
                        &change,
                        &change_origin,
                    )
                    .await;
                } else {
                    // Record the new version into the catalog now, on
                    // announcement (independent of whether we pull the bytes).
                    if let Err(error) = database.record_version(
                        *file_id,
                        content_hash,
                        version_origin(&change_origin),
                        *size as i64,
                    ) {
                        log::error!(
                            "FileMetadataChanged: failed to record version for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                    }
                    // A newer version supersedes any local tombstone (restore
                    // after delete). No-op if not tombstoned.
                    if let Err(error) = database.restore_file(*file_id) {
                        log::error!(
                            "FileMetadataChanged: failed to clear tombstone for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                    }

                    // Forward immediately so the catalog propagates tree-wide
                    // regardless of whether we pull the bytes.
                    forward_to_peers(
                        &configuration,
                        &runtime_configuration,
                        &change,
                        &change_origin,
                    )
                    .await;

                    // Pull the new bytes to update any local sync directory that
                    // holds this file.
                    request_pull_from_origin(
                        &runtime_configuration,
                        &change_origin,
                        *file_id,
                        content_hash.clone(),
                        *size,
                        bus::MaterializePlacement::Change,
                    )
                    .await;
                }
            }
            Change::FileMoved {
                file_id,
                logical_path,
                modified_at,
            } => {
                // TODO: Don't unwrap.
                // TODO: Should this be include? Currently this WILL NOT WORK since add file
                // doesn't consider subtags. We would need to get a list of *all* tags (incuding
                // subdags) when adding the file to make it work.
                // -> Maybe make it configurable in the config, per-sync directory.
                let file_tags =
                    match database.tag_ids_for_file(*file_id, store::SubtagRule::Exclude) {
                        Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                        Err(error) => {
                            log::error!(
                                "FileMoved: failed to get tags for {}: {:?}; skipping",
                                file_id.to_string(),
                                error
                            );
                            continue;
                        }
                    };

                // Last-writer-wins on the path clock: apply only if this move is
                // strictly newer than our recorded path change. If it lost, do
                // not reposition bytes or forward it (mirrors FileDeleted).
                match database.update_file_logical_path(*file_id, logical_path, *modified_at) {
                    Ok(true) => {}
                    Ok(false) => {
                        log::debug!(
                            "Ignoring FileMoved for {} (a newer path change supersedes it)",
                            file_id.to_string()
                        );
                        continue;
                    }
                    Err(error) => {
                        log::error!(
                            "Failed to update logical path for file {}: {:?}; skipping",
                            file_id.to_string(),
                            error
                        );
                        continue;
                    }
                }

                for sync_directory in &configuration.sync_directories {
                    if let ChangeOrigin::Local { directory_path } = &change_origin
                        && directory_path == &sync_directory.path
                    {
                        // If the file is already modified in the origin, we don't need to take
                        // any action.
                        continue;
                    };

                    if let SyncType::TagBased {
                        tags: sync_directory_tags,
                    } = &sync_directory.sync_type
                        && !placement::contains_all_tags(sync_directory_tags, &file_tags)
                    {
                        // If the directory is tag based and the file *does not* have all the
                        // tags the sync directory does, skip this sync directory.
                        continue;
                    }

                    // This means the event didn't originate from this sync directory itself and
                    // the tags match, thus we may want to apply the change. Resolve where this
                    // directory should physically place the file from its new logical path.
                    let physical_path = sync_directory
                        .sync_type
                        .physical_for(logical_path, *file_id);
                    // TODO: Handle result.
                    let _ = command_sender.send(SyncDirectoryCommand::MoveFile {
                        file_id: *file_id,
                        physical_path,
                        sync_directory_path: sync_directory.path.clone(),
                    });
                }

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::FileDeleted {
                file_id,
                deleted_at,
            } => {
                // Soft-delete: `remove_file` sets the tombstone
                // (`deleted = 1`, `deleted_at`) instead of removing the row, and
                // applies last-writer-wins — the delete is only applied if
                // `deleted_at` is newer than the file's latest version
                // `observed_at`. The `file_versions` history is kept so the
                // tombstone reconciles offline-safely and can be restored by a
                // newer edit (restore-after-delete).
                //
                // TODO: Should this be include? Currently this WILL NOT WORK since add file
                // doesn't consider subtags. We would need to get a list of *all* tags (incuding
                // subdags) when adding the file to make it work.
                // -> Maybe make it configurable in the config, per-sync directory.
                let file_tags =
                    match database.tag_ids_for_file(*file_id, store::SubtagRule::Exclude) {
                        Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                        Err(error) => {
                            log::error!(
                                "FileDeleted: failed to get tags for {}: {:?}; skipping",
                                file_id.to_string(),
                                error
                            );
                            continue;
                        }
                    };

                // Idempotent-redelivery guard: if we already hold a tombstone
                // for this file, we're in the same terminal state as the
                // sender. Skip the DB write, the per-sync-directory fan-out,
                // and the forward. Without this, a peer redelivering a delete
                // we've already applied would spuriously re-run `RemoveFile`
                // (which fails with `FailedRemovingFile` because the
                // per-sync-directory row is already gone) and re-broadcast the
                // change, causing tombstones to pile up across the mesh on
                // every reconnect.
                match database.file_deletion_state(*file_id) {
                    Ok(Some(state)) if state.deleted => {
                        log::debug!(
                            "Ignoring FileDeleted for {} (already tombstoned)",
                            file_id.to_string()
                        );
                        continue;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        log::error!(
                            "FileDeleted: failed to read deletion state for {}: {:?}; skipping",
                            file_id.to_string(),
                            error
                        );
                        continue;
                    }
                }

                match database.remove_file(*file_id, *deleted_at) {
                    Ok(true) => {}
                    Ok(false) => {
                        // A newer edit or restore out-dated this delete
                        // (last-writer-wins): the file stays live. Do not
                        // remove it from sync directories or forward the
                        // delete.
                        log::debug!(
                            "Ignoring FileDeleted for {} (a newer version supersedes it)",
                            file_id.to_string()
                        );
                        continue;
                    }
                    Err(error) => {
                        log::error!(
                            "Failed to remove file {}: {:?}; skipping",
                            file_id.to_string(),
                            error
                        );
                        continue;
                    }
                }

                for sync_directory in &configuration.sync_directories {
                    if let ChangeOrigin::Local { directory_path } = &change_origin
                        && directory_path == &sync_directory.path
                    {
                        // If the file came from this directory, it is already removed. We
                        // can just skip this directory.
                        continue;
                    };

                    if let SyncType::TagBased {
                        tags: sync_directory_tags,
                    } = &sync_directory.sync_type
                        && !placement::contains_all_tags(sync_directory_tags, &file_tags)
                    {
                        // If the directory is tag based and the file *does not* have all the
                        // tags the sync directory does, skip this sync directory.
                        continue;
                    }

                    // This means the event didn't originate from this sync directory itself, thus
                    // we may want to apply it.
                    // TODO: Handle result.
                    let _ = command_sender.send(SyncDirectoryCommand::RemoveFile {
                        file_id: *file_id,
                        sync_directory_path: sync_directory.path.clone(),
                    });
                }

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            // An inbound `FileRestored` from a peer: the peer un-deleted a file
            // and already confirmed its bytes were recoverable, so this is
            // authoritative. Mirror `FileMetadataChanged` — record the restored
            // version (its `restored_at` becomes the version's `observed_at`,
            // beating any local `deleted_at` under LWW), clear our tombstone,
            // forward onward, and pull the bytes into any local sync directory
            // that wants them. No local-availability gate here: only the
            // *originating* device gates restore on availability.
            Change::FileRestored {
                file_id,
                content_hash,
                size,
                restored_at,
            } => {
                // Skip only if this hash is already our latest catalog version
                // AND the file is already live — otherwise a restore that clears
                // a tombstone (or reverts to an older-but-restored hash) is a
                // genuine state change we must apply.
                let current_hash = database
                    .latest_version(*file_id)
                    .ok()
                    .flatten()
                    .map(|version| version.content_hash);
                let already_live = matches!(
                    database.file_deletion_state(*file_id),
                    Ok(Some(state)) if !state.deleted
                );

                if current_hash.as_deref() == Some(content_hash.as_str()) && already_live {
                    log::debug!(
                        "Ignoring no-op FileRestored for {} (already the current, live version)",
                        file_id.to_string()
                    );
                    forward_to_peers(
                        &configuration,
                        &runtime_configuration,
                        &change,
                        &change_origin,
                    )
                    .await;
                    continue;
                }

                // Apply the restore under three-way LWW using the peer's
                // `restored_at` stamp (preserved verbatim from the wire), so it
                // orders correctly against our own `deleted_at`. No version is
                // fabricated: the restored version is the file's latest existing
                // version, which we already have in our history. If a newer
                // local delete out-votes the restore, `apply_restore` leaves the
                // tombstone and we skip the byte pull.
                let restored = match database.apply_restore(*file_id, *restored_at) {
                    Ok(restored) => restored,
                    Err(error) => {
                        log::error!(
                            "FileRestored: failed to apply restore for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                        false
                    }
                };

                // Always forward so the announcement propagates the tree, even
                // if it lost LWW locally (a downstream peer may still be behind).
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;

                // Pull the bytes to update any local sync directory that should
                // hold this now-live file — only if the restore actually won
                // (otherwise the file stays tombstoned and wants no bytes).
                if restored {
                    request_pull_from_origin(
                        &runtime_configuration,
                        &change_origin,
                        *file_id,
                        content_hash.clone(),
                        *size,
                        bus::MaterializePlacement::Change,
                    )
                    .await;
                }
            }
            // Every tag mutation below carries `modified_at`, stamped on the
            // originating device and preserved across the wire. It is passed
            // straight to the DB layer, which applies last-writer-wins: an
            // older change is a no-op. This makes both live application and
            // reconciliation replay idempotent and convergent.
            Change::TagAdded {
                tag_id,
                tag_name,
                color,
                metadata: _,
                modified_at,
            } => {
                if let Err(error) = database.add_tag(*tag_id, tag_name, color, *modified_at) {
                    log::error!(
                        "Failed to add tag {} ({}): {:?}",
                        tag_id.to_string(),
                        tag_name,
                        error
                    );
                }
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagRenamed {
                tag_id,
                tag_name,
                modified_at,
            } => {
                if let Err(error) = database.update_tag_name(*tag_id, tag_name, *modified_at) {
                    log::error!("Failed to rename tag {}: {:?}", tag_id.to_string(), error);
                }
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagRecolored {
                tag_id,
                color,
                modified_at,
            } => {
                // Carries the full new color; applied with the same `modified_at`
                // LWW guard as the other tag mutations, then forwarded so peers
                // converge. Mirrors `TagRenamed`.
                if let Err(error) = database.update_tag_color(*tag_id, color, *modified_at) {
                    log::error!("Failed to recolor tag {}: {:?}", tag_id.to_string(), error);
                }
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagChanged {
                tag_id: _,
                metadata: _,
                modified_at: _,
            } => {
                // Tag metadata is not yet stored (the whole `MetadataFormat`
                // API is `todo!()` in tagsy-core). When metadata lands, apply
                // it here with the same `modified_at` LWW guard as the other
                // tag mutations and forward. Deliberately not forwarded until
                // then, so we never propagate state we can't apply.
            }
            Change::TagRemoved {
                tag_id,
                modified_at,
            } => {
                // Soft-delete: set the tombstone (`deleted = 1`) and bump
                // `modified_at` to the delete time. A tag reuses its
                // `modified_at` as its last-writer-wins clock, so the delete is
                // applied only if it is newer than the stored value (a newer
                // rename/recolor resurrects the tag). Forwarded either way so
                // the tombstone propagates; a stale delete is a DB no-op.
                match database.remove_tag(*tag_id, *modified_at) {
                    Ok(true) => {}
                    Ok(false) => {
                        log::debug!(
                            "Ignoring TagRemoved for {} (a newer edit supersedes it)",
                            tag_id.to_string()
                        );
                    }
                    Err(error) => {
                        log::error!("Failed to remove tag {}: {:?}", tag_id.to_string(), error);
                    }
                }
                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::FileTagged {
                file_id,
                tag_id,
                metadata: _,
                modified_at,
            } => {
                if let Err(error) = database.tag_file(*tag_id, *file_id, *modified_at) {
                    log::error!(
                        "Failed to tag file {} with {}: {:?}",
                        file_id.to_string(),
                        tag_id.to_string(),
                        error
                    );
                }

                // The file's tag set changed, so its tag-based placement may be
                // stale: a file that just gained a directory's tags should be
                // materialized there. This is also the recovery path for the
                // tag-vs-content reconciliation race (a peer transfer that
                // materialized before this `FileTagged` arrived placed the file
                // only where tags already matched). Re-run placement now, and if
                // the bytes are not local, fetch them.
                //
                // The synchronous DB step runs here on the loop, but the
                // follow-up (`fetch_and_place_deferred`) must NOT be awaited on
                // this loop: it blocks for the whole network fetch, and it
                // finishes by enqueueing a `DaemonMessage::Materialize` onto
                // *this* loop's own channel. Awaiting it stalls the
                // single-threaded consumer (so the `Materialize` it produces
                // can never be dequeued) and, in the meantime, blocks every
                // other `DaemonMessage` behind it — including UI-visible
                // change events. Spawn instead; the follow-up holds only
                // owned, `Send` data by design. See the mirror comment on
                // `DaemonMessage::ReconcilePlacement`.
                if let Some(deferred) =
                    placement::plan_placement(&command_sender, &database, *file_id)
                {
                    let pending_fetches = pending_fetches.clone();
                    let change_sender = change_sender.clone();
                    let operations = operations.clone();

                    tokio::spawn(async move {
                        placement::fetch_and_place_deferred(
                            &pending_fetches,
                            &change_sender,
                            &operations,
                            deferred,
                        )
                        .await;
                    });
                }

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::FileTagChanged {
                file_id: _,
                tag_id: _,
                metadata: _,
                modified_at: _,
            } => {
                // Relationship metadata: deferred with the rest of the metadata
                // API. See `TagChanged`.
            }
            Change::FileUntagged {
                file_id,
                tag_id,
                modified_at,
            } => {
                if let Err(error) = database.untag_file(*tag_id, *file_id, *modified_at) {
                    log::error!(
                        "Failed to untag file {} from {}: {:?}",
                        file_id.to_string(),
                        tag_id.to_string(),
                        error
                    );
                }

                // The file's tag set changed: a file that just lost a
                // directory's tags should be dropped from it. Re-run placement
                // (symmetric with `FileTagged`). A removal never defers, but use
                // the same two-step API for consistency.
                //
                // Spawn the async follow-up for the same reason as
                // `FileTagged` (see the comment there): even though the
                // untag path never actually defers a fetch, awaiting it on
                // this loop would still block every subsequent
                // `DaemonMessage` until the manager replies, and keeping the
                // two arms structurally identical avoids future footguns.
                if let Some(deferred) =
                    placement::plan_placement(&command_sender, &database, *file_id)
                {
                    let pending_fetches = pending_fetches.clone();
                    let change_sender = change_sender.clone();
                    let operations = operations.clone();

                    tokio::spawn(async move {
                        placement::fetch_and_place_deferred(
                            &pending_fetches,
                            &change_sender,
                            &operations,
                            deferred,
                        )
                        .await;
                    });
                }

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagTagged {
                taggee_id,
                tag_id,
                metadata: _,
                modified_at,
            } => {
                if let Err(error) = database.tag_tag(*tag_id, *taggee_id, *modified_at) {
                    log::error!(
                        "Failed to tag tag {} with {}: {:?}",
                        taggee_id.to_string(),
                        tag_id.to_string(),
                        error
                    );
                }

                // NOTE: Currently this is correct, but if we change the subtag rules on the
                // sync directories we will have to update the sync directories
                // here too.

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
            Change::TagTagChanged {
                taggee_id: _,
                tag_id: _,
                metadata: _,
                modified_at: _,
            } => {
                // Relationship metadata: deferred with the rest of the metadata
                // API. See `TagChanged`.
            }
            Change::TagUntagged {
                taggee_id,
                tag_id,
                modified_at,
            } => {
                if let Err(error) = database.untag_tag(*tag_id, *taggee_id, *modified_at) {
                    log::error!(
                        "Failed to untag tag {} from {}: {:?}",
                        taggee_id.to_string(),
                        tag_id.to_string(),
                        error
                    );
                }

                // NOTE: Currently this is correct, but if we change the subtag rules on the
                // sync directories we will have to update the sync directories
                // here too.

                forward_to_peers(
                    &configuration,
                    &runtime_configuration,
                    &change,
                    &change_origin,
                )
                .await;
            }
        }

        // Publish the applied change to UI-facing API subscribers. This is the
        // shared site every arm that does not `continue` falls through to; the
        // arms that do `continue` emit for themselves. See `EVENT PUBLISHING`
        // on `handle_changes` for the full list and why it is suboptimal.
        //
        // Best-effort: if there are no subscribers, or the channel is full and
        // a subscriber lags, the send/receive machinery handles it (the
        // subscriber observes `Lagged`, mapped to `Resynced` by the transport).
        let _ = event_sender.send(change);
    }

    log::info!("handle_changes task exited");
}

/// End-to-end coverage for creation-time tag rules, driving the real
/// `handle_changes` loop.
///
/// These tests are deliberately not unit tests of the matcher (that lives in
/// `configuration::tests`). What needs pinning down here is *where* rules run:
/// on the two local creation paths and nowhere else. That boundary is a
/// property of the call sites, so it can only be observed by feeding real
/// messages onto the ingest bus and watching what comes out.
#[cfg(test)]
mod tag_rule_tests {
    use std::time::Duration;

    use super::*;
    use crate::configuration::TagRule;

    /// How long to wait for a change to surface on the event stream. Generous:
    /// a slow machine must not produce a flaky pass, and the negative tests
    /// spend this in full.
    const SETTLE: Duration = Duration::from_millis(500);

    /// A running `handle_changes` over a scratch catalog, with no sync
    /// directories and no peers.
    ///
    /// Both are absent on purpose: with no sync directory nothing is
    /// materialized to disk and with no peer nothing is forwarded, which
    /// strips the loop down to the catalog write and the event publication —
    /// exactly the two effects these tests care about. Every change
    /// `handle_changes` applies is still published to `events`, so the event
    /// stream is a faithful view of what was decided.
    ///
    /// The catalog is a temp *file* rather than `:memory:` because the bundled
    /// [`api::Api`] opens its own read handle by path, exactly as it does in
    /// production; an in-memory database would not be shared between the two.
    struct Harness {
        changes: UnboundedSender<DaemonMessage>,
        events: tokio::sync::broadcast::Receiver<Change>,
        api: api::Api,
        shutdown: CancellationToken,
        data_dir: std::path::PathBuf,
        /// Held only to keep the channel open. The sync-directory manager is
        /// never started, and dropping this would make `handle_changes`'s sends
        /// fail rather than simply go nowhere.
        _commands: tokio::sync::mpsc::UnboundedReceiver<SyncDirectoryCommand>,
    }

    impl Harness {
        fn new(tag_rules: Vec<TagRule>) -> Self {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let data_dir = std::env::temp_dir().join(format!(
                "tagsy-tag-rule-test-{}-{}",
                std::process::id(),
                unique
            ));
            std::fs::create_dir_all(&data_dir).expect("create test data dir");
            let main_db_path = data_dir.join("main.db");

            let configuration = Configuration {
                sync_directories: Vec::new(),
                listen_port: None,
                peers: Vec::new(),
                tags: Vec::new(),
                preview_generation_policy: crate::configuration::PreviewGenerationPolicy::Never,
                editor_rules: Vec::new(),
                tag_rules,
            };
            let compiled = Arc::new(CompiledTagRules::compile(&configuration.tag_rules));
            let runtime_configuration =
                Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));
            let database = CatalogStore::initialize(&main_db_path).expect("open test db");

            let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
            let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
            let (event_sender, event_receiver) = tokio::sync::broadcast::channel(64);
            let shutdown = CancellationToken::new();
            let pending_fetches = crate::fetch::PendingFetches::new(runtime_configuration.clone());
            let operations = crate::operations::Operations::new();

            let api = api::Api::new(
                main_db_path,
                change_sender.clone(),
                command_sender.clone(),
                event_sender.clone(),
                pending_fetches.clone(),
                data_dir.join("fetch-temp"),
                operations.clone(),
                Vec::new(),
                compiled.clone(),
            );

            tokio::spawn(handle_changes(
                configuration,
                compiled,
                runtime_configuration.clone(),
                pending_fetches,
                crate::preview_fetch::PendingPreviews::new(runtime_configuration),
                database,
                change_receiver,
                change_sender.clone(),
                command_sender,
                event_sender,
                operations,
                shutdown.clone(),
            ));

            Self {
                changes: change_sender,
                events: event_receiver,
                api,
                shutdown,
                data_dir,
                _commands: command_receiver,
            }
        }

        /// Upload a file and wait until it is in the catalog, returning its id
        /// and the tags the announcement carried.
        async fn upload(&mut self, path: &str, tags: Vec<TagId>) -> (FileId, Vec<TagId>) {
            let file_id = FileId::new();
            self.send(DaemonMessage::AnnounceProvided {
                file_id,
                logical_path: Some(LogicalPath::new(path)),
                content_hash: format!("hash-{path}"),
                size: 1,
                tags,
            });
            let tags = self
                .expect("the upload announcement", |change| {
                    added_tags(change, file_id)
                })
                .await;
            (file_id, tags)
        }

        fn send(&self, message: DaemonMessage) {
            self.changes.send(message).expect("ingest bus is alive");
        }

        /// Wait for the first published change satisfying `predicate`, or panic
        /// once `SETTLE` elapses.
        async fn expect<T>(&mut self, what: &str, predicate: impl Fn(&Change) -> Option<T>) -> T {
            let deadline = tokio::time::Instant::now() + SETTLE;
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                let change = tokio::time::timeout(remaining, self.events.recv())
                    .await
                    .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
                    .expect("event stream stayed open");
                if let Some(value) = predicate(&change) {
                    return value;
                }
            }
        }

        /// Assert no published change satisfies `predicate` within `SETTLE`.
        async fn expect_none(&mut self, what: &str, predicate: impl Fn(&Change) -> bool) {
            let deadline = tokio::time::Instant::now() + SETTLE;
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return;
                }
                match tokio::time::timeout(remaining, self.events.recv()).await {
                    // Elapsed without a disqualifying change: the assertion holds.
                    Err(_) => return,
                    Ok(Ok(change)) => assert!(!predicate(&change), "unexpected {what}: {change:?}"),
                    Ok(Err(_)) => return,
                }
            }
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.shutdown.cancel();
            let _ = std::fs::remove_dir_all(&self.data_dir);
        }
    }

    fn markdown_rule(tag_id: TagId) -> Vec<TagRule> {
        vec![TagRule {
            pattern: r"\.md$".to_owned(),
            tags: vec![tag_id],
        }]
    }

    /// The tags carried by the `FileMetadataAdded` published for `file_id`.
    fn added_tags(change: &Change, file_id: FileId) -> Option<Vec<TagId>> {
        match change {
            Change::FileMetadataAdded {
                file_id: got, tags, ..
            } if *got == file_id => Some(tags.clone()),
            _ => None,
        }
    }

    /// Hook 1: a client upload (`Api::upload_file`) whose logical path matches
    /// gets the rule's tag, and it is carried on the announcement so peers
    /// learn of it too.
    #[tokio::test]
    async fn upload_applies_a_matching_rule() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));
        let file_id = FileId::new();

        harness.send(DaemonMessage::AnnounceProvided {
            file_id,
            logical_path: Some(LogicalPath::new("notes/todo.md")),
            content_hash: "hash".to_owned(),
            size: 1,
            tags: Vec::new(),
        });

        let tags = harness
            .expect("the upload announcement", |change| {
                added_tags(change, file_id)
            })
            .await;
        assert_eq!(tags, vec![tag_id]);
    }

    /// A non-matching upload is left exactly as the caller specified.
    #[tokio::test]
    async fn upload_without_a_match_is_untouched() {
        let mut harness = Harness::new(markdown_rule(TagId::new()));
        let file_id = FileId::new();

        harness.send(DaemonMessage::AnnounceProvided {
            file_id,
            logical_path: Some(LogicalPath::new("notes/todo.txt")),
            content_hash: "hash".to_owned(),
            size: 1,
            tags: Vec::new(),
        });

        let tags = harness
            .expect("the upload announcement", |change| {
                added_tags(change, file_id)
            })
            .await;
        assert!(tags.is_empty());
    }

    /// Rule tags are merged with the caller's, never substituted for them.
    #[tokio::test]
    async fn upload_merges_rule_tags_with_caller_tags() {
        let rule_tag = TagId::new();
        let caller_tag = TagId::new();
        let mut harness = Harness::new(markdown_rule(rule_tag));
        let file_id = FileId::new();

        harness.send(DaemonMessage::AnnounceProvided {
            file_id,
            logical_path: Some(LogicalPath::new("notes/todo.md")),
            content_hash: "hash".to_owned(),
            size: 1,
            tags: vec![caller_tag],
        });

        let tags = harness
            .expect("the upload announcement", |change| {
                added_tags(change, file_id)
            })
            .await;
        assert_eq!(tags, vec![caller_tag, rule_tag]);
    }

    /// A caller-supplied tag that a rule would also assign appears once.
    #[tokio::test]
    async fn upload_does_not_duplicate_an_already_supplied_tag() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));
        let file_id = FileId::new();

        harness.send(DaemonMessage::AnnounceProvided {
            file_id,
            logical_path: Some(LogicalPath::new("notes/todo.md")),
            content_hash: "hash".to_owned(),
            size: 1,
            tags: vec![tag_id],
        });

        let tags = harness
            .expect("the upload announcement", |change| {
                added_tags(change, file_id)
            })
            .await;
        assert_eq!(tags, vec![tag_id]);
    }

    /// Hook 2: a file appearing in a local sync directory is a creation too,
    /// and rules apply to it on the same terms.
    #[tokio::test]
    async fn local_file_added_applies_a_matching_rule() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));
        let file_id = FileId::new();

        harness.send(DaemonMessage::Change(
            Ingest::Content(ContentChange::FileAdded {
                file_id,
                logical_path: LogicalPath::new("notes/todo.md"),
                content: crate::file_bytes::FileBytes::InMemory(b"x".to_vec()),
                content_hash: "hash".to_owned(),
                size: 1,
                tags: Vec::new(),
            }),
            ChangeOrigin::Local {
                directory_path: std::path::PathBuf::new(),
            },
        ));

        let tags = harness
            .expect("the local ingestion announcement", |change| {
                added_tags(change, file_id)
            })
            .await;
        assert_eq!(tags, vec![tag_id]);
    }

    /// The central negative case: rules run only on the device that creates a
    /// file. A peer-originated add already carries whatever tags its origin's
    /// rules assigned, so re-applying ours would let two devices with different
    /// rule sets disagree about the same file forever.
    #[tokio::test]
    async fn peer_file_added_does_not_apply_rules() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));
        let file_id = FileId::new();

        harness.send(DaemonMessage::Change(
            Ingest::Content(ContentChange::FileAdded {
                file_id,
                logical_path: LogicalPath::new("notes/todo.md"),
                content: crate::file_bytes::FileBytes::InMemory(b"x".to_vec()),
                content_hash: "hash".to_owned(),
                size: 1,
                tags: Vec::new(),
            }),
            ChangeOrigin::Peer {
                public_key: "a-peer".to_owned(),
            },
        ));

        let tags = harness
            .expect("the inbound announcement", |change| {
                added_tags(change, file_id)
            })
            .await;
        assert!(
            tags.is_empty(),
            "a peer's file must not be re-tagged by our rules"
        );
    }

    /// The other central negative case, and the one the feature was scoped
    /// around: renaming a file into a matching path does *not* tag it. Rules
    /// are a creation-time default; once a file exists its tags belong to the
    /// user. See `TagRule` for why re-running on move has no correct answer.
    #[tokio::test]
    async fn moving_a_file_into_a_matching_path_does_not_apply_rules() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));
        let file_id = FileId::new();

        // Create it under a name no rule matches.
        harness.send(DaemonMessage::AnnounceProvided {
            file_id,
            logical_path: Some(LogicalPath::new("notes/todo.txt")),
            content_hash: "hash".to_owned(),
            size: 1,
            tags: Vec::new(),
        });
        let tags = harness
            .expect("the upload announcement", |change| {
                added_tags(change, file_id)
            })
            .await;
        assert!(tags.is_empty(), "precondition: created untagged");

        // Rename it onto a path the rule matches.
        harness.send(DaemonMessage::Change(
            Ingest::from_change(Change::FileMoved {
                file_id,
                logical_path: LogicalPath::new("notes/todo.md"),
                modified_at: clock::now_millis(),
            }),
            ChangeOrigin::Local {
                directory_path: std::path::PathBuf::new(),
            },
        ));

        harness
            .expect_none("tagging triggered by a move", |change| {
                matches!(change, Change::FileTagged { file_id: got, .. } if *got == file_id)
            })
            .await;
    }

    /// Replacing a file's content is not a creation either, so it cannot pick
    /// up tags — even when the file's path matches a rule.
    #[tokio::test]
    async fn editing_content_does_not_apply_rules() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));
        let file_id = FileId::new();

        harness.send(DaemonMessage::AnnounceProvided {
            file_id,
            logical_path: Some(LogicalPath::new("notes/todo.md")),
            content_hash: "hash".to_owned(),
            size: 1,
            tags: Vec::new(),
        });
        harness
            .expect("the upload announcement", |change| {
                added_tags(change, file_id)
            })
            .await;

        // A content-only republication (`Api::edit_file`): no logical path.
        harness.send(DaemonMessage::AnnounceProvided {
            file_id,
            logical_path: None,
            content_hash: "hash2".to_owned(),
            size: 2,
            tags: Vec::new(),
        });

        harness
            .expect_none("tagging triggered by an edit", |change| {
                matches!(change, Change::FileTagged { file_id: got, .. } if *got == file_id)
            })
            .await;
    }

    /// `retag` is the recovery path for the "rules do not run on move"
    /// restriction: a file renamed into a matching path stays untagged until
    /// the operator asks for it, and then gets tagged.
    #[tokio::test]
    async fn retag_catches_up_a_file_a_rule_now_matches() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));

        let (file_id, tags) = harness.upload("notes/todo.txt", Vec::new()).await;
        assert!(tags.is_empty(), "precondition: created untagged");

        harness.send(DaemonMessage::Change(
            Ingest::from_change(Change::FileMoved {
                file_id,
                logical_path: LogicalPath::new("notes/todo.md"),
                modified_at: clock::now_millis(),
            }),
            ChangeOrigin::Local {
                directory_path: std::path::PathBuf::new(),
            },
        ));
        harness
            .expect("the move to be applied", |change| {
                matches!(change, Change::FileMoved { file_id: got, .. } if *got == file_id)
                    .then_some(())
            })
            .await;

        let summary = harness.api.retag(false).expect("retag succeeds");
        assert_eq!(summary.files_scanned, 1);
        assert_eq!(summary.files_changed, 1);
        assert_eq!(summary.tags_applied, 1);

        let applied = harness
            .expect("the retagging", |change| match change {
                Change::FileTagged {
                    file_id: got,
                    tag_id,
                    ..
                } if *got == file_id => Some(*tag_id),
                _ => None,
            })
            .await;
        assert_eq!(applied, tag_id);
    }

    /// A file that already carries the tag is not re-enqueued, so a second run
    /// is a no-op. Without this, `retag` on a large catalog would flood the
    /// bus (and every peer) with redundant changes on every invocation.
    #[tokio::test]
    async fn retag_is_idempotent() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));

        // Created with a matching name, so the creation-time rule already
        // applied the tag.
        let (_file_id, tags) = harness.upload("notes/todo.md", Vec::new()).await;
        assert_eq!(tags, vec![tag_id]);

        let summary = harness.api.retag(false).expect("retag succeeds");
        assert_eq!(summary.files_scanned, 1);
        assert_eq!(
            summary.tags_applied, 0,
            "the tag is already applied; nothing to do"
        );
        assert_eq!(summary.files_changed, 0);
    }

    /// A dry run reports the same plan but enqueues nothing.
    #[tokio::test]
    async fn retag_dry_run_changes_nothing() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));

        let (file_id, _) = harness.upload("notes/todo.txt", Vec::new()).await;
        harness.send(DaemonMessage::Change(
            Ingest::from_change(Change::FileMoved {
                file_id,
                logical_path: LogicalPath::new("notes/todo.md"),
                modified_at: clock::now_millis(),
            }),
            ChangeOrigin::Local {
                directory_path: std::path::PathBuf::new(),
            },
        ));
        harness
            .expect("the move to be applied", |change| {
                matches!(change, Change::FileMoved { file_id: got, .. } if *got == file_id)
                    .then_some(())
            })
            .await;

        let summary = harness.api.retag(true).expect("dry run succeeds");
        assert_eq!(summary.tags_applied, 1, "the plan is still reported");

        harness
            .expect_none("tagging during a dry run", |change| {
                matches!(change, Change::FileTagged { .. })
            })
            .await;

        // And the plan is still there to be applied for real afterwards.
        let summary = harness.api.retag(false).expect("retag succeeds");
        assert_eq!(summary.tags_applied, 1);
    }

    /// `retag` never removes a tag, not even one no rule would assign. Nothing
    /// distinguishes a rule-applied tag from a hand-applied one, so removal
    /// could not be done without risking the user's own tagging.
    #[tokio::test]
    async fn retag_never_removes_tags() {
        let rule_tag = TagId::new();
        let manual_tag = TagId::new();
        let mut harness = Harness::new(markdown_rule(rule_tag));

        // Carries a tag no rule mentions, on a path no rule matches.
        let (_file_id, tags) = harness.upload("notes/todo.txt", vec![manual_tag]).await;
        assert_eq!(tags, vec![manual_tag]);

        let summary = harness.api.retag(false).expect("retag succeeds");
        assert_eq!(summary.tags_applied, 0);

        harness
            .expect_none("any untagging", |change| {
                matches!(change, Change::FileUntagged { .. })
            })
            .await;
    }

    /// A tombstoned file is skipped: tagging it would change nothing visible
    /// and would resurrect the relationship in every peer's catalog.
    #[tokio::test]
    async fn retag_skips_deleted_files() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(markdown_rule(tag_id));

        let (file_id, _) = harness.upload("notes/todo.txt", Vec::new()).await;
        harness.send(DaemonMessage::Change(
            Ingest::from_change(Change::FileMoved {
                file_id,
                logical_path: LogicalPath::new("notes/todo.md"),
                modified_at: clock::now_millis(),
            }),
            ChangeOrigin::Local {
                directory_path: std::path::PathBuf::new(),
            },
        ));
        harness.api.delete_file(file_id).expect("delete enqueued");
        harness
            .expect("the deletion to be applied", |change| {
                matches!(change, Change::FileDeleted { file_id: got, .. } if *got == file_id)
                    .then_some(())
            })
            .await;

        let summary = harness.api.retag(false).expect("retag succeeds");
        assert_eq!(summary.files_scanned, 0);
        assert_eq!(summary.tags_applied, 0);
    }

    /// The report distinguishes the two independent faults: a pattern that
    /// does not compile, and a tag id that names nothing.
    #[tokio::test]
    async fn tag_rule_report_lists_invalid_patterns_and_unknown_tags() {
        let unknown_tag = TagId::new();
        let harness = Harness::new(vec![
            TagRule {
                pattern: "*.md".to_owned(),
                tags: vec![TagId::new()],
            },
            TagRule {
                pattern: r"\.md$".to_owned(),
                tags: vec![unknown_tag],
            },
        ]);

        let report = harness.api.tag_rule_report().expect("report succeeds");
        assert_eq!(report.active, 1, "only the valid rule is live");
        assert_eq!(report.invalid.len(), 1);
        assert!(
            report.invalid[0].contains("*.md"),
            "the diagnostic names the offending pattern: {}",
            report.invalid[0]
        );
        assert_eq!(
            report.unknown_tags,
            vec![unknown_tag],
            "no tag with this id has ever been created"
        );
    }

    /// A broken rule does not stop the daemon, and does not stop its siblings
    /// from working. This is the availability property `CompiledTagRules`
    /// documents, observed end to end.
    #[tokio::test]
    async fn a_broken_rule_does_not_disable_the_others() {
        let tag_id = TagId::new();
        let mut harness = Harness::new(vec![
            TagRule {
                pattern: "*.md".to_owned(),
                tags: vec![TagId::new()],
            },
            TagRule {
                pattern: r"\.md$".to_owned(),
                tags: vec![tag_id],
            },
        ]);
        let file_id = FileId::new();

        harness.send(DaemonMessage::AnnounceProvided {
            file_id,
            logical_path: Some(LogicalPath::new("notes/todo.md")),
            content_hash: "hash".to_owned(),
            size: 1,
            tags: Vec::new(),
        });

        let tags = harness
            .expect("the upload announcement", |change| {
                added_tags(change, file_id)
            })
            .await;
        assert_eq!(tags, vec![tag_id]);
    }
}
