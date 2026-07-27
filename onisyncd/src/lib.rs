//! Core onisync runtime as a library.
//!
//! This crate used to be a pure binary whose entire runtime lived in
//! `main.rs`. It has been split so that the runtime is callable as a library
//! function ([`run`]): the desktop binary (`main.rs`) is a thin CLI wrapper,
//! and other frontends (e.g. an Android native library) can link this crate
//! and call [`run`] directly without a `main()`.
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
use onisync_core::state::{
    Change, ChangeOrigin, Frame, ManifestEntry, RelationshipManifestEntry, Sync as SyncMessage,
    TagManifestEntry,
};
use onisync_core::{FileId, LogicalPath, PhysicalPath, TagId, TransferId};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_util::sync::CancellationToken;

use crate::bus::{ContentChange, DaemonMessage, Ingest};
use crate::configuration::{Configuration, Peer, RuntimeConfiguration, SyncType};
use crate::database::FileDatabase;
use crate::directory_manager::{SyncDirectoryCommand, SyncDirectoryManager};
use crate::fetch::PendingFetches;
use crate::file_bytes::FileBytes;
use crate::identity::{HandshakeMessage, Identity};
use crate::paths::Paths;
use crate::transfer::{ReceiveOutcome, TransferMessage, spawn_receiver, spawn_sender};

pub mod api;
pub mod bus;
pub mod configuration;
pub mod control;
pub mod database;
pub mod directory_manager;
pub mod fetch;
pub mod file_bytes;
pub mod identity;
pub mod paths;
pub mod transfer;
pub mod transport;
pub mod watcher;

/// A resolved sync-directory destination for a content-bearing change, plus how
/// that directory should be told to place the bytes. Produced by
/// `handle_changes` after the origin-skip and tag-match filtering, then turned
/// into a [`SyncDirectoryCommand`] with the actual [`FileBytes`] once the
/// move-vs-copy policy has been applied.
enum ContentTarget {
    Create {
        file_id: FileId,
        physical_path: PhysicalPath,
        sync_directory_path: std::path::PathBuf,
    },
    Change {
        file_id: FileId,
        sync_directory_path: std::path::PathBuf,
    },
}

/// Why the peer session started a receiver transfer, and what to do with the
/// received bytes on completion. Carried alongside a transfer's outcome so the
/// session's completion handler can dispatch correctly.
enum ReceiverPurpose {
    /// Reconciliation / live-change pull: materialize the received bytes into
    /// our sync directories and record the version, placing per `placement`.
    Materialize {
        file_id: FileId,
        content_hash: String,
        origin: ChangeOrigin,
        placement: bus::MaterializePlacement,
    },
    /// On-demand fetch (`onisync edit`): follow-up action from the fetch engine
    /// (deliver to the local waiter, or relay upward).
    Fetch { then: fetch::FoundThen },
}

impl ContentTarget {
    fn into_command(self, content: FileBytes) -> SyncDirectoryCommand {
        match self {
            ContentTarget::Create {
                file_id,
                physical_path,
                sync_directory_path,
            } => SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content,
                sync_directory_path,
            },
            ContentTarget::Change {
                file_id,
                sync_directory_path,
            } => SyncDirectoryCommand::ChangeFile {
                file_id,
                content,
                sync_directory_path,
            },
        }
    }
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
    change_sender: UnboundedSender<DaemonMessage>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
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
#[derive(Debug)]
pub enum RunError {
    /// The identity key could not be loaded from `identity_file`.
    Identity {
        path: PathBuf,
        source: std::io::Error,
    },
    /// Opening the main database failed.
    Database(database::DatabaseError),
    /// Binding the peer-sync listener failed.
    Bind {
        address: String,
        source: std::io::Error,
    },
}

impl std::fmt::Display for RunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Identity { path, source } => write!(
                formatter,
                "failed to load identity key at {}: {source}",
                path.display()
            ),
            RunError::Database(error) => {
                write!(formatter, "failed to open main database: {error:?}")
            }
            RunError::Bind { address, source } => {
                write!(
                    formatter,
                    "failed to bind peer listener to {address}: {source}"
                )
            }
        }
    }
}

impl std::error::Error for RunError {}

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

/// Start the onisync sync engine, returning a UI-facing [`Api`](api::Api)
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

    // Shared table of in-flight on-demand fetches (`onisync edit`). Seeded by
    // `handle_changes` for local-origin requests and by peer sessions for
    // relayed ones; replies are routed by `request_id`. Owns the peer runtime so
    // its engine operations are plain methods. Cheap to clone (Arcs).
    let pending_fetches = crate::fetch::PendingFetches::new(runtime_configuration.clone());

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
    let database = FileDatabase::initialize(&main_db_path).map_err(RunError::Database)?;
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
            .name("onisync-sync-directories".to_owned())
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
        runtime_configuration.clone(),
        pending_fetches.clone(),
        database,
        change_receiver,
        change_sender.clone(),
        command_sender.clone(),
        event_sender,
        shutdown.token().child_token(),
    ));

    // The routing handles every peer-connection task needs. Built once and
    // cloned per spawned task (below, and in the accept loop inside `driver`).
    let peer_context = PeerContext {
        runtime_configuration: runtime_configuration.clone(),
        pending_fetches: pending_fetches.clone(),
        change_sender: change_sender.clone(),
        command_sender: command_sender.clone(),
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

        log::info!("onisync runtime stopped cleanly");
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

                run_peer_session(
                    &peer.public_key,
                    &peer.name,
                    &main_db_path,
                    outgoing,
                    incoming,
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
/// (`connect_to_peer`) paths because the post-handshake behaviour is identical:
/// build and send our manifest, register an outbound channel, then loop over
/// outbound `Frame`s and inbound WebSocket frames.
///
/// Opens its own read-only handle on the main DB. The DB is shared with
/// `handle_changes` and with other connection tasks; SQLite serialises these
/// accesses at the file level. Writes still only happen from `handle_changes`.
async fn run_peer_session<S>(
    peer_public_key: &str,
    peer_name: &str,
    main_db_path: &std::path::Path,
    mut outgoing: SplitSink<WebSocketStream<S>, Message>,
    mut incoming: SplitStream<WebSocketStream<S>>,
    context: PeerContext,
    shutdown: &CancellationToken,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let PeerContext {
        runtime_configuration,
        pending_fetches,
        change_sender,
        command_sender,
    } = context;

    // FileDatabase wraps a rusqlite Connection which is Send but not Sync.
    // We must never hold `&FileDatabase` across an `.await` in this task,
    // otherwise tokio::spawn rejects the future as non-Send. All sync helpers
    // below take `&FileDatabase` synchronously and return owned data; this
    // function does the awaits separately.
    //
    // The database path is supplied by the caller. This connection is
    // READ-ONLY: the session makes reconciliation decisions from it but routes
    // every write through `change_sender` to `handle_changes`, the sole
    // main-database writer.
    let database = match FileDatabase::initialize(main_db_path) {
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

    // Per-link file transfers. `transfers` demuxes inbound `Sync::Transfer*`
    // frames (by `TransferId`) to the endpoint task serving them — a sender we
    // started (answering a peer's `TransferStart`) or a receiver we started
    // (pulling a file we reconciled as wanted). When a receiver finishes it
    // reports on `receiver_done`, which the select loop drains to materialize
    // the bytes and drop the demux entry.
    let mut transfers: HashMap<TransferId, UnboundedSender<TransferMessage>> = HashMap::new();

    let (receiver_done_tx, mut receiver_done_rx) =
        tokio::sync::mpsc::unbounded_channel::<(TransferId, ReceiverPurpose, ReceiveOutcome)>();

    // Temp directory for in-flight received files. Kept per-session under the
    // system temp dir; a completed transfer's temp file is then materialized
    // (moved) into the sync directories.
    let transfer_temp_dir = std::env::temp_dir().join(format!(
        "onisync-transfer-{}-{}",
        std::process::id(),
        peer_public_key
    ));

    // Start a receiver pull for `file_id`/`content_hash` on this link, tagged
    // with `purpose` (what to do with the bytes once received). Spawns the
    // receiver endpoint and a bridge that forwards its outcome onto
    // `receiver_done_rx`. Returns the `(transfer_id, inbound_sender)` for the
    // caller to register in the demux table.
    let start_pull = {
        let our_sender = our_sender.clone();
        let receiver_done_tx = receiver_done_tx.clone();
        let transfer_temp_dir = transfer_temp_dir.clone();

        move |file_id: FileId,
              content_hash: String,
              expected_size: u64,
              purpose: ReceiverPurpose| {
            let transfer_id = TransferId::new();
            let temp_path = transfer_temp_dir.join(transfer_id.to_string());
            let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();

            let inbound = spawn_receiver(
                transfer_id,
                file_id,
                content_hash,
                expected_size,
                temp_path,
                our_sender.clone(),
                Frame::Sync,
                outcome_tx,
            );

            let done_tx = receiver_done_tx.clone();
            tokio::spawn(async move {
                if let Ok(outcome) = outcome_rx.await {
                    let _ = done_tx.send((transfer_id, purpose, outcome));
                }
            });

            (transfer_id, inbound)
        }
    };

    if let Err(error) = tokio::fs::create_dir_all(&transfer_temp_dir).await {
        log::warn!(
            "Failed to create transfer temp dir for {peer_name}: {error}; transfers to this peer will fail"
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
                        "Peer {peer_name} already has an outbound sender; inbound-only mode for this connection"
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
                        let purpose = ReceiverPurpose::Materialize {
                            file_id,
                            content_hash: content_hash.clone(),
                            origin: ChangeOrigin::Peer {
                                public_key: peer_public_key.to_owned(),
                            },
                            placement,
                        };
                        let (transfer_id, inbound) =
                            start_pull(file_id, content_hash, expected_size, purpose);
                        transfers.insert(transfer_id, inbound);
                    }
                }
            }
            completed = receiver_done_rx.recv() => {
                let Some((transfer_id, purpose, outcome)) = completed else {
                    // The done channel is never fully dropped while the session
                    // lives (we hold a sender clone), so `None` only at teardown.
                    continue;
                };
                transfers.remove(&transfer_id);
                let content = match outcome {
                    ReceiveOutcome::Complete(content) => content,
                    ReceiveOutcome::Failed(error) => {
                        log::warn!(
                            "Transfer {transfer_id} from {peer_name} failed: {error}"
                        );
                        // A fetch that fails to receive its bytes must still
                        // report failure to its waiter/parent so the fetch does
                        // not hang.
                        if let ReceiverPurpose::Fetch { then } = purpose {
                            match then {
                                fetch::FoundThen::DeliverLocal(sender) => {
                                    let _ = sender.send(Err(bus::FetchError::NotAvailable));
                                }
                                fetch::FoundThen::RelayUp {
                                    parent_peer,
                                    request_id,
                                    ..
                                } => {
                                    if let Some(sender) =
                                        peer_outbound(&runtime_configuration, &parent_peer).await
                                    {
                                        let _ = sender.send(Frame::Sync(
                                            SyncMessage::FetchMissing { request_id },
                                        ));
                                    }
                                }
                            }
                        }
                        continue;
                    }
                };
                match purpose {
                    ReceiverPurpose::Materialize {
                        file_id,
                        content_hash,
                        origin,
                        placement,
                    } => {
                        log::debug!(
                            "Transfer {transfer_id} from {peer_name} completed for {}; materializing",
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
                                "change_sender closed; cannot materialize transfer for {}: {error}",
                                file_id.to_string()
                            );
                            break;
                        }
                    }
                    ReceiverPurpose::Fetch { then } => match then {
                        fetch::FoundThen::DeliverLocal(sender) => {
                            let _ = sender.send(Ok(content));
                        }
                        fetch::FoundThen::RelayUp {
                            parent_peer,
                            request_id,
                            file_id,
                            content_hash,
                            size,
                        } => {
                            // Relay: hold the received bytes so our parent can
                            // pull them, then announce (content-less, but
                            // carrying the size) upward.
                            pending_fetches
                                .cache_relay_file(file_id, content_hash.clone(), content)
                                .await;
                            if let Some(sender) =
                                peer_outbound(&runtime_configuration, &parent_peer).await
                            {
                                let _ = sender.send(Frame::Sync(SyncMessage::FetchFound {
                                    request_id,
                                    file_id,
                                    content_hash,
                                    size,
                                }));
                            }
                        }
                    },
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
                        // Confirm the peer is registered (so the pull transfers
                        // below have a live link to drive) before running the
                        // synchronous reconciliation. Doing the DB work outside
                        // of any held `RwLockReadGuard` keeps this future `Send`
                        // (FileDatabase isn't Sync).
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

                        // Capture the announced file_ids before `reconcile_peer_manifest`
                        // consumes `entries`; used for the placement sweep below.
                        let announced_file_ids: Vec<FileId> =
                            entries.iter().map(|entry| entry.file_id).collect();

                        let (wanted, deletions) =
                            reconcile_peer_manifest(peer_name, entries, &database);

                        // Apply peer deletions that won last-writer-wins by
                        // enqueuing them through the sole DB writer.
                        for WantedDeletion {
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

                        // Files we are pulling as a result of catalog reconciliation
                        // (below); excluded from the placement sweep so we do not
                        // double-fetch them.
                        let pulling: HashSet<FileId> =
                            wanted.iter().map(|wanted| wanted.file_id).collect();
                        // Start a pull transfer for each wanted file: we are the
                        // receiver/driver, the peer serves via its own sender
                        // when it sees our `TransferStart`. `placement` is
                        // `Create` for files we've never seen (using the
                        // manifest's `logical_path`) and `Change` for files we
                        // already know — see `reconcile_peer_manifest`.
                        for WantedFile {
                            file_id,
                            content_hash,
                            size,
                            placement,
                        } in wanted
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
                            let purpose = ReceiverPurpose::Materialize {
                                file_id,
                                content_hash: content_hash.clone(),
                                origin: ChangeOrigin::Peer {
                                    public_key: peer_public_key.to_owned(),
                                },
                                placement,
                            };
                            let (transfer_id, inbound) =
                                start_pull(file_id, content_hash, size as u64, purpose);
                            transfers.insert(transfer_id, inbound);
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
                        // `FetchRequest`s and awaits `FetchFound` replies that
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
                    Frame::Sync(SyncMessage::FetchRequest {
                        request_id,
                        file_id,
                        expected_hash,
                    }) => {
                        // A peer is asking us (recursively) for a file's bytes.
                        // Answer locally if we hold matching content (with its
                        // size, so the puller can cap its window); otherwise
                        // relay to our other peers. The bytes themselves are
                        // pulled over a transfer later.
                        let local_size =
                            local_hash_matches(&command_sender, file_id, &expected_hash).await;
                        log::debug!(
                            "FetchRequest from {peer_name} for {} (expected_hash {}): \
                             local_size={local_size:?}",
                            file_id.to_string(),
                            expected_hash,
                        );
                        pending_fetches
                            .handle_incoming_request(
                                peer_public_key,
                                request_id,
                                file_id,
                                expected_hash,
                                local_size,
                            )
                            .await;
                    }
                    Frame::Sync(SyncMessage::FetchFound {
                        request_id,
                        file_id,
                        content_hash,
                        size,
                    }) => {
                        // Content-less: a child announced it has the file (and
                        // its size). Pull the bytes from that child over a
                        // transfer, then either deliver locally (origin) or relay
                        // upward.
                        let action = pending_fetches
                            .handle_incoming_found(
                                peer_public_key,
                                request_id,
                                file_id,
                                content_hash,
                                size,
                            )
                            .await;
                        if let Some(fetch::FoundAction::Receive {
                            from_peer,
                            file_id,
                            expected_hash,
                            expected_size,
                            then,
                        }) = action
                        {
                            // The child that answered is `from_peer`, which is
                            // this very connection (`peer_public_key`); pull over
                            // our own link. The size carried by `FetchFound` caps
                            // the request window at EOF.
                            let _ = from_peer; // == peer_public_key; kept for clarity
                            let (transfer_id, inbound) = start_pull(
                                file_id,
                                expected_hash,
                                expected_size,
                                ReceiverPurpose::Fetch { then },
                            );
                            transfers.insert(transfer_id, inbound);
                        }
                    }
                    Frame::Sync(SyncMessage::FetchMissing { request_id }) => {
                        pending_fetches
                            .handle_incoming_missing(peer_public_key, request_id)
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
                                "No outbound channel registered for {peer_name}; \
                                 cannot answer tag manifest"
                            );
                            continue;
                        };
                        reconcile_peer_tag_manifest(
                            peer_name,
                            peer_public_key,
                            definitions,
                            relationships,
                            &database,
                            &outbound,
                            &change_sender,
                        );
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
                                "No outbound channel for {peer_name}; \
                                 dropping response to TagRequest"
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
                    // A peer opened a transfer: we are the holder/sender. Read
                    // the file's bytes off disk (as a `FileToCopy`, so nothing
                    // is buffered) and spawn a sender endpoint to serve them.
                    Frame::Sync(SyncMessage::TransferStart {
                        transfer_id,
                        file_id,
                        content_hash,
                    }) => {
                        let (respond_to, response) = tokio::sync::oneshot::channel();
                        if command_sender
                            .send(SyncDirectoryCommand::ReadFile { file_id, respond_to })
                            .is_err()
                        {
                            log::error!(
                                "command_sender closed; cannot serve transfer {transfer_id} \
                                 to {peer_name}"
                            );
                            break;
                        }
                        let read_result = response.await.ok().flatten();
                        log::debug!(
                            "TransferStart {transfer_id} from {peer_name} for {} \
                             (content_hash {}): local read {}",
                            file_id.to_string(),
                            content_hash,
                            match &read_result {
                                Some((_, _, local_hash)) =>
                                    format!("hash {local_hash}"),
                                None => "absent".to_owned(),
                            },
                        );
                        // Resolve who serves the bytes, in priority order:
                        //   1. a matching file in our sync directories
                        //   2. a temporary provider (a local client serving on
                        //      demand, e.g. the CLI uploading)
                        //   3. a file we are holding as a fetch relay (evicted).
                        // Each yields an endpoint we register in the demux table.
                        let sync_content = match read_result {
                            Some((_physical_path, content, local_hash))
                                if local_hash == content_hash =>
                            {
                                Some(content)
                            }
                            _ => None,
                        };
                        let inbound = if let Some(content) = sync_content {
                            log::debug!(
                                "TransferStart {transfer_id}: serving {} from a sync directory",
                                file_id.to_string()
                            );
                            Some(spawn_sender(transfer_id, content, our_sender.clone(), Frame::Sync))
                        } else if let Some(provider) =
                            pending_fetches.provider_for(file_id, &content_hash).await
                        {
                            log::debug!(
                                "TransferStart {transfer_id}: serving {} from a local provider",
                                file_id.to_string()
                            );
                            Some(spawn_sender(transfer_id, provider, our_sender.clone(), Frame::Sync))
                        } else {
                            pending_fetches
                                .take_fetch_cached(file_id, &content_hash)
                                .await
                                .map(|content| {
                                    log::debug!(
                                        "TransferStart {transfer_id}: serving {} from the fetch cache",
                                        file_id.to_string()
                                    );
                                    spawn_sender(transfer_id, content, our_sender.clone(), Frame::Sync)
                                })
                        };
                        match inbound {
                            Some(inbound) => {
                                // Deliver the opening `Start` so the sender knows
                                // the transfer began (it only acts on requests).
                                let _ = inbound.send(TransferMessage::Start {
                                    file_id,
                                    content_hash,
                                });
                                transfers.insert(transfer_id, inbound);
                            }
                            None => {
                                log::warn!(
                                    "Transfer {transfer_id} from {peer_name}: {} not available \
                                     (no sync dir, provider, or fetch cache); aborting",
                                    file_id.to_string()
                                );
                                let _ = our_sender.send(Frame::Sync(SyncMessage::TransferAbort {
                                    transfer_id,
                                    reason: "file not available".to_owned(),
                                }));
                            }
                        }
                    }
                    // Chunk requests / chunks / aborts belong to an existing
                    // transfer: demux to its endpoint by `transfer_id`.
                    Frame::Sync(sync @ SyncMessage::TransferChunkRequest { .. })
                    | Frame::Sync(sync @ SyncMessage::TransferChunk { .. })
                    | Frame::Sync(sync @ SyncMessage::TransferAbort { .. }) => {
                        if let Some((transfer_id, message)) = TransferMessage::from_sync(sync) {
                            match transfers.get(&transfer_id) {
                                Some(endpoint) => {
                                    let is_abort =
                                        matches!(message, TransferMessage::Abort { .. });
                                    if endpoint.send(message).is_err() || is_abort {
                                        // Endpoint gone, or the transfer aborted:
                                        // drop the demux entry.
                                        transfers.remove(&transfer_id);
                                    }
                                }
                                None => {
                                    // Benign and expected: a completed receiver
                                    // removes its demux entry while the sender's
                                    // windowed in-flight chunks (offsets past
                                    // EOF) are still arriving. Trace, not debug.
                                    log::trace!(
                                        "Peer {peer_name} sent frame for unknown transfer \
                                         {transfer_id}; ignoring (late/orphaned)"
                                    );
                                }
                            }
                        }
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

/// Read every file's full version history from the main DB and pack into a
/// `Vec<ManifestEntry>` suitable for `Sync::Manifest`. Files without any
/// recorded version are skipped (with a warning).
fn build_local_manifest(database: &FileDatabase) -> Result<Vec<ManifestEntry>, String> {
    let rows = database
        .manifest_entries()
        .map_err(|e| format!("manifest_entries: {e:?}"))?;
    Ok(rows
        .into_iter()
        .map(
            |(file_id, history, latest_observed_at, logical_path, deleted, deleted_at)| {
                ManifestEntry {
                    file_id,
                    history,
                    latest_observed_at,
                    logical_path,
                    deleted,
                    deleted_at,
                }
            },
        )
        .collect())
}

/// One reconciliation outcome: pull `content_hash` for `file_id` from the peer
/// and materialize it with `placement`.
///
/// Placement is `Create` when we've never seen this file locally (the
/// manifest's `logical_path` gives us where to put it) and `Change` when we
/// already know the file and are only fetching newer bytes.
#[derive(Debug, Clone)]
pub struct WantedFile {
    pub file_id: FileId,
    pub content_hash: String,
    /// The peer's latest-version content size in bytes (from the manifest
    /// history), recorded alongside the hash when we catalog the version.
    pub size: i64,
    pub placement: bus::MaterializePlacement,
}

/// A file deletion learned from a peer's manifest that wins last-writer-wins
/// against our local state (the peer's `deleted_at` is newer than our latest
/// version `observed_at`). Applied by enqueuing a `Change::FileDeleted`.
#[derive(Debug, Clone)]
pub struct WantedDeletion {
    pub file_id: FileId,
    pub deleted_at: i64,
}

/// Compare the peer's manifest against our local `file_versions` table and
/// decide which files we should pull from them.
///
/// Returns each wanted file paired with the peer's latest content hash and a
/// placement describing how the eventual `Materialize` should route the bytes.
/// The caller starts a pull transfer per returned entry.
///
/// Pure synchronous function: no `.await`, no `RwLock`, no channels/transfers.
/// Lets callers hold `&FileDatabase` (which is `!Sync`) without making their
/// future non-`Send`, and keeps the decision testable. **Note:** for unknown
/// files this function does *not* insert the `files` row itself; the caller
/// must `add_file` before starting the pull (mirroring the live
/// `FileMetadataAdded` handler).
///
/// Categories per entry:
/// - **Unknown file_id**: we've never seen this file — request it and place
///   with `Create { logical_path, tags: <empty> }`. Tags arrive independently
///   via `Sync::TagManifest`; `reconcile_tag_placement` will re-place the file
///   into any TagBased sync directory that later matches.
/// - **Equal latest**: identical state — nothing to do.
/// - **Sender's latest hash appears in our history**: they are behind. Their
///   side will request from us when they process our manifest; we do nothing.
/// - **Our latest hash appears in their history**: we are behind — request the
///   newer bytes (`Change` placement, we already hold the file row).
/// - **Divergent**: neither side's latest appears in the other's history. Newer
///   `latest_observed_at` wins. If theirs wins, request. If ours wins, do
///   nothing (their side will accept ours via the symmetric path). Divergence
///   is logged at `error!` level with a TODO for a future deadletter store.
fn reconcile_peer_manifest(
    peer_name: &str,
    entries: Vec<ManifestEntry>,
    database: &FileDatabase,
) -> (Vec<WantedFile>, Vec<WantedDeletion>) {
    log::info!(
        "Reconciling {} manifest entries from {peer_name}",
        entries.len()
    );

    let mut wanted = Vec::new();
    let mut deletions = Vec::new();
    for entry in entries {
        // Deletion tombstones take precedence over content reconciliation. If
        // the peer advertises a delete, apply last-writer-wins against our
        // latest local version: a delete newer than our latest edit wins (we
        // apply it); an edit newer than the delete keeps the file (the normal
        // content path below handles pulling/keeping bytes). This is the
        // restore-after-delete rule.
        if entry.deleted {
            let ours_observed_at = database
                .latest_version(entry.file_id)
                .ok()
                .flatten()
                .map(|version| version.observed_at)
                .unwrap_or(0);
            if entry.deleted_at > ours_observed_at {
                log::debug!(
                    "Applying peer delete for {} from {peer_name} (deleted_at={} > ours_observed_at={ours_observed_at})",
                    entry.file_id.to_string(),
                    entry.deleted_at,
                );
                deletions.push(WantedDeletion {
                    file_id: entry.file_id,
                    deleted_at: entry.deleted_at,
                });
            }
            // Whether or not the delete won, do not also request bytes for a
            // tombstoned entry.
            continue;
        }

        let decision = match decide_request(database, &entry) {
            Ok(decision) => decision,
            Err(error) => {
                log::error!(
                    "Reconciliation lookup failed for {}: {error:?}",
                    entry.file_id.to_string()
                );
                continue;
            }
        };
        // The hash and size we want are the peer's latest for this file.
        let their_latest = entry
            .history
            .last()
            .map(|(_, hash, size)| (hash.clone(), *size));
        // Placement depends on whether we already know the file locally: an
        // unknown file must be materialized as `Create` (using the manifest's
        // `logical_path`) so the sync-directory dispatch can place it.
        let known = database.file_exists(entry.file_id).unwrap_or(false);
        let placement_for_request = |entry: &ManifestEntry| -> bus::MaterializePlacement {
            if known {
                bus::MaterializePlacement::Change
            } else {
                // Tags are deliberately empty here; they are reconciled via
                // `Sync::TagManifest` and, when they land, the incoming
                // `FileTagged` handler runs `reconcile_tag_placement` to
                // re-place the file into any newly-matching TagBased sync
                // directories using the already-materialized bytes as a
                // source. This gives order-independence between the file and
                // tag manifests without enforcing a global ordering.
                bus::MaterializePlacement::Create {
                    logical_path: entry.logical_path.clone(),
                    tags: Vec::new(),
                }
            }
        };
        match decision {
            ReconcileDecision::Nothing => {}
            ReconcileDecision::Request(reason) => {
                log::debug!(
                    "Requesting {} from {peer_name}: {reason}",
                    entry.file_id.to_string()
                );
                if let Some((hash, size)) = their_latest {
                    let placement = placement_for_request(&entry);
                    wanted.push(WantedFile {
                        file_id: entry.file_id,
                        content_hash: hash,
                        size,
                        placement,
                    });
                }
            }
            ReconcileDecision::Divergent {
                ours_observed_at,
                request,
            } => {
                // TODO: When a deadletter / conflict store exists, preserve
                // the losing version there instead of just logging.
                log::error!(
                    "Divergent history for {} between us and {peer_name} (our latest observed_at={ours_observed_at}, theirs={}). {}.",
                    entry.file_id.to_string(),
                    entry.latest_observed_at,
                    if request {
                        "Their version wins; requesting"
                    } else {
                        "Our version wins; keeping"
                    },
                );
                if request && let Some((hash, size)) = their_latest {
                    let placement = placement_for_request(&entry);
                    wanted.push(WantedFile {
                        file_id: entry.file_id,
                        content_hash: hash,
                        size,
                        placement,
                    });
                }
            }
        }
    }

    (wanted, deletions)
}

enum ReconcileDecision {
    Nothing,
    Request(&'static str),
    Divergent {
        ours_observed_at: i64,
        request: bool,
    },
}

/// Pure decision function: given our local DB and the peer's entry, what
/// should we do? Separated from `handle_peer_manifest` so it can be reasoned
/// about (and later tested) without touching channels.
fn decide_request(
    database: &FileDatabase,
    entry: &ManifestEntry,
) -> Result<ReconcileDecision, database::DatabaseError> {
    let our_history = database.version_history(entry.file_id)?;

    if our_history.is_empty() {
        return Ok(ReconcileDecision::Request("unknown file"));
    }

    let their_latest = match entry.history.last() {
        Some((_, hash, _)) => hash.as_str(),
        None => return Ok(ReconcileDecision::Nothing),
    };
    let our_latest = our_history
        .last()
        .expect("checked non-empty above")
        .1
        .as_str();

    if our_latest == their_latest {
        return Ok(ReconcileDecision::Nothing);
    }

    let our_hashes: HashSet<&str> = our_history
        .iter()
        .map(|(_, hash, _)| hash.as_str())
        .collect();
    let their_hashes: HashSet<&str> = entry
        .history
        .iter()
        .map(|(_, hash, _)| hash.as_str())
        .collect();

    let they_have_our_latest = their_hashes.contains(our_latest);
    let we_have_their_latest = our_hashes.contains(their_latest);

    match (they_have_our_latest, we_have_their_latest) {
        // Their latest is somewhere in our history → they are strictly behind.
        // They'll request from us when they process our manifest.
        (_, true) => Ok(ReconcileDecision::Nothing),
        // Our latest is somewhere in their history → we are strictly behind.
        (true, false) => Ok(ReconcileDecision::Request("we are behind")),
        // Neither side knows the other's latest hash → divergent.
        (false, false) => {
            let ours_observed_at = database
                .latest_version(entry.file_id)?
                .map(|version| version.observed_at)
                .unwrap_or(0);
            let request = entry.latest_observed_at > ours_observed_at;
            Ok(ReconcileDecision::Divergent {
                ours_observed_at,
                request,
            })
        }
    }
}

/// Read `file_id`'s bytes from local sync directories, but only return them if
/// they hash to `expected_hash`. Used by the on-demand fetch engine to decide
/// whether this node can satisfy a `Sync::FetchRequest` locally.
///
/// Returns `Some(bytes)` on a hash match, `None` if the file is absent locally
/// or its local content does not match the requested hash (in which case the
/// request should be forwarded to peers).
async fn read_local_if_hash_matches(
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

/// Resolve a peer's outbound `Frame` sender by public key, if connected.
async fn peer_outbound(
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    public_key: &str,
) -> Option<UnboundedSender<Frame>> {
    runtime_configuration
        .read()
        .await
        .peers
        .get(public_key)
        .and_then(|runtime_peer| runtime_peer.outbound.clone())
}

/// Ask the peer that announced a change (`change_origin`) to serve us its
/// bytes: send a `StartReceive` command to that peer's live session, which owns
/// the transfer machinery. No-op if the change is local-origin or the peer has
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
                    "Peer {public_key} command channel closed; cannot pull {}; reconciliation will retry on reconnect",
                    file_id.to_string()
                );
            }
        }
        None => {
            log::debug!(
                "Announcing peer {public_key} has no live session; deferring pull of {} to reconciliation",
                file_id.to_string()
            );
        }
    }
}

/// Whether a local sync directory holds `file_id` with content hashing to
/// `expected_hash`, without buffering the bytes. Used to decide whether we can
/// answer a relayed `Sync::FetchRequest` (as a content-less `FetchFound`) or
/// must forward it onward.
/// If we hold `file_id` locally with content matching `expected_hash`, return
/// `Some(size_in_bytes)`; otherwise `None`. The size is read from the bytes we
/// would serve, so a `FetchFound` we send can carry it and the puller can cap
/// its request window at EOF.
async fn local_hash_matches(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    file_id: FileId,
    expected_hash: &str,
) -> Option<u64> {
    let (respond_to, response) = tokio::sync::oneshot::channel();
    if command_sender
        .send(SyncDirectoryCommand::ReadFile {
            file_id,
            respond_to,
        })
        .is_err()
    {
        return None;
    }
    match response.await {
        Ok(Some((_physical_path, content, content_hash))) => {
            if content_hash != expected_hash {
                log::debug!(
                    "local_hash_matches: {} held locally but hash {} != expected {}",
                    file_id.to_string(),
                    content_hash,
                    expected_hash,
                );
                return None;
            }
            match content.byte_len().await {
                Ok(size) => Some(size),
                Err(error) => {
                    log::warn!(
                        "local_hash_matches: {} matched but size read failed: {error}; answering without a size hint",
                        file_id.to_string()
                    );
                    // Treat as held-but-size-unknown: 0 disables the puller's cap.
                    Some(0)
                }
            }
        }
        Ok(None) => {
            log::debug!(
                "local_hash_matches: {} not held in any sync directory",
                file_id.to_string()
            );
            None
        }
        Err(_) => None,
    }
}

/// Build our local tag manifest: lightweight definition entries (id +
/// `modified_at`) plus every relationship (with its soft-delete state). The
/// tag counterpart of [`build_local_manifest`].
fn build_local_tag_manifest(
    database: &FileDatabase,
) -> Result<(Vec<TagManifestEntry>, Vec<RelationshipManifestEntry>), String> {
    let definitions = database
        .tag_manifest_entries()
        .map_err(|e| format!("tag_manifest_entries: {e:?}"))?;
    let relationships = database
        .relationship_manifest_entries()
        .map_err(|e| format!("relationship_manifest_entries: {e:?}"))?;
    Ok((definitions, relationships))
}

/// Reconcile a peer's tag manifest against ours.
///
/// - **Definitions**: for each tag whose `modified_at` is newer than ours (or
///   that we don't know), enqueue a `TagRequest`; the peer answers with a
///   `Change::TagAdded` carrying the full definition. Older/equal definitions
///   are skipped — the peer will request ours via the symmetric path.
/// - **Relationships**: applied directly. Each carries its whole state, so we
///   translate it into the matching relationship `Change` (tag/untag) stamped
///   with the peer's `modified_at` and hand it to the single DB-writer, which
///   enforces last-writer-wins. This routes through the same code path as a
///   live relationship change, keeping behaviour uniform.
///
/// Pure of `.await` and holds no lock: takes `&FileDatabase` synchronously so
/// the caller's future stays `Send`.
fn reconcile_peer_tag_manifest(
    peer_name: &str,
    peer_public_key: &str,
    definitions: Vec<TagManifestEntry>,
    relationships: Vec<RelationshipManifestEntry>,
    database: &FileDatabase,
    outbound: &UnboundedSender<Frame>,
    change_sender: &UnboundedSender<DaemonMessage>,
) {
    log::info!(
        "Reconciling {} tag definitions and {} relationships from {peer_name}",
        definitions.len(),
        relationships.len()
    );

    for definition in definitions {
        let ours = match database.tag_modified_at(definition.tag_id) {
            Ok(value) => value,
            Err(error) => {
                log::error!(
                    "tag_modified_at failed for {}: {error:?}",
                    definition.tag_id.to_string()
                );
                continue;
            }
        };
        // Act when we don't know the tag, or the peer's is strictly newer.
        let need = match ours {
            None => true,
            Some(ours) => definition.modified_at > ours,
        };
        if !need {
            continue;
        }

        if definition.deleted {
            // The peer's newer state is a tombstone. Apply it directly through
            // the single DB writer (a tag delete bumps `modified_at`, so the
            // same LWW comparison decides delete-vs-edit). No `TagRequest`: there
            // is no definition to fetch for a deleted tag.
            if let Err(error) = change_sender.send(DaemonMessage::Change(
                Ingest::from_change(Change::TagRemoved {
                    tag_id: definition.tag_id,
                    modified_at: definition.modified_at,
                }),
                ChangeOrigin::Peer {
                    public_key: peer_public_key.to_owned(),
                },
            )) {
                log::error!("change_sender closed; cannot apply reconciled tag delete: {error}");
                return;
            }
        } else {
            // A live tag whose definition is newer than ours: request it.
            let frame = Frame::Sync(SyncMessage::TagRequest {
                tag_id: definition.tag_id,
            });
            if let Err(error) = outbound.send(frame) {
                log::warn!("Failed to enqueue TagRequest for {peer_name}: {error}");
                return;
            }
        }
    }

    for relationship in relationships {
        // LWW pre-check: skip anything not newer than what we hold. The DB layer
        // also enforces this, but skipping here avoids bus traffic for no-ops.
        let ours = match database.relationship_modified_at(
            relationship.tag_id,
            &relationship.target_id,
            relationship.kind.into(),
        ) {
            Ok(value) => value,
            Err(error) => {
                log::error!("relationship_modified_at failed: {error:?}");
                continue;
            }
        };
        if let Some(ours) = ours
            && relationship.modified_at <= ours
        {
            continue;
        }

        let Some(change) = relationship_to_change(&relationship) else {
            log::warn!(
                "Skipping relationship with unparseable target_id {}",
                relationship.target_id
            );
            continue;
        };
        if let Err(error) = change_sender.send(DaemonMessage::Change(
            Ingest::from_change(change),
            ChangeOrigin::Peer {
                public_key: peer_public_key.to_owned(),
            },
        )) {
            log::error!("change_sender closed; cannot apply reconciled relationship: {error}");
            return;
        }
    }
}

/// Translate a reconciled relationship manifest entry into the equivalent
/// relationship `Change`, carrying the entry's `modified_at` so
/// last-writer-wins is preserved. Returns `None` if `target_id` doesn't parse
/// as the id kind.
fn relationship_to_change(entry: &RelationshipManifestEntry) -> Option<Change> {
    use onisync_core::state::RelationshipKind;
    let change = match (entry.kind, entry.deleted) {
        (RelationshipKind::File, false) => Change::FileTagged {
            file_id: FileId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            metadata: None,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::File, true) => Change::FileUntagged {
            file_id: FileId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::Tag, false) => Change::TagTagged {
            taggee_id: TagId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            metadata: None,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::Tag, true) => Change::TagUntagged {
            taggee_id: TagId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            modified_at: entry.modified_at,
        },
    };
    Some(change)
}

/// Answer a peer's `TagRequest` with the full tag definition as a
/// `Change::TagAdded`, or `TagNotFound` if we no longer hold the tag. The tag
/// counterpart of [`build_request_response`].
fn build_tag_request_response(peer_name: &str, tag_id: TagId, database: &FileDatabase) -> Frame {
    match database.tag_definition(tag_id) {
        Ok(Some((name, color, modified_at))) => Frame::Change(Change::TagAdded {
            tag_id,
            tag_name: name,
            color,
            metadata: None,
            modified_at,
        }),
        Ok(None) => {
            log::warn!(
                "TagRequest from {peer_name} for {} but we no longer hold it",
                tag_id.to_string()
            );
            Frame::Sync(SyncMessage::TagNotFound { tag_id })
        }
        Err(error) => {
            log::error!(
                "tag_definition failed for {} requested by {peer_name}: {error:?}",
                tag_id.to_string()
            );
            Frame::Sync(SyncMessage::TagNotFound { tag_id })
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

fn contains_all_tags(sync_directory_tags: &[TagId], file_tags: &[TagId]) -> bool {
    sync_directory_tags
        .iter()
        .all(|tag_id| file_tags.contains(tag_id))
}

/// The effective tag set to use when deciding `Create` placement for a
/// materialized file: the union of the tags carried on the placement and the
/// file's current tags in the database.
///
/// The carried tags cover a live `FileMetadataAdded` whose `FileTagged`
/// relationships may not be applied yet; the DB tags cover a `Manifest`
/// reconcile pull, which carries empty tags (it cannot know them at pull time)
/// but whose `FileTagged` changes have been applied by the time the pull's
/// `Materialize` runs. Using the union places the file correctly in both cases.
fn effective_placement_tags(carried: &[TagId], db_tags: &[TagId]) -> Vec<TagId> {
    let mut effective = carried.to_vec();
    for tag_id in db_tags {
        if !effective.contains(tag_id) {
            effective.push(*tag_id);
        }
    }
    effective
}

/// Owned data a deferred tag placement needs to fetch and materialize a file's
/// bytes. Produced by the synchronous [`reconcile_tag_placement`] DB step and
/// consumed by the async [`fetch_and_place_deferred`]. Deliberately holds no
/// `!Send` `&FileDatabase` borrow so the fetch future stays `Send`.
struct DeferredPlacement {
    file_id: FileId,
    logical_path: LogicalPath,
    file_tags: Vec<TagId>,
    /// The file's latest catalog version hash, or `None` if the file has no
    /// recorded version (in which case there is nothing to fetch by).
    latest_hash: Option<String>,
    /// Resolves to `true` if the manager deferred placement (wants the file but
    /// has no local bytes to source), `false` otherwise.
    deferred: tokio::sync::oneshot::Receiver<bool>,
}

/// Ask the sync-directory manager to re-evaluate `file_id`'s TagBased placement
/// against its current tag set. Called whenever the file's tags change
/// (`FileTagged` / `FileUntagged`), so a file that gained a directory's tags is
/// placed there and one that lost them is dropped. Also the recovery path for
/// the tag-vs-content reconciliation race (see
/// `SyncDirectoryCommand::ReconcileTagPlacement`).
///
/// This is the **synchronous** DB step: it does all `&FileDatabase` reads and
/// sends the command, returning a [`DeferredPlacement`] for the caller to
/// `await` via [`fetch_and_place_deferred`]. Splitting it this way keeps the
/// caller's future `Send` — no `!Send` database borrow is held across an
/// `.await`. Returns `None` when there is nothing further to do (read error /
/// closed channel).
fn reconcile_tag_placement(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    database: &FileDatabase,
    file_id: FileId,
) -> Option<DeferredPlacement> {
    let logical_path = match database.logical_path_for_file_id(file_id) {
        Ok(logical_path) => logical_path,
        Err(error) => {
            log::debug!(
                "reconcile_tag_placement: no logical path for {} ({:?}); skipping",
                file_id.to_string(),
                error
            );
            return None;
        }
    };

    let file_tags = match database.tag_ids_for_file(file_id, database::SubtagRule::Exclude) {
        Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
        Err(error) => {
            log::error!(
                "reconcile_tag_placement: failed to read tags for {}: {:?}",
                file_id.to_string(),
                error
            );
            return None;
        }
    };

    // Read the latest catalog version hash now, alongside the other DB reads, so
    // the async follow-up needs no database access (and thus no `!Send` borrow
    // across an await). `None` = no recorded version (nothing to fetch by).
    let latest_hash = match database.latest_version(file_id) {
        Ok(version) => version.map(|version| version.content_hash),
        Err(error) => {
            log::error!(
                "reconcile_tag_placement: failed to read latest version for {}: {:?}",
                file_id.to_string(),
                error
            );
            None
        }
    };

    let (respond_to, deferred) = tokio::sync::oneshot::channel();
    if let Err(error) = command_sender.send(SyncDirectoryCommand::ReconcileTagPlacement {
        file_id,
        logical_path: logical_path.clone(),
        file_tags: file_tags.clone(),
        respond_to,
    }) {
        log::error!(
            "reconcile_tag_placement: command channel closed for {}: {error}",
            file_id.to_string()
        );
        return None;
    }

    Some(DeferredPlacement {
        file_id,
        logical_path,
        file_tags,
        latest_hash,
        deferred,
    })
}

/// The async follow-up to [`reconcile_tag_placement`]. If the manager deferred
/// placement (a TagBased directory wants the file but no local copy exists to
/// source its bytes), fetch the bytes on demand — keyed by the file's latest
/// catalog version hash — and enqueue a [`DaemonMessage::Materialize`] to place
/// them. This is the fix for "adding the tags a sync directory requires doesn't
/// pull a file that isn't local yet".
///
/// Takes only owned, `Send` data so the enclosing future can be spawned.
async fn fetch_and_place_deferred(
    pending_fetches: &PendingFetches,
    change_sender: &UnboundedSender<DaemonMessage>,
    placement: DeferredPlacement,
) {
    let DeferredPlacement {
        file_id,
        logical_path,
        file_tags,
        latest_hash,
        deferred,
    } = placement;

    // Placement fully resolved locally (or the manager task went away): done.
    match deferred.await {
        Ok(true) => {}
        Ok(false) => return,
        Err(_) => {
            log::warn!(
                "reconcile_tag_placement: manager dropped responder for {}; cannot tell if a fetch is needed",
                file_id.to_string()
            );
            return;
        }
    }

    // A TagBased directory wants this file but we hold no local copy. Fetch the
    // bytes on demand using the file's latest catalog version hash. Because the
    // catalog is byte-independent, this hash is present for any file we know
    // about even though its bytes are not (yet) local.
    let Some(expected_hash) = latest_hash else {
        // No version has ever been recorded. Nothing to fetch by; a future
        // announcement / materialize will place it. Soft deferral.
        log::debug!(
            "reconcile_tag_placement: no recorded version for {}; leaving placement for a future announcement",
            file_id.to_string()
        );
        return;
    };

    // Drive the fetch directly against the fetch engine rather than routing a
    // `DaemonMessage::Fetch` back onto the ingest bus: we are *inside* the
    // single-threaded `handle_changes` consumer, so awaiting a reply to a
    // message we enqueue onto our own channel would deadlock. `start_local_fetch`
    // floods the live peer tree and resolves the oneshot when the bytes arrive.
    log::debug!(
        "reconcile_tag_placement: fetching {} at catalog hash {} to place it locally",
        file_id.to_string(),
        expected_hash,
    );
    let (fetch_respond, fetch_result) = tokio::sync::oneshot::channel();
    pending_fetches
        .start_local_fetch(file_id, expected_hash.clone(), fetch_respond)
        .await;

    let content = match fetch_result.await {
        Ok(Ok(content)) => {
            log::debug!(
                "reconcile_tag_placement: fetch of {} succeeded; materializing",
                file_id.to_string()
            );
            content
        }
        Ok(Err(error)) => {
            // No peer had the bytes (or it timed out). Soft deferral: a later
            // reconnect / announcement retries placement.
            log::debug!(
                "reconcile_tag_placement: fetch of {} failed ({error:?}); placement deferred until a peer can serve it",
                file_id.to_string()
            );
            return;
        }
        Err(_) => {
            log::warn!(
                "reconcile_tag_placement: fetch engine dropped responder for {}",
                file_id.to_string()
            );
            return;
        }
    };

    // Place the fetched bytes via the normal materialize pipeline, which creates
    // the file in every matching sync directory (tag-filtered). Fire-and-forget
    // onto our own bus: processed on a later loop iteration, so this does not
    // block or deadlock the current one.
    if let Err(error) = change_sender.send(DaemonMessage::Materialize {
        file_id,
        content,
        content_hash: expected_hash,
        // Bytes sourced by our own on-demand fetch, not a specific announcing
        // peer. `Materialize` no longer records a version or forwards, so the
        // origin is only a sentinel here.
        origin: ChangeOrigin::Local {
            directory_path: std::path::PathBuf::new(),
        },
        placement: bus::MaterializePlacement::Create {
            logical_path,
            tags: file_tags,
        },
    }) {
        log::error!(
            "reconcile_tag_placement: change channel closed; cannot materialize fetched bytes for {}: {error}",
            file_id.to_string()
        );
    }
}

// This is the single change-handling pipeline task; its parameters are the
// distinct long-lived handles it owns (a change receiver, database, event
// sender) plus the routing handles it shares. They don't form a reusable
// cluster the way `PeerContext` does, so they're kept as plain arguments
// rather than bundled into a single-use struct.
#[allow(clippy::too_many_arguments)]
async fn handle_changes(
    configuration: Configuration,
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    pending_fetches: PendingFetches,
    mut database: FileDatabase,
    mut change_receiver: tokio::sync::mpsc::UnboundedReceiver<DaemonMessage>,
    change_sender: UnboundedSender<DaemonMessage>,
    command_sender: UnboundedSender<SyncDirectoryCommand>,
    event_sender: tokio::sync::broadcast::Sender<Change>,
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

    /// Dispatch a content-bearing change to matching sync directories, applying
    /// the move-vs-copy policy.
    ///
    /// `content` is a [`FileBytes`] that may still live on disk. This function
    /// decides, per the number of matching sync directories (`N`), how each one
    /// obtains the bytes:
    ///
    /// - `N == 0`: nothing is dispatched. A `FileToMove` source is left in
    ///   place (no auto-cleanup this pass; see `file_bytes` docs).
    /// - `N == 1`: the single directory receives the producer's original
    ///   variant. A `FileToMove` stays a move — zero extra copies, the common
    ///   single-directory win.
    /// - `N > 1`: a `FileToMove` can be honored only once, so every directory
    ///   instead receives a `FileToCopy` of the source and, after dispatch, the
    ///   source is removed here (preserving the "move" intent: the source does
    ///   not survive ingestion). `InMemory` is cloned per directory as before.
    async fn dispatch_content_to_sync_directories(
        command_sender: &UnboundedSender<SyncDirectoryCommand>,
        targets: Vec<ContentTarget>,
        content: FileBytes,
    ) {
        let source_path = content.path().map(|path| path.to_path_buf());
        let move_intent = matches!(content, FileBytes::FileToMove(_));

        match targets.len() {
            0 => {
                // No matching sync directory. Drop the content; a move source
                // is intentionally left in place (documented no-cleanup).
            }
            1 => {
                let target = targets.into_iter().next().expect("len checked == 1");
                let _ = command_sender.send(target.into_command(content));
            }
            _ => {
                // Multiple destinations. A destructive move can be honored by
                // exactly one consumer, so give the earlier targets a
                // `FileToCopy` and the LAST target a `FileToMove` (when the
                // source may be discarded), consuming the source.
                //
                // We must NOT eagerly delete the move source here: `CreateFile`
                // commands are processed asynchronously by the sync-directory
                // manager, so a delete issued now would race ahead of the copies
                // and leave them reading a missing file (observed as
                // `FailedAddingFile`, dropping the file everywhere). Commands are
                // processed in FIFO order, so the copies read the source while it
                // still exists and the trailing move consumes it last. If the
                // content was not a move (`FileToCopy`/`InMemory`), no consumer
                // deletes the source and the producer keeps ownership.
                let target_count = targets.len();
                for (index, target) in targets.into_iter().enumerate() {
                    let is_last = index + 1 == target_count;
                    let per_dir = match &source_path {
                        Some(path) => {
                            if is_last && move_intent {
                                FileBytes::FileToMove(path.clone())
                            } else {
                                FileBytes::FileToCopy(path.clone())
                            }
                        }
                        // No backing path => in-memory: clone the buffer.
                        None => match &content {
                            FileBytes::InMemory(bytes) => FileBytes::InMemory(bytes.clone()),
                            // Unreachable: source_path is None only for InMemory.
                            _ => unreachable!("no source path implies InMemory content"),
                        },
                    };
                    let _ = command_sender.send(target.into_command(per_dir));
                }
            }
        }
    }

    /// Handle a [`ContentChange`] (`FileAdded`/`FileChanged` carrying
    /// [`FileBytes`]): persist the version, dispatch bytes to matching sync
    /// directories, and forward a wire `Change` to peers.
    async fn handle_content_change(
        configuration: &Configuration,
        runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
        database: &mut FileDatabase,
        command_sender: &UnboundedSender<SyncDirectoryCommand>,
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
                tags,
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
                    if let Err(error) = database.add_file(file_id, &logical_path) {
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
                        for tag_id in &tags {
                            let modified_at = database::now_millis();
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
                            && !contains_all_tags(sync_directory_tags, &tags)
                        {
                            continue;
                        }

                        let physical_path = sync_directory
                            .sync_type
                            .physical_for(&logical_path, file_id);
                        targets.push(ContentTarget::Create {
                            file_id,
                            physical_path,
                            sync_directory_path: sync_directory.path.clone(),
                        });
                    }

                    dispatch_and_forward(
                        configuration,
                        runtime_configuration,
                        command_sender,
                        targets,
                        content,
                        &change_origin,
                        WireKind::Added {
                            file_id,
                            logical_path,
                            content_hash,
                            size,
                            tags,
                        },
                    )
                    .await;
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
                        .tag_ids_for_file(file_id, database::SubtagRule::Exclude)
                        .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                        .unwrap_or_else(|error| {
                            log::error!(
                                "Failed to read local tags for {}: {:?}",
                                file_id.to_string(),
                                error
                            );
                            Vec::new()
                        });

                    let targets =
                        change_targets(configuration, &change_origin, file_id, &local_file_tags);
                    dispatch_and_forward(
                        configuration,
                        runtime_configuration,
                        command_sender,
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
                }
            }
            ContentChange::FileChanged {
                file_id,
                content,
                content_hash,
                size,
            } => {
                let file_tags =
                    match database.tag_ids_for_file(file_id, database::SubtagRule::Exclude) {
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

                let targets = change_targets(configuration, &change_origin, file_id, &file_tags);
                dispatch_and_forward(
                    configuration,
                    runtime_configuration,
                    command_sender,
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
            }
        }
    }

    /// Build the list of sync directories that should receive a `ChangeFile`
    /// for `file_id`, applying the origin-skip and tag-match filters.
    fn change_targets(
        configuration: &Configuration,
        change_origin: &ChangeOrigin,
        file_id: FileId,
        file_tags: &[TagId],
    ) -> Vec<ContentTarget> {
        let mut targets = Vec::new();
        for sync_directory in &configuration.sync_directories {
            if let ChangeOrigin::Local { directory_path } = change_origin
                && directory_path == &sync_directory.path
            {
                continue;
            };

            if let SyncType::TagBased {
                tags: sync_directory_tags,
            } = &sync_directory.sync_type
                && !contains_all_tags(sync_directory_tags, file_tags)
            {
                continue;
            }

            targets.push(ContentTarget::Change {
                file_id,
                sync_directory_path: sync_directory.path.clone(),
            });
        }
        targets
    }

    /// The metadata-only wire `Change` to announce to peers for a local content
    /// ingestion. `Change` no longer carries bytes; peers pull them separately.
    enum WireKind {
        Added {
            file_id: FileId,
            logical_path: LogicalPath,
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
                    content_hash,
                    size,
                    tags,
                } => Change::FileMetadataAdded {
                    file_id,
                    logical_path,
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
    async fn dispatch_and_forward(
        configuration: &Configuration,
        runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
        command_sender: &UnboundedSender<SyncDirectoryCommand>,
        targets: Vec<ContentTarget>,
        content: FileBytes,
        change_origin: &ChangeOrigin,
        wire: WireKind,
    ) {
        dispatch_content_to_sync_directories(command_sender, targets, content).await;
        let change = wire.into_change();
        forward_to_peers(configuration, runtime_configuration, &change, change_origin).await;
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
        // for a file's bytes (from `onisync edit`): satisfy it locally if we
        // hold matching content, otherwise flood a `FetchRequest` to peers. A
        // `Change` falls through to the DB-writer pipeline below.
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

                pending_fetches
                    .start_local_fetch(file_id, expected_hash, respond_to)
                    .await;

                continue;
            }
            DaemonMessage::ReconcilePlacement { file_id } => {
                // Connect-time placement sweep, handed off from a peer session so
                // the fetch runs here (not on the session's frame loop). If a
                // TagBased sync directory wants this file but we lack its bytes,
                // fetch them on demand and place them.
                //
                // The synchronous DB step (`reconcile_tag_placement`) runs on
                // this loop, but the follow-up (`fetch_and_place_deferred`) must
                // NOT be awaited here: it blocks for the whole network fetch, and
                // it finishes by enqueueing a `DaemonMessage::Materialize` onto
                // *this* loop's own channel. Awaiting it therefore stalls the
                // single-threaded consumer so the `Materialize` it produces can
                // never be dequeued — the file is fetched, "materialized", but
                // never placed, and the next reconcile re-fetches it forever.
                // Spawn it instead (it holds only owned, Send data by design) so
                // the loop stays free to process the resulting `Materialize`.
                if let Some(deferred) = reconcile_tag_placement(&command_sender, &database, file_id)
                {
                    let pending_fetches = pending_fetches.clone();
                    let change_sender = change_sender.clone();
                    tokio::spawn(async move {
                        fetch_and_place_deferred(&pending_fetches, &change_sender, deferred).await;
                    });
                }
                continue;
            }
            DaemonMessage::CatalogFile {
                file_id,
                logical_path,
                content_hash,
                size,
                origin,
            } => {
                // A peer session's `Manifest` reconciliation decided to catalog
                // this file/version. We are the sole main-DB writer, so the
                // write happens here. Insert the `files` row if new, then append
                // the version (byte-independent catalog; the bytes are pulled
                // separately on the session link).
                if !database.file_exists(file_id).unwrap_or(false)
                    && let Err(error) = database.add_file(file_id, &logical_path)
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
                            .tag_ids_for_file(file_id, database::SubtagRule::Exclude)
                            .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                            .unwrap_or_else(|error| {
                                log::error!(
                                    "Materialize: failed to read tags for {}: {:?}; using carried tags only",
                                    file_id.to_string(),
                                    error
                                );
                                Vec::new()
                            });
                        let effective_tags = effective_placement_tags(&tags, &db_tags);

                        let mut targets = Vec::new();
                        for sync_directory in &configuration.sync_directories {
                            if let SyncType::TagBased {
                                tags: sync_directory_tags,
                            } = &sync_directory.sync_type
                                && !contains_all_tags(sync_directory_tags, &effective_tags)
                            {
                                continue;
                            }
                            let physical_path = sync_directory
                                .sync_type
                                .physical_for(&logical_path, file_id);
                            targets.push(ContentTarget::Create {
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
                            .tag_ids_for_file(file_id, database::SubtagRule::Exclude)
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
                        change_targets(&configuration, &sentinel, file_id, &file_tags)
                    }
                };
                dispatch_content_to_sync_directories(&command_sender, targets, content).await;
                // No `forward_to_peers` here: the announcement was already
                // forwarded when it was first handled (announce time). `origin`
                // is unused now that we neither record nor re-announce here.
                let _ = origin;
                continue;
            }
            DaemonMessage::AnnounceProvided {
                file_id,
                logical_path,
                content_hash,
                size,
                tags,
            } => {
                // A local client (CLI) uploaded/edited a file it serves on
                // demand. Record it locally and announce metadata-only to peers;
                // peers pull the bytes from the registered provider. No local
                // sync-directory placement: a CLI upload targets peers (files
                // already in a sync directory are synced without the CLI).
                let change = match logical_path {
                    Some(logical_path) => {
                        if let Err(error) = database.add_file(file_id, &logical_path) {
                            log::error!(
                                "AnnounceProvided: failed to add file {} ({}): {:?}",
                                file_id.to_string(),
                                logical_path,
                                error
                            );
                            continue;
                        }
                        Change::FileMetadataAdded {
                            file_id,
                            logical_path,
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
                    &runtime_configuration,
                    &mut database,
                    &command_sender,
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
                    if let Err(error) = database.add_file(*file_id, logical_path) {
                        log::error!(
                            "Failed to add file {} ({}): {:?}; skipping change",
                            file_id.to_string(),
                            logical_path,
                            error
                        );
                        continue;
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
                // whether we pull the bytes. A downstream peer that then
                // `TransferStart`s against us before (or without) us holding the
                // bytes gets a graceful `TransferAbort` and fetches elsewhere /
                // on demand.
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
            } => {
                // TODO: Don't unwrap.
                // TODO: Should this be include? Currently this WILL NOT WORK since add file
                // doesn't consider subtags. We would need to get a list of *all* tags (incuding
                // subdags) when adding the file to make it work.
                // -> Maybe make it configurable in the config, per-sync directory.
                let file_tags =
                    match database.tag_ids_for_file(*file_id, database::SubtagRule::Exclude) {
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

                if let Err(error) = database.update_file_logical_path(*file_id, logical_path) {
                    log::error!(
                        "Failed to update logical path for file {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    continue;
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
                        && !contains_all_tags(sync_directory_tags, &file_tags)
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
                    match database.tag_ids_for_file(*file_id, database::SubtagRule::Exclude) {
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

                match database.remove_file(*file_id, *deleted_at) {
                    Ok(true) => {}
                    Ok(false) => {
                        // A newer edit out-dated this delete (last-writer-wins):
                        // the file stays live. Do not remove it from sync
                        // directories or forward the delete.
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
                        && !contains_all_tags(sync_directory_tags, &file_tags)
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
                // API is `todo!()` in onisync-core). When metadata lands, apply
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
                if let Some(deferred) =
                    reconcile_tag_placement(&command_sender, &database, *file_id)
                {
                    fetch_and_place_deferred(&pending_fetches, &change_sender, deferred).await;
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
                if let Some(deferred) =
                    reconcile_tag_placement(&command_sender, &database, *file_id)
                {
                    fetch_and_place_deferred(&pending_fetches, &change_sender, deferred).await;
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

        // Publish the applied change to UI-facing API subscribers (plan 5.5).
        // Best-effort: if there are no subscribers, or the channel is full and
        // a subscriber lags, the send/receive machinery handles it (the
        // subscriber observes `Lagged`, mapped to `Resynced` by the transport).
        let _ = event_sender.send(change);
    }

    log::info!("handle_changes task exited");
}

#[cfg(test)]
mod reconcile_tests {
    use onisync_core::state::ManifestEntry;

    use super::*;

    fn memory_db() -> FileDatabase {
        FileDatabase::initialize(":memory:").expect("open in-memory db")
    }

    /// A file the peer has but we've never seen IS reconciled with a `Create`
    /// placement carrying the manifest's `logical_path`: this is the
    /// offline-creation catch-up case (a file created on the peer while we
    /// were disconnected must sync on reconnect). Tags are left empty; they
    /// are reconciled independently via `Sync::TagManifest` and applied by
    /// `reconcile_tag_placement` when the corresponding `FileTagged` arrives.
    #[test]
    fn unknown_file_is_requested_as_create() {
        let database = memory_db();
        let file_id = FileId::new();
        let logical_path = LogicalPath::new("subdir/new.txt");
        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "aaaa".to_owned(), 1), (2, "bbbb".to_owned(), 1)],
            latest_observed_at: 100,
            logical_path: logical_path.clone(),
            deleted: false,
            deleted_at: 0,
        };

        let (wanted, deletions) = reconcile_peer_manifest("peer", vec![entry], &database);
        assert!(deletions.is_empty());
        assert_eq!(wanted.len(), 1);
        assert_eq!(wanted[0].file_id, file_id);
        assert_eq!(wanted[0].content_hash, "bbbb");
        match &wanted[0].placement {
            bus::MaterializePlacement::Create {
                logical_path: got_logical_path,
                tags,
            } => {
                assert_eq!(got_logical_path, &logical_path);
                assert!(tags.is_empty(), "tags reconcile via Sync::TagManifest");
            }
            other => panic!("expected Create placement, got {other:?}"),
        }
    }

    /// A file whose latest hash we already hold is not wanted.
    #[test]
    fn equal_latest_is_not_wanted() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"))
            .unwrap();
        database
            .record_version(file_id, "bbbb", "local", 1)
            .unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "bbbb".to_owned(), 1)],
            latest_observed_at: 100,
            logical_path: LogicalPath::new("f.txt"),
            deleted: false,
            deleted_at: 0,
        };

        let (wanted, deletions) = reconcile_peer_manifest("peer", vec![entry], &database);
        assert!(deletions.is_empty());
        assert!(wanted.is_empty());
    }

    /// When we are strictly behind (our latest is in the peer's history but not
    /// vice versa), the file is wanted at the peer's newer hash. Since the
    /// file is already known locally, placement is `Change`.
    #[test]
    fn behind_is_wanted_as_change() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"))
            .unwrap();
        database.record_version(file_id, "v1", "local", 1).unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "v1".to_owned(), 1), (2, "v2".to_owned(), 1)],
            latest_observed_at: 100,
            logical_path: LogicalPath::new("f.txt"),
            deleted: false,
            deleted_at: 0,
        };

        let (wanted, deletions) = reconcile_peer_manifest("peer", vec![entry], &database);
        assert!(deletions.is_empty());
        assert_eq!(wanted.len(), 1);
        assert_eq!(wanted[0].file_id, file_id);
        assert_eq!(wanted[0].content_hash, "v2");
        assert!(
            matches!(wanted[0].placement, bus::MaterializePlacement::Change),
            "known file placement must be Change, got {:?}",
            wanted[0].placement
        );
    }

    /// A reconcile pull carries empty tags, but by the time it materializes the
    /// file's tags are in the DB (applied from the TagManifest). The effective
    /// placement tags must include the DB tags so the file lands in its
    /// matching TagBased directories on the first materialize (the
    /// first-connect fix).
    #[test]
    fn effective_tags_include_db_tags_when_carried_is_empty() {
        let db_a = TagId::new();
        let db_b = TagId::new();

        let effective = effective_placement_tags(&[], &[db_a, db_b]);

        assert!(effective.contains(&db_a));
        assert!(effective.contains(&db_b));
        assert_eq!(effective.len(), 2);
    }

    /// The carried tags (from a live `FileMetadataAdded`) are preserved even if
    /// the DB has none yet, and the union deduplicates overlap.
    #[test]
    fn effective_tags_union_dedups_carried_and_db() {
        let shared = TagId::new();
        let carried_only = TagId::new();
        let db_only = TagId::new();

        let effective = effective_placement_tags(&[shared, carried_only], &[shared, db_only]);

        assert!(effective.contains(&shared));
        assert!(effective.contains(&carried_only));
        assert!(effective.contains(&db_only));
        assert_eq!(effective.len(), 3, "shared tag must not be duplicated");
    }

    /// A peer's delete tombstone whose `deleted_at` is newer than our latest
    /// version's `observed_at` wins: we schedule the deletion and do not pull
    /// bytes for it.
    #[test]
    fn peer_delete_newer_than_our_edit_is_applied() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"))
            .unwrap();
        // Our latest version's observed_at is "now". A delete stamped far in
        // the future beats it.
        database.record_version(file_id, "v1", "local", 1).unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "v1".to_owned(), 1)],
            latest_observed_at: 0,
            logical_path: LogicalPath::new("f.txt"),
            deleted: true,
            deleted_at: database::now_millis() + 10_000,
        };

        let (wanted, deletions) = reconcile_peer_manifest("peer", vec![entry], &database);
        assert!(wanted.is_empty(), "a tombstoned file must not be pulled");
        assert_eq!(deletions.len(), 1);
        assert_eq!(deletions[0].file_id, file_id);
    }

    /// A peer's delete tombstone older than our latest edit loses: the file is
    /// kept (no deletion scheduled). This is the restore-after-delete rule.
    #[test]
    fn peer_delete_older_than_our_edit_is_ignored() {
        let mut database = memory_db();
        let file_id = FileId::new();
        database
            .add_file(file_id, &LogicalPath::new("f.txt"))
            .unwrap();
        // Our latest version's observed_at is "now"; a delete stamped in the
        // past loses.
        database.record_version(file_id, "v1", "local", 1).unwrap();

        let entry = ManifestEntry {
            file_id,
            history: vec![(1, "v1".to_owned(), 1)],
            latest_observed_at: 0,
            logical_path: LogicalPath::new("f.txt"),
            deleted: true,
            deleted_at: 1,
        };

        let (wanted, deletions) = reconcile_peer_manifest("peer", vec![entry], &database);
        assert!(deletions.is_empty(), "stale delete must not win");
        // Equal latest hash means nothing to pull either.
        assert!(wanted.is_empty());
    }
}
