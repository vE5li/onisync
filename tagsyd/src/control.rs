//! Daemon local control endpoint.
//!
//! On Linux the sync engine and the DB live in a long-running (systemd) daemon,
//! while the UI is a *separate* process. Because [`CatalogStore`] is
//! single-owner, the UI cannot open the DB itself — it must ask the daemon.
//! This module is that channel.
//!
//! ## Transport
//!
//! A **Unix-domain-socket** control listener at
//! [`paths::control_socket_path`](crate::paths::control_socket_path) (the fixed
//! `/run/tagsy/tagsy.sock`) — **not** a TCP port. Security is entirely
//! filesystem permissions: the runtime directory `/run/tagsy` is created
//! `0700` and owned by the service user (via `RuntimeDirectory=tagsy` in the
//! systemd unit), so only that user can connect and nothing is exposed on any
//! network interface. There is therefore **no auth handshake** for local
//! control (unlike the ed25519 peer handshake on the peer-sync port).
//!
//! The existing WebSocket/`Frame` framing (tokio + tokio-tungstenite) is reused
//! over the [`UnixListener`], so the networking code stays unified. The wire
//! payloads, however, are a **distinct message category** from the peer
//! `Change`/`Sync` protocol: peer framing is about *sync*; control framing
//! ([`ControlFrame`]) carries the UI-facing API requests/responses/events.
//!
//! ## Relationship to the peer-sync port
//!
//! The `0.0.0.0:{listen_port}` listener is unchanged — it remains the remote
//! peer-sync port. UI control is **never** routed through it. A local UI client
//! is conceptually "another kind of client" of the daemon: it issues
//! queries/commands and subscribes to the change stream, reusing the same
//! broadcast plumbing (`event_sender` / [`Api::subscribe`]) that
//! `forward_to_peers` uses for peers.
//!
//! ## Two halves
//!
//! - [`serve_control`] is the **daemon side**: accepts connections, decodes
//!   [`ControlRequest`]s, dispatches them to the in-process [`Api`], and
//!   streams [`ApiEvent`]s back as [`ControlFrame::Event`].
//! - [`IpcClientBackend`] is the **client side** IPC-client backend: it
//!   connects to the socket, serializes API calls, and returns results/events.
//!   It implements [`TransportBackend`], so the Dart UI (via
//!   `flutter_rust_bridge`) and the `tagsy` CLI talk to it exactly as they
//!   would the in-process backend.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tagsy_core::{FileId, FileInfo, Preview, TagId};
use serde::{Deserialize, Serialize};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_util::sync::CancellationToken;

use crate::api::{Api, ApiError, ApiEvent, EditOutcome, QueryResult, RetagSummary, TagRuleReport};
use crate::configuration::EditorRule;
use crate::store::{DeletedRule, SubtagRule, Tag};
use crate::transport::{EventStream, OperationStream, OperationUpdate, TransportBackend};

/// A UI-facing API call, sent by a control client to the daemon.
///
/// One variant per [`Api`] method. Requests are matched to their
/// [`ControlResponse`] by the [`ControlFrame::Request`] `id`, so multiple calls
/// may be in flight on one connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    /// Resolve a full-or-short file id prefix to a single `FileId`. Answered
    /// with [`ControlResponse::FileId`] (or an `Error`).
    ResolveFileId {
        prefix: String,
    },
    /// Resolve a full-or-short tag id prefix to a single `TagId`. Answered with
    /// [`ControlResponse::TagId`] (or an `Error`).
    ResolveTagId {
        prefix: String,
    },
    TagsForFile {
        file_id: FileId,
        subtag_rule: SubtagRule,
    },
    /// Run a free-form query (`$tag`, `!tag`, and name substrings) and return
    /// both the matching files and tags. Tag tokens are resolved in the daemon.
    /// `deleted_rule` toggles whether tombstoned files/tags participate (see
    /// [`Api::run_query`](crate::api::Api::run_query)).
    /// Answered with [`ControlResponse::QueryResult`].
    RunQuery {
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    },
    /// Get a single file's info by id. Answered with [`ControlResponse::File`]
    /// (or `Error(UnknownId)`). `deleted_rule` toggles tombstone visibility;
    /// see [`Api::get_file`](crate::api::Api::get_file).
    GetFile {
        file_id: FileId,
        deleted_rule: DeletedRule,
    },
    /// Get a single tag by id. Answered with [`ControlResponse::Tag`] (or
    /// `Error(UnknownId)`).
    GetTag {
        tag_id: TagId,
        deleted_rule: DeletedRule,
    },
    SubtagsForTag {
        tag_id: TagId,
        subtag_rule: SubtagRule,
    },
    TagsForTag {
        tag_id: TagId,
        subtag_rule: SubtagRule,
    },
    // Writes.
    CreateTag {
        name: String,
        color: String,
    },
    DeleteTag {
        tag_id: TagId,
    },
    RestoreTag {
        tag_id: TagId,
    },
    RenameTag {
        tag_id: TagId,
        name: String,
    },
    SetTagColor {
        tag_id: TagId,
        color: String,
    },
    /// Upload a file the client provides on demand. The client does *not* send
    /// the bytes; it sends the logical name, the precomputed BLAKE3
    /// `content_hash`, and tags, then serves chunks via the provider protocol
    /// (see [`ControlFrame::ProviderChunkRequest`]). Answered with
    /// [`ControlResponse::FileId`] once the upload has been handed off to every
    /// connected storing peer (or there were none to serve).
    UploadFile {
        path_name: String,
        content_hash: String,
        /// The file's content size in bytes, computed by the client alongside
        /// `content_hash`.
        size: u64,
        tags: Vec<TagId>,
    },
    /// Replace an existing file's content, provided on demand like
    /// [`ControlRequest::UploadFile`]. Answered with [`ControlResponse::Ok`]
    /// once the new content has been handed off.
    EditFile {
        file_id: FileId,
        content_hash: String,
        /// The file's new content size in bytes, computed by the client.
        size: u64,
    },
    /// Start an external edit. The daemon returns the path the client should
    /// hand to an editor — either the file's real sync-dir path (edit in
    /// place) or a per-request temp file under `fetch_temp_dir` named after
    /// the file's logical basename. Answered with
    /// [`ControlResponse::FilePath`].
    BeginEdit {
        file_id: FileId,
    },
    /// Complete an external edit. The daemon re-hashes the bytes at `path`,
    /// publishes a new version if different from the current recorded hash,
    /// and cleans up any daemon-owned temp. Answered with
    /// [`ControlResponse::EditOutcome`].
    FinishEdit {
        file_id: FileId,
        path: PathBuf,
    },
    /// Abort an external edit without publishing. Cleans up any daemon-owned
    /// temp at `path`. Answered with [`ControlResponse::Ok`].
    CancelEdit {
        path: PathBuf,
    },
    /// Fetch a file's content on demand (from a peer if not local). Answered
    /// with [`ControlResponse::FilePath`] or an error. `expected_hash` gates
    /// which content is accepted.
    FetchFile {
        file_id: FileId,
        expected_hash: String,
    },
    /// Get the preview for a file's current content (cached / generated /
    /// peer-fetched). Answered with [`ControlResponse::Preview`].
    GetPreview {
        file_id: FileId,
    },
    /// Resolve a file's absolute on-disk path if present locally. Answered with
    /// [`ControlResponse::LocalPath`].
    LocalPathForFile {
        file_id: FileId,
    },
    DeleteFile {
        file_id: FileId,
    },
    RestoreFile {
        file_id: FileId,
    },
    MoveFile {
        file_id: FileId,
        logical_path: String,
    },
    TagFile {
        tag_id: TagId,
        file_id: FileId,
    },
    UntagFile {
        tag_id: TagId,
        file_id: FileId,
    },
    TagTag {
        parent_id: TagId,
        subtag_id: TagId,
    },
    UntagTag {
        parent_id: TagId,
        subtag_id: TagId,
    },
    /// Subscribe to the event stream. After this is accepted, the daemon starts
    /// emitting [`ControlFrame::Event`]s on this connection; the response is
    /// [`ControlResponse::Subscribed`].
    Subscribe,
    /// Purge the entire preview cache. Answered with
    /// [`ControlResponse::PurgedPreviews`] carrying the number of cached
    /// previews removed.
    PurgePreviews,
    /// Read the daemon's external-editor rules. Answered with
    /// [`ControlResponse::EditorRules`].
    EditorRules,
    /// Re-apply the configured tag rules to the existing catalog. Answered
    /// with [`ControlResponse::Retagged`]. With `dry_run` the daemon plans and
    /// reports the work without enqueuing it.
    Retag {
        dry_run: bool,
    },
    /// Diagnose the configured tag rules. Answered with
    /// [`ControlResponse::TagRuleReport`].
    TagRuleReport,
    /// Snapshot every currently-active sync operation. Answered with
    /// [`ControlResponse::Operations`].
    ListOperations,
    /// Subscribe to the operation stream. After this is accepted the daemon
    /// starts emitting [`ControlFrame::OperationEvent`]s on this connection;
    /// the response is [`ControlResponse::OperationsSubscribed`].
    SubscribeOperations,
}

/// The result of a [`ControlRequest`], returned as [`ControlFrame::Response`].
///
/// Every variant is either the success payload of the matching request or the
/// single serializable [`ApiError`]. The client maps these back onto
/// the [`TransportBackend`] return types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    /// A single file's info (answer to [`ControlRequest::GetFile`]).
    File(FileInfo),
    /// A single tag (answer to [`ControlRequest::GetTag`]).
    Tag(Tag),
    TagIds(Vec<TagId>),
    FileIds(Vec<FileId>),
    /// The files and tags matching a query (answer to
    /// [`ControlRequest::RunQuery`]).
    QueryResult(QueryResult),
    TagId(TagId),
    FileId(FileId),
    /// Path to a daemon-owned temp file holding a fetched file's content
    /// (answer to [`ControlRequest::FetchFile`]).
    ///
    /// The client and daemon are co-located and share this filesystem; the
    /// client consumes the temp file with move semantics (rename into place or
    /// delete). No file bytes cross the socket.
    FilePath(PathBuf),
    /// A file's preview (answer to [`ControlRequest::GetPreview`]).
    /// [`Preview::None`] is a valid result (the content has no preview).
    Preview(Preview),
    /// A file's absolute on-disk path, or `None` if not present locally (answer
    /// to [`ControlRequest::LocalPathForFile`]).
    LocalPath(Option<PathBuf>),
    /// The outcome of an external edit (answer to
    /// [`ControlRequest::FinishEdit`]).
    EditOutcome(crate::api::EditOutcome),
    /// A write/command that returns no payload succeeded.
    Ok,
    /// The subscription was established; events will follow on this connection.
    Subscribed,
    /// The number of cached previews removed (answer to
    /// [`ControlRequest::PurgePreviews`]).
    PurgedPreviews(usize),
    /// The daemon's external-editor rules (answer to
    /// [`ControlRequest::EditorRules`]).
    EditorRules(Vec<EditorRule>),
    /// What a retag did, or would do under `dry_run` (answer to
    /// [`ControlRequest::Retag`]).
    Retagged(RetagSummary),
    /// Tag-rule diagnostics (answer to [`ControlRequest::TagRuleReport`]).
    TagRuleReport(TagRuleReport),
    /// A snapshot of currently-active sync operations (answer to
    /// [`ControlRequest::ListOperations`]).
    Operations(Vec<crate::operations::Operation>),
    /// The operation subscription was established; operation events will follow
    /// on this connection.
    OperationsSubscribed,
    /// The request failed. Carries the single UI-facing error type.
    Error(ApiError),
}

/// Every control-socket message, in either direction.
///
/// This is the control counterpart to the peer
/// [`Frame`](tagsy_core::state::Frame): same WebSocket text framing, disjoint
/// message set. `Request`/`Response` carry a correlation `id`; `Event` is
/// unsolicited (pushed after `Subscribe`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlFrame {
    /// Client -> daemon: an API call tagged with a per-connection request id.
    Request { id: u64, request: ControlRequest },
    /// Daemon -> client: the reply to the request with matching `id`.
    Response { id: u64, response: ControlResponse },
    /// Daemon -> client: an unsolicited event on a subscribed connection.
    Event(ApiEvent),
    /// Daemon -> client: an unsolicited sync-operation event on a connection
    /// that sent [`ControlRequest::SubscribeOperations`].
    OperationEvent(crate::operations::OperationEvent),

    // Reverse-direction request/reply used while the client is serving an
    // upload/edit's bytes on demand. Correlated by `chunk_id` (per connection).
    /// Daemon -> client: send the chunk of the in-flight upload/edit at
    /// `offset` (the client knows which file it is currently providing).
    ProviderChunkRequest { chunk_id: u64, offset: u64 },
    /// Client -> daemon: the requested chunk. `last` marks end of file.
    ProviderChunkReply {
        chunk_id: u64,
        bytes: Vec<u8>,
        last: bool,
    },
}

/// Bind the control socket and serve control clients until `shutdown` fires.
///
/// Binds a [`UnixListener`] at `socket_path`, removing any stale socket file
/// left by a previous run first (a leftover socket makes `bind` fail with
/// `AddrInUse`). Each accepted connection is handled on its own task by
/// [`handle_control_connection`]; all share the one in-process [`Api`].
///
/// This is wired into the section-3 shutdown path by the caller: it runs inside
/// the runtime driver's `select!` and returns when `shutdown` is cancelled, at
/// which point the socket file is removed on the way out.
pub async fn serve_control(
    api: Api,
    socket_path: PathBuf,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    // A leftover socket file (unclean shutdown) would make bind() fail with
    // AddrInUse. It is safe to remove: the runtime directory (/run/tagsy) is
    // owned by the single service user, and a second live daemon for the same
    // user is not a supported configuration. (systemd's RuntimeDirectory
    // normally clears this on start; removing it here also covers non-systemd
    // launches.)
    if socket_path.exists()
        && let Err(error) = tokio::fs::remove_file(&socket_path).await
    {
        log::warn!(
            "Failed to remove stale control socket {}: {error}",
            socket_path.display()
        );
    }

    let listener = UnixListener::bind(&socket_path)?;
    log::info!("Control socket listening on {}", socket_path.display());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::info!("Shutdown requested; stopping control socket");
                break;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _address)) => {
                        tokio::spawn(handle_control_connection(
                            api.clone(),
                            stream,
                            shutdown.child_token(),
                        ));
                    }
                    Err(error) => {
                        log::warn!("Control socket accept error: {error}");
                        break;
                    }
                }
            }
        }
    }

    // Best-effort cleanup so the next daemon start binds cleanly.
    let _ = tokio::fs::remove_file(&socket_path).await;
    Ok(())
}

/// Serve a single control client for the life of its connection.
///
/// Completes the WebSocket handshake over the Unix stream, then loops:
/// - decode inbound [`ControlFrame::Request`]s, dispatch to the [`Api`], and
///   reply with a [`ControlFrame::Response`];
/// - on [`ControlRequest::Subscribe`], subscribe to the [`Api`] event stream
///   and forward every [`ApiEvent`] as a [`ControlFrame::Event`].
///
/// Only one subscription per connection is needed (the UI subscribes once);
/// a second `Subscribe` simply replaces the stream.
async fn handle_control_connection(api: Api, stream: UnixStream, shutdown: CancellationToken) {
    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws_stream) => ws_stream,
        Err(error) => {
            log::warn!("Control WebSocket handshake failed: {error}");
            return;
        }
    };
    log::debug!("Control client connected");

    let (mut outgoing, mut incoming) = ws_stream.split();

    // Populated once the client sends `Subscribe`. `None` until then so an
    // un-subscribed connection never wakes on the event branch.
    let mut events: Option<EventStream> = None;

    // Populated once the client sends `SubscribeOperations`. Independent of the
    // change-event subscription above so a client can take one, both, or
    // neither.
    let mut operation_events: Option<OperationStream> = None;

    // Provider protocol state for this connection. A `ProviderSource` (held by
    // the transfer subsystem) asks for a chunk by sending `(offset, reply)` on
    // `provider_req`; we assign a `chunk_id`, remember the reply oneshot, and
    // send a `ProviderChunkRequest` to the client. The client's
    // `ProviderChunkReply` resolves it. `active_provider` records what this
    // connection is currently serving so we can unregister it on disconnect.
    let (provider_req_tx, mut provider_req_rx) =
        mpsc::unbounded_channel::<crate::transfer::ProviderChunkRequest>();
    let (provider_done_tx, mut provider_done_rx) = mpsc::unbounded_channel::<()>();
    let mut provider_pending: HashMap<u64, crate::transfer::ProviderChunkReply> = HashMap::new();
    let mut next_chunk_id: u64 = 0;
    // (file_id, content_hash) currently registered as a provider on this conn.
    let mut active_provider: Option<(FileId, String)> = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::debug!("Shutdown requested; closing control client");
                break;
            }
            // Forward live events to a subscribed client. `recv` on a `None`
            // stream is never polled because of the `if let` guard.
            event = async { events.as_mut().unwrap().recv().await }, if events.is_some() => {
                match event {
                    Some(event) => {
                        if let Err(error) =
                            send_control(&mut outgoing, &ControlFrame::Event(event)).await
                        {
                            log::debug!("Failed to push control event: {error}");
                            break;
                        }
                    }
                    None => {
                        // The runtime's event bus closed (shutdown).
                        break;
                    }
                }
            }
            // Forward live operation events to a subscribed client. A lag on the
            // operation broadcast surfaces as `Resynced`, which we forward
            // verbatim so the client re-snapshots via `ListOperations`.
            operation = async { operation_events.as_mut().unwrap().recv().await }, if operation_events.is_some() => {
                match operation {
                    Some(OperationUpdate::Event(event)) => {
                        if let Err(error) =
                            send_control(&mut outgoing, &ControlFrame::OperationEvent(event)).await
                        {
                            log::debug!("Failed to push operation event: {error}");
                            break;
                        }
                    }
                    // A lag: the client should re-snapshot. There is no dedicated
                    // "resync operations" frame; the change-stream `Resynced`
                    // already prompts a full re-fetch of live state, so drop the
                    // marker here and let the next event catch the client up.
                    Some(OperationUpdate::Resynced) => {}
                    None => break,
                }
            }
            // A provider source wants a chunk from the client: forward it as a
            // `ProviderChunkRequest` and remember where to route the reply.
            request = provider_req_rx.recv() => {
                let Some((offset, reply)) = request else { continue; };
                let chunk_id = next_chunk_id;
                next_chunk_id += 1;
                provider_pending.insert(chunk_id, reply);
                if let Err(error) = send_control(
                    &mut outgoing,
                    &ControlFrame::ProviderChunkRequest { chunk_id, offset },
                )
                .await
                {
                    log::debug!("Failed to send provider chunk request: {error}");
                    break;
                }
            }
            // A transfer of the provided file completed: tell the client it may
            // release the file (via an event), and unregister the provider.
            done = provider_done_rx.recv() => {
                if done.is_none() { continue; }
                if let Some((file_id, content_hash)) = active_provider.take() {
                    api.unregister_provider(file_id, &content_hash).await;
                    if let Err(error) = send_control(
                        &mut outgoing,
                        &ControlFrame::Event(ApiEvent::ProviderReleased { file_id }),
                    )
                    .await
                    {
                        log::debug!("Failed to send provider-released event: {error}");
                        break;
                    }
                }
            }
            inbound = incoming.next() => {
                let Some(message) = inbound else {
                    log::debug!("Control client closed the connection");
                    break;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        log::debug!("Control client read error: {error}");
                        break;
                    }
                };
                // Ignore non-data frames (ping/pong/close handled by the lib).
                if message.is_ping() || message.is_pong() || message.is_close() {
                    continue;
                }
                let frame: ControlFrame = match decode_frame(&message) {
                    Ok(frame) => frame,
                    Err(error) => {
                        // A frame we cannot decode means the stream is out of
                        // sync (or the peer is buggy). Continuing to read would
                        // misinterpret subsequent bytes; close the connection so
                        // the client reconnects cleanly instead of hanging on a
                        // request whose response never comes.
                        log::warn!("Malformed control frame: {error}; closing connection");
                        break;
                    }
                };
                match frame {
                    ControlFrame::ProviderChunkReply { chunk_id, bytes, last } => {
                        if let Some(reply) = provider_pending.remove(&chunk_id) {
                            let _ = reply.send(Ok((bytes, last)));
                        } else {
                            log::warn!("Provider reply for unknown chunk id {chunk_id}");
                        }
                    }
                    ControlFrame::Request { id, request } => {
                        // Uploads/edits register a provider for this connection;
                        // capture the provider source + what it serves.
                        let response = dispatch(
                            &api,
                            request,
                            &mut events,
                            &mut operation_events,
                            &provider_req_tx,
                            &provider_done_tx,
                            &mut active_provider,
                        )
                        .await;
                        if let Err(error) =
                            send_control(&mut outgoing, &ControlFrame::Response { id, response }).await
                        {
                            log::debug!("Failed to send control response: {error}");
                            break;
                        }
                    }
                    other => {
                        log::warn!("Control client sent an unexpected frame: {other:?}; ignoring");
                    }
                }
            }
        }
    }

    // Connection closing: drop any provider we registered so stale entries do
    // not linger.
    if let Some((file_id, content_hash)) = active_provider.take() {
        api.unregister_provider(file_id, &content_hash).await;
    }

    log::debug!("Control client disconnected");
}

/// Execute one [`ControlRequest`] against the in-process [`Api`] and produce a
/// [`ControlResponse`].
///
/// Most reads are synchronous on [`Api`] (each opens its own short-lived
/// read-only handle) and writes enqueue a `Change` and return immediately.
/// `FetchFile` and `LocalPathForFile` are genuinely async (a channel round-trip
/// into the daemon), so this function is `async`. Nothing holds a
/// `&CatalogStore` across an `.await`. `Subscribe` mutates the caller's
/// `events` slot.
#[allow(clippy::too_many_arguments)]
async fn dispatch(
    api: &Api,
    request: ControlRequest,
    events: &mut Option<EventStream>,
    operation_events: &mut Option<OperationStream>,
    provider_req_tx: &mpsc::UnboundedSender<crate::transfer::ProviderChunkRequest>,
    provider_done_tx: &mpsc::UnboundedSender<()>,
    active_provider: &mut Option<(FileId, String)>,
) -> ControlResponse {
    match request {
        ControlRequest::ResolveFileId { prefix } => match api.resolve_file_id(&prefix) {
            Ok(file_id) => ControlResponse::FileId(file_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::ResolveTagId { prefix } => match api.resolve_tag_id(&prefix) {
            Ok(tag_id) => ControlResponse::TagId(tag_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagsForFile {
            file_id,
            subtag_rule,
        } => match api.tags_for_file(file_id, subtag_rule) {
            Ok(tag_ids) => ControlResponse::TagIds(tag_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::RunQuery {
            query,
            subtag_rule,
            deleted_rule,
        } => match api.run_query(&query, subtag_rule, deleted_rule) {
            Ok(result) => ControlResponse::QueryResult(result),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::GetFile {
            file_id,
            deleted_rule,
        } => match api.get_file(file_id, deleted_rule) {
            Ok(file) => ControlResponse::File(file),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::GetTag {
            tag_id,
            deleted_rule,
        } => match api.get_tag(tag_id, deleted_rule) {
            Ok(tag) => ControlResponse::Tag(tag),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::SubtagsForTag {
            tag_id,
            subtag_rule,
        } => match api.subtags_for_tag(tag_id, subtag_rule) {
            Ok(tag_ids) => ControlResponse::TagIds(tag_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagsForTag {
            tag_id,
            subtag_rule,
        } => match api.tags_for_tag(tag_id, subtag_rule) {
            Ok(tag_ids) => ControlResponse::TagIds(tag_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::CreateTag { name, color } => match api.create_tag(name, color) {
            Ok(tag_id) => ControlResponse::TagId(tag_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::DeleteTag { tag_id } => match api.delete_tag(tag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::RestoreTag { tag_id } => match api.restore_tag(tag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::RenameTag { tag_id, name } => match api.rename_tag(tag_id, name) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::SetTagColor { tag_id, color } => match api.set_tag_color(tag_id, color) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::UploadFile {
            path_name,
            content_hash,
            size,
            tags,
        } => {
            match api.upload_file(path_name, content_hash.clone(), size, tags) {
                Ok(file_id) => {
                    // Register this connection as the temporary provider so
                    // peers can pull the bytes on demand.
                    let source = std::sync::Arc::new(crate::transfer::ProviderSource::new(
                        provider_req_tx.clone(),
                        provider_done_tx.clone(),
                    ));
                    api.register_provider(file_id, content_hash.clone(), source)
                        .await;
                    *active_provider = Some((file_id, content_hash));
                    ControlResponse::FileId(file_id)
                }
                Err(error) => ControlResponse::Error(error),
            }
        }
        ControlRequest::EditFile {
            file_id,
            content_hash,
            size,
        } => match api.edit_file(file_id, content_hash.clone(), size) {
            Ok(()) => {
                let source = std::sync::Arc::new(crate::transfer::ProviderSource::new(
                    provider_req_tx.clone(),
                    provider_done_tx.clone(),
                ));
                api.register_provider(file_id, content_hash.clone(), source)
                    .await;
                *active_provider = Some((file_id, content_hash));
                ControlResponse::Ok
            }
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::FetchFile {
            file_id,
            expected_hash,
        } => match api.fetch_file(file_id, expected_hash).await {
            Ok(path) => ControlResponse::FilePath(path),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::BeginEdit { file_id } => match api.begin_edit(file_id).await {
            Ok(path) => ControlResponse::FilePath(path),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::FinishEdit { file_id, path } => {
            match api.finish_edit(file_id, path).await {
                Ok(outcome) => ControlResponse::EditOutcome(outcome),
                Err(error) => ControlResponse::Error(error),
            }
        }
        ControlRequest::CancelEdit { path } => match api.cancel_edit(path) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::GetPreview { file_id } => match api.get_preview(file_id).await {
            Ok(preview) => ControlResponse::Preview(preview),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::LocalPathForFile { file_id } => {
            match api.local_path_for_file(file_id).await {
                Ok(path) => ControlResponse::LocalPath(path),
                Err(error) => ControlResponse::Error(error),
            }
        }
        ControlRequest::DeleteFile { file_id } => match api.delete_file(file_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::RestoreFile { file_id } => match api.restore_file(file_id).await {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::MoveFile {
            file_id,
            logical_path,
        } => match api.move_file(file_id, logical_path) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagFile { tag_id, file_id } => match api.tag_file(tag_id, file_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::UntagFile { tag_id, file_id } => match api.untag_file(tag_id, file_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagTag {
            parent_id,
            subtag_id,
        } => match api.tag_tag(parent_id, subtag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::UntagTag {
            parent_id,
            subtag_id,
        } => match api.untag_tag(parent_id, subtag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::Subscribe => {
            *events = Some(EventStream::InProcess(api.subscribe()));
            ControlResponse::Subscribed
        }
        ControlRequest::PurgePreviews => match api.purge_previews().await {
            Ok(purged) => ControlResponse::PurgedPreviews(purged),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::EditorRules => ControlResponse::EditorRules(api.editor_rules()),
        ControlRequest::Retag { dry_run } => match api.retag(dry_run) {
            Ok(summary) => ControlResponse::Retagged(summary),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagRuleReport => match api.tag_rule_report() {
            Ok(report) => ControlResponse::TagRuleReport(report),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::ListOperations => ControlResponse::Operations(api.list_operations()),
        ControlRequest::SubscribeOperations => {
            *operation_events = Some(OperationStream::InProcess(api.subscribe_operations()));
            ControlResponse::OperationsSubscribed
        }
    }
}

/// Encode a [`ControlFrame`] to a binary WebSocket message.
///
/// The control protocol uses **binary msgpack** (via `rmp_serde`), not JSON
/// text. This matters for the provider protocol: JSON encodes a `Vec<u8>` chunk
/// as an array of decimal numbers (~4-6x blow-up), producing multi-hundred-KB
/// frames that are both slow and fragile; msgpack encodes bytes compactly and
/// unambiguously. It also mirrors the peer `Frame` wire format (also `rmp`).
fn encode_frame(frame: &ControlFrame) -> Result<Message, String> {
    let bytes = rmp_serde::to_vec_named(frame).map_err(|error| format!("serialize: {error}"))?;
    Ok(Message::binary(bytes))
}

/// Decode a [`ControlFrame`] from an inbound WebSocket message.
///
/// Accepts binary (msgpack) frames. Non-data frames (ping/pong/close) are
/// filtered by callers before this is reached.
fn decode_frame(message: &Message) -> Result<ControlFrame, String> {
    rmp_serde::from_slice(&message.clone().into_data())
        .map_err(|error| format!("deserialize: {error}"))
}

async fn send_control(
    outgoing: &mut SplitSink<WebSocketStream<UnixStream>, Message>,
    frame: &ControlFrame,
) -> Result<(), String> {
    let message = encode_frame(frame)?;
    outgoing
        .send(message)
        .await
        .map_err(|error| format!("send: {error}"))
}

/// The IPC-client transport backend.
///
/// A thin embedded Rust client that connects to the daemon's control socket,
/// serializes [`TransportBackend`] calls into [`ControlRequest`]s, and awaits
/// the matching [`ControlResponse`]. It is the Linux-daemon counterpart to
/// `InProcessBackend`; the Dart UI (and the `tagsy` CLI) never learn which
/// they hold.
///
/// A single background reader task owns the socket's read half and
/// demultiplexes inbound frames: [`ControlFrame::Response`]s are matched to
/// waiters by `id`; [`ControlFrame::Event`]s are pushed onto a broadcast
/// channel that [`subscribe`](TransportBackend::subscribe) taps.
#[derive(Clone)]
pub struct IpcClientBackend {
    inner: Arc<IpcClientInner>,
}

/// Read the chunk of `path` starting at `offset` (bounded by the transfer chunk
/// size), returning the bytes and whether it reached end-of-file. Client side
/// of the provider protocol.
async fn read_provider_chunk(path: &Path, offset: u64) -> (Vec<u8>, bool) {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let chunk_size = crate::transfer::CHUNK_SIZE;
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) => {
            log::warn!("Provider: failed to open {}: {error}", path.display());
            return (Vec::new(), true);
        }
    };
    let total = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    let mut file = file;
    if file.seek(std::io::SeekFrom::Start(offset)).await.is_err() {
        return (Vec::new(), true);
    }
    let mut buffer = vec![0u8; chunk_size];
    let mut filled = 0;
    while filled < chunk_size {
        match file.read(&mut buffer[filled..]).await {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(error) => {
                log::warn!("Provider: read error on {}: {error}", path.display());
                return (Vec::new(), true);
            }
        }
    }
    buffer.truncate(filled);
    let last = offset + filled as u64 >= total;
    (buffer, last)
}

struct IpcClientInner {
    /// Write half of the control socket, behind a mutex so concurrent API
    /// calls serialize their frames without interleaving bytes.
    writer: Mutex<SplitSink<WebSocketStream<UnixStream>, Message>>,
    /// Correlation-id -> oneshot for the response of an in-flight request.
    pending: Mutex<HashMap<u64, oneshot::Sender<ControlResponse>>>,
    /// Monotonic request-id source.
    next_id: AtomicU64,
    /// Broadcast of events received on this connection. `subscribe` taps it.
    events: tokio::sync::broadcast::Sender<ApiEvent>,
    /// Broadcast of operation events received on this connection.
    /// `subscribe_operations` taps it.
    operation_events: tokio::sync::broadcast::Sender<crate::operations::OperationEvent>,
    /// The local file this client is currently serving as a temporary provider
    /// (an in-flight upload/edit). The reader task answers the daemon's
    /// `ProviderChunkRequest`s by reading chunks from this path.
    provider_path: Mutex<Option<PathBuf>>,
}

impl IpcClientBackend {
    /// Connect to the daemon's default control socket
    /// ([`paths::control_socket_path`](crate::paths::control_socket_path)).
    pub async fn connect_default() -> Result<Self, ApiError> {
        Self::connect(crate::paths::control_socket_path()).await
    }

    /// Connect to the daemon control socket at `socket_path`.
    ///
    /// Establishes the WebSocket handshake over the Unix stream (the daemon
    /// speaks WS to reuse the peer framing code) and spawns the demultiplexing
    /// reader task. Errors are surfaced as [`ApiError::Transport`] so the UI
    /// sees the single API error type.
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, ApiError> {
        let socket_path = socket_path.as_ref();
        let stream = UnixStream::connect(socket_path).await.map_err(|error| {
            ApiError::Transport(format!(
                "connect to control socket {}: {error}",
                socket_path.display()
            ))
        })?;

        // tokio-tungstenite needs a client request even over a Unix socket;
        // the URI is a placeholder the daemon ignores (there is no routing).
        let request = "ws://localhost/"
            .into_client_request()
            .map_err(|error| ApiError::Transport(format!("build ws request: {error}")))?;
        let (ws_stream, _response) = tokio_tungstenite::client_async(request, stream)
            .await
            .map_err(|error| ApiError::Transport(format!("control ws handshake: {error}")))?;

        let (outgoing, mut incoming) = ws_stream.split();

        let (events, _) = tokio::sync::broadcast::channel(1024);
        let (operation_events, _) = tokio::sync::broadcast::channel(1024);
        let inner = Arc::new(IpcClientInner {
            writer: Mutex::new(outgoing),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            events: events.clone(),
            operation_events: operation_events.clone(),
            provider_path: Mutex::new(None),
        });

        // Reader task: demultiplex responses (to waiters) and events (to the
        // broadcast). Ends when the socket closes, waking any pending waiter
        // with a dropped sender (surfaced as a Transport error).
        let reader_inner = inner.clone();
        tokio::spawn(async move {
            while let Some(message) = incoming.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        log::debug!("Control client read error: {error}");
                        break;
                    }
                };
                if message.is_ping() || message.is_pong() || message.is_close() {
                    continue;
                }
                let frame: ControlFrame = match decode_frame(&message) {
                    Ok(frame) => frame,
                    Err(error) => {
                        // Out-of-sync stream: stop reading so pending waiters are
                        // failed (below) rather than left hanging on a response
                        // that will never be correctly decoded.
                        log::warn!("Malformed control frame from daemon: {error}; closing");
                        break;
                    }
                };
                match frame {
                    ControlFrame::Response { id, response } => {
                        if let Some(sender) = reader_inner.pending.lock().await.remove(&id) {
                            let _ = sender.send(response);
                        } else {
                            log::warn!("Control response for unknown request id {id}");
                        }
                    }
                    ControlFrame::Event(event) => {
                        // Best-effort: if no one is subscribed, drop.
                        let _ = reader_inner.events.send(event);
                    }
                    ControlFrame::OperationEvent(event) => {
                        // Best-effort: if no one is subscribed, drop.
                        let _ = reader_inner.operation_events.send(event);
                    }
                    // The daemon is pulling a chunk of the file we're currently
                    // providing (an in-flight upload/edit). Read it from the
                    // local file and reply.
                    ControlFrame::ProviderChunkRequest { chunk_id, offset } => {
                        let path = reader_inner.provider_path.lock().await.clone();
                        let (bytes, last) = match path {
                            Some(path) => read_provider_chunk(&path, offset).await,
                            None => {
                                log::warn!("Provider chunk requested but no active provider file");
                                (Vec::new(), true)
                            }
                        };
                        let reply = ControlFrame::ProviderChunkReply {
                            chunk_id,
                            bytes,
                            last,
                        };
                        let message = match encode_frame(&reply) {
                            Ok(message) => message,
                            Err(error) => {
                                log::warn!("serialize provider chunk reply: {error}");
                                continue;
                            }
                        };
                        let mut writer = reader_inner.writer.lock().await;
                        if let Err(error) = writer.send(message).await {
                            log::debug!("Failed to send provider chunk reply: {error}");
                            break;
                        }
                    }
                    ControlFrame::Request { .. } | ControlFrame::ProviderChunkReply { .. } => {
                        log::warn!("Daemon sent an unexpected frame to a client; ignoring");
                    }
                }
            }
            // Socket closed: fail every outstanding request so callers unblock.
            reader_inner.pending.lock().await.clear();
            log::debug!("Control client reader task ended");
        });

        let client = Self { inner };

        // Subscribe to the daemon's event stream **once, up front**, for the
        // whole life of the connection. The daemon only forwards `ApiEvent`s to
        // a client after it receives a `Subscribe` request (see `dispatch`);
        // without this, the reader task above would never observe any
        // `ControlFrame::Event`, so `TransportBackend::subscribe` (which just
        // taps the local broadcast fed by that reader) would stay silent and
        // the UI would never live-update. This is the IPC-path counterpart to
        // the in-process backend, where `subscribe` reaches the live bus
        // directly. Repeated UI-side `subscribe` calls then share this one
        // daemon subscription.
        match client.call(ControlRequest::Subscribe).await? {
            ControlResponse::Subscribed => {}
            other => return Err(unexpected(other)),
        }

        // Likewise subscribe to the operation stream once, up front, so the
        // reader task observes `ControlFrame::OperationEvent`s and the local
        // `operation_events` broadcast is fed for `subscribe_operations` taps.
        match client.call(ControlRequest::SubscribeOperations).await? {
            ControlResponse::OperationsSubscribed => {}
            other => return Err(unexpected(other)),
        }

        Ok(client)
    }

    /// Send a request and await its response, correlating by a fresh id.
    async fn call(&self, request: ControlRequest) -> Result<ControlResponse, ApiError> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.inner.pending.lock().await.insert(id, sender);

        let frame = ControlFrame::Request { id, request };
        let message = encode_frame(&frame)
            .map_err(|error| ApiError::Transport(format!("serialize request: {error}")))?;
        {
            let mut writer = self.inner.writer.lock().await;
            writer
                .send(message)
                .await
                .map_err(|error| ApiError::Transport(format!("send request: {error}")))?;
        }

        receiver.await.map_err(|_| {
            ApiError::Transport("control connection closed before response".to_owned())
        })
    }
}

/// Block until the daemon reports `file_id` has been handed off
/// ([`ApiEvent::ProviderReleased`]), or the event stream ends.
async fn wait_for_release(
    events: &mut tokio::sync::broadcast::Receiver<ApiEvent>,
    file_id: FileId,
) {
    loop {
        match events.recv().await {
            Ok(ApiEvent::ProviderReleased { file_id: released }) if released == file_id => {
                return;
            }
            Ok(_) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// Collapse a [`ControlResponse`] error variant into the `Result` the
/// [`TransportBackend`] surface expects; otherwise report the unexpected shape.
fn unexpected(response: ControlResponse) -> ApiError {
    match response {
        ControlResponse::Error(error) => error,
        other => ApiError::Transport(format!("unexpected control response: {other:?}")),
    }
}

// The `TransportBackend` trait declares each method as
// `-> impl Future<...> + Send`; a plain `async fn` in the impl satisfies that
// (matching `InProcessBackend`). Each call maps 1:1 onto a `ControlRequest`
// and pattern-matches the expected `ControlResponse`, treating anything else
// (including `ControlResponse::Error`) via `unexpected`.
impl TransportBackend for IpcClientBackend {
    async fn resolve_file_id(&self, prefix: String) -> Result<FileId, ApiError> {
        match self.call(ControlRequest::ResolveFileId { prefix }).await? {
            ControlResponse::FileId(file_id) => Ok(file_id),
            other => Err(unexpected(other)),
        }
    }

    async fn resolve_tag_id(&self, prefix: String) -> Result<TagId, ApiError> {
        match self.call(ControlRequest::ResolveTagId { prefix }).await? {
            ControlResponse::TagId(tag_id) => Ok(tag_id),
            other => Err(unexpected(other)),
        }
    }

    async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self
            .call(ControlRequest::TagsForFile {
                file_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::TagIds(tag_ids) => Ok(tag_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn run_query(
        &self,
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<QueryResult, ApiError> {
        match self
            .call(ControlRequest::RunQuery {
                query,
                subtag_rule,
                deleted_rule,
            })
            .await?
        {
            ControlResponse::QueryResult(result) => Ok(result),
            other => Err(unexpected(other)),
        }
    }

    async fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, ApiError> {
        match self
            .call(ControlRequest::GetFile {
                file_id,
                deleted_rule,
            })
            .await?
        {
            ControlResponse::File(file) => Ok(file),
            other => Err(unexpected(other)),
        }
    }

    async fn get_tag(&self, tag_id: TagId, deleted_rule: DeletedRule) -> Result<Tag, ApiError> {
        match self
            .call(ControlRequest::GetTag {
                tag_id,
                deleted_rule,
            })
            .await?
        {
            ControlResponse::Tag(tag) => Ok(tag),
            other => Err(unexpected(other)),
        }
    }

    async fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self
            .call(ControlRequest::SubtagsForTag {
                tag_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::TagIds(tag_ids) => Ok(tag_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self
            .call(ControlRequest::TagsForTag {
                tag_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::TagIds(tag_ids) => Ok(tag_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        match self.call(ControlRequest::CreateTag { name, color }).await? {
            ControlResponse::TagId(tag_id) => Ok(tag_id),
            other => Err(unexpected(other)),
        }
    }

    async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self.call(ControlRequest::DeleteTag { tag_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn restore_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self.call(ControlRequest::RestoreTag { tag_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::RenameTag { tag_id, name })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn set_tag_color(&self, tag_id: TagId, color: String) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::SetTagColor { tag_id, color })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// Upload a file by serving it as a temporary chunk provider (no bytes are
    /// loaded into memory or sent up front). Computes the content hash by
    /// streaming `path`, registers `path` as the file this connection serves,
    /// sends the metadata upload request, then blocks until the daemon reports
    /// the content has been handed off (a peer completed pulling it), or the
    /// connection ends.
    async fn upload_file(
        &self,
        path: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        let (content_hash, size) = crate::file_bytes::hash_and_len(&path).await?;
        // Subscribe before sending so we cannot miss the release event.
        let mut events = self.inner.events.subscribe();
        *self.inner.provider_path.lock().await = Some(path);

        let file_id = match self
            .call(ControlRequest::UploadFile {
                path_name,
                content_hash,
                size,
                tags,
            })
            .await?
        {
            ControlResponse::FileId(file_id) => file_id,
            other => return Err(unexpected(other)),
        };

        wait_for_release(&mut events, file_id).await;
        *self.inner.provider_path.lock().await = None;
        Ok(file_id)
    }

    /// Edit (replace) a file's content, serving the new bytes as a temporary
    /// provider. Same handoff semantics as [`Self::upload_file`].
    async fn edit_file(&self, file_id: FileId, path: PathBuf) -> Result<(), ApiError> {
        let (content_hash, size) = crate::file_bytes::hash_and_len(&path).await?;
        let mut events = self.inner.events.subscribe();
        *self.inner.provider_path.lock().await = Some(path);

        match self
            .call(ControlRequest::EditFile {
                file_id,
                content_hash,
                size,
            })
            .await?
        {
            ControlResponse::Ok => {}
            other => return Err(unexpected(other)),
        }

        wait_for_release(&mut events, file_id).await;
        *self.inner.provider_path.lock().await = None;
        Ok(())
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        match self
            .call(ControlRequest::FetchFile {
                file_id,
                expected_hash,
            })
            .await?
        {
            ControlResponse::FilePath(path) => Ok(path),
            other => Err(unexpected(other)),
        }
    }

    /// Start an external edit. Thin IPC wrapper: the daemon does the actual
    /// work (local-path check or on-demand fetch into a per-request temp) and
    /// returns the path. No chunk-provider setup is needed here — unlike
    /// [`Self::edit_file`], the client does not stream bytes in either
    /// direction. The daemon reads the edited bytes off its own filesystem
    /// when [`Self::finish_edit`] runs (client and daemon share it).
    async fn begin_edit(&self, file_id: FileId) -> Result<PathBuf, ApiError> {
        match self.call(ControlRequest::BeginEdit { file_id }).await? {
            ControlResponse::FilePath(path) => Ok(path),
            other => Err(unexpected(other)),
        }
    }

    async fn finish_edit(&self, file_id: FileId, path: PathBuf) -> Result<EditOutcome, ApiError> {
        match self
            .call(ControlRequest::FinishEdit { file_id, path })
            .await?
        {
            ControlResponse::EditOutcome(outcome) => Ok(outcome),
            other => Err(unexpected(other)),
        }
    }

    async fn cancel_edit(&self, path: PathBuf) -> Result<(), ApiError> {
        match self.call(ControlRequest::CancelEdit { path }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn get_preview(&self, file_id: FileId) -> Result<Preview, ApiError> {
        match self.call(ControlRequest::GetPreview { file_id }).await? {
            ControlResponse::Preview(preview) => Ok(preview),
            other => Err(unexpected(other)),
        }
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        match self
            .call(ControlRequest::LocalPathForFile { file_id })
            .await?
        {
            ControlResponse::LocalPath(path) => Ok(path),
            other => Err(unexpected(other)),
        }
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self.call(ControlRequest::DeleteFile { file_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn restore_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self.call(ControlRequest::RestoreFile { file_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::MoveFile {
                file_id,
                logical_path,
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::TagFile { tag_id, file_id })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::UntagFile { tag_id, file_id })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::TagTag {
                parent_id,
                subtag_id,
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::UntagTag {
                parent_id,
                subtag_id,
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn purge_previews(&self) -> Result<usize, ApiError> {
        match self.call(ControlRequest::PurgePreviews).await? {
            ControlResponse::PurgedPreviews(purged) => Ok(purged),
            other => Err(unexpected(other)),
        }
    }

    async fn editor_rules(&self) -> Result<Vec<EditorRule>, ApiError> {
        match self.call(ControlRequest::EditorRules).await? {
            ControlResponse::EditorRules(rules) => Ok(rules),
            other => Err(unexpected(other)),
        }
    }

    async fn retag(&self, dry_run: bool) -> Result<RetagSummary, ApiError> {
        match self.call(ControlRequest::Retag { dry_run }).await? {
            ControlResponse::Retagged(summary) => Ok(summary),
            other => Err(unexpected(other)),
        }
    }

    async fn tag_rule_report(&self) -> Result<TagRuleReport, ApiError> {
        match self.call(ControlRequest::TagRuleReport).await? {
            ControlResponse::TagRuleReport(report) => Ok(report),
            other => Err(unexpected(other)),
        }
    }

    fn subscribe(&self) -> EventStream {
        EventStream::Ipc(self.inner.events.subscribe())
    }

    async fn list_operations(&self) -> Result<Vec<crate::operations::Operation>, ApiError> {
        match self.call(ControlRequest::ListOperations).await? {
            ControlResponse::Operations(operations) => Ok(operations),
            other => Err(unexpected(other)),
        }
    }

    fn subscribe_operations(&self) -> OperationStream {
        OperationStream::Ipc(self.inner.operation_events.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every control frame must round-trip through the binary codec unchanged.
    /// The provider chunk reply is exercised explicitly at a realistic chunk
    /// size because a JSON codec would serialize its `Vec<u8>` as a giant
    /// number-array, which is exactly the failure the binary codec prevents.
    #[test]
    fn frames_round_trip_through_binary_codec() {
        let file_id = FileId::new();

        let frames = vec![
            ControlFrame::Request {
                id: 7,
                request: ControlRequest::EditFile {
                    file_id,
                    content_hash: "deadbeef".to_owned(),
                    size: 123,
                },
            },
            ControlFrame::Request {
                id: 8,
                request: ControlRequest::UploadFile {
                    path_name: "notes.txt".to_owned(),
                    content_hash: "cafef00d".to_owned(),
                    size: 456,
                    tags: vec![TagId::new()],
                },
            },
            ControlFrame::Response {
                id: 7,
                response: ControlResponse::FileId(file_id),
            },
            ControlFrame::ProviderChunkRequest {
                chunk_id: 3,
                offset: 65536,
            },
            ControlFrame::ProviderChunkReply {
                chunk_id: 3,
                bytes: vec![0xABu8; crate::transfer::CHUNK_SIZE],
                last: true,
            },
        ];

        for frame in frames {
            let message = encode_frame(&frame).expect("encode");
            let decoded = decode_frame(&message).expect("decode");
            // Compare via debug repr (ControlFrame has no PartialEq).
            assert_eq!(format!("{frame:?}"), format!("{decoded:?}"));
        }
    }

    /// A large chunk reply must not be mis-decoded as another variant (the
    /// original bug surfaced as "missing field content_hash" when a big frame
    /// was parsed against a request shape). msgpack is length-prefixed and
    /// self-describing, so a reply decodes only as a reply.
    #[test]
    fn large_chunk_reply_does_not_alias_a_request() {
        let reply = ControlFrame::ProviderChunkReply {
            chunk_id: 0,
            bytes: vec![0x00u8; 475_000],
            last: false,
        };
        let message = encode_frame(&reply).expect("encode");
        match decode_frame(&message).expect("decode") {
            ControlFrame::ProviderChunkReply { .. } => {}
            other => panic!("large chunk reply decoded as {other:?}"),
        }
    }
}
