//! Preview resolution: turning a cache miss into a [`Preview`], either by
//! generating locally (when this build and this device's policy both allow
//! it) or by asking peers.
//!
//! Every item in this module that touches generation itself is gated on the
//! `preview-generation` feature; callers (in `lib.rs`) call the ungated
//! entry points ([`maybe_eager_preview`], [`resolve_preview`],
//! [`preview_extension_for`], [`try_serve_generated_preview`]) without
//! needing their own `cfg` blocks.

use tagsy_core::state::Frame;
#[cfg(feature = "preview-generation")]
use tagsy_core::state::Sync as SyncMessage;
use tagsy_core::{FileId, Preview};
use tokio::sync::mpsc::UnboundedSender;

use crate::bus::{DaemonMessage, PreviewError};
use crate::configuration::Configuration;
use crate::directory_manager::SyncDirectoryCommand;
#[cfg(feature = "preview-generation")]
use crate::file_bytes::FileBytes;
use crate::preview_fetch::{PendingPreviews, PreviewReply};
#[cfg(feature = "preview-generation")]
use crate::store::CatalogStore;

/// Whether this build can generate previews at all: the `preview-generation`
/// feature (and its `image`/`pdfium` deps) is compiled in.
pub(crate) const PREVIEW_GENERATION_COMPILED: bool = cfg!(feature = "preview-generation");

/// If this device is configured for **eager** previews, kick off preview
/// generation for `file_id` now (fire-and-forget), so the preview is warm in
/// the cache before anyone requests it.
///
/// Implemented by enqueuing a fire-and-forget [`DaemonMessage::GetPreview`]
/// (the reply is discarded): it reuses the exact resolve-and-cache path, runs
/// the CPU-heavy generation in `spawn_blocking` off the writer loop, and is a
/// cheap no-op when the preview is already cached. Call this only after the
/// file's bytes are known to be present locally (a completed peer transfer or a
/// locally-observed file), so the local-first `resolve_preview` generates from
/// disk rather than fetching from a peer.
///
/// A no-op unless the policy is [`PreviewGenerationPolicy::Eager`].
pub(crate) fn maybe_eager_preview(
    configuration: &Configuration,
    change_sender: &UnboundedSender<DaemonMessage>,
    file_id: FileId,
) {
    if !configuration.preview_generation_policy.is_eager() {
        return;
    }

    // Throwaway responder: we don't consume the result here, we only want the
    // side effect of generating + caching it.
    let (respond_to, _discard) = tokio::sync::oneshot::channel();

    if change_sender
        .send(DaemonMessage::GetPreview {
            file_id,
            respond_to,
        })
        .is_err()
    {
        log::debug!(
            "maybe_eager_preview: change channel closed; skipping eager preview for {}",
            file_id.to_string()
        );
    } else {
        log::debug!(
            "maybe_eager_preview: enqueued eager preview generation for {}",
            file_id.to_string()
        );
    }
}

/// Resolve the preview for `(file_id, content_hash)` when it is not cached.
///
/// Presence-first, mirroring the byte-fetch policy: if this device *can
/// generate* (`can_generate`) and the file is present locally (its bytes are on
/// disk and hash-match), generate the preview here (off the async runtime, via
/// `spawn_blocking`); otherwise flood a `PreviewRequest` across the peer tree
/// and take the first responder.
///
/// The two negative outcomes are deliberately distinct:
/// - a locally-determined "no preview" (un-previewable type), or a peer that
///   generated and found none, is an **authoritative** `Ok(Preview::None)` —
///   cacheable;
/// - "we could not obtain one this time" (local bytes absent/racing or a
///   generation panic, *and* no reachable peer served one) is
///   `Err(PreviewError::Unavailable)` — a **transient** result the caller must
///   not cache, so a later request retries.
///
/// A local generation failure (including a panic) does not resolve on its own:
/// it falls through to the peer request, and only peer exhaustion / a torn
/// channel produces `Unavailable`.
///
/// `can_generate` is `false` on a `Never`-policy device (or a build without the
/// `preview-generation` feature); such a device skips local generation and only
/// ever obtains previews from peers.
///
/// Runs off the DB-writer loop; the caller re-enters via
/// [`DaemonMessage::ApplyPreview`] to cache the result (or forward the
/// transient error).
pub(crate) async fn resolve_preview(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    pending_previews: &PendingPreviews,
    file_id: FileId,
    content_hash: &str,
    can_generate: bool,
    extension: Option<String>,
) -> Result<Preview, PreviewError> {
    let short = content_hash.get(..8).unwrap_or(content_hash);

    // 1. Can we generate locally? Only if this device's policy permits it and
    // the generation stack is compiled in. If so, and the file is present
    // locally, generate from our own bytes. The block is `cfg`-gated because it
    // references `generate_preview_from_local`, which only exists with the
    // feature; `can_generate` is always `false` without it, so this is a no-op
    // either way.
    #[cfg(feature = "preview-generation")]
    if can_generate {
        // NOTE: `read_local_if_hash_matches` issues
        // `SyncDirectoryCommand::ReadFile`, which reads *and hashes the whole
        // file* to verify it matches `content_hash` (O(size)).
        let local_read_start = std::time::Instant::now();
        let local = crate::read_local_if_hash_matches(command_sender, file_id, content_hash).await;

        log::debug!(
            "resolve_preview[{short}]: local ReadFile+verify for {} took {:?} (present={})",
            file_id.to_string(),
            local_read_start.elapsed(),
            local.is_some()
        );

        if let Some(file_bytes) = local
            && let Some(preview) =
                generate_preview_from_local(file_id, file_bytes, extension.clone()).await
        {
            return Ok(preview);
        }

        log::debug!(
            "resolve_preview[{short}]: local generation did not produce a preview for {}; asking \
             peers",
            file_id.to_string()
        );
    }

    // Without the feature, local generation is compiled out, so these are dead;
    // silence the unused warnings.
    #[cfg(not(feature = "preview-generation"))]
    let _ = (can_generate, command_sender, &extension);

    // 2. Not present locally: ask peers. First responder wins; exhaustion or
    // timeout resolves to `None`.
    let peer_start = std::time::Instant::now();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    pending_previews
        .request_preview_local(file_id, content_hash.to_owned(), reply_tx)
        .await;

    // A peer's `Data` is authoritative (it may even be an authoritative
    // `Preview::None`) and is cached. `Miss` (no reachable peer holds it) and a
    // torn reply channel are both *transient*: caching them would permanently
    // mask a preview that a currently-offline holder can serve later, so they
    // become `Unavailable` and the caller leaves the cache untouched.
    let result = match reply_rx.await {
        Ok(PreviewReply::Data(preview)) => Ok(preview),
        Ok(PreviewReply::Miss) | Err(_) => Err(PreviewError::Unavailable),
    };

    log::debug!(
        "resolve_preview[{short}]: peer fetch for {} took {:?} (ok={})",
        file_id.to_string(),
        peer_start.elapsed(),
        result.is_ok()
    );

    result
}

/// Generate a [`Preview`] from `file_bytes` off the async runtime.
///
/// Hands the `FileBytes` to `preview::generate` inside `spawn_blocking`, which
/// reads the source itself (a bounded prefix for most kinds; the on-disk path
/// directly for a file-backed video).
///
/// Returns `Some(preview)` on success (including `Some(Preview::None)` for
/// un-previewable content — an authoritative negative result), or `None` if a
/// preview could not be produced (the source could not be read — row present
/// but file gone/racing — or the generation task panicked), so the caller can
/// fall back to asking peers rather than caching a spurious negative.
///
/// Only compiled with the `preview-generation` feature; all call sites are
/// guarded by `can_generate`, which is `false` without it.
/// Look up a file's lowercase extension (no dot, e.g. `"jpg"`) from its logical
/// name in the main DB, for use as a preview type-detection hint.
///
/// Returns `None` if the file is unknown, has no extension, or the DB read
/// fails — the generator then relies on magic-byte sniffing alone.
#[cfg(feature = "preview-generation")]
pub(crate) fn preview_extension_for(database: &CatalogStore, file_id: FileId) -> Option<String> {
    let logical_path = database.logical_path_for_file_id(file_id).ok()?;

    std::path::Path::new(logical_path.as_str())
        .extension()
        .map(|extension| extension.to_string_lossy().to_lowercase())
}

#[cfg(feature = "preview-generation")]
async fn generate_preview_from_local(
    file_id: FileId,
    file_bytes: FileBytes,
    extension: Option<String>,
) -> Option<Preview> {
    // `preview::generate` reads its own source: it pulls a bounded prefix into
    // memory for classification and image/PDF/text, and hands ffmpeg the
    // file-backed source's path directly for video (no read-through). So we no
    // longer read the bytes here — we move the `FileBytes` into the blocking
    // task. The read + decode/resize/encode all happen on the blocking pool.
    let blocking_start = std::time::Instant::now();
    match tokio::task::spawn_blocking(move || {
        crate::preview::generate(&file_bytes, extension.as_deref())
    })
    .await
    {
        // An authoritative result (including `Some(Preview::None)` for
        // un-previewable content).
        Ok(Some(preview)) => {
            log::debug!(
                "generate_preview_from_local: spawn_blocking(generate) for {} took {:?}",
                file_id.to_string(),
                blocking_start.elapsed()
            );

            Some(preview)
        }
        // The source could not be read (a mid-operation race after the earlier
        // presence check). Fall through to peers rather than caching a spurious
        // negative.
        Ok(None) => {
            log::debug!(
                "generate_preview_from_local: source unreadable for {}; asking peers",
                file_id.to_string()
            );

            None
        }
        Err(error) => {
            log::error!(
                "generate_preview_from_local: task panicked for {}: {error}",
                file_id.to_string()
            );

            // A panic is *not* an authoritative "no preview": returning
            // `Some(Preview::None)` here would cache a permanent negative for
            // content another device might preview fine. Fall through to `None`
            // so `resolve_preview` asks peers, and only genuine peer exhaustion
            // yields the transient `PreviewError::Unavailable`.
            None
        }
    }
}

/// Tier-2 of the peer `PreviewRequest` handler: try to *generate* a preview
/// from local bytes and send it as `PreviewData`. Returns `true` if a
/// `PreviewData` was sent, `false` if the caller should relay instead.
///
/// Two implementations by feature: with `preview-generation`, it generates when
/// `can_generate` and the content is present locally; without the feature it is
/// a no-op that always returns `false` (so the request is relayed), consuming
/// its arguments so the call site needs no `cfg`.
#[cfg(feature = "preview-generation")]
pub(crate) async fn try_serve_generated_preview(
    our_sender: &UnboundedSender<Frame>,
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    can_generate: bool,
    peer_name: &str,
    file_id: FileId,
    content_hash: &str,
    extension: Option<String>,
) -> bool {
    if !can_generate {
        return false;
    }

    let Some(file_bytes) =
        crate::read_local_if_hash_matches(command_sender, file_id, content_hash).await
    else {
        return false;
    };

    let Some(preview) = generate_preview_from_local(file_id, file_bytes, extension).await else {
        return false;
    };

    log::debug!(
        "peer[{peer_name}]: served generated PreviewData {} [{}]",
        file_id.to_string(),
        content_hash.get(..8).unwrap_or(content_hash)
    );

    let _ = our_sender.send(Frame::Sync(SyncMessage::PreviewData {
        file_id,
        content_hash: content_hash.to_owned(),
        preview,
    }));

    true
}

/// See the feature-enabled variant. Without `preview-generation` this device
/// cannot generate, so it never serves a generated preview.
#[cfg(not(feature = "preview-generation"))]
pub(crate) async fn try_serve_generated_preview(
    _our_sender: &UnboundedSender<Frame>,
    _command_sender: &UnboundedSender<SyncDirectoryCommand>,
    _can_generate: bool,
    _peer_name: &str,
    _file_id: FileId,
    _content_hash: &str,
    _extension: Option<String>,
) -> bool {
    false
}
