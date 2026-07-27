//! Process-global runtime ownership.
//!
//! On Android the sync engine must keep running after the UI is closed. The
//! Flutter activity (and its Dart isolate) is destroyed when the app is swiped
//! away, but a **foreground service** in the same process keeps the process
//! alive. So the runtime cannot be owned by the Dart-side [`OniSyncApp`]; it is
//! owned here, in a **process-global**, started by the foreground service
//! (over JNI, see [`crate::jni`]) and merely *attached to* by the UI when it
//! is open.
//!
//! There is exactly one runtime per process. `start` is idempotent: whoever
//! gets there first (normally the service) wins; later callers (the UI, or a
//! service restart) attach to the already-running instance. This preserves the
//! core's single-writer model — one `RuntimeHandle`, one DB writer — while
//! both the service and the UI reach it.

use std::sync::{Mutex, OnceLock};

use onisyncd::paths::Paths;
use onisyncd::transport::Backend;

use crate::runtime::{RuntimeHandle, StartError};

/// What this process is attached to.
///
/// Two very different topologies share one process-global slot:
///
/// - [`Runtime::InProcess`] (Android, single-process desktop) **owns** the sync
///   engine: a [`RuntimeHandle`] holding the runtime thread, the DB, and the
///   shutdown signal. Tearing it down stops sync.
/// - [`Runtime::Ipc`] (Linux daemon topology) owns **nothing** but a
///   [`Backend::Ipc`] connection to the already-running daemon that owns the
///   DB. Dropping it just closes the control-socket connection; the daemon
///   keeps syncing.
enum Runtime {
    /// This process hosts the engine (Android / single-process desktop).
    InProcess(RuntimeHandle),
    /// This process is a UI client attached to the daemon over IPC (Linux).
    Ipc(Backend),
}

impl Runtime {
    /// The UI-facing backend, regardless of topology.
    fn backend(&self) -> Backend {
        match self {
            Runtime::InProcess(handle) => handle.backend(),
            Runtime::Ipc(backend) => backend.clone(),
        }
    }

    /// This device's public key, if this process owns an identity.
    ///
    /// Only the in-process topology has a local identity; the IPC client is a
    /// mere UI attached to the daemon and does not hold the daemon's key.
    fn public_key(&self) -> Option<String> {
        match self {
            Runtime::InProcess(handle) => Some(handle.public_key().to_owned()),
            Runtime::Ipc(_) => None,
        }
    }
}

/// The one runtime for this process. `None` until started, back to `None`
/// after [`stop`].
fn slot() -> &'static Mutex<Option<Runtime>> {
    static SLOT: OnceLock<Mutex<Option<Runtime>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

/// Start the process-global runtime, or do nothing if it is already running.
///
/// Idempotent and safe to call from both the foreground service and the UI.
/// Returns this device's public key on success.
pub fn start(configuration_json: &str, paths: Paths) -> Result<String, StartError> {
    crate::logging::init();

    let mut guard = slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(runtime) = guard.as_ref() {
        // Already running (e.g. the service started it and now the UI attaches,
        // or vice versa). Reuse it. An IPC client has no local public key.
        return Ok(runtime.public_key().unwrap_or_default());
    }

    let handle = RuntimeHandle::start(configuration_json, paths)?;
    let public_key = handle.public_key().to_owned();
    *guard = Some(Runtime::InProcess(handle));
    Ok(public_key)
}

/// Attach to an already-running daemon over IPC (Linux desktop topology).
///
/// Unlike [`start`], this process does **not** own the sync engine or the DB:
/// the systemd daemon does. This opens a control-socket connection
/// ([`Backend::ipc_default`], `/run/onisync/onisync.sock`) and stores it in the
/// process-global slot so the UI reads/writes through the daemon.
///
/// Idempotent: if a runtime (of either topology) is already present, this
/// reuses it rather than opening a second connection.
pub async fn attach() -> Result<(), StartError> {
    crate::logging::init();

    {
        let guard = slot()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_some() {
            return Ok(());
        }
    }

    // Connect outside the lock: the handshake is async and a `std::Mutex`
    // guard must not be held across `.await`.
    let backend = Backend::ipc_default().await.map_err(StartError::Ipc)?;

    let mut guard = slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Re-check: another caller may have attached while we were connecting. If
    // so, drop our just-opened connection and reuse theirs.
    if guard.is_none() {
        *guard = Some(Runtime::Ipc(backend));
    }
    Ok(())
}

/// Stop the process-global runtime if running (idempotent).
///
/// Intended for the service `onDestroy`. Does **not** stop when the UI merely
/// closes — the service keeps the runtime alive; only the service tears it
/// down.
pub fn stop() {
    let runtime = {
        let mut guard = slot()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.take()
    };
    // Only the in-process topology owns a runtime thread to join; dropping the
    // IPC variant just closes the control connection (the daemon keeps running).
    if let Some(Runtime::InProcess(handle)) = runtime {
        handle.stop();
    }
}

/// The UI-facing backend for the running runtime, or `None` if not started.
pub fn backend() -> Option<Backend> {
    let guard = slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.as_ref().map(|runtime| runtime.backend())
}

/// This device's public key, or `None` if the runtime is not started or this
/// process is an IPC client (which has no local identity).
pub fn public_key() -> Option<String> {
    let guard = slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.as_ref().and_then(|runtime| runtime.public_key())
}

/// Whether the process-global runtime is currently running.
pub fn is_running() -> bool {
    let guard = slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.is_some()
}
