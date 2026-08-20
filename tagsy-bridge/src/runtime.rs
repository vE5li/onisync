//! Native runtime lifecycle for embedded frontends.
//!
//! On Android there is no `main()` and no `#[tokio::main]`: the app loads this
//! native library and calls in. The OS also freezes background threads under
//! Doze unless a foreground service keeps the process alive, so the tokio
//! runtime that drives sync must live on a thread this crate owns and starts
//! explicitly.
//!
//! [`RuntimeHandle`] encapsulates that lifecycle:
//!
//! 1. [`RuntimeHandle::start`] builds a multi-thread tokio runtime **manually**
//!    on a dedicated OS thread (never `#[tokio::main]`), performs the fallible
//!    startup ([`tagsyd::run`]) on that runtime, and returns once the UI-facing
//!    [`Backend`] is ready.
//! 2. The runtime thread then drives the sync engine to completion.
//! 3. [`RuntimeHandle::stop`] triggers the [`ShutdownSignal`] and joins the
//!    thread, so the service `onDestroy` tears everything down cleanly.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use tagsyd::ShutdownSignal;
use tagsyd::configuration::{Configuration, ConfigurationError};
use tagsyd::identity::Identity;
use tagsyd::paths::Paths;
use tagsyd::transport::Backend;

/// Why the native runtime could not be started.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The configuration JSON supplied by the frontend was invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// Building the dedicated-thread tokio runtime failed.
    #[error("failed to build tokio runtime: {0}")]
    Runtime(#[source] std::io::Error),
    /// The sync engine failed its fallible startup (identity, DB, bind).
    #[error(transparent)]
    Run(#[from] tagsyd::RunError),
    /// Bootstrapping on-disk state (data directory or identity key) failed.
    #[error("failed to bootstrap on-disk state at {}: {source}", path.display())]
    Bootstrap {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The runtime thread exited before it reported readiness.
    #[error("runtime thread exited before startup completed")]
    Cancelled,
    /// Attaching to the daemon over IPC failed (Linux desktop topology): the
    /// control socket could not be reached or the handshake failed. Usually
    /// means the daemon is not running.
    #[error("failed to attach to the tagsy daemon: {0}")]
    Ipc(#[source] tagsyd::api::ApiError),
}

/// A running tagsy core hosted on a dedicated thread.
///
/// Holds the [`ShutdownSignal`] used to stop the engine, the join handle for
/// the runtime thread, and the [`Backend`] the UI talks to. Construct one with
/// [`RuntimeHandle::start`]; drop it or call [`RuntimeHandle::stop`] to shut
/// down.
pub struct RuntimeHandle {
    backend: Backend,
    shutdown: ShutdownSignal,
    thread: Option<JoinHandle<()>>,
    /// This device's base64 ed25519 public key (for peer pairing).
    public_key: String,
}

impl RuntimeHandle {
    /// Start the sync engine on a dedicated thread and return once the
    /// UI-facing [`Backend`] is ready.
    ///
    /// `configuration_json` is parsed with [`Configuration::from_str`] (no
    /// panics — a bad config surfaces as [`StartError::Configuration`]). The
    /// tokio runtime is built by hand on the spawned thread; this call blocks
    /// only until startup either succeeds (returning `Self`) or fails.
    ///
    /// On first launch the data directory is created and this device's
    /// ed25519 identity is generated and persisted, so the frontend does not
    /// need a separate keygen step. An existing identity is never overwritten.
    pub fn start(configuration_json: &str, paths: Paths) -> Result<Self, StartError> {
        let configuration =
            Configuration::from_str(configuration_json).map_err(StartError::Configuration)?;

        let public_key = bootstrap_on_disk_state(&paths)?;

        let shutdown = ShutdownSignal::new();

        // Channel to hand the startup outcome (the ready `Backend` or the
        // startup error) back from the runtime thread to this caller.
        let (ready_sender, ready_receiver) = mpsc::channel::<Result<Backend, StartError>>();

        let thread_shutdown = shutdown.clone();
        let thread = thread::Builder::new()
            .name("tagsy-runtime".to_owned())
            .spawn(move || {
                run_thread(configuration, paths, thread_shutdown, ready_sender);
            })
            .map_err(StartError::Runtime)?;

        // Wait for the thread to report readiness. If the sender is dropped
        // without a message, the thread died before startup completed.
        match ready_receiver.recv() {
            Ok(Ok(backend)) => Ok(Self {
                backend,
                shutdown,
                thread: Some(thread),
                public_key,
            }),
            Ok(Err(error)) => {
                // Startup failed; the thread is already unwinding. Join it so
                // we do not leak the OS thread.
                let _ = thread.join();
                Err(error)
            }
            Err(_) => {
                let _ = thread.join();
                Err(StartError::Cancelled)
            }
        }
    }

    /// The UI-facing transport backend. Clone it to hand to the API layer;
    /// every clone shares the one running engine.
    pub fn backend(&self) -> Backend {
        self.backend.clone()
    }

    /// This device's base64 ed25519 public key — the value a peer must add to
    /// its own config to pair with this device.
    pub fn public_key(&self) -> &str {
        &self.public_key
    }

    /// Request a clean shutdown and join the runtime thread.
    ///
    /// Idempotent-safe to call once; consumes the handle. Triggers the
    /// [`ShutdownSignal`] so the engine drains its tasks, then waits for the
    /// runtime thread to exit. Intended for the Android service `onDestroy`.
    pub fn stop(mut self) {
        self.shutdown.shutdown();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for RuntimeHandle {
    /// If the handle is dropped without an explicit [`RuntimeHandle::stop`],
    /// still tear the engine down cleanly rather than leaking the thread.
    fn drop(&mut self) {
        self.shutdown.shutdown();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Ensure the data directory exists and this device has an identity key, and
/// return this device's base64 ed25519 public key.
///
/// Idempotent: the directory is created if missing, and the identity is
/// generated + persisted only when no key file exists yet (an existing key is
/// loaded rather than regenerated). On Android both paths live under
/// app-private storage, so this needs no permissions.
fn bootstrap_on_disk_state(paths: &Paths) -> Result<String, StartError> {
    std::fs::create_dir_all(paths.data_dir()).map_err(|source| StartError::Bootstrap {
        path: paths.data_dir().to_path_buf(),
        source,
    })?;

    let identity = if paths.identity_path().exists() {
        Identity::load(paths.identity_path()).map_err(|source| StartError::Bootstrap {
            path: paths.identity_path().to_path_buf(),
            source,
        })?
    } else {
        if let Some(parent) = paths.identity_path().parent() {
            std::fs::create_dir_all(parent).map_err(|source| StartError::Bootstrap {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let identity = Identity::generate();
        identity
            .save(paths.identity_path())
            .map_err(|source| StartError::Bootstrap {
                path: paths.identity_path().to_path_buf(),
                source,
            })?;

        log::info!(
            "generated device identity at {} (public key {})",
            paths.identity_path().display(),
            identity.public_key()
        );

        identity
    };

    Ok(identity.public_key())
}

/// Body of the dedicated runtime thread.
///
/// Builds the tokio runtime by hand (deliberately not `#[tokio::main]`) and
/// blocks on it. Startup runs first and its outcome is reported back over
/// `ready_sender`; on success the driver future is awaited to completion,
/// which is where the engine actually does its work until `shutdown` fires.
fn run_thread(
    configuration: Configuration,
    paths: Paths,
    shutdown: ShutdownSignal,
    ready_sender: mpsc::Sender<Result<Backend, StartError>>,
) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("tagsy-worker")
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready_sender.send(Err(StartError::Runtime(error)));
            return;
        }
    };

    runtime.block_on(async move {
        let (api, driver) = match tagsyd::run(configuration, paths, shutdown).await {
            Ok(pair) => pair,
            Err(error) => {
                let _ = ready_sender.send(Err(StartError::Run(error)));
                return;
            }
        };

        // Startup succeeded: hand the ready backend to the caller and then
        // drive the engine until shutdown is observed.
        if ready_sender.send(Ok(Backend::in_process(api))).is_err() {
            // The caller went away before we reported readiness; nothing to
            // drive for.
            return;
        }

        if let Err(error) = driver.await {
            log::error!("tagsy runtime exited with error: {error}");
        }
    });
}
