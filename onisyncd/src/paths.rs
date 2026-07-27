//! Central place for resolving on-disk locations under the onisync data
//! directory. Keeping this in one struct avoids re-building paths by hand in
//! every call site.
//!
//! Each frontend constructs a [`Paths`] value explicitly:
//!
//! - the desktop binary reads the environment (see `main.rs`),
//! - Android would pass `getFilesDir()` through the bridge.
//!
//! No frontend-agnostic environment lookup lives here: a panic deep in the
//! library would crash an Android app without a shell environment.

use std::path::{Path, PathBuf};

/// Resolved on-disk locations for a single onisync instance.
///
/// `data_dir` holds the databases (`main.db`, per-sync-directory `*.db`).
/// `identity_file` is the path to this machine's long-lived identity key.
#[derive(Debug, Clone)]
pub struct Paths {
    data_dir: PathBuf,
    identity_file: PathBuf,
}

impl Paths {
    /// Build a `Paths` from an explicit data directory and identity-file path.
    pub fn new(data_dir: impl Into<PathBuf>, identity_file: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            identity_file: identity_file.into(),
        }
    }

    /// The onisync data directory holding the databases.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// This machine's long-lived identity key.
    pub fn identity_path(&self) -> &Path {
        &self.identity_file
    }

    /// The main `FileDatabase` shared across the daemon.
    pub(crate) fn main_db_path(&self) -> PathBuf {
        self.data_dir.join("main.db")
    }

    /// The per-sync-directory `SyncDirectoryDatabase`, named after the
    /// directory it tracks (e.g. a directory `testcloud` maps to
    /// `testcloud.db`).
    pub(crate) fn sync_directory_db_path(&self, sync_directory: &Path) -> PathBuf {
        let name = sync_directory
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unnamed".to_owned());
        self.data_dir.join(format!("{name}.db"))
    }

    /// Directory holding daemon-owned temp files produced by on-demand fetches
    /// (`fetch_file`).
    ///
    /// A completed fetch materializes its bytes here and hands the caller the
    /// path with **move semantics**: the caller (the local CLI or the
    /// in-process UI, both co-located with the daemon and sharing this
    /// filesystem) must consume the file by renaming it into place or
    /// deleting it. If a caller crashes before consuming, the file is
    /// orphaned; [`Self::clean_fetch_temp_dir`] sweeps such leftovers on
    /// daemon start.
    ///
    /// It lives under `data_dir` (rather than the system temp dir) so the
    /// daemon owns and can clean it, and so a fetch temp and a download
    /// destination under the same data root tend to share a filesystem
    /// (cheap rename).
    pub(crate) fn fetch_temp_dir(&self) -> PathBuf {
        self.data_dir.join("fetch-temp")
    }

    /// Remove any orphaned files left in the fetch-temp directory by callers
    /// that crashed before consuming their fetched file, then ensure the
    /// directory exists. Best-effort: called on daemon start.
    pub(crate) async fn clean_fetch_temp_dir(&self) -> std::io::Result<()> {
        let dir = self.fetch_temp_dir();
        // Remove the whole directory (clearing any orphans) and recreate it.
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        tokio::fs::create_dir_all(&dir).await
    }
}

/// The daemon's local control socket.
///
/// This is a **fixed, well-known absolute path**, deliberately with **no
/// environment-variable resolution and no fallback**. The daemon and every
/// client (the CLI, later the desktop UI) must agree on the exact same
/// address, and a fallback chain silently lets them disagree: a systemd
/// *system* service has no `XDG_RUNTIME_DIR` and would bind
/// `/tmp/onisync.sock`, while an interactive CLI would look in
/// `/run/user/1000/onisync.sock` and fail. One constant removes that whole
/// class of bug.
///
/// The directory `/run/onisync` is owned and secured by systemd via
/// `RuntimeDirectory=onisync` in the unit (see `module.nix`): it is created
/// `0700` and owned by the service user on start and removed on stop. That
/// filesystem permission is the entire security model for local control —
/// nothing is exposed on any network interface, so no auth handshake is needed
/// here.
///
/// Callers that genuinely need a different location (tests, a non-systemd
/// launch) pass an explicit path to
/// [`serve_control`](crate::control::serve_control)
/// / [`IpcClientBackend::connect`](crate::control::IpcClientBackend::connect)
/// and the CLI `--socket` flag, rather than relying on discovery here.
pub const CONTROL_SOCKET_PATH: &str = "/run/onisync/onisync.sock";

/// Path to the daemon's local control socket.
///
/// Returns the fixed [`CONTROL_SOCKET_PATH`]. See its docs for why there is no
/// environment lookup or fallback.
pub fn control_socket_path() -> PathBuf {
    PathBuf::from(CONTROL_SOCKET_PATH)
}
