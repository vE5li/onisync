//! Video frame-extraction preview generation, via a pinned ffmpeg/ffprobe.

use std::io::Cursor;
use std::path::Path;
use std::sync::OnceLock;

use image::ImageReader;
use tagsy_core::Preview;

use super::MAX_IMAGE_EDGE;
use crate::file_bytes::FileBytes;

/// Extract a single representative frame from a video and turn it into a small
/// PNG preview.
///
/// Shells out to a pinned `ffmpeg`/`ffprobe` (see [`ffmpeg_dir`]): probe the
/// duration, seek to ~10% in (skipping black intros/title cards), decode one
/// frame scaled to the thumbnail box, and let ffmpeg emit it as PNG on stdout.
///
/// Returns `None` (→ [`Preview::None`]) if ffmpeg is unavailable, the video
/// can't be probed/decoded, or anything else goes wrong — video previews then
/// degrade gracefully rather than failing the whole preview.
///
/// Runs synchronously (already on the blocking pool). ffmpeg needs a seekable
/// file (seeking a container over stdin is awkward), so a file-backed `source`
/// is handed to ffmpeg by its *own* path — no copy. An in-memory source is
/// written to a temp file first (cleaned up on drop).
pub(super) fn generate_video(source: &FileBytes) -> Option<Preview> {
    use std::process::Command;

    let dir = ffmpeg_dir()?;
    let ffmpeg = std::path::Path::new(dir).join("ffmpeg");
    let ffprobe = std::path::Path::new(dir).join("ffprobe");

    // File-backed sources feed ffmpeg directly; only an in-memory source needs
    // a throwaway temp file. The `TempVideo` (when present) must outlive the
    // borrowed `input` path, so it is bound here and kept alive for the whole
    // function; it removes the temp file on drop.
    let temp = match source.path() {
        Some(_) => None,
        None => match source {
            FileBytes::InMemory(bytes) => Some(TempVideo::create(bytes)?),
            _ => unreachable!("FileBytes::path() is None only for InMemory"),
        },
    };
    let input: &Path = match (source.path(), &temp) {
        (Some(path), _) => path,
        (None, Some(temp)) => temp.path(),
        // `source.path()` is `None` only for `InMemory`, for which `temp` is
        // always `Some` above.
        (None, None) => unreachable!("in-memory video without a temp file"),
    };

    // Probe duration so we can seek ~10% in. Best-effort: if probing fails we
    // fall back to seeking to a small fixed offset.
    let seek_seconds = probe_duration_seconds(&ffprobe, input)
        .map(|duration| duration * 0.10)
        // Clamp: never seek past a very short clip; a tiny offset still skips a
        // pure-black first frame on most videos.
        .map(|offset| offset.clamp(0.0, 600.0))
        .unwrap_or(1.0);

    let start = std::time::Instant::now();
    // `-ss` before `-i` is a fast (keyframe) seek. One frame, scaled to fit the
    // thumbnail box preserving aspect ratio (`force_original_aspect_ratio`),
    // PNG to stdout.
    let output = Command::new(&ffmpeg)
        .args([
            "-loglevel",
            "error",
            "-ss",
            &format!("{seek_seconds:.3}"),
            "-i",
        ])
        .arg(input)
        .args([
            "-frames:v",
            "1",
            "-vf",
            &format!(
                "scale={edge}:{edge}:force_original_aspect_ratio=decrease",
                edge = MAX_IMAGE_EDGE
            ),
            "-f",
            "image2",
            "-c:v",
            "png",
            "-",
        ])
        .output()
        .ok()?;

    if !output.status.success() {
        log::debug!(
            "preview: ffmpeg frame extraction failed (status {:?}): {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        return None;
    }

    // ffmpeg emitted a PNG already scaled to fit the box, so decode just to read
    // its dimensions (and to re-encode canonically). It's already tiny, so this
    // is cheap.
    let decoded = ImageReader::new(Cursor::new(&output.stdout))
        .with_guessed_format()
        .ok()?
        .decode()
        .ok()?;
    let width = decoded.width();
    let height = decoded.height();

    log::debug!(
        "preview: video {} → {width}x{height} frame @ {seek_seconds:.1}s in {:?}",
        input.display(),
        start.elapsed()
    );

    Some(Preview::Image {
        bytes: output.stdout,
        width,
        height,
    })
}

/// Probe a video's duration in seconds via `ffprobe`, or `None` if it can't be
/// determined.
fn probe_duration_seconds(ffprobe: &std::path::Path, input: &std::path::Path) -> Option<f64> {
    use std::process::Command;

    let output = Command::new(ffprobe)
        .args([
            "-loglevel",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
        ])
        .arg(input)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

/// Directory containing the pinned `ffmpeg`/`ffprobe` binaries, from
/// `ONISYNC_FFMPEG_PATH` (set by the packaging wrapper / dev shell; see
/// `flake.nix`). `None` — and thus no video previews — if unset.
///
/// We deliberately do *not* fall back to a `$PATH` lookup: the daemon may run
/// under systemd with a minimal `PATH`, and silently using whatever `ffmpeg`
/// happens to be around is worse than a clean "no preview".
fn ffmpeg_dir() -> Option<&'static str> {
    static DIR: OnceLock<Option<String>> = OnceLock::new();
    DIR.get_or_init(|| match std::env::var("ONISYNC_FFMPEG_PATH") {
        Ok(dir) => Some(dir),
        Err(_) => {
            log::warn!(
                "preview: ONISYNC_FFMPEG_PATH is unset; video previews are disabled on this device"
            );
            None
        }
    })
    .as_deref()
}

/// A temp file holding video bytes for ffmpeg, removed on drop.
struct TempVideo {
    path: std::path::PathBuf,
}

impl TempVideo {
    fn create(bytes: &[u8]) -> Option<Self> {
        use std::io::Write;
        // A unique name in the system temp dir; the content isn't sensitive and
        // is short-lived. Include pid + a counter to avoid collisions across
        // concurrent generations.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("tagsy-preview-{}-{n}.video", std::process::id()));
        let mut file = std::fs::File::create(&path).ok()?;
        file.write_all(bytes).ok()?;
        file.flush().ok()?;
        Some(Self { path })
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempVideo {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::Preview;

    use super::super::tests::from_bytes;
    use super::super::{Kind, classify, generate};

    #[test]
    fn mp4_is_classified_as_video() {
        // Minimal MP4 `ftyp` box (isom brand) — enough for `infer`'s magic
        // detection, which is all `classify` relies on.
        let mp4: &[u8] = &[
            0x00, 0x00, 0x00, 0x18, b'f', b't', b'y', b'p', b'i', b's', b'o', b'm', 0x00, 0x00,
            0x02, 0x00, b'i', b's', b'o', b'm', b'i', b's', b'o', b'2',
        ];
        assert!(matches!(classify(mp4, None), Kind::Video));
    }

    #[test]
    fn video_preview_degrades_gracefully_without_ffmpeg() {
        // With ONISYNC_FFMPEG_PATH unset (or ffmpeg unable to decode this stub),
        // video generation must return `Preview::None` rather than panicking.
        // We don't assert a rendered image here because it depends on ffmpeg
        // being available *and* the bytes being a real decodable video; the
        // contract under test is "never crashes".
        let mp4: &[u8] = &[
            0x00, 0x00, 0x00, 0x18, b'f', b't', b'y', b'p', b'i', b's', b'o', b'm', 0x00, 0x00,
            0x02, 0x00, b'i', b's', b'o', b'm', b'i', b's', b'o', b'2',
        ];
        match generate(&from_bytes(mp4), None) {
            Some(Preview::None) => {}
            Some(Preview::Image { .. }) => {} // real ffmpeg somehow decoded it — fine
            other => panic!("unexpected preview kind for video: {other:?}"),
        }
    }
}
