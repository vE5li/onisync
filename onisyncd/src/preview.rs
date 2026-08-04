//! Local preview generation.
//!
//! Turns a file's raw bytes into a small [`Preview`] — a low-resolution image,
//! a short text snippet, or [`Preview::None`] for anything we can't (or won't)
//! preview. This is the *producer* side of the preview feature; caching,
//! invalidation, and peer fetch live elsewhere (`database.rs`,
//! `preview_fetch.rs`).
//!
//! ## Determinism
//!
//! Previews are keyed by `(file_id, content_hash)` in the cache and on the
//! wire, and every peer runs this same generator. They are deterministic *in
//! kind* (the same bytes always yield an image, or always text, or always
//! none), but the exact encoded bytes of an image preview are **not** required
//! to be identical across peers — image-codec output can differ by library
//! version. Any valid preview of the requested content is acceptable, which is
//! why the peer-fetch layer takes the first responder and drops the rest.
//!
//! ## Blocking
//!
//! Decoding, resizing, and re-encoding an image is CPU-bound and must never run
//! on a Tokio worker thread. Call [`generate`] from inside
//! `tokio::task::spawn_blocking`; it is a plain synchronous function operating
//! on an owned byte buffer for exactly that reason.

use image::ImageReader;
use onisync_core::Preview;
use std::io::Cursor;
use std::sync::OnceLock;

use pdfium_render::prelude::{PdfRenderConfig, Pdfium};

/// Longest edge, in pixels, of a generated image preview. Small on purpose: a
/// preview is a thumbnail hint, not a viewable image.
const MAX_IMAGE_EDGE: u32 = 96;

/// Encoded-image preview size is bounded implicitly by [`MAX_IMAGE_EDGE`]; this
/// caps how many *source* bytes we are willing to decode so a hostile or
/// enormous image can't exhaust memory in the blocking task. Images larger than
/// this get no preview rather than risking an OOM.
const MAX_IMAGE_SOURCE_BYTES: usize = 32 * 1024 * 1024;

/// Maximum length, in bytes, of a text preview snippet. Truncated on a UTF-8
/// character boundary, so the emitted `String` may be slightly shorter.
const MAX_TEXT_BYTES: usize = 256;

/// How many leading bytes of an unknown file we sniff to decide "is this
/// text?". Enough to catch binary content early without reading whole files.
const TEXT_SNIFF_BYTES: usize = 1024;

/// Generate a preview from a file's raw `bytes`.
///
/// Never fails: anything we can't turn into an image or text snippet becomes
/// [`Preview::None`], which is itself a cacheable ("no preview for this
/// content") result. Synchronous and CPU-bound — invoke via `spawn_blocking`.
pub fn generate(bytes: &[u8]) -> Preview {
    match classify(bytes) {
        Kind::Image => generate_image(bytes).unwrap_or(Preview::None),
        Kind::Pdf => generate_pdf(bytes).unwrap_or(Preview::None),
        Kind::Video => generate_video(bytes).unwrap_or(Preview::None),
        Kind::Text => generate_text(bytes),
        Kind::Other => Preview::None,
    }
}

enum Kind {
    Image,
    Pdf,
    Video,
    Text,
    Other,
}

/// Decide what kind of preview `bytes` warrant, sniffing magic bytes (never
/// trusting a filename — we don't have one here anyway).
fn classify(bytes: &[u8]) -> Kind {
    if let Some(kind) = infer::get(bytes) {
        if kind.matcher_type() == infer::MatcherType::Image {
            return Kind::Image;
        }
        if kind.matcher_type() == infer::MatcherType::Video {
            return Kind::Video;
        }
        if kind.mime_type() == "application/pdf" {
            return Kind::Pdf;
        }
        // A recognized non-image, non-video, non-PDF type (archive, other
        // document, ...). We don't preview these yet.
        return Kind::Other;
    }

    // `infer` didn't recognize a magic signature. Treat it as text if a leading
    // window is valid, mostly-printable UTF-8; otherwise give up.
    if looks_like_text(bytes) {
        Kind::Text
    } else {
        Kind::Other
    }
}

/// Heuristic: is the leading window of `bytes` valid UTF-8 with no NUL bytes and
/// few control characters? Empty input counts as text (an empty snippet).
fn looks_like_text(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return true;
    }

    let window = &bytes[..bytes.len().min(TEXT_SNIFF_BYTES)];

    // A NUL byte is the classic binary tell.
    if window.contains(&0) {
        return false;
    }

    // Decode as UTF-8 up to the last complete character in the window (the
    // window may split a multi-byte char at its tail; that's fine).
    let text = match std::str::from_utf8(window) {
        Ok(text) => text,
        Err(error) => match std::str::from_utf8(&window[..error.valid_up_to()]) {
            Ok(text) if error.valid_up_to() > 0 => text,
            _ => return false,
        },
    };

    // Reject if too many control characters (excluding common whitespace).
    let control = text
        .chars()
        .filter(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        .count();
    let total = text.chars().count().max(1);
    (control * 100 / total) < 5
}

/// Decode, downscale, and re-encode an image into a tiny PNG preview.
///
/// Returns `None` (→ [`Preview::None`]) if the source is too large to decode
/// safely or if any decode/encode step fails.
fn generate_image(bytes: &[u8]) -> Option<Preview> {
    if bytes.len() > MAX_IMAGE_SOURCE_BYTES {
        log::debug!(
            "preview: image source {} bytes exceeds cap {MAX_IMAGE_SOURCE_BYTES}; no preview",
            bytes.len()
        );
        return None;
    }

    let reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .ok()?;

    // Decoding the *full* source image to a bitmap is by far the most expensive
    // step for a large photo (a few-MB JPEG can expand to tens of MB of pixels),
    // so time decode, resize, and encode separately.
    let decode_start = std::time::Instant::now();
    let decoded = reader.decode().ok()?;
    let decode_elapsed = decode_start.elapsed();

    // Downscale preserving aspect ratio; `thumbnail` uses a fast filter and
    // never upscales past the requested box.
    let resize_start = std::time::Instant::now();
    let thumbnail = decoded.thumbnail(MAX_IMAGE_EDGE, MAX_IMAGE_EDGE);
    let width = thumbnail.width();
    let height = thumbnail.height();
    let resize_elapsed = resize_start.elapsed();

    let encode_start = std::time::Instant::now();
    let mut encoded = Vec::new();
    thumbnail
        .write_to(&mut Cursor::new(&mut encoded), image::ImageFormat::Png)
        .ok()?;
    let encode_elapsed = encode_start.elapsed();

    log::debug!(
        "preview: image {} src bytes → {width}x{height} thumbnail: decode={:?} resize={:?} \
         encode={:?}",
        bytes.len(),
        decode_elapsed,
        resize_elapsed,
        encode_elapsed
    );

    Some(Preview::Image {
        bytes: encoded,
        width,
        height,
    })
}

/// Render the first page of a PDF to a small PNG preview.
///
/// Returns `None` (→ [`Preview::None`]) if pdfium is unavailable, the document
/// fails to load, or it has no pages. The rendered page is a raster (mostly
/// text/line-art), so PNG is used for the encode — it stays sharp and is
/// typically smaller than JPEG for such content.
///
/// pdfium is bound once, lazily (see [`pdfium`]); it is not thread-safe, so the
/// `thread_safe` crate feature serializes all calls behind a mutex. Preview
/// generation already runs on the blocking pool, so this cost is off the async
/// runtime.
fn generate_pdf(bytes: &[u8]) -> Option<Preview> {
    if bytes.len() > MAX_IMAGE_SOURCE_BYTES {
        log::debug!(
            "preview: PDF source {} bytes exceeds cap {MAX_IMAGE_SOURCE_BYTES}; no preview",
            bytes.len()
        );
        return None;
    }

    let pdfium = pdfium()?;

    let render_start = std::time::Instant::now();
    let document = match pdfium.load_pdf_from_byte_slice(bytes, None) {
        Ok(document) => document,
        Err(error) => {
            log::debug!("preview: failed to load PDF: {error:?}; no preview");
            return None;
        }
    };

    let pages = document.pages();
    let first_page = match pages.first() {
        Ok(page) => page,
        Err(error) => {
            log::debug!("preview: PDF has no first page: {error:?}; no preview");
            return None;
        }
    };

    // Render the first page directly at the thumbnail box, preserving aspect
    // ratio (pdfium fits within the given width/height). Rendering straight to
    // the small size avoids rasterizing a full-resolution page bitmap.
    let config = PdfRenderConfig::new()
        .set_target_width(MAX_IMAGE_EDGE as i32)
        .set_maximum_height(MAX_IMAGE_EDGE as i32);

    let rendered = match first_page.render_with_config(&config) {
        Ok(bitmap) => bitmap,
        Err(error) => {
            log::debug!("preview: failed to render PDF page: {error:?}; no preview");
            return None;
        }
    };
    let render_elapsed = render_start.elapsed();

    let dynamic = match rendered.as_image() {
        Ok(image) => image,
        Err(error) => {
            log::debug!("preview: failed to convert rendered PDF page to image: {error:?}");
            return None;
        }
    };
    let width = dynamic.width();
    let height = dynamic.height();

    let encode_start = std::time::Instant::now();
    let mut encoded = Vec::new();
    dynamic
        .write_to(&mut Cursor::new(&mut encoded), image::ImageFormat::Png)
        .ok()?;
    let encode_elapsed = encode_start.elapsed();

    log::debug!(
        "preview: PDF {} src bytes → {width}x{height} page-1 thumbnail: render={:?} encode={:?}",
        bytes.len(),
        render_elapsed,
        encode_elapsed
    );

    Some(Preview::Image {
        bytes: encoded,
        width,
        height,
    })
}

/// Lazily bind to the pdfium shared library, once for the process.
///
/// Resolution order:
/// 1. `ONISYNC_PDFIUM_LIB_PATH` — a directory containing `libpdfium.so`, set by
///    the packaging wrapper (see `flake.nix`) so the pinned nixpkgs build is
///    used deterministically.
/// 2. the system library (`bind_to_system_library`) as a fallback for dev.
///
/// Returns `None` (logged once) if neither can be bound; PDF previews then
/// degrade to [`Preview::None`] rather than failing the whole preview.
fn pdfium() -> Option<&'static Pdfium> {
    static PDFIUM: OnceLock<Option<Pdfium>> = OnceLock::new();

    PDFIUM
        .get_or_init(|| {
            let bindings = match std::env::var("ONISYNC_PDFIUM_LIB_PATH") {
                Ok(dir) => Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(
                    &dir,
                ))
                .or_else(|error| {
                    log::warn!(
                        "preview: ONISYNC_PDFIUM_LIB_PATH set but binding failed ({error:?}); \
                         trying system library"
                    );
                    Pdfium::bind_to_system_library()
                }),
                Err(_) => Pdfium::bind_to_system_library(),
            };

            match bindings {
                Ok(bindings) => Some(Pdfium::new(bindings)),
                Err(error) => {
                    log::warn!(
                        "preview: could not bind to a pdfium library ({error:?}); PDF previews \
                         are disabled on this device"
                    );
                    None
                }
            }
        })
        .as_ref()
}

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
/// Runs synchronously (already on the blocking pool). The video bytes are
/// written to a temp file first: seeking needs the container, which is awkward
/// to stream over stdin.
fn generate_video(bytes: &[u8]) -> Option<Preview> {
    use std::process::Command;

    let dir = ffmpeg_dir()?;
    let ffmpeg = std::path::Path::new(dir).join("ffmpeg");
    let ffprobe = std::path::Path::new(dir).join("ffprobe");

    // Write the source to a temp file (uniquely named; cleaned up on drop).
    let temp = TempVideo::create(bytes)?;
    let input = temp.path();

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
        "preview: video {} src bytes → {width}x{height} frame @ {seek_seconds:.1}s in {:?}",
        bytes.len(),
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
        let path = std::env::temp_dir().join(format!(
            "onisync-preview-{}-{n}.video",
            std::process::id()
        ));
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

/// Build a short, sanitized text snippet from the start of `bytes`.
fn generate_text(bytes: &[u8]) -> Preview {
    let window = &bytes[..bytes.len().min(MAX_TEXT_BYTES)];

    // Decode the largest valid UTF-8 prefix (the window may end mid-character).
    let text = match std::str::from_utf8(window) {
        Ok(text) => text,
        Err(error) => std::str::from_utf8(&window[..error.valid_up_to()]).unwrap_or(""),
    };

    // Drop NULs / stray control chars but keep newlines and tabs so multi-line
    // text previews render sensibly.
    let sanitized: String = text
        .chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\r' | '\t'))
        .collect();

    Preview::Text(sanitized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_becomes_text_preview() {
        let preview = generate(b"hello world\nsecond line");
        match preview {
            Preview::Text(text) => {
                assert!(text.starts_with("hello world"));
                assert!(text.contains('\n'));
            }
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[test]
    fn text_is_truncated_on_char_boundary() {
        // A long multi-byte string; ensure we never panic and stay within the
        // byte cap.
        let source = "é".repeat(1000);
        let preview = generate(source.as_bytes());
        match preview {
            Preview::Text(text) => assert!(text.len() <= MAX_TEXT_BYTES),
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[test]
    fn binary_with_nul_has_no_preview() {
        let bytes = [0u8, 1, 2, 3, 255, 254, 0, 42];
        assert_eq!(generate(&bytes), Preview::None);
    }

    #[test]
    fn empty_input_is_empty_text() {
        assert_eq!(generate(b""), Preview::Text(String::new()));
    }

    #[test]
    fn pdf_is_classified_as_pdf() {
        // Minimal but valid-enough PDF header; classification is by magic bytes.
        let pdf = b"%PDF-1.4\n1 0 obj<<>>endobj\n";
        assert!(matches!(classify(pdf), Kind::Pdf));
    }

    #[test]
    fn pdf_preview_renders_or_degrades_gracefully() {
        // A tiny one-page PDF. If pdfium is bound (ONISYNC_PDFIUM_LIB_PATH set,
        // as in the dev shell), we expect an image preview; otherwise generation
        // degrades to `Preview::None` rather than panicking. Either outcome is
        // acceptable here — the point is that the PDF path never crashes.
        const ONE_PAGE_PDF: &[u8] = b"%PDF-1.1\n\
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n\
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n\
3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]>>endobj\n\
trailer<</Root 1 0 R>>\n%%EOF";

        match generate(ONE_PAGE_PDF) {
            Preview::Image {
                bytes,
                width,
                height,
            } => {
                assert!(!bytes.is_empty());
                assert!(width <= MAX_IMAGE_EDGE && height <= MAX_IMAGE_EDGE);
            }
            // pdfium unavailable, or this minimal PDF was rejected by the
            // parser — both fine; we only require no panic.
            Preview::None => {}
            other => panic!("unexpected preview kind for PDF: {other:?}"),
        }
    }

    #[test]
    fn mp4_is_classified_as_video() {
        // Minimal MP4 `ftyp` box (isom brand) — enough for `infer`'s magic
        // detection, which is all `classify` relies on.
        let mp4: &[u8] = &[
            0x00, 0x00, 0x00, 0x18, b'f', b't', b'y', b'p', b'i', b's', b'o', b'm', 0x00, 0x00,
            0x02, 0x00, b'i', b's', b'o', b'm', b'i', b's', b'o', b'2',
        ];
        assert!(matches!(classify(mp4), Kind::Video));
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
        match generate(mp4) {
            Preview::None => {}
            Preview::Image { .. } => {} // real ffmpeg somehow decoded it — fine
            other => panic!("unexpected preview kind for video: {other:?}"),
        }
    }

    #[test]
    fn small_png_becomes_image_preview() {
        // Encode a tiny solid image, then round-trip it through the generator.
        let mut source = Vec::new();
        let image = image::RgbImage::from_pixel(200, 120, image::Rgb([10, 20, 30]));
        image::DynamicImage::ImageRgb8(image)
            .write_to(&mut Cursor::new(&mut source), image::ImageFormat::Png)
            .unwrap();

        match generate(&source) {
            Preview::Image {
                bytes,
                width,
                height,
            } => {
                assert!(!bytes.is_empty());
                assert!(width <= MAX_IMAGE_EDGE && height <= MAX_IMAGE_EDGE);
                // Aspect ratio preserved: wider than tall.
                assert!(width >= height);
            }
            other => panic!("expected image, got {other:?}"),
        }
    }
}
