//! Local preview generation.
//!
//! Turns a file's raw bytes into a small [`Preview`] — a low-resolution image,
//! a short text snippet, or [`Preview::None`] for anything we can't (or won't)
//! preview. This is the *producer* side of the preview feature; caching,
//! invalidation, and peer fetch live elsewhere (`store/previews.rs`,
//! `preview_fetch.rs`).
//!
//! This module owns the shared plumbing — the byte caps, the source reader, and
//! the [`classify`] type-detector — and dispatches to one backend per kind,
//! each in its own submodule with no shared state:
//!
//! - [`image`] — raster images, via the `image` crate.
//! - [`pdf`] — first-page render, via pdfium.
//! - [`video`] — one frame, via a pinned ffmpeg/ffprobe.
//! - [`text`] — a sanitized UTF-8 snippet.
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
//! `tokio::task::spawn_blocking`; it is a plain synchronous function for
//! exactly that reason. Because it already runs on the blocking pool, it reads
//! its source ([`FileBytes`]) with synchronous `std::fs`, avoiding a second
//! async hop — and, crucially, letting the file-backed video path hand ffmpeg
//! the source's *own* on-disk path instead of copying it through memory into a
//! throwaway temp file.

mod image;
mod pdf;
mod text;
mod video;

use image::generate_image;
use pdf::generate_pdf;
use tagsy_core::Preview;
use text::generate_text;
use video::generate_video;

use crate::file_bytes::FileBytes;

/// Longest edge, in pixels, of a generated image preview. Small on purpose: a
/// preview is a thumbnail hint, not a viewable image.
pub(super) const MAX_IMAGE_EDGE: u32 = 96;

/// Encoded-image preview size is bounded implicitly by [`MAX_IMAGE_EDGE`]; this
/// caps how many *source* bytes we are willing to decode so a hostile or
/// enormous image can't exhaust memory in the blocking task. Images larger than
/// this get no preview rather than risking an OOM.
pub(super) const MAX_IMAGE_SOURCE_BYTES: usize = 32 * 1024 * 1024;

/// Maximum length, in bytes, of a text preview snippet. Truncated on a UTF-8
/// character boundary, so the emitted `String` may be slightly shorter.
pub(super) const MAX_TEXT_BYTES: usize = 256;

/// How many leading bytes of an unknown file we sniff to decide "is this
/// text?". Enough to catch binary content early without reading whole files.
const TEXT_SNIFF_BYTES: usize = 1024;

/// Generate a preview from a file's content, described by `source`.
///
/// `source` is a [`FileBytes`]: either bytes already in memory or a path to the
/// content on disk. Classification and the image/PDF/text generators read the
/// content into a bounded in-memory buffer (they must decode/snippet it), but
/// the *video* generator uses the source's on-disk path directly when it has
/// one — ffmpeg needs a seekable file, and a `FileToCopy`/`FileToMove` already
/// is one, so we avoid copying the (potentially large) video through memory
/// into a temp file. Only an in-memory video source is spilled to a temp file.
///
/// `extension` is the file's lowercase extension (no dot, e.g. `"jpg"`) taken
/// from its *logical* name, when known. It is used as a fallback (and a
/// tie-breaker) for type detection: the bytes on disk are content-addressed and
/// stored without an extension, and magic-byte sniffing occasionally misses
/// (unusual leading bytes, truncated reads), so the extension is a valuable
/// second signal.
///
/// Returns:
/// - `Some(preview)` — an authoritative result. `Some(Preview::None)` is the
///   cacheable "this content has no preview" (un-previewable type, or a decode
///   the generators declined); it is a *result*, not a failure.
/// - `None` — the source could not be read at all (e.g. the file vanished
///   between an earlier presence check and this call). The caller should treat
///   this as "no preview *this time*" and fall back to asking peers, rather
///   than caching a spurious negative.
///
/// The distinction matters: caching a read-race as `Preview::None` would mask a
/// preview a peer (or a later retry) could still produce.
///
/// Synchronous and CPU-bound — invoke via `spawn_blocking`.
pub fn generate(source: &FileBytes, extension: Option<&str>) -> Option<Preview> {
    // A small leading window is enough for magic-byte sniffing and the text
    // heuristic; read only that to classify, so a huge video is not pulled into
    // memory just to decide it is a video.
    let header = match read_source_bounded(source, TEXT_SNIFF_BYTES) {
        Ok(header) => header,
        Err(error) => {
            log::debug!("preview: could not read source header: {error}");
            return None;
        }
    };

    let preview = match classify(&header, extension) {
        // Image and PDF need the whole (bounded) content in memory to decode.
        Kind::Image | Kind::Pdf => {
            let bytes = match read_source_bounded(source, MAX_IMAGE_SOURCE_BYTES) {
                Ok(bytes) => bytes,
                Err(error) => {
                    log::debug!("preview: could not read source content: {error}");
                    return None;
                }
            };
            // Re-classifying on the full buffer only disambiguates image vs.
            // PDF; a larger read cannot turn either into a non-image kind.
            if matches!(classify(&bytes, extension), Kind::Pdf) {
                generate_pdf(&bytes).unwrap_or(Preview::None)
            } else {
                generate_image(&bytes).unwrap_or(Preview::None)
            }
        }
        // Video prefers the source's own on-disk path (no copy); only an
        // in-memory source is spilled to a temp file inside `generate_video`.
        Kind::Video => generate_video(source).unwrap_or(Preview::None),
        // The text snippet only ever needs the leading window we already read.
        Kind::Text => generate_text(&header),
        Kind::Other => Preview::None,
    };

    Some(preview)
}

/// Read up to `max_len` leading bytes of `source` into memory, synchronously.
///
/// In-memory sources are sliced; file-backed sources are read with `std::fs`
/// (this runs on the blocking pool, so blocking I/O is intended). Bounded so a
/// hostile or enormous file cannot exhaust memory.
fn read_source_bounded(source: &FileBytes, max_len: usize) -> std::io::Result<Vec<u8>> {
    use std::io::Read;

    match source.path() {
        // File-backed: read at most `max_len` bytes from the front.
        Some(path) => {
            let file = std::fs::File::open(path)?;
            let mut buffer = Vec::new();
            file.take(max_len as u64).read_to_end(&mut buffer)?;
            Ok(buffer)
        }
        // In-memory: slice the buffer we already hold.
        None => match source {
            FileBytes::InMemory(bytes) => Ok(bytes[..bytes.len().min(max_len)].to_vec()),
            // `path()` is `None` only for `InMemory`.
            _ => unreachable!("FileBytes::path() is None only for InMemory"),
        },
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Image,
    Pdf,
    Video,
    Text,
    Other,
}

/// Decide what kind of preview `bytes` warrant.
///
/// Magic-byte sniffing (`infer`) is tried first; on a hit its verdict wins. If
/// it does not recognize the content, we fall back to the file's `extension`
/// (from its logical name), then to a text heuristic. The extension fallback is
/// what makes previews work for the content-addressed on-disk store (files are
/// named by id, with no extension) when magic detection is inconclusive.
pub(super) fn classify(bytes: &[u8], extension: Option<&str>) -> Kind {
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

    // Magic detection was inconclusive. Try the extension.
    if let Some(kind) = classify_by_extension(extension) {
        return kind;
    }

    // Last resort: treat it as text if a leading window is valid, mostly-
    // printable UTF-8; otherwise give up.
    if looks_like_text(bytes) {
        Kind::Text
    } else {
        Kind::Other
    }
}

/// Map a lowercase file extension to a preview [`Kind`], or `None` for an
/// unknown/absent extension.
fn classify_by_extension(extension: Option<&str>) -> Option<Kind> {
    let extension = extension?;
    let kind = match extension {
        // Raster images the `image` crate can decode.
        "png" | "jpg" | "jpeg" | "gif" | "bmp" | "webp" | "tif" | "tiff" | "ico" => Kind::Image,
        "pdf" => Kind::Pdf,
        // Containers ffmpeg can pull a frame from.
        "mp4" | "m4v" | "mov" | "mkv" | "webm" | "avi" | "wmv" | "flv" | "mpg" | "mpeg" | "3gp"
        | "ogv" => Kind::Video,
        // Common text / code / markup.
        "txt" | "md" | "markdown" | "log" | "json" | "yaml" | "yml" | "toml" | "ini" | "cfg"
        | "conf" | "csv" | "tsv" | "xml" | "html" | "htm" | "css" | "rs" | "py" | "js" | "ts"
        | "tsx" | "jsx" | "c" | "h" | "cpp" | "hpp" | "cc" | "java" | "kt" | "go" | "rb"
        | "php" | "sh" | "bash" | "zsh" | "sql" | "swift" | "dart" | "lua" | "pl" => Kind::Text,
        _ => return None,
    };
    Some(kind)
}

/// Heuristic: is the leading window of `bytes` valid UTF-8 with no NUL bytes
/// and few control characters? Empty input counts as text (an empty snippet).
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Wrap raw bytes as an in-memory [`FileBytes`] source for the tests, which
    /// exercise the byte-decoding paths (image/PDF/text/classification). The
    /// file-backed path differs only in *where* the bytes come from, which is
    /// covered by the read helper. Shared by the backend submodules' tests.
    pub(super) fn from_bytes(bytes: &[u8]) -> FileBytes {
        FileBytes::InMemory(bytes.to_vec())
    }

    #[test]
    fn extension_classifies_when_magic_bytes_are_inconclusive() {
        // Bytes that `infer` does not recognize, but whose extension does.
        let bytes = b"\x01\x02\x03not a known magic\x04\x05";
        assert!(matches!(classify(bytes, Some("mp4")), Kind::Video));
        assert!(matches!(classify(bytes, Some("pdf")), Kind::Pdf));
        assert!(matches!(classify(bytes, Some("jpg")), Kind::Image));
        assert!(matches!(classify(bytes, Some("json")), Kind::Text));
        // Unknown extension + unknown bytes → Other (not text, since NUL-free
        // but not clearly text either; falls through to the text heuristic).
        assert!(matches!(classify(bytes, Some("bin")), Kind::Other));
    }

    #[test]
    fn magic_bytes_win_over_extension() {
        // A real PNG whose extension lies ("txt"); magic detection must win so
        // we still produce an image preview, not a text snippet.
        let mut png = Vec::new();
        ::image::DynamicImage::ImageRgb8(::image::RgbImage::from_pixel(
            10,
            10,
            ::image::Rgb([1, 2, 3]),
        ))
        .write_to(
            &mut std::io::Cursor::new(&mut png),
            ::image::ImageFormat::Png,
        )
        .unwrap();
        assert!(matches!(classify(&png, Some("txt")), Kind::Image));
    }

    #[test]
    fn extension_is_case_insensitive_via_caller() {
        // `classify` expects a lowercase extension; the helper that extracts it
        // lowercases. Verify the table matches lowercase.
        assert!(matches!(
            classify_by_extension(Some("jpeg")),
            Some(Kind::Image)
        ));
        assert!(classify_by_extension(Some("JPEG")).is_none());
        assert!(classify_by_extension(None).is_none());
    }
}
