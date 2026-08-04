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
        Kind::Text => generate_text(bytes),
        Kind::Other => Preview::None,
    }
}

enum Kind {
    Image,
    Text,
    Other,
}

/// Decide what kind of preview `bytes` warrant, sniffing magic bytes (never
/// trusting a filename — we don't have one here anyway).
fn classify(bytes: &[u8]) -> Kind {
    if let Some(kind) = infer::get(bytes) {
        return if kind.matcher_type() == infer::MatcherType::Image {
            Kind::Image
        } else {
            // A recognized non-image type (archive, video, document, ...). We
            // don't preview these yet.
            Kind::Other
        };
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
