//! Raster-image preview generation.

use std::io::Cursor;

use image::ImageReader;
use tagsy_core::Preview;

use super::{MAX_IMAGE_EDGE, MAX_IMAGE_SOURCE_BYTES};

/// Decode, downscale, and re-encode an image into a tiny PNG preview.
///
/// Returns `None` (→ [`Preview::None`]) if the source is too large to decode
/// safely or if any decode/encode step fails.
pub(super) fn generate_image(bytes: &[u8]) -> Option<Preview> {
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tagsy_core::Preview;

    use super::super::tests::from_bytes;
    use super::super::{MAX_IMAGE_EDGE, generate};

    #[test]
    fn small_png_becomes_image_preview() {
        // Encode a tiny solid image, then round-trip it through the generator.
        let mut source = Vec::new();
        let image = image::RgbImage::from_pixel(200, 120, image::Rgb([10, 20, 30]));
        image::DynamicImage::ImageRgb8(image)
            .write_to(&mut Cursor::new(&mut source), image::ImageFormat::Png)
            .unwrap();

        match generate(&from_bytes(&source), None) {
            Some(Preview::Image {
                bytes,
                width,
                height,
            }) => {
                assert!(!bytes.is_empty());
                assert!(width <= MAX_IMAGE_EDGE && height <= MAX_IMAGE_EDGE);
                // Aspect ratio preserved: wider than tall.
                assert!(width >= height);
            }
            other => panic!("expected image, got {other:?}"),
        }
    }
}
