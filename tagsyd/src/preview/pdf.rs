//! PDF first-page preview generation, via pdfium.

use std::io::Cursor;
use std::sync::OnceLock;

use pdfium_render::prelude::{PdfRenderConfig, Pdfium};
use tagsy_core::Preview;

use super::{MAX_IMAGE_EDGE, MAX_IMAGE_SOURCE_BYTES};

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
pub(super) fn generate_pdf(bytes: &[u8]) -> Option<Preview> {
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
                Ok(dir) => {
                    Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(&dir))
                        .or_else(|error| {
                            log::warn!(
                                "preview: ONISYNC_PDFIUM_LIB_PATH set but binding failed \
                                 ({error:?}); trying system library"
                            );
                            Pdfium::bind_to_system_library()
                        })
                }
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

#[cfg(test)]
mod tests {
    use tagsy_core::Preview;

    use super::super::tests::from_bytes;
    use super::super::{Kind, MAX_IMAGE_EDGE, classify, generate};

    #[test]
    fn pdf_is_classified_as_pdf() {
        // Minimal but valid-enough PDF header; classification is by magic bytes.
        let pdf = b"%PDF-1.4\n1 0 obj<<>>endobj\n";
        assert!(matches!(classify(pdf, None), Kind::Pdf));
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

        match generate(&from_bytes(ONE_PAGE_PDF), None) {
            Some(Preview::Image {
                bytes,
                width,
                height,
            }) => {
                assert!(!bytes.is_empty());
                assert!(width <= MAX_IMAGE_EDGE && height <= MAX_IMAGE_EDGE);
            }
            // pdfium unavailable, or this minimal PDF was rejected by the
            // parser — both fine; we only require no panic.
            Some(Preview::None) => {}
            other => panic!("unexpected preview kind for PDF: {other:?}"),
        }
    }
}
