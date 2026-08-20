//! Text-snippet preview generation.

use tagsy_core::Preview;

use super::MAX_TEXT_BYTES;

/// Build a short, sanitized text snippet from the start of `bytes`.
pub(super) fn generate_text(bytes: &[u8]) -> Preview {
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
    use tagsy_core::Preview;

    use super::super::tests::from_bytes;
    use super::super::{MAX_TEXT_BYTES, generate};

    #[test]
    fn plain_text_becomes_text_preview() {
        let preview = generate(&from_bytes(b"hello world\nsecond line"), None);
        match preview {
            Some(Preview::Text(text)) => {
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
        let preview = generate(&from_bytes(source.as_bytes()), None);
        match preview {
            Some(Preview::Text(text)) => assert!(text.len() <= MAX_TEXT_BYTES),
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[test]
    fn binary_with_nul_has_no_preview() {
        let bytes = [0u8, 1, 2, 3, 255, 254, 0, 42];
        assert_eq!(generate(&from_bytes(&bytes), None), Some(Preview::None));
    }

    #[test]
    fn empty_input_is_empty_text() {
        assert_eq!(
            generate(&from_bytes(b""), None),
            Some(Preview::Text(String::new()))
        );
    }
}
