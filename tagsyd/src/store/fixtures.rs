//! Test fixtures shared by every module in `store`.

use tagsy_core::{FileId, TagId};

use super::CatalogStore;

pub(super) fn memory_db() -> CatalogStore {
    CatalogStore::initialize(":memory:").expect("open in-memory db")
}

/// Build a `FileId` from a 32-char hex string so tests can control the exact
/// prefix relationships between ids.
pub(super) fn file_id_from_hex(hex: &str) -> FileId {
    FileId::from_string(hex).expect("valid hex uuid")
}

/// Build a `TagId` from a 32-char hex string so tests can control the exact
/// prefix relationships between ids.
pub(super) fn tag_id_from_hex(hex: &str) -> TagId {
    TagId::from_string(hex).expect("valid hex uuid")
}
