//! The catalog: the authoritative index of what exists, and the sole writer
//! to the main database.
//!
//! Today this holds only the pure placement-decision logic pulled out of
//! `handle_changes` in `lib.rs`; the actor itself (`Catalog`, `handle_changes`
//! and its message types) moves here in a later phase.

pub mod placement;
pub mod previews;
