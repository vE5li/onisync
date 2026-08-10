//! Peer-facing logic: reconciling a peer's manifests against our local
//! state.
//!
//! Today this holds only the pure planning functions pulled out of `lib.rs`;
//! the session machinery that calls them (`run_peer_session`,
//! `handle_connection`, `connect_to_peer`, …) still lives there and moves
//! here in a later phase.

pub mod plan;
pub mod plan_tags;
