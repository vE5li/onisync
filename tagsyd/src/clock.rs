//! Wall-clock access.
//!
//! Its own module because a clock is not persistence: every last-writer-wins
//! decision in the daemon is stamped from here, and keeping that one call in
//! one place is what makes "never restamp a peer's timestamp" auditable.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time as unix milliseconds.
///
/// Used to stamp `modified_at` on locally-originated tag mutations. Peer
/// changes carry their own `modified_at` and must NOT be restamped with this
/// (that would let a receiver's clock override the last-writer-wins
/// comparison).
pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
