//! Native bridge library for embedded frontends.
//!
//! This crate is the `cdylib` the Android app (and, later, a single-process
//! desktop build) loads. It links the [`onisync`] core as an `rlib` and
//! re-exposes the UI-facing API to Dart via `flutter_rust_bridge`.
//!
//! It adds no business logic. Its whole job is lifecycle + transport glue:
//!
//! - [`runtime`] — build the tokio runtime by hand on a dedicated thread (never
//!   `#[tokio::main]`) and manage its start/stop lifecycle
//!   ([`RuntimeHandle`](runtime::RuntimeHandle)).
//! - [`service`] — a process-global runtime owned by the Android foreground
//!   service, so sync survives the UI closing.
//! - [`logging`] — route the core's `log` output to logcat on Android.
//! - [`api`] — the Dart-facing facade ([`OniSyncApp`](api::OniSyncApp)) that
//!   `flutter_rust_bridge` generates bindings for.
//! - `jni` (Android only) — entry points the Kotlin service calls to start/stop
//!   the process-global runtime.
//!
//! ## Generated bindings
//!
//! Enabling the `flutter_rust_bridge` feature turns on the `#[frb(...)]`
//! annotations the codegen consumes. Regenerate the Dart/Rust glue with:
//!
//! ```text
//! flutter_rust_bridge_codegen generate
//! ```
//!
//! (configured by `flutter_rust_bridge.yaml` once the Flutter app tree exists).
//! Without the feature the crate still builds as a plain Rust library, so
//! `cargo check` works with no Flutter toolchain present.

pub mod api;
pub mod logging;
pub mod runtime;
pub mod service;

#[cfg(target_os = "android")]
pub mod jni;

// The `flutter_rust_bridge_codegen` tool writes `frb_generated.rs` next to this
// file and expects it to be a module. It does not exist until codegen has run,
// so it is gated behind the `generated` feature (turned on by
// flutter_rust_bridge.yaml's `rust_features`). This keeps a plain
// `cargo check` — and even `--features flutter_rust_bridge` before the first
// codegen run — compiling.
#[cfg(feature = "generated")]
mod frb_generated;
