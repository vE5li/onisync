//! Log routing for embedded frontends.
//!
//! The desktop binary uses `env_logger`, which writes to stderr — there is no
//! stderr on Android. This module installs `android_logger` so the core's
//! `log` output goes to logcat instead. On non-Android targets it is a no-op,
//! so a host-side harness can still link the bridge without a second logger
//! fighting `env_logger`.

/// Initialize log routing appropriately for the current platform.
///
/// Safe to call more than once; only the first call installs a logger. The
/// frontend should call this once at startup (before
/// [`crate::runtime::RuntimeHandle::start`]) so early runtime logs are
/// captured.
pub fn init() {
    #[cfg(target_os = "android")]
    {
        use android_logger::Config;
        android_logger::init_once(
            Config::default()
                .with_max_level(log::LevelFilter::Debug)
                .with_tag("tagsy"),
        );
    }

    // Desktop: the app process has no logger of its own. Install env_logger so
    // client-side IPC diagnostics are visible on stderr. `try_init` is
    // idempotent-safe (returns Err if a logger is already set).
    #[cfg(not(target_os = "android"))]
    {
        let _ = env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .try_init();
    }

    install_panic_hook();
}

/// Route Rust panics through `log::error!` so they show up in logcat.
///
/// A panic in a spawned tokio task unwinds that task silently: the default
/// panic handler writes to stderr, which Android discards, so the only visible
/// symptom is a downstream channel closing. This hook makes the panic message
/// and location visible where every other log goes.
fn install_panic_hook() {
    use std::sync::Once;
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let location = info
                .location()
                .map(|location| location.to_string())
                .unwrap_or_else(|| "unknown location".to_owned());
            let message = info.payload().downcast_ref::<&str>().map_or_else(
                || {
                    info.payload()
                        .downcast_ref::<String>()
                        .map_or("<non-string panic payload>", |string| string.as_str())
                },
                |string| *string,
            );
            log::error!("PANIC at {location}: {message}");
            previous(info);
        }));
    });
}
