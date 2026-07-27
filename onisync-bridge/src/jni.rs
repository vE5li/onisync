//! JNI entry points for the Android foreground service (plan section 8).
//!
//! These let `OniSyncService` (Kotlin) drive the process-global runtime
//! ([`crate::service`]) directly, so sync keeps running after the Flutter UI
//! is closed (the service, and thus the process and the Rust runtime thread,
//! stays alive via its ongoing notification).
//!
//! Function names follow the JNI mangling for
//! `com.example.onisync_app.OniSyncService`. If you rename the app package or
//! the service class, these must be renamed to match.

#![cfg(target_os = "android")]

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::jstring;
use onisyncd::paths::Paths;

/// `OniSyncService.nativeStart(dataDir, identityFile, configJson): String?`
///
/// Starts the process-global runtime (idempotent) and returns this device's
/// public key, or `null` on failure (the error is logged to logcat).
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_example_onisync_1app_OniSyncService_nativeStart<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    data_dir: JString<'local>,
    identity_file: JString<'local>,
    config_json: JString<'local>,
) -> jstring {
    let data_dir = match string_arg(&mut env, &data_dir) {
        Some(value) => value,
        None => return std::ptr::null_mut(),
    };
    let identity_file = match string_arg(&mut env, &identity_file) {
        Some(value) => value,
        None => return std::ptr::null_mut(),
    };
    let config_json = match string_arg(&mut env, &config_json) {
        Some(value) => value,
        None => return std::ptr::null_mut(),
    };

    match crate::service::start(&config_json, Paths::new(data_dir, identity_file)) {
        Ok(public_key) => match env.new_string(&public_key) {
            Ok(java_string) => java_string.into_raw(),
            Err(error) => {
                log::error!("nativeStart: failed to build return string: {error}");
                std::ptr::null_mut()
            }
        },
        Err(error) => {
            log::error!("nativeStart: failed to start runtime: {error}");
            std::ptr::null_mut()
        }
    }
}

/// `OniSyncService.nativeStop()`
///
/// Stops the process-global runtime (idempotent). Called from the service
/// `onDestroy`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_example_onisync_1app_OniSyncService_nativeStop<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) {
    crate::service::stop();
}

/// Decode a Java string argument, logging and returning `None` on failure.
fn string_arg(env: &mut JNIEnv<'_>, value: &JString<'_>) -> Option<String> {
    match env.get_string(value) {
        Ok(java_string) => Some(java_string.into()),
        Err(error) => {
            log::error!("JNI: failed to decode string argument: {error}");
            None
        }
    }
}
