//! JNI entry points for the Android foreground service.
//!
//! These let `TagsyService` (Kotlin) drive the process-global runtime
//! ([`crate::service`]) directly, so sync keeps running after the Flutter UI
//! is closed (the service, and thus the process and the Rust runtime thread,
//! stays alive via its ongoing notification).
//!
//! Function names follow the JNI mangling for
//! `com.example.tagsy_app.TagsyService`. If you rename the app package or
//! the service class, these must be renamed to match.

#![cfg(target_os = "android")]

use jni::objects::{JClass, JString};
use jni::sys::jstring;
use jni::{Env, EnvUnowned};
use tagsyd::paths::Paths;

/// `TagsyService.nativeStart(dataDir, identityFile, configJson): String?`
///
/// Starts the process-global runtime (idempotent) and returns this device's
/// public key, or `null` on failure (the error is logged to logcat).
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_example_tagsy_1app_TagsyService_nativeStart<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    data_dir: JString<'local>,
    identity_file: JString<'local>,
    config_json: JString<'local>,
) -> jstring {
    // In jni 0.22 a native method receives an FFI-safe `EnvUnowned`, which
    // lacks the full JNI API. Upgrade it to a real `Env` for the duration of
    // the call via `with_env`; the closure returns a `jni::Result` and the
    // `LogErrorAndDefault` policy logs any error/panic and returns the
    // default (a null `jstring`), preserving the previous "null on failure,
    // logged to logcat" contract.
    env.with_env(|env| -> jni::errors::Result<jstring> {
        let data_dir = string_arg(env, &data_dir)?;
        let identity_file = string_arg(env, &identity_file)?;
        let config_json = string_arg(env, &config_json)?;

        match crate::service::start(&config_json, Paths::new(data_dir, identity_file)) {
            Ok(public_key) => Ok(env.new_string(&public_key)?.into_raw()),
            Err(error) => {
                log::error!("nativeStart: failed to start runtime: {error}");
                Ok(std::ptr::null_mut())
            }
        }
    })
    .resolve::<jni::errors::LogErrorAndDefault>()
}

/// `TagsyService.nativeStop()`
///
/// Stops the process-global runtime (idempotent). Called from the service
/// `onDestroy`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_example_tagsy_1app_TagsyService_nativeStop<'local>(
    _env: EnvUnowned<'local>,
    _class: JClass<'local>,
) {
    crate::service::stop();
}

/// Decode a Java string argument. The error is propagated to the caller's
/// error policy (which logs it) rather than handled here.
fn string_arg(env: &mut Env<'_>, value: &JString<'_>) -> jni::errors::Result<String> {
    value.try_to_string(env)
}
