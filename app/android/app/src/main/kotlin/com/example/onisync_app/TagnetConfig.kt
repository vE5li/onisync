/*
 * Single source of truth for the onisync engine's startup inputs on Android.
 *
 * The engine needs three strings at start-up: a JSON configuration, a data
 * directory (app-private), and an identity-file path (also app-private).
 *
 * The JSON is bundled into the APK as an Android asset at
 * `assets/onisync_config.json`. The file is *not* source: it is copied in at
 * build time by the flake's `run-android` apps from `app/config/<name>.json`,
 * selected by the `ONISYNC_CONFIG` env var — that is how one repository can
 * flash two devices with different peer configs. Editing peer identities
 * therefore means editing files under `app/config/`, not this class.
 *
 * A trivial `$DOCUMENTS` placeholder in the JSON is substituted for the
 * device's public Documents directory (resolved via [Environment] at runtime),
 * so the config file itself contains no device-specific paths.
 *
 * Both callers use [OniSyncConfig.build]:
 *   * [OniSyncService] (Kotlin) uses it to feed nativeStart.
 *   * [MainActivity] exposes the same values to Dart over a MethodChannel
 *     (see [CHANNEL_NAME]); AndroidBootstrap.connect() reads them from there
 *     and passes them to OniSyncApp.start (which is idempotent — since the
 *     service has already started the runtime, the Dart-side JSON is just an
 *     assertion that it wants the same configuration).
 */

package com.example.onisync_app

import android.content.Context
import android.os.Environment
import java.io.FileNotFoundException

/** Everything the native runtime needs to start on this device. */
data class OniSyncStartupInputs(
    val configJson: String,
    val dataDir: String,
    val identityFile: String,
)

object OniSyncConfig {
    /** MethodChannel name Dart uses to fetch these inputs. */
    const val CHANNEL_NAME = "onisync_app/config"

    /** Method on [CHANNEL_NAME] that returns a Map of the fields above. */
    const val METHOD_GET_STARTUP_INPUTS = "getStartupInputs"

    /** Path of the bundled config asset inside the APK's `assets/` tree. */
    private const val CONFIG_ASSET = "onisync_config.json"

    /**
     * Placeholder in the bundled JSON replaced with the device's public
     * Documents directory at runtime. Keeps device-specific paths out of the
     * checked-in config files (they only exist on-device).
     */
    private const val DOCUMENTS_PLACEHOLDER = "\$DOCUMENTS"

    /**
     * Build the runtime's startup inputs for this device.
     *
     * `context` is used to resolve app-private storage (`filesDir`) and to
     * open the bundled config asset. Throws if the asset is missing — that
     * means the APK was built without a `ONISYNC_CONFIG` selection (the flake
     * refuses to build in that case, so seeing this at runtime means someone
     * built the APK by hand).
     */
    fun build(context: Context): OniSyncStartupInputs {
        // App-private storage: inotify works here with no storage permission.
        // Identity + per-directory DBs live here (not user-browsable).
        val dataDir = context.filesDir.absolutePath
        val identityFile = "$dataDir/identity.key"

        // Shared external storage: Documents/onisync is browsable in the Files
        // app / Gallery and survives uninstall. Writing here needs "All files
        // access" (MANAGE_EXTERNAL_STORAGE), which MainActivity gates on
        // before starting the service, so create_dir_all succeeds. Watcher
        // (inotify) reliability on shared storage varies by device (POC caveat).
        val documents =
            Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOCUMENTS)

        // Bundled per-device config. The Rust `Configuration` type
        // (onisyncd/src/configuration.rs) parses this JSON; the schema and
        // valid values are documented there.
        val template = try {
            context.assets.open(CONFIG_ASSET).bufferedReader().use { it.readText() }
        } catch (e: FileNotFoundException) {
            throw IllegalStateException(
                "Bundled onisync config asset '$CONFIG_ASSET' is missing. This APK " +
                    "was built without ONISYNC_CONFIG set; rebuild with " +
                    "ONISYNC_CONFIG=<name> nix run .#run-android (see app/config/).",
                e,
            )
        }
        val configJson = template.replace(DOCUMENTS_PLACEHOLDER, documents.absolutePath)

        return OniSyncStartupInputs(
            configJson = configJson,
            dataDir = dataDir,
            identityFile = identityFile,
        )
    }
}
