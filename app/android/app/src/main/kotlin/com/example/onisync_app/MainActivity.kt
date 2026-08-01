package com.example.onisync_app

import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.provider.Settings
import android.util.Log
import io.flutter.embedding.android.FlutterActivity
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.plugin.common.MethodChannel

class MainActivity : FlutterActivity() {

    companion object {
        private const val TAG = "onisync"
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        maybeStartRuntime()
    }

    /**
     * Expose the Kotlin-side [OniSyncConfig] to Dart via a MethodChannel.
     *
     * The Dart bootstrap (android_bootstrap.dart) calls
     * `OniSyncConfig.CHANNEL_NAME` / `getStartupInputs` to fetch the same config
     * JSON, data dir, and identity-file path this activity's companion
     * [OniSyncService] uses for nativeStart. Keeping the literal on the Kotlin
     * side means there is exactly one copy in the source tree.
     */
    override fun configureFlutterEngine(flutterEngine: FlutterEngine) {
        super.configureFlutterEngine(flutterEngine)

        MethodChannel(flutterEngine.dartExecutor.binaryMessenger, OniSyncConfig.CHANNEL_NAME)
            .setMethodCallHandler { call, result ->
                when (call.method) {
                    OniSyncConfig.METHOD_GET_STARTUP_INPUTS -> {
                        val inputs = OniSyncConfig.build(this)
                        result.success(
                            mapOf(
                                "configJson" to inputs.configJson,
                                "dataDir" to inputs.dataDir,
                                "identityFile" to inputs.identityFile,
                            )
                        )
                    }
                    OniSyncConfig.METHOD_GET_DOWNLOADS_DIR -> {
                        result.success(OniSyncConfig.downloadsDir())
                    }
                    else -> result.notImplemented()
                }
            }
    }

    // The user may grant "All files access" in Settings and return here; re-check
    // and start the runtime on resume so we don't require an app restart.
    override fun onResume() {
        super.onResume()
        maybeStartRuntime()
    }

    /**
     * Start the foreground service that owns the native runtime, but only once
     * we can actually write the sync directory in shared storage.
     *
     * The sync directory lives at Documents/onisync (shared external storage), so
     * the engine's create_dir_all needs "All files access". If we started the
     * service without it, the engine would fail to create the directory and
     * silently drop it (directory_manager.rs filter_map). So: gate here.
     *
     * Starting the service is idempotent (the process-global runtime is created
     * once, crate::service::start), so calling this repeatedly is safe.
     */
    private fun maybeStartRuntime() {
        if (!hasAllFilesAccess()) {
            requestAllFilesAccess()
            return
        }

        val intent = Intent(this, OniSyncService::class.java)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(intent)
        } else {
            startService(intent)
        }
    }

    private fun hasAllFilesAccess(): Boolean {
        // MANAGE_EXTERNAL_STORAGE exists on R+ (API 30). On older versions the
        // legacy WRITE_EXTERNAL_STORAGE model applies and shared storage is
        // writable without this gate, so treat it as granted.
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            Environment.isExternalStorageManager()
        } else {
            true
        }
    }

    private fun requestAllFilesAccess() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return
        Log.i(TAG, "Requesting All files access so the sync directory is browsable")
        try {
            // Deep-link straight to this app's toggle.
            val intent = Intent(
                Settings.ACTION_MANAGE_APP_ALL_FILES_ACCESS_PERMISSION,
                Uri.parse("package:$packageName"),
            )
            startActivity(intent)
        } catch (error: Exception) {
            // Some OEMs don't support the per-app deep link; fall back to the
            // full list of apps requesting all-files access.
            Log.w(TAG, "Per-app all-files settings unavailable, opening list: $error")
            startActivity(Intent(Settings.ACTION_MANAGE_ALL_FILES_ACCESS_PERMISSION))
        }
    }
}
