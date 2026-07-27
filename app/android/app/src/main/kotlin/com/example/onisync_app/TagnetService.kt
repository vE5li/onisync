/*
 * Foreground service hosting the onisync native runtime (portability plan
 * section 8).
 *
 * This is what keeps sync running after the Flutter UI is closed. The activity
 * and this service share one process; when the app is swiped away the Flutter
 * engine is destroyed but this foreground service (and its ongoing
 * notification) keeps the PROCESS alive, so the Rust runtime thread it started
 * over JNI keeps running.
 *
 * Ownership: this service owns the process-global runtime (crate::service via
 * the nativeStart/nativeStop JNI entry points). The Dart UI, when open, merely
 * attaches to the same global for reads/events.
 */

package com.example.onisync_app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Build
import android.os.IBinder
import android.util.Log

class OniSyncService : Service() {

    companion object {
        private const val CHANNEL_ID = "onisync_sync"
        private const val NOTIFICATION_ID = 1
        private const val TAG = "onisync"

        init {
            // Loads libonisync_bridge.so (bundled in jniLibs/<abi>/), which
            // exposes the nativeStart/nativeStop symbols below.
            System.loadLibrary("onisync_bridge")
        }
    }

    /** Start the process-global runtime; returns this device's public key, or null on failure. */
    private external fun nativeStart(
        dataDir: String,
        identityFile: String,
        configJson: String,
    ): String?

    /** Stop the process-global runtime. */
    private external fun nativeStop()

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // Promote to foreground immediately so the process (and the native
        // tokio runtime + inotify) survives the UI closing and Doze.
        startForeground(NOTIFICATION_ID, buildNotification())

        // Runtime inputs (config JSON + data / identity paths) live in
        // OniSyncConfig, the single source of truth on Android. The Dart side
        // reads the same values via a MethodChannel (see MainActivity) so the
        // literal is never duplicated.
        val inputs = OniSyncConfig.build(this)

        val publicKey = nativeStart(inputs.dataDir, inputs.identityFile, inputs.configJson)
        if (publicKey != null) {
            Log.i(TAG, "OniSyncService started runtime; device public key $publicKey")
        } else {
            Log.e(TAG, "OniSyncService: nativeStart failed")
        }

        // START_STICKY: if the OS kills us under memory pressure, restart so
        // sync resumes (the runtime re-scans + reconciles on reconnect).
        return START_STICKY
    }

    override fun onDestroy() {
        nativeStop()
        super.onDestroy()
    }

    // Not a bound service.
    override fun onBind(intent: Intent?): IBinder? = null

    private fun buildNotification(): Notification {
        val builder = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Notification.Builder(this, CHANNEL_ID)
        } else {
            @Suppress("DEPRECATION")
            Notification.Builder(this)
        }
        return builder
            .setContentTitle("onisync")
            .setContentText("Syncing in the background")
            .setOngoing(true)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .build()
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "onisync sync",
                NotificationManager.IMPORTANCE_LOW,
            )
            val manager = getSystemService(NotificationManager::class.java)
            manager.createNotificationChannel(channel)
        }
    }
}
