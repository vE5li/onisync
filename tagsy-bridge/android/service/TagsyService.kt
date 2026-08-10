/*
 * Foreground service scaffolding for the tagsy Android app
 * (portability plan section 8).
 *
 * This is a STUB to drop into the Flutter app's
 * android/app/src/main/kotlin/<package>/ directory once `flutter create` has
 * generated the app tree. It shows the intended lifecycle wiring:
 *
 *   - startForeground() with an ongoing notification so the OS does not freeze
 *     the tokio runtime / inotify under Doze (plan section 8),
 *   - the native runtime is owned by the Rust bridge (tagsy-bridge crate):
 *     TagsyApp.start(...) on service start, TagsyApp.shutdown() on destroy.
 *
 * The actual TagsyApp handle lives on the Dart side (via flutter_rust_bridge);
 * this service only guarantees the process stays alive. Depending on the app's
 * architecture you either:
 *   (a) keep the TagsyApp handle in Dart and let this service just hold the
 *       foreground notification (simplest), or
 *   (b) call the generated bindings from a background FlutterEngine here.
 * Option (a) is the minimal path and what the plan assumes.
 *
 * package + real notification channel setup are intentionally omitted; fill
 * them in against the generated app.
 */

// package com.example.tagsy

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.Build
import android.os.IBinder

class TagsyService : Service() {

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // Promote to a foreground service immediately so the process (and the
        // native tokio runtime + inotify watches) survives Doze.
        startForeground(NOTIFICATION_ID, buildNotification())

        // START_STICKY: if the OS kills us under memory pressure, restart the
        // service (the plan's lifecycle re-scan on resume then reconciles any
        // changes missed while frozen).
        return START_STICKY
    }

    override fun onDestroy() {
        // The Rust runtime (TagsyApp) is torn down from Dart; this service
        // just relinquishes the foreground notification.
        super.onDestroy()
    }

    // This service is not bound; it runs standalone.
    override fun onBind(intent: Intent?): IBinder? = null

    private fun buildNotification(): Notification {
        val builder = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Notification.Builder(this, CHANNEL_ID)
        } else {
            @Suppress("DEPRECATION")
            Notification.Builder(this)
        }
        return builder
            .setContentTitle("tagsy")
            .setContentText("Syncing in the background")
            .setOngoing(true)
            // .setSmallIcon(R.drawable.ic_notification) // supply against the app
            .build()
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "tagsy sync",
                NotificationManager.IMPORTANCE_LOW,
            )
            val manager = getSystemService(NotificationManager::class.java)
            manager.createNotificationChannel(channel)
        }
    }

    companion object {
        private const val CHANNEL_ID = "tagsy_sync"
        private const val NOTIFICATION_ID = 1
    }
}
