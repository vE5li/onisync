package com.example.tagsy_app

import android.app.Activity
import android.content.ActivityNotFoundException
import android.content.Intent
import android.net.Uri
import android.webkit.MimeTypeMap
import androidx.core.content.FileProvider
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.plugin.common.MethodChannel
import java.io.File

/**
 * Bridge for the Flutter [AndroidEditorLauncher].
 *
 * Kotlin side of the "open a file in an external editor and tell me when the
 * user comes back" contract. Owns two responsibilities:
 *
 *  * `launch` — build a `content://` URI for the file (via our FileProvider,
 *    so external apps can actually read it — raw filesystem paths under
 *    `filesDir` are not exposed on modern Android) and fire an
 *    `ACTION_EDIT` intent (falling back to `ACTION_VIEW`). MIME is sniffed
 *    from the file's logical name extension.
 *
 *  * `editorReturned` — invoked from [MainActivity.onResume] on the *first*
 *    resume after a successful `launch`, telling Flutter the user came back.
 *    That is the strongest "editing finished" signal available on Android:
 *    `ACTION_EDIT` targets do not reliably return a result via
 *    `startActivityForResult`. If the user came back for another reason
 *    (task switcher, notification) the follow-up `finishEdit` will
 *    hash the bytes and no-op — correctness is preserved.
 *
 * The FileProvider authority is declared in the manifest as
 * `${applicationId}.editorprovider` and its exposed paths in
 * `res/xml/tagsy_editor_paths.xml`.
 */
class EditorChannel(private val activity: Activity) {
    private var channel: MethodChannel? = null

    /** True while a launched editor has not yet triggered an onResume. */
    private var pendingResume: Boolean = false

    fun register(engine: FlutterEngine) {
        val channel = MethodChannel(engine.dartExecutor.binaryMessenger, CHANNEL)
        this.channel = channel
        channel.setMethodCallHandler { call, result ->
            when (call.method) {
                "launch" -> handleLaunch(call.arguments as Map<*, *>, result)
                else -> result.notImplemented()
            }
        }
    }

    /**
     * Called by [MainActivity.onResume]. If a launch is in flight, tell
     * Flutter the user returned and clear the pending flag so subsequent
     * resumes (task switcher, notifications) do not fire spurious returns.
     */
    fun onActivityResumed() {
        if (!pendingResume) return
        pendingResume = false
        channel?.invokeMethod("editorReturned", null)
    }

    private fun handleLaunch(args: Map<*, *>, result: MethodChannel.Result) {
        val path = args["path"] as? String
        val logicalName = args["logicalName"] as? String
        if (path == null || logicalName == null) {
            result.error("bad_args", "path and logicalName are required", null)
            return
        }

        val file = File(path)
        if (!file.exists()) {
            result.error("not_found", "file does not exist: $path", null)
            return
        }

        // Build the FileProvider URI. The authority matches the manifest
        // `<provider>` and the exposed paths under `res/xml/tagsy_editor_paths.xml`
        // must cover wherever `path` lives (currently: the daemon's fetch
        // temp dir under `filesDir`, and — for Branch A — the sync directory
        // under external storage).
        val authority = "${activity.packageName}.editorprovider"
        val uri: Uri = try {
            FileProvider.getUriForFile(activity, authority, file)
        } catch (error: IllegalArgumentException) {
            result.error(
                "unsupported_path",
                "FileProvider cannot serve $path: ${error.message}",
                null,
            )
            return
        }

        // MIME sniff from the file's *logical* extension (the on-disk name
        // matches, since the daemon materializes fetches as
        // <uuid>/<logical_basename>). Falls back to */*  so at least
        // something gets offered even for extensionless files.
        val ext = logicalName.substringAfterLast('.', missingDelimiterValue = "")
            .lowercase()
        val mime = MimeTypeMap.getSingleton().getMimeTypeFromExtension(ext)
            ?: "*/*"

        // Try ACTION_EDIT first (proper "the user should modify this file"
        // semantic; the target sees the URI as read+write). Fall back to
        // ACTION_VIEW so files whose only handler declares `VIEW` still
        // launch — the user will just have a viewer instead of an editor,
        // and our post-hoc hash check turns that into a no-op.
        val flags = Intent.FLAG_GRANT_READ_URI_PERMISSION or
            Intent.FLAG_GRANT_WRITE_URI_PERMISSION

        val edit = Intent(Intent.ACTION_EDIT).apply {
            setDataAndType(uri, mime)
            addFlags(flags)
        }
        val view = Intent(Intent.ACTION_VIEW).apply {
            setDataAndType(uri, mime)
            addFlags(flags)
        }

        val chosen: Intent =
            if (edit.resolveActivity(activity.packageManager) != null) edit
            else if (view.resolveActivity(activity.packageManager) != null) view
            else {
                result.success(false)
                return
            }

        // Wrap in a chooser so the user sees the picker even if a default
        // has been set. That gives them a chance to pick a real editor when
        // ACTION_VIEW is what resolved.
        val chooser = Intent.createChooser(chosen, null)

        try {
            pendingResume = true
            activity.startActivity(chooser)
            result.success(true)
        } catch (error: ActivityNotFoundException) {
            pendingResume = false
            result.success(false)
        } catch (error: SecurityException) {
            pendingResume = false
            result.error("security", error.message ?: "security error", null)
        }
    }

    companion object {
        private const val CHANNEL = "tagsy_app/editor"
    }
}
