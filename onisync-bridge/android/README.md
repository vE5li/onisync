# Building the onisync POC for a phone

The Rust side is complete: `onisync-bridge` is a `cdylib` that boots the sync
engine on a dedicated thread, generates the device identity on first launch,
and exposes the API to Dart. The Flutter app tree (`app/`) is tracked in git —
including the Dart sources under `app/lib/` (only `app/lib/rust/` is generated)
and the hand-merged Android glue — so a fresh clone already has it; nothing
needs to be scaffolded. The Flutter toolchain + Android SDK are provided by the
dev shell (`flake.nix`), so everything below runs inside `nix develop`.

The Android build uses a hardcoded config (outbound-only, no peers, one
app-private synced dir). The Dart sources live in `app/lib/` (tracked; only
`app/lib/rust/` is generated); the Android backend is in
`app/lib/bootstrap/android_bootstrap.dart`, selected at build time via
`--dart-define=ONISYNC_BACKEND=android`.

## The short way: `nix run`

Each README step is wrapped as a flake app (see `flake.nix`). All of them assume
you run from the repo root (override with `ONISYNC_ROOT`), and pull in the Flutter
toolchain + Android SDK/NDK automatically.

```sh
nix run .#run-android    # codegen -> build-native-android -> launch
```

`run-android` is the safe default: it regenerates the bindings, rebuilds the
native `.so`, then launches. For a tight edit-Dart/re-run loop, `launch-android`
skips those rebuild steps and just runs `flutter run` — use it only when codegen
and the `.so` are already up to date (i.e. you didn't touch the Rust API).

Or run the steps individually:

| App                  | Does                                                       |
| -------------------- | ---------------------------------------------------------- |
| `nix run .#codegen`              | `flutter_rust_bridge_codegen generate`.        |
| `nix run .#build-native-android` | cross-compile the `.so`(s) into `app/.../jniLibs`. |
| `nix run .#launch-android`       | `flutter run --release` on a connected device (no rebuild). |
| `nix run .#run-android`          | all of the above, in order.                    |

Native ABIs default to `arm64-v8a`; override with e.g.
`ONISYNC_ANDROID_ABIS="arm64-v8a x86_64" nix run .#build-native-android`.

### One-time prerequisites

- Accept the Android licenses: `nix develop -c flutter doctor --android-licenses`.
- Plug in a USB-debugging phone (or start an emulator); confirm with
  `nix develop -c flutter devices`.

You should see `onisync running — 0 tag(s)`. Logs go to logcat under the tag
`onisync` (`adb logcat -s onisync`).

## Re-scaffolding the app tree (rarely needed)

`app/` is tracked, so you normally never regenerate it. If you ever need to
re-scaffold from scratch (e.g. after a major Flutter upgrade), the tree was
originally created inside `nix develop` with:

- `flutter create --platforms=android,linux --project-name onisync_app app`,
- `(cd app && flutter pub add path_provider flutter_rust_bridge 'receive_sharing_intent:>=1.8.1 <1.9.0')`,
- merge the Android glue:
  - merge `manifest/AndroidManifest.xml` (permissions + `<service>`) into
    `app/android/app/src/main/AndroidManifest.xml`,
  - copy `service/OniSyncService.kt` into
    `app/android/app/src/main/kotlin/<package>/` and set its `package`,
  - set `minSdkVersion 26` in `app/android/app/build.gradle`.

`flutter create` overwrites `app/lib/main.dart` with its default counter app, so
restore the tracked Dart sources afterwards with `git checkout -- app/lib`. Then
`nix run .#codegen`, `nix run .#build-native-android`, and `nix run .#run-android`.

## Notes / limitations of the POC

- Config is hardcoded in `app/lib/bootstrap/android_bootstrap.dart`; there is no
  settings UI or peer entry yet.
- The foreground service (`OniSyncService.kt`) is a stub that only holds the
  ongoing notification. For a real background lifecycle it must be started from
  the app; for a foreground POC you can skip starting it.
- Rebuild the `.so`s (`nix run .#build-native-android`) and rerun codegen
  (`nix run .#codegen`) whenever `onisync-bridge`'s Rust API changes.
