# AGENTS.md

## Project

OniSync is a file synchronization system with tag-based organization. The
repo is a Cargo workspace (`onisync-core`, `onisyncd`, `onisync`,
`onisync-bridge`) plus a Flutter app under `app/`.

## Version control

This repo uses [`jj`](https://jj-vcs.github.io/jj/). Use the `jj` CLI for all
version-control operations; do not invoke `git` directly.

## Build / run

Use the helper apps defined in `flake.nix` instead of raw `cargo` / `flutter`
invocations:

- `nix run .#codegen` — regenerate the Dart↔Rust bindings.
- `nix run .#run-android` — codegen + native `.so` + launch on Android.
- `nix run .#launch-android` — fast path, no rebuild.
- `nix run .#run-android-clean` — uninstall first (wipes local data), then run.
- `nix run .#build-native-android` — just cross-compile the native `.so`.
- `nix run .#run-linux` — codegen + launch the Linux desktop app.
- `nix run .#launch-linux` — fast path, no codegen.

See `flake.nix` for the full list and required env vars (e.g.
`ONISYNC_CONFIG`, `ONISYNC_DEVICE`).
