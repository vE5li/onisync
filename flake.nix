{
  inputs = {
    flake-utils = {
      url = "github:numtide/flake-utils";
    };

    nixpkgs = {
      # Pinned to the exact revision the NixOS host runs
      # (vE5li/infrastructure flake.lock). The Flutter Linux runner uses GTK +
      # EGL and, at runtime, epoxy dlopen()s the system GPU driver from
      # /run/opengl-driver. That driver is built with the host's glibc/mesa, so
      # if this flake's nixpkgs diverges the two glibcs mismatch and EGL init
      # fails ("No provider of eglGetPlatformDisplayEXT found"). Matching the
      # host revision keeps a single glibc/mesa and lets the app use the normal
      # NixOS graphics path with no EGL wrapping. Bump this together with the
      # host when it updates.
      url = "github:NixOS/nixpkgs/9ae611a455b90cf061d8f332b977e387bda8e1ca";
    };

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
    };
  };

  outputs = {
    self,
    flake-utils,
    nixpkgs,
    rust-overlay,
  }:
    {
      overlays.default = final: prev: {
        onisyncd = final.callPackage ./nix/onisyncd.nix {};
        onisync = final.callPackage ./nix/onisync.nix {};
      };

      nixosModules.default = import ./nix/module.nix self;
    }
    // flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = (import nixpkgs) {
          inherit system;
          overlays = [self.overlays.default (import rust-overlay)];
          config.android_sdk.accept_license = true;
          config.allowUnfree = true;
        };

        # Android SDK + NDK. The NDK cross-compiles the Rust core to Android
        # ABIs (rusqlite's bundled SQLite is built with the NDK C toolchain,
        # and cargo-ndk locates it via ANDROID_NDK_HOME / ANDROID_NDK_ROOT).
        # The SDK (platform-tools, build-tools, a platform, cmdline-tools) is
        # what the Flutter tool drives to build/install the app; Flutter finds
        # it via ANDROID_HOME / ANDROID_SDK_ROOT.
        # Pinned so the dev shell is reproducible. This MUST match the
        # `flutter.ndkVersion` baked into the Flutter release in this nixpkgs
        # (currently 28.2.13676358): plugin modules like `:jni` request that
        # exact version, and if it is not present at `ndk/<version>/` Gradle
        # tries to download one into the read-only Nix store and fails. Pinning
        # it here makes nixpkgs create `ndk/28.2.13676358/`, which Gradle finds.
        # Check `flutter.ndkVersion` if you bump Flutter.
        ndkVersion = "28.2.13676358";
        # Platform/build-tools match the Flutter release's defaults
        # (compileSdk/targetSdk 36, build-tools 35.0.0 for the R8 minify step).
        # As with the NDK, an unavailable version makes Gradle try to download
        # into the read-only store. Platform 35 is included as well because
        # Flutter plugin modules (e.g. jni_flutter, pulled in by
        # flutter_rust_bridge) pin their own compileSdk at 35. Bump these
        # together with Flutter.
        buildToolsVersion = "35.0.0";
        androidComposition = pkgs.androidenv.composeAndroidPackages {
          includeNDK = true;
          ndkVersions = [ndkVersion];
          platformVersions = ["36" "35"];
          buildToolsVersions = [buildToolsVersion];
          # A plugin's native build (via flutter_rust_bridge) requests this
          # exact CMake; provide it so Gradle doesn't try to download it.
          cmakeVersions = ["3.22.1"];
          cmdLineToolsVersion = "13.0";
        };
        androidSdkRoot = "${androidComposition.androidsdk}/libexec/android-sdk";
        # Canonical versioned NDK path (nixpkgs also exposes `ndk-bundle`, but
        # Gradle expects `ndk/<version>/`).
        androidNdkRoot = "${androidSdkRoot}/ndk/${ndkVersion}";

        # JDK for the Flutter Android (Gradle) build. Matches the Java 17
        # source/target compatibility in app/android/app/build.gradle.kts.
        jdk = pkgs.jdk17;

        # Tools every Android/Flutter step needs on PATH.
        androidTools = with pkgs; [
          (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
          cargo-ndk
          flutter
          flutter_rust_bridge_codegen
          # flutter_rust_bridge_codegen shells out to `cargo expand`; provide it
          # so codegen is reproducible instead of auto-installing it at runtime.
          cargo-expand
          # Gradle (Flutter's Android build) needs a JDK.
          jdk
          # Used by run-android to pick the first android device id out of
          # `flutter devices --machine` (there is no stable "android" alias).
          jq
        ];

        # Tools the Flutter **Linux desktop** build needs (plan sections 6-7,
        # two-process topology). Unlike Android, `flutter build linux` drives a
        # CMake + Ninja + clang toolchain and links against GTK3; none of these
        # are pulled in by the Android tooling, so they must be listed
        # explicitly. `cargo build -p onisync-bridge` (invoked from the Linux
        # runner's CMake hook) reuses the Rust toolchain already on PATH.
        linuxDesktopTools = with pkgs; [
          cmake
          ninja
          clang
          gtk3
          glib
          pcre2
          # `flutter build linux` links the runner against GTK3/GLib via
          # pkg-config; pkg-config itself is already in the dev shell's
          # nativeBuildInputs.
        ];

        # Environment shared by the dev shell and the `nix run` app scripts, so
        # a script produces the same build whether invoked directly or from a
        # `nix develop` prompt.
        androidEnv = {
          # cargo-ndk reads these to find the NDK clang/CC/AR toolchain.
          ANDROID_NDK_HOME = androidNdkRoot;
          ANDROID_NDK_ROOT = androidNdkRoot;
          # Flutter/Gradle locate the Android SDK through these.
          ANDROID_HOME = androidSdkRoot;
          ANDROID_SDK_ROOT = androidSdkRoot;
          # Gradle finds the JDK here (the `jdk` on PATH is not enough for
          # every Gradle invocation).
          JAVA_HOME = "${jdk}";
          # Flutter bundles its own Gradle-driven build; let it use the
          # Nix-provided AAPT2 instead of downloading one that won't run on
          # NixOS.
          GRADLE_OPTS = "-Dorg.gradle.project.android.aapt2FromMavenOverride=${androidSdkRoot}/build-tools/${buildToolsVersion}/aapt2";
        };

        # Turn a bash body into a `nix run`-able app, with the Android tools on
        # PATH and the shared env exported. `set -euo pipefail` and a `cd` to
        # the invoking directory's flake root are prepended.
        androidEnvExports =
          pkgs.lib.concatStringsSep "\n"
          (pkgs.lib.mapAttrsToList (name: value: "export ${name}=${pkgs.lib.escapeShellArg value}") androidEnv);

        # Turn a bash body into a `nix run`-able app named `onisync-<name>`, with
        # the toolchains on PATH and the shared env exported.
        mkApp = name: body: let
          script = pkgs.writeShellApplication {
            name = "onisync-${name}";
            # Both tool sets are on PATH: the Android steps ignore the desktop
            # tools and vice versa, but the Linux desktop apps (`run-linux`)
            # need CMake/Ninja/clang/GTK from `linuxDesktopTools`.
            runtimeInputs = androidTools ++ linuxDesktopTools;
            text = ''
              ${androidEnvExports}
              # Run from the repo root regardless of where `nix run` was invoked.
              # All command bodies use paths relative to the repo root (e.g.
              # `cp onisync-bridge/... app/...`), so we must actually chdir there;
              # `cd "$PWD"` would leave us wherever the user invoked `nix run`
              # (e.g. inside app/), breaking those relative paths. Resolve the
              # root explicitly: honour an override, else ask git for the
              # toplevel, else fall back to the current directory.
              root="''${ONISYNC_ROOT:-}"
              if [ -z "$root" ]; then
                root="$(${pkgs.git}/bin/git rev-parse --show-toplevel 2>/dev/null || true)"
              fi
              cd "''${root:-$PWD}"
              ${body}
            '';
          };
        in {
          type = "app";
          program = "${script}/bin/onisync-${name}";
        };

        # Build the `apps` output from an attrset of `{ <name> = <bash body>; }`,
        # deriving each app's derivation name (`onisync-<name>`) from its attr key
        # so the two never drift.
        mkApps = pkgs.lib.mapAttrs mkApp;

        # The Flutter app tree (app/) — including the Dart sources under app/lib/
        # (minus the generated app/lib/rust/) and the hand-merged Android glue —
        # is tracked in git and is the source of truth. It is never regenerated;
        # the one-time scaffolding is documented in onisync-bridge/android/README.md.

        # Generate the Dart <-> Rust bindings.
        codegenBody = ''
          flutter_rust_bridge_codegen generate \
            --config-file flutter_rust_bridge.yaml
        '';

        # Cross-compile the native .so(s) into the app's jniLibs for a given set
        # of ABIs. `$abis` (space-separated cargo-ndk ABI names, e.g.
        # "arm64-v8a x86_64") must be set by the caller; helper only.
        buildNativeForAbisBody = ''
          targets=()
          for abi in $abis; do targets+=("-t" "$abi"); done
          cargo ndk "''${targets[@]}" \
            -o app/android/app/src/main/jniLibs \
            build --release -p onisync-bridge --features generated
        '';

        # Standalone build step: cross-compile for a fixed ABI set. Defaults to
        # arm64-v8a (physical devices); override with ONISYNC_ANDROID_ABIS
        # (space-separated) e.g. to produce a multi-ABI release build.
        buildNativeAndroidBody = ''
          abis="''${ONISYNC_ANDROID_ABIS:-arm64-v8a}"
          ${buildNativeForAbisBody}
        '';

        # Resolve the target android device AND its ABI. Flutter's `-d` matches a
        # device *id/name*, not a platform, and android device ids are serial
        # numbers (no stable "android" alias), so resolve the first connected
        # android device from `flutter devices --machine`. We also read its
        # `targetPlatform` (e.g. "android-x64") and map it to the matching
        # cargo-ndk ABI, so the native build targets exactly the device we run
        # on — an x86_64 emulator otherwise silently runs against a stale/absent
        # x86_64 .so while only arm64-v8a was (re)built, and frb then misreads
        # the mismatched wire format ("Bad state: ...").
        #
        # With more than one android device connected, set ONISYNC_DEVICE to an
        # id/name (see `flutter devices`) to pick one.
        pickAndroidDevice = ''
          selector='.[0] // empty'
          if [ -n "''${ONISYNC_DEVICE:-}" ]; then
            selector='map(select(.id == "'"$ONISYNC_DEVICE"'" or .name == "'"$ONISYNC_DEVICE"'"))[0] // empty'
          fi
          read -r device platform < <(
            flutter devices --machine \
              | jq -r 'map(select(.targetPlatform | startswith("android"))) | '"$selector"' | "\(.id) \(.targetPlatform)"'
          ) || true
          if [ -z "$device" ]; then
            echo "No android device found. Connect a device (adb devices) and retry." >&2
            exit 1
          fi
          case "$platform" in
            android-arm64) device_abi="arm64-v8a" ;;
            android-x64)   device_abi="x86_64" ;;
            android-arm)   device_abi="armeabi-v7a" ;;
            android-x86)   device_abi="x86" ;;
            *)
              echo "Unknown android targetPlatform '$platform'; defaulting ABI to arm64-v8a." >&2
              device_abi="arm64-v8a"
              ;;
          esac
        '';

        # Copy the per-device runtime config selected by $ONISYNC_CONFIG into
        # the Android asset the Kotlin runtime reads at start-up. This is what
        # puts one of the files under app/config/ inside the APK.
        selectAndroidConfigBody = ''
          mkdir -p app/android/app/src/main/assets
          cp "app/config/''${ONISYNC_CONFIG}.json" \
             "app/android/app/src/main/assets/onisync_config.json"
        '';

        # Fast path: pick the device and launch, no native rebuild. Assumes the
        # .so for the device's ABI is already current (see launch-android).
        launchAndroidBody = ''
          ${pickAndroidDevice}
          ${selectAndroidConfigBody}
          # Select the in-process-engine backend at build time.
          ( cd app && flutter run --release -d "$device" \
              --dart-define=ONISYNC_BACKEND=android )
        '';

        # Full path: pick the device, build the native .so for exactly THAT
        # device's ABI, then launch. Building the device's own ABI (rather than a
        # fixed default) is what keeps an x86_64 emulator from running against a
        # stale/absent x86_64 .so while only arm64-v8a was rebuilt.
        runAndroidLaunchBody = ''
          ${pickAndroidDevice}
          ${selectAndroidConfigBody}
          abis="$device_abi"
          ${buildNativeForAbisBody}
          ( cd app && flutter run --release -d "$device" \
              --dart-define=ONISYNC_BACKEND=android )
        '';

        # Like run-android, but wipes the app's local data first by uninstalling the
        # existing package. `flutter run`/`flutter install -r` only *replace* the
        # APK and keep app-private storage (the DB *and* identity.key under
        # filesDir), so an explicit uninstall is the only way to start from a
        # clean slate. This regenerates the device identity (new public key) and
        # an empty database on next launch. The package id matches
        # app/android/app/build.gradle.kts (applicationId).
        runAndroidCleanBody = ''
          echo "Uninstalling com.example.onisync_app to wipe local data..."
          # adb ships with the composed Android SDK's platform-tools; reference
          # it by absolute path rather than assuming it is on PATH. Don't fail if
          # the package isn't installed yet.
          "${androidSdkRoot}/platform-tools/adb" uninstall com.example.onisync_app || true
          # Rebuild the native .so for the device's ABI before running: a fresh
          # install (or a cleaned tree) has no bundled library, and `flutter run`
          # alone does not build it, so the app would crash with
          # "libonisync_bridge.so not found". Building the device's own ABI also
          # avoids the stale-.so / frb wire mismatch on x86_64 emulators.
          ${runAndroidLaunchBody}
        '';

        # --- Linux desktop (two-process topology, plan sections 6-7) ---------

        # Build/run the Flutter Linux desktop app. Unlike Android, the native
        # library is built and bundled by the runner's CMake hook
        # (app/linux/CMakeLists.txt) during `flutter run`, so there is no
        # separate native-build step here. The daemon (onisyncd) is a separate,
        # long-lived process the user runs via systemd or cargo; the flake does
        # not build or manage it, and the app attaches to its control socket at
        # launch.
        launchLinuxBody = ''
          # Select the daemon-attach backend at build time (the Dart sources are
          # shared with Android; only this define differs).
          ( cd app && flutter run -d linux \
              --dart-define=ONISYNC_BACKEND=linux )
        '';
      in {
        formatter = pkgs.alejandra;

        packages = rec {
          onisyncd = pkgs.onisyncd;
          onisync = pkgs.onisync;
          default = onisync;
        };

        apps = mkApps {
          # Shared across platforms.
          codegen = codegenBody;

          # --- Android apps --------------------------------------------------
          # Full build-and-run: regenerate bindings, rebuild the native .so for
          # the connected device's ABI, then launch. The safe default; safe to
          # re-run.
          #
          # Requires ONISYNC_CONFIG=<name> (see app/config/) so the APK bundles
          # a per-device runtime config. With more than one android device
          # connected also set ONISYNC_DEVICE=<id|name> to pick which one to
          # flash; see the run-android-phone / run-android-sylvie-phone
          # convenience apps below for the two-phone workflow.
          run-android = ''
            ${codegenBody}
            ${runAndroidLaunchBody}
          '';
          # Fast path: just `flutter run`, assuming codegen + the native .so are
          # already up to date. Use for a tight edit-Dart/re-run loop; if you
          # changed the Rust API or the .so is missing, use run-android instead.
          launch-android = launchAndroidBody;
          # Like run-android but uninstalls first to wipe local data (new
          # identity + empty DB). Use after a schema change or to reset a device.
          run-android-clean = runAndroidCleanBody;
          # Individual build step, exposed for manual use / overriding ABIs
          # (defaults to arm64-v8a; set ONISYNC_ANDROID_ABIS for a release build).
          build-native-android = buildNativeAndroidBody;

          # --- Linux desktop apps (two-process topology) ---------------------
          # Full build-and-run: regenerate bindings, then launch (the native
          # library is built by the CMake hook during `flutter run`). Safe to
          # re-run.
          run-linux = ''
            ${codegenBody}
            ${launchLinuxBody}
          '';
          # Fast path: just `flutter run`, assuming codegen is up to date. Use
          # for a tight edit-Dart/re-run loop; re-run codegen (or use run-linux)
          # after a Rust API change.
          launch-linux = launchLinuxBody;
        };

        devShell =
          pkgs.mkShell
          ({
              nativeBuildInputs =
                androidTools
                ++ linuxDesktopTools
                ++ (with pkgs; [pkg-config]);
              buildInputs = with pkgs; [
                openssl
              ];

              RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
            }
            // androidEnv);
      }
    );
}
