{
  lib,
  rustPlatform,
  makeWrapper,
  pdfium-binaries,
  ffmpeg,
  # Whether to build with the `preview-generation` cargo feature: the image +
  # pdfium (+ ffmpeg, for video) generation stack. A device whose
  # `preview_generation_policy` is `Lazy`/`Eager` needs this (else the daemon
  # falls back to `Never` at startup); a `Never`-only device can build without
  # it to drop those deps.
  withPreviewGeneration ? true,
}:
rustPlatform.buildRustPackage {
  pname = "tagsyd";
  version = "0.1.0";

  src = lib.cleanSource ../.;

  cargoLock = {
    lockFile = ../Cargo.lock;
  };

  # Only build and install the `tagsyd` daemon binary from the workspace.
  # `preview-generation` (image + pdfium) is a default feature; drop it with
  # `--no-default-features` when this build should not generate previews.
  cargoBuildFlags =
    ["--package" "tagsyd"]
    ++ lib.optionals (!withPreviewGeneration) ["--no-default-features"];
  cargoTestFlags = ["--package" "tagsyd"];

  # pdfium is only needed (and only referenced) when generation is compiled in.
  nativeBuildInputs = lib.optionals withPreviewGeneration [makeWrapper];

  # Point a generation-capable binary at its runtime preview tools:
  #  - pdfium: bound at runtime via libloading (not linked), so ONISYNC_PDFIUM_LIB_PATH
  #    gives the directory holding `libpdfium.so` (pinned nixpkgs pdfium-binaries).
  #  - ffmpeg/ffprobe: shelled out to for video frame extraction, so
  #    ONISYNC_FFMPEG_PATH gives the directory holding those binaries.
  # Only wrapped for generation-capable builds; a no-generation build never
  # references pdfium or ffmpeg.
  postInstall = lib.optionalString withPreviewGeneration ''
    wrapProgram $out/bin/tagsyd \
      --set-default ONISYNC_PDFIUM_LIB_PATH ${pdfium-binaries}/lib \
      --set-default ONISYNC_FFMPEG_PATH ${ffmpeg}/bin
  '';

  meta = {
    description = "Tagsy file synchronization daemon";
    mainProgram = "tagsyd";
  };
}
