{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage {
  pname = "onisyncd";
  version = "0.1.0";

  src = lib.cleanSource ../.;

  cargoLock = {
    lockFile = ../Cargo.lock;
  };

  # Only build and install the `onisyncd` daemon binary from the workspace.
  cargoBuildFlags = ["--package" "onisyncd"];
  cargoTestFlags = ["--package" "onisyncd"];

  meta = {
    description = "OniSync file synchronization daemon";
    mainProgram = "onisyncd";
  };
}
