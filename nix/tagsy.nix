{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage {
  pname = "tagsy";
  version = "0.1.0";

  src = lib.cleanSource ../.;

  cargoLock = {
    lockFile = ../Cargo.lock;
  };

  cargoBuildFlags = ["--package" "tagsy"];
  cargoTestFlags = ["--package" "tagsy"];

  meta = {
    description = "Tagsy CLI client";
    mainProgram = "tagsy";
  };
}
