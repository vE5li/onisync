{
  lib,
  rustPlatform,
}:
rustPlatform.buildRustPackage {
  pname = "onisync";
  version = "0.1.0";

  src = lib.cleanSource ../.;

  cargoLock = {
    lockFile = ../Cargo.lock;
  };

  cargoBuildFlags = ["--package" "onisync"];
  cargoTestFlags = ["--package" "onisync"];

  meta = {
    description = "OniSync CLI client";
    mainProgram = "onisync";
  };
}
