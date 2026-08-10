self: {
  pkgs,
  lib,
  config,
  ...
}: {
  options.services.tagsy = with lib; {
    # enable = lib.mkEnableOption "tagsy service";

    user = mkOption {
      type = types.str;
      description = "User account under which tagsy runs.";
    };

    group = mkOption {
      type = types.str;
      description = "Group under which tagsy runs.";
    };

    enable-preview-generation = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Whether this host's daemon is built with the `preview-generation`
        cargo feature (the image + pdfium thumbnail-generation stack).

        A host whose `preview_generation_policy` (set in the JSON
        configuration file) is `Lazy` or `Eager` needs this enabled — the
        daemon otherwise cannot generate previews and falls back to `Never`
        at startup (logging an error). A host that only ever caches/serves
        previews obtained from peers (`Never`) can disable this to drop the
        image/pdfium dependencies from its build.

        Ignored if `package` is set explicitly.
      '';
    };

    package = mkOption {
      type = types.package;
      # Build the daemon with the feature selected by
      # `enable-preview-generation`. Uses `callPackage` on the package
      # definition directly so it works whether or not the host applies the
      # flake overlay.
      default = pkgs.callPackage ../nix/tagsyd.nix {
        withPreviewGeneration = config.services.tagsy.enable-preview-generation;
      };
      defaultText = literalExpression ''
        pkgs.callPackage ./nix/tagsyd.nix { withPreviewGeneration = <enable-preview-generation>; }
      '';
      description = "The tagsy daemon package to use.";
    };

    configuration-file = mkOption {
      type = types.path;
      description = "Path to the configuration file";
    };

    state-directory = mkOption {
      type = types.str;
      default = "tagsy";
      description = ''
        Name of the systemd StateDirectory, created under /var/lib and
        owned by the service user. Used as the default data directory.
      '';
    };

    data-directory = mkOption {
      type = types.path;
      default = "/var/lib/${config.services.tagsy.state-directory}";
      defaultText = literalExpression ''"/var/lib/''${state-directory}"'';
      description = "Path to the data directory";
    };

    private-key-file = mkOption {
      type = types.path;
      description = "Path to the private key file";
    };
  };

  config = with config.services.tagsy; {
    systemd.services.tagsy = {
      enable = true;

      wantedBy = ["multi-user.target"];
      after = ["network.target"];

      serviceConfig = {
        ExecStart = "${lib.getExe package} run ${configuration-file}";
        Restart = "on-failure";
        RestartSec = 5;
        User = user;
        Group = group;
        StateDirectory = state-directory;

        # Local control socket (portability plan section 7). systemd creates
        # /run/tagsy owned by the service user and tears it down on stop; its
        # 0700 mode is the entire security model for local control (nothing is
        # exposed on the network). The daemon binds, and clients connect to,
        # the fixed /run/tagsy/tagsy.sock — no XDG_RUNTIME_DIR guessing.
        RuntimeDirectory = "tagsy";
        RuntimeDirectoryMode = "0700";
      };

      environment = {
        RUST_LOG = "debug";
        ONISYNC_DATA_DIR = "${data-directory}";
        ONISYNC_PRIVATE_KEY_FILE = "${private-key-file}";
      };
    };

    # TODO: Put behind proper option.
    networking.firewall = {
      allowedTCPPorts = [3468];
    };
  };
}
