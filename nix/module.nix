self: {
  pkgs,
  lib,
  config,
  ...
}: {
  options.services.onisync = with lib; {
    # enable = lib.mkEnableOption "onisync service";

    user = mkOption {
      type = types.str;
      description = "User account under which onisync runs.";
    };

    group = mkOption {
      type = types.str;
      description = "Group under which onisync runs.";
    };

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.system}.onisyncd;
      defaultText = literalExpression "onisync.packages.\${system}.onisyncd";
      description = "The onisync daemon package to use.";
    };

    configuration-file = mkOption {
      type = types.path;
      description = "Path to the configuration file";
    };

    state-directory = mkOption {
      type = types.str;
      default = "onisync";
      description = ''
        Name of the systemd StateDirectory, created under /var/lib and
        owned by the service user. Used as the default data directory.
      '';
    };

    data-directory = mkOption {
      type = types.path;
      default = "/var/lib/${config.services.onisync.state-directory}";
      defaultText = literalExpression ''"/var/lib/''${state-directory}"'';
      description = "Path to the data directory";
    };

    private-key-file = mkOption {
      type = types.path;
      description = "Path to the private key file";
    };
  };

  config = with config.services.onisync; {
    systemd.services.onisync = {
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
        # /run/onisync owned by the service user and tears it down on stop; its
        # 0700 mode is the entire security model for local control (nothing is
        # exposed on the network). The daemon binds, and clients connect to,
        # the fixed /run/onisync/onisync.sock — no XDG_RUNTIME_DIR guessing.
        RuntimeDirectory = "onisync";
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
