{
  description = "Providarr — self-hosted TMDb/TheTVDB metadata proxy with Postgres caching";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      forAllSystems =
        f: nixpkgs.lib.genAttrs systems (system: f system (import nixpkgs { inherit system; }));

      mkToolchain = pkgs: [
        pkgs.cargo
        pkgs.rustc
        pkgs.rustfmt
        pkgs.clippy
        pkgs.rust-analyzer
        pkgs.pkg-config
        # Integration tests create isolated schemas in a live Postgres.
        pkgs.postgresql_16
        pkgs.git
        pkgs.curl
        pkgs.jq
      ];

      mkPackage =
        pkgs:
        pkgs.rustPlatform.buildRustPackage {
          pname = "providarr";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          # The tests need a running Postgres; run them from `nix develop` instead.
          doCheck = false;
          nativeBuildInputs = [ pkgs.pkg-config ];
          meta = {
            description = "Self-hosted TMDb/TheTVDB metadata proxy with Postgres caching";
            homepage = "https://github.com/MagicBOTAlex/Providarr";
            license = pkgs.lib.licenses.mit;
            mainProgram = "providarr";
            platforms = pkgs.lib.platforms.unix;
          };
        };
    in
    {
      packages = forAllSystems (
        system: pkgs:
        let
          providarr = mkPackage pkgs;
        in
        {
          default = providarr;
          inherit providarr;
        }
      );

      apps = forAllSystems (
        system: pkgs: {
          default = {
            type = "app";
            program = "${(mkPackage pkgs)}/bin/providarr";
            meta.description = "Run the Providarr metadata proxy";
          };
        }
      );

      devShells = forAllSystems (
        system: pkgs: {
          default = pkgs.mkShell {
            packages = mkToolchain pkgs;
            shellHook = ''
              echo "Providarr dev shell"
              echo "  rustc: $(rustc --version 2>/dev/null || echo '(missing)')"
              echo "  cargo: $(cargo --version 2>/dev/null || echo '(missing)')"
              echo "  psql:  $(psql --version 2>/dev/null || echo '(missing)')"
              echo
              echo "  cp .env.example .env   # set TMDB_API_TOKEN / TVDB_API_KEY / DATABASE_URL"
              echo "  cargo run              # listens on 0.0.0.0:4155"
            '';
          };
        }
      );

      formatter = forAllSystems (system: pkgs: pkgs.nixfmt);

      # NixOS module: `services.providarr`.
      nixosModules.default =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.providarr;
          configFile = pkgs.writeText "providarr-config.json" (
            builtins.toJSON (
              lib.recursiveUpdate {
                server = {
                  host = cfg.host;
                  port = cfg.port;
                };
                logging = {
                  enabled = true;
                  dir = "/var/lib/providarr/logs";
                };
              } cfg.settings
            )
          );
        in
        {
          options.services.providarr = {
            enable = lib.mkEnableOption "Providarr metadata proxy";

            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.stdenv.hostPlatform.system}.providarr;
              defaultText = lib.literalExpression "providarr.packages.\${system}.providarr";
              description = "The Providarr package to use.";
            };

            user = lib.mkOption {
              type = lib.types.str;
              default = "providarr";
              description = "User the service runs as.";
            };

            group = lib.mkOption {
              type = lib.types.str;
              default = "providarr";
              description = "Group the service runs as.";
            };

            host = lib.mkOption {
              type = lib.types.str;
              default = "0.0.0.0";
              description = "Address the HTTP server listens on.";
            };

            port = lib.mkOption {
              type = lib.types.port;
              default = 4155;
              description = "Port the HTTP server listens on.";
            };

            openFirewall = lib.mkOption {
              type = lib.types.bool;
              default = false;
              description = "Open the listen port in the firewall.";
            };

            environmentFile = lib.mkOption {
              type = lib.types.nullOr lib.types.path;
              default = null;
              example = "/run/secrets/providarr.env";
              description = ''
                File containing the runtime secrets, loaded into the service
                environment: `DATABASE_URL`, `TMDB_API_TOKEN` and `TVDB_API_KEY`.
              '';
            };

            settings = lib.mkOption {
              type = lib.types.attrs;
              default = { };
              description = ''
                Extra Providarr `config.json` contents, merged over the generated
                `server` and `logging` sections (for example `cache`, `inbound` or
                `providers`).
              '';
            };
          };

          config = lib.mkIf cfg.enable {
            users.users.${cfg.user} = {
              isSystemUser = true;
              group = cfg.group;
              home = "/var/lib/providarr";
            };
            users.groups.${cfg.group} = { };

            systemd.services.providarr = {
              description = "Providarr metadata proxy";
              wantedBy = [ "multi-user.target" ];
              wants = [ "network-online.target" ];
              after = [
                "network-online.target"
                "postgresql.service"
              ];

              serviceConfig = {
                ExecStart = "${cfg.package}/bin/providarr";
                User = cfg.user;
                Group = cfg.group;
                WorkingDirectory = "/var/lib/providarr";
                StateDirectory = "providarr";
                Environment = [ "PROVIDARR_CONFIG=${configFile}" ];
                EnvironmentFile = lib.optional (cfg.environmentFile != null) cfg.environmentFile;
                Restart = "on-failure";
                RestartSec = 5;

                NoNewPrivileges = true;
                ProtectSystem = "strict";
                ProtectHome = true;
                PrivateTmp = true;
                ProtectKernelTunables = true;
                ProtectControlGroups = true;
                RestrictAddressFamilies = [
                  "AF_INET"
                  "AF_INET6"
                  "AF_UNIX"
                ];
                ReadWritePaths = [ "/var/lib/providarr" ];
                CapabilityBoundingSet = "";
                AmbientCapabilities = "";
              };
            };

            networking.firewall.allowedTCPPorts = lib.optional cfg.openFirewall cfg.port;
          };
        };
    };
}
