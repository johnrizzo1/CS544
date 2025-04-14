{
  description = "A Nix based AI/ML/DS Lab";

  inputs = {
    devenv-root = {
      url = "file+file:///dev/null";
      flake = false;
    };
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";
    devenv.url = "github:cachix/devenv";
    nix2container.url = "github:nlewo/nix2container";
    nix2container.inputs.nixpkgs.follows = "nixpkgs";
    mk-shell-bin.url = "github:rrbutani/nix-mk-shell-bin";
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
    services-flake.url = "github:juspay/services-flake";
    northwind.url = "github:pthom/northwind_psql";
    northwind.flake = false;
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs = inputs@{ flake-parts, devenv-root, devenv, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.devenv.flakeModule
        inputs.process-compose-flake.flakeModule
      ];
      systems = [ "x86_64-linux" "i686-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];

      perSystem = { config, self', inputs', pkgs, system, lib, ... }: {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          # overlays = lib.attrValues self.overlays;
          config.allowUnfree = true;
          config.cudaSupport = pkgs.stdenv.isLinux;
        };

        packages.default = self'.packages.nixailab;
        process-compose."nixailab" = pc:
          let
            rosettaPkgs =
              if pkgs.stdenv.isDarwin && pkgs.stdenv.isAarch64
              then pkgs.pkgsx86_64Darwin
              else pkgs;
            
            dbName = "sample";
            dataDirBase = "$HOME/.cache/.nixailab/";
          in
          {
            imports = [
              inputs.services-flake.processComposeModules.default
            ];

            services = {
              postgres."pg1" = {
                enable = true;
                initialDatabases = [
                  {
                    name = dbName;
                    schemas = [ "${inputs.northwind}/northwind.sql" ];
                  }
                ];
                listen_addresses = "127.0.0.1";
                port = 5432;
                extensions = extensions: [
                  extensions.postgis
                  extensions.timescaledb
                  extensions.age
                  # extensions.pgvector
                ];
                initialScript.before = ''
                  CREATE DATABASE mimic WITH OWNER jrizzo;
                  CREATE EXTENSION IF NOT EXISTS timescaledb;
                  CREATE EXTENSION IF NOT EXISTS postgis;
                  CREATE EXTENSION IF NOT EXISTS age;
                  -- CREATE EXTENSION IF NOT EXISTS pgvector;
                '';
                settings.shared_preload_libraries = "timescaledb";
              };
            };

            settings.processes = {
              pgweb =
                let
                  pgcfg = pc.config.services.postgres.pg1;
                in
                {
                  environment.PGWEB_DATABASE_URL = pgcfg.connectionURI { inherit dbName; };
                  command = pkgs.pgweb;
                  depends_on."pg1".condition = "process_healthy";
                };

              test_postgres = {
                command = pkgs.writeShellApplication {
                  name = "pg1-test";
                  runtimeInputs = [ pc.config.services.postgres.pg1.package ];
                  text = ''
                    echo 'SELECT version();' | psql -h 127.0.0.1 ${dbName}
                  '';
                };
                depends_on."pg1".condition = "process_healthy";
              };
            };
          };

        devenv.shells.default = {
          devenv.root =
            let
              devenvRootFileContent = builtins.readFile devenv-root.outPath;
            in
            lib.mkIf (devenvRootFileContent != "") devenvRootFileContent;

          name = "nixailab";

          imports = [
            # This is just like the imports in devenv.nix.
            # See https://devenv.sh/guides/using-with-flake-parts/#import-a-devenv-module
            # ./devenv-foo.nix
          ];

          # https://devenv.sh/reference/options/
          packages = with pkgs; [ 
            config.packages.default 
            git # Code management
            jq  # Query JSON
            yq  # Query YAML
            wget
            curl
            apacheKafka
            postgresql
            (python3.withPackages(ps: with ps; [ 
              python-dotenv
              pandas
              numpy
              sqlalchemy
              psycopg2
            ]))
          ] ++ lib.optionals pkgs.stdenv.isDarwin [
            darwin.apple_sdk.frameworks.CoreFoundation
            darwin.apple_sdk.frameworks.Security
            darwin.apple_sdk.frameworks.SystemConfiguration
          ] ++ lib.optionals pkgs.stdenv.isLinux [
            rstudio
          ];

          # R setup
          languages.r.enable = true;

          # Python setup
          languages.python.enable = true;
          languages.python.venv.enable = true;
          languages.python.uv.enable = true;
          # languages.python.poetry.enable = true;
          languages.javascript.enable = true;
          languages.javascript.npm.enable = true;
          languages.javascript.pnpm.enable = true;
          languages.javascript.yarn.enable = true;
          languages.typescript.enable = true;

          difftastic.enable = true;
          dotenv.enable = true;

          cachix.enable = true;
          cachix.pull = [ "pre-commit-hooks" ];

          enterShell = ''
            echo "Welcome to the nix ai/ml/ds toolkit"
            echo '-----------------------------------'
            echo -n 'Git:    '; git --version
            echo -n 'Python: '; python --version
            echo -n 'CUDA:   '; python -c "import torch; print(torch.cuda.is_available());"
            echo -n 'MPS:    '; python -c "import torch; print(torch.backends.mps.is_available());"
            echo ""
          '';
        };
      };

      flake = {
        # The usual flake attributes can be defined here, including system-
        # agnostic ones like nixosModule and system-enumerating ones, although
        # those are more easily expressed in perSystem.
      };
    };
}
