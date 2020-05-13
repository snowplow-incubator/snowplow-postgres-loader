let
  unstable = import (fetchTarball https://github.com/domenkozar/hie-nix/tarball/master) {};
  pkgs = import <nixpkgs> {};
  python-deps = python-packages: with python-packages; [
    boto3
    pika
    pyyaml
    requests
  ];
  python-with-deps = pkgs.python37.withPackages python-deps;
in
pkgs.stdenv.mkDerivation rec {
  name = "pg-loader";

  # The packages in the `buildInputs` list will be added to the PATH in our shell
  buildInputs = [
    pkgs.postgresql
    pkgs.awscli
    python-with-deps

    pkgs.figlet
  ];

  shellHook = ''
    export NIX_NAME='' + name + '' && figlet $NIX_NAME
  '';
}
