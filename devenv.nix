{ pkgs, ... }:

{
  dotenv.enable = true;

  packages = with pkgs; [
    cargo-release
    cargo-watch
    cargo-expand
  ];

  languages.rust = {
    enable = true;
    channel = "stable";
  };
}
