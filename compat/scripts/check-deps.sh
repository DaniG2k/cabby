#!/usr/bin/env bash
# Checks the licenses and bans of every dependency Cabby can ship (deny.toml)
# with the pinned cargo-deny. Run it before every release (docs/releasing.md)
# and after adding or upgrading a dependency.
#   compat/scripts/check-deps.sh
# Uses `cargo deny` when the pinned version is installed, else runs it through
# mise (`mise exec cargo:cargo-deny@<version>`, installed on first use).
set -euo pipefail
cd "$(dirname "$0")/../.."
version=0.20.2
if [ "$(cargo deny --version 2>/dev/null)" = "cargo-deny $version" ]; then
  exec cargo deny --locked check licenses bans
elif command -v mise >/dev/null; then
  exec mise exec "cargo:cargo-deny@$version" -- cargo deny --locked check licenses bans
else
  echo "cargo-deny $version is needed: cargo install --locked cargo-deny --version $version" >&2
  exit 1
fi
