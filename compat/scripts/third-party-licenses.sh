#!/usr/bin/env bash
# Writes THIRD_PARTY_LICENSES.md for release artifacts: the Rust crates of the
# release build (cargo-about over `-p cabby-server` without mruby), then the
# notices Cabby carries itself from compat/licenses. `--mruby` describes a build
# with `--features mruby` instead. Output: $OUT, default THIRD_PARTY_LICENSES.md.
set -euo pipefail
cd "$(dirname "$0")/../.."
out="${OUT:-THIRD_PARTY_LICENSES.md}"
features=()
notices=(anycable-MIT-LICENSE anyt-core-MIT-LICENSE go-mruby-LICENSE)
if [ "${1:-}" = "--mruby" ]; then
  features=(--features mruby)
  notices+=(mruby-MITL mruby-env-LICENSE)
fi
cargo about generate --config about.toml --manifest-path crates/cabby-server/Cargo.toml \
  --no-default-features ${features[@]+"${features[@]}"} --locked --fail about.hbs -o "$out"
{
  printf '\n# Notices Cabby carries itself\n\n'
  printf 'Cabby is a port of the AnyCable Go server, its tests derive from AnyCable, AnyT and\n'
  printf 'go-mruby, and its mruby binding follows the go-mruby API.\n'
  for n in "${notices[@]}"; do
    printf '\n## %s\n\n```text\n' "$n"
    cat "compat/licenses/$n"
    printf '```\n'
  done
} >>"$out"
echo "wrote $out"
