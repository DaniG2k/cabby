#!/usr/bin/env bash
# Vendors mruby 1.2.0 (plus mruby-env and the generated build outputs) into
# crates/cabby-mruby-sys from the reference build, so Cabby builds mruby with a
# C compiler only (no Ruby, no bison). See docs/mruby.md.
#
# Input: compat/reference/vendorlib/go-mruby/mruby-build/mruby, produced by
# `cargo xtask reference-mruby` (upstream's Makefile in ruby:2.7 with
# MRUBY_COMMIT=1.2.0 MRUBY_CONFIG=etc/build_config.rb). The script copies:
#   mruby/       the mruby sources compiled into libmruby.a (core, mrblib, the
#                default gembox minus the command-line tools, mruby-compiler),
#                their Ruby sources (for regeneration only) and license files;
#   mruby-env/   iij/mruby-env at the commit the reference build cloned;
#   generated/   y.tab.c (bison), mrblib.c and gem_init.c files (mrbc bytecode);
#   SHA256SUMS   of every vendored file.
# It refuses to run unless the reference libmruby.a matches compat/upstream.lock.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
src="${root}/compat/reference/vendorlib/go-mruby/mruby-build/mruby"
dst="${root}/crates/cabby-mruby-sys"

want="$(sed -n 's/^libmruby_a_sha256 = "\([0-9a-f]*\)".*/\1/p' "${root}/compat/upstream.lock")"
have="$(sha256sum "${src}/build/host/lib/libmruby.a" | cut -d' ' -f1)"
if [[ "${want}" != "${have}" ]]; then
  echo "reference libmruby.a is ${have}, compat/upstream.lock pins ${want}" >&2
  exit 1
fi

gems=(
  mruby-array-ext mruby-enum-ext mruby-enum-lazy mruby-enumerator mruby-fiber mruby-hash-ext
  mruby-kernel-ext mruby-math mruby-numeric-ext mruby-object-ext mruby-objectspace mruby-print
  mruby-proc-ext mruby-random mruby-range-ext mruby-sprintf mruby-string-ext mruby-struct
  mruby-symbol-ext mruby-time mruby-toplevel-ext
)

rm -rf "${dst}/mruby" "${dst}/mruby-env" "${dst}/generated"
mkdir -p "${dst}/mruby" "${dst}/mruby-env" "${dst}/generated"

copy() { # copy <from-root> <to-root> <relative path>...
  local from="$1" to="$2"
  shift 2
  for rel in "$@"; do
    mkdir -p "${to}/$(dirname "${rel}")"
    cp -p "${from}/${rel}" "${to}/${rel}"
  done
}

cd "${src}"
copy . "${dst}/mruby" MITL LEGAL AUTHORS README.md mrbgems/default.gembox
mapfile -t core < <(find include src mrblib -type f \( -name '*.h' -o -name '*.c' -o -name '*.rb' \) | sort)
copy . "${dst}/mruby" "${core[@]}"
for gem in "${gems[@]}" mruby-compiler; do
  mapfile -t files < <(find "mrbgems/${gem}" -type f | grep -Ev '/(bin)?test/' | sort)
  copy . "${dst}/mruby" "${files[@]}"
done

cd "${src}/build/mrbgems/mruby-env"
copy . "${dst}/mruby-env" mrbgem.rake README.md src/env.c mrblib/env.rb

cd "${src}/build/host"
mapfile -t generated < <(find mrblib mrbgems -name 'mrblib.c' -o -name 'gem_init.c' -o -name 'y.tab.c' | sort)
copy . "${dst}/generated" "${generated[@]}"

cd "${dst}"
find mruby mruby-env generated -type f | LC_ALL=C sort | xargs sha256sum > SHA256SUMS
echo "vendored $(wc -l < SHA256SUMS) files into ${dst}"
