#!/usr/bin/env bash
# Builds a Cabby release on a macOS (Apple silicon) machine with Docker Desktop
# (docs/releasing.md):
#
#   compat/scripts/release.sh             dry run (the default): build, package, check
#   compat/scripts/release.sh --publish   the same, then a draft release and the
#                                         :<version> image
#   compat/scripts/release.sh --promote   after reviewing the draft: publish it and
#                                         move :latest to the :<version> image
#
# For the workspace version (Cargo.toml) the dry run
#   1. checks the dependency licenses (compat/scripts/check-deps.sh) and that
#      CHANGELOG.md has a section for the version (compat/scripts/release-meta.sh);
#   2. builds `cabby` without mruby (`-p cabby-server --no-default-features`) for
#      x86_64-unknown-linux-gnu and aarch64-unknown-linux-gnu against glibc 2.28
#      (cargo zigbuild), plus `cabby-healthcheck` for the image, and natively for
#      aarch64-apple-darwin, with every local path remapped (RUSTFLAGS);
#   3. fails if a binary still contains a local path (/Users/..., /home/... other
#      than /home/builder), or if a Linux binary needs a glibc symbol newer than 2.28
#      (objdump -T in Docker), and runs each binary's --version;
#   4. writes THIRD_PARTY_LICENSES.md (cargo-about), the release LICENSE (Cabby's plus
#      AnyCable's notice), README.md (dist/README.md) and the release notes, which
#      name the source commit;
#   5. packs cabby-<version>-<triple>.tar.gz with exactly cabby, LICENSE,
#      THIRD_PARTY_LICENSES.md, README.md and CHANGELOG.md at the archive root (bsdtar,
#      root-owned, no macOS metadata), checks each listing, and writes SHA256SUMS;
#   6. builds the image for the native platform and tests it (non-root, healthy,
#      broadcasts on 8090 and not on 8080 with a secret, the SQLite broker on a named
#      volume at /var/lib/cabby), then builds the multi-arch image (linux/amd64,
#      linux/arm64) with the desktop-linux builder.
# --publish first refuses unless the CHANGELOG section is dated, the tree is clean,
# HEAD is origin/main, gh may push to DaniG2k/cabby-releases, and neither the
# release v<version> nor the image ghcr.io/danig2k/cabby:<version> exists yet. Then
# it runs the dry run, creates the draft release with the archives and SHA256SUMS,
# and pushes the image as :<version> only.
# --promote refuses unless the draft exists, then publishes it and points :latest at
# the :<version> image (a pre-release keeps :latest where it is).
#
# Output: target/release-dist/<version>/. Tools: zig and cargo-zigbuild, cargo-about
# and cargo-deny through mise (pinned below), Docker with buildx, gh for publishing.
set -euo pipefail
cd "$(dirname "$0")/../.."
root="$PWD"

ZIG="zig@0.15.2"
ZIGBUILD="cargo:cargo-zigbuild@0.24.1"
CARGO_ABOUT="cargo:cargo-about[features=cli]@0.9.3"
BUILDER="desktop-linux"
IMAGE="ghcr.io/danig2k/cabby"
RELEASES_REPO="DaniG2k/cabby-releases"
SOURCE_REPO="DaniG2k/cabby"
LINUX_TARGETS=(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu)
MAC_TARGET="aarch64-apple-darwin"
GLIBC_MAX="2.28"
# Pinned by digest: Alpine with binutils for objdump. The Linux binaries run in the
# release image's base, read from the Dockerfile.
ALPINE="alpine:3.22@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f"
ARCHIVE_FILES=(cabby LICENSE THIRD_PARTY_LICENSES.md README.md CHANGELOG.md)

mode=dry-run
case "${1:-}" in
  "") ;;
  --publish) mode=publish ;;
  --promote) mode=promote ;;
  -h | --help) awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"; exit 0 ;;
  *) echo "usage: $0 [--publish | --promote]" >&2; exit 2 ;;
esac

step() { printf '\n==> %s\n' "$*"; }
fail() { echo "release: $*" >&2; exit 1; }

version="$(sed -n '/^\[workspace\.package\]/,/^\[/s/^version = "\(.*\)"$/\1/p' Cargo.toml)"
[ -n "$version" ] || fail "no [workspace.package] version in Cargo.toml"
release_exists() { gh release view "v$version" --repo "$RELEASES_REPO" >/dev/null 2>&1; }
image_exists() { docker buildx imagetools inspect "$IMAGE:$version" >/dev/null 2>&1; }

if [ "$mode" = promote ]; then
  step "promote $version"
  gh auth status >/dev/null 2>&1 || fail "gh isn't logged in"
  release_exists || fail "there is no release v$version in $RELEASES_REPO: run --publish first"
  [ "$(gh release view "v$version" --repo "$RELEASES_REPO" --json isDraft --jq .isDraft)" = true ] ||
    fail "v$version in $RELEASES_REPO is already published"
  image_exists || fail "$IMAGE:$version doesn't exist: run --publish first"
  prerelease="$(compat/scripts/release-meta.sh check "v$version" | sed -n 's/^prerelease=//p')"
  gh release edit "v$version" --repo "$RELEASES_REPO" --draft=false
  echo "published https://github.com/$RELEASES_REPO/releases/tag/v$version"
  if [ "$prerelease" = true ]; then
    echo "a pre-release: $IMAGE:latest stays where it is"
  else
    docker buildx imagetools create -t "$IMAGE:latest" "$IMAGE:$version"
    echo "$IMAGE:latest now points at $IMAGE:$version"
  fi
  exit 0
fi

[ "$(uname -s)/$(uname -m)" = "Darwin/arm64" ] || fail "run this on macOS on Apple silicon (the $MAC_TARGET build is native)"
command -v mise >/dev/null || fail "mise is needed for the pinned tools"
docker info >/dev/null 2>&1 || fail "Docker isn't running"
docker buildx inspect "$BUILDER" >/dev/null 2>&1 || fail "no buildx builder named $BUILDER (Docker Desktop)"
tar --version 2>/dev/null | grep -q bsdtar || fail "tar must be bsdtar (the macOS tar)"
DISTROLESS="$(sed -n 's/^FROM \(gcr\.io\/distroless\/[^ ]*@sha256:[0-9a-f]*\).*/\1/p' Dockerfile)"
[ -n "$DISTROLESS" ] || fail "no pinned distroless base (FROM gcr.io/distroless/...@sha256:...) in the Dockerfile"

step "version and changelog"
meta="$(compat/scripts/release-meta.sh check "v$version")"
echo "$meta"
prerelease="$(sed -n 's/^prerelease=//p' <<<"$meta")"
dated="$(sed -n 's/^dated=//p' <<<"$meta")"
commit="$(git rev-parse HEAD)"
if [ "$mode" = publish ]; then
  step "publishing checks"
  [ "$dated" = true ] || fail "the CHANGELOG.md section for $version has no date (## [$version] - YYYY-MM-DD)"
  [ -z "$(git status --porcelain)" ] || fail "the working tree has changes: commit them first"
  git fetch --quiet origin main
  [ "$commit" = "$(git rev-parse origin/main)" ] || fail "HEAD ($commit) is not origin/main: release from main"
  gh auth status >/dev/null 2>&1 || fail "gh isn't logged in"
  [ "$(gh api "repos/$RELEASES_REPO" --jq .permissions.push 2>/dev/null)" = true ] ||
    fail "gh may not push to $RELEASES_REPO"
  ! release_exists || fail "the release v$version already exists in $RELEASES_REPO"
  ! image_exists || fail "the image $IMAGE:$version already exists"
  echo "publishing $version from $commit"
elif [ "$dated" != true ]; then
  echo "note: the CHANGELOG section is not dated yet (fine for a dry run, required for --publish)"
fi
out="$root/target/release-dist/$version"
rm -rf "$out"
mkdir -p "$out"

step "dependency licenses and bans"
compat/scripts/check-deps.sh

step "build"
# No local path may end up in a binary: the home directory, the registry sources
# and this checkout are remapped (a later --remap-path-prefix wins over an earlier).
cargo_home="${CARGO_HOME:-$HOME/.cargo}"
export RUSTFLAGS="--remap-path-prefix=$HOME=/home/builder --remap-path-prefix=$cargo_home/registry/src=/cargo/registry/src --remap-path-prefix=$root=/build/cabby"
for t in "${LINUX_TARGETS[@]}"; do
  mise exec "$ZIG" "$ZIGBUILD" -- cargo zigbuild --release --locked -p cabby-server \
    --bin cabby --bin cabby-healthcheck --no-default-features --target "$t.$GLIBC_MAX"
done
mise exec -- cargo build --release --locked -p cabby-server --bin cabby --no-default-features --target "$MAC_TARGET"
unset RUSTFLAGS
bins=()
for t in "${LINUX_TARGETS[@]}"; do bins+=("target/$t/release/cabby" "target/$t/release/cabby-healthcheck"); done
bins+=("target/$MAC_TARGET/release/cabby")

step "no local paths in the binaries"
for b in "${bins[@]}"; do
  leaks="$(strings -a "$b" | grep -oE '/(Users|home)/[^/[:space:]"]*' | grep -vx '/home/builder' | sort -u || true)"
  [ -z "$leaks" ] || fail "$b contains local paths: $(tr '\n' ' ' <<<"$leaks")"
  echo "$b: clean"
done

step "glibc $GLIBC_MAX baseline and smoke tests"
expected="cabby $version (AnyCable 1.6.21 compatible)"
version_sort() { sort -t. -k1,1n -k2,2n -k3,3n "$@"; }
for t in "${LINUX_TARGETS[@]}"; do
  case "$t" in x86_64-*) platform=linux/amd64 ;; aarch64-*) platform=linux/arm64 ;; esac
  dir="$root/target/$t/release"
  symbols="$(docker run --rm --platform "$platform" -v "$dir:/dist:ro" "$ALPINE" sh -c \
    'apk add --no-cache -q binutils >/dev/null && for b in /dist/cabby /dist/cabby-healthcheck; do objdump -T "$b"; done')"
  newest="$( (grep -oE 'GLIBC_[0-9]+(\.[0-9]+)*' <<<"$symbols" || true) | sed 's/^GLIBC_//' | version_sort -u | tail -1)"
  [ -n "$newest" ] || fail "$t: objdump -T found no GLIBC_ symbols"
  [ "$(printf '%s\n%s\n' "$newest" "$GLIBC_MAX" | version_sort | tail -1)" = "$GLIBC_MAX" ] ||
    fail "$t needs GLIBC_$newest, newer than $GLIBC_MAX"
  got="$(docker run --rm --platform "$platform" -v "$dir:/dist:ro" --entrypoint /dist/cabby "$DISTROLESS" --version)"
  [ "$got" = "$expected" ] || fail "$t --version printed '$got'"
  echo "$t: newest glibc symbol GLIBC_$newest; $got"
done
got="$("target/$MAC_TARGET/release/cabby" --version)"
[ "$got" = "$expected" ] || fail "$MAC_TARGET --version printed '$got'"
echo "$MAC_TARGET: $got"

step "notices, README and release notes"
mise exec "$CARGO_ABOUT" -- env OUT="$out/THIRD_PARTY_LICENSES.md" compat/scripts/third-party-licenses.sh
grep -q '^## sqlite-PUBLIC-DOMAIN' "$out/THIRD_PARTY_LICENSES.md" || fail "THIRD_PARTY_LICENSES.md lacks SQLite's notice"
{
  cat LICENSE
  printf '\n\n---\n\nPortions derived from AnyCable\n==============================\n\n'
  printf 'Cabby is derived from AnyCable (https://github.com/anycable/anycable), which is\n'
  printf 'released under the MIT License reproduced below.\n\n'
  cat compat/licenses/anycable-MIT-LICENSE
} >"$out/LICENSE"
sed "s/@VERSION@/$version/g" dist/README.md >"$out/README.md"
{
  compat/scripts/release-meta.sh notes "$version"
  echo
  echo "### Install"
  echo
  echo "Download the archive for your platform and \`SHA256SUMS\`, then check it with"
  echo "\`shasum -a 256 -c SHA256SUMS --ignore-missing\` (or \`sha256sum -c --ignore-missing\`)."
  echo "Docker: \`docker pull $IMAGE:$version\`. Guides: https://docs.cabbyhq.com/install"
  echo
  echo "Built from $SOURCE_REPO commit [\`${commit:0:12}\`](https://github.com/$SOURCE_REPO/commit/$commit)."
} >"$out/notes.md"

step "archives"
export COPYFILE_DISABLE=1 # macOS tar: no AppleDouble ._ entries
for t in "${LINUX_TARGETS[@]}" "$MAC_TARGET"; do
  name="cabby-$version-$t"
  stage="$out/stage/$name"
  mkdir -p "$stage"
  cp "target/$t/release/cabby" "$stage/cabby"
  cp "$out/LICENSE" "$out/THIRD_PARTY_LICENSES.md" "$out/README.md" "$stage/"
  cp CHANGELOG.md "$stage/"
  chmod 0755 "$stage/cabby"
  chmod 0644 "$stage/LICENSE" "$stage/THIRD_PARTY_LICENSES.md" "$stage/README.md" "$stage/CHANGELOG.md"
  tar --no-mac-metadata --no-xattrs --format ustar --uid 0 --gid 0 --uname root --gname root \
    -czf "$out/$name.tar.gz" -C "$stage" "${ARCHIVE_FILES[@]}"
  # The install docs run `tar -xzf` and expect ./cabby: the files at the archive
  # root, in this order, root-owned, cabby executable.
  listing="$(tar -tvf "$out/$name.tar.gz")"
  echo "$listing"
  [ "$(awk '{print $NF}' <<<"$listing" | tr '\n' ' ')" = "${ARCHIVE_FILES[*]} " ] || fail "$name.tar.gz: unexpected entries"
  [ "$(awk '{print $3 ":" $4}' <<<"$listing" | sort -u)" = "root:root" ] || fail "$name.tar.gz: entries not owned by root"
  awk '$NF == "cabby"' <<<"$listing" | grep -q '^-rwxr-xr-x' || fail "$name.tar.gz: cabby is not executable"
done
(cd "$out" && shasum -a 256 cabby-*.tar.gz | LC_ALL=C sort -k2 >SHA256SUMS && shasum -a 256 -c SHA256SUMS)

step "image"
ctx="$out/image"
for t in "${LINUX_TARGETS[@]}"; do
  case "$t" in x86_64-*) arch=amd64 ;; aarch64-*) arch=arm64 ;; esac
  mkdir -p "$ctx/dist/linux-$arch"
  cp "target/$t/release/cabby" "target/$t/release/cabby-healthcheck" "$ctx/dist/linux-$arch/"
done
cp Dockerfile "$out/LICENSE" "$out/THIRD_PARTY_LICENSES.md" "$ctx/"
test_tag="cabby-release-test:$version"
volume="cabby-release-test-$$"
containers=()
cleanup() {
  for c in ${containers[@]+"${containers[@]}"}; do docker rm -f "$c" >/dev/null 2>&1 || true; done
  docker volume rm "$volume" >/dev/null 2>&1 || true
  docker image rm "$test_tag" >/dev/null 2>&1 || true
}
trap cleanup EXIT
wait_healthy() {
  local health=starting
  for _ in $(seq 1 30); do
    health="$(docker inspect -f '{{.State.Health.Status}}' "$1")"
    [ "$health" = healthy ] && return 0
    sleep 1
  done
  docker logs "$1"
  fail "the container is $health after 30 s"
}
docker buildx build --builder "$BUILDER" --platform linux/arm64 --build-arg "VERSION=$version" \
  --load -t "$test_tag" "$ctx"
got="$(docker run --rm "$test_tag" --version)"
[ "$got" = "$expected" ] || fail "image --version printed '$got'"
[ "$(docker inspect -f '{{.Config.User}}' "$test_tag")" = nonroot ] || fail "the image doesn't run as nonroot"
cid="$(docker run -d "$test_tag" --norpc --public)"
containers+=("$cid")
wait_healthy "$cid"
echo "image: $got; runs as nonroot; healthy"

# The Kamal set-up (docs/deploy-kamal.md): with a secret, broadcasts are served on
# CABBY_HTTP_BROADCAST_PORT=8090 and not on the published port.
cid="$(docker run -d -p 127.0.0.1::8080 -p 127.0.0.1::8090 -e CABBY_SECRET=s3cret -e CABBY_BROADCAST_KEY=bkey \
  -e CABBY_HTTP_BROADCAST_PORT=8090 "$test_tag" --norpc)"
containers+=("$cid")
p8080="$(docker port "$cid" 8080/tcp | head -1 | cut -d: -f2)"
p8090="$(docker port "$cid" 8090/tcp | head -1 | cut -d: -f2)"
for _ in $(seq 1 30); do curl -sf "localhost:$p8080/health" >/dev/null && break; sleep 1; done
post() {
  curl -s -o /dev/null -w '%{http_code}' -X POST "localhost:$1/_broadcast" -H 'Authorization: Bearer bkey' \
    -H 'Content-Type: application/json' -d '{"stream":"demo","data":"{}"}'
}
on_8090="$(post "$p8090")"
on_8080="$(post "$p8080")"
[ "$on_8090" = 201 ] && [ "$on_8080" = 404 ] || { docker logs "$cid"; fail "broadcasts: 8090 answered $on_8090, 8080 answered $on_8080"; }
echo "image: broadcasts on 8090 ($on_8090), not on 8080 ($on_8080)"

# The SQLite broker on a new named volume at /var/lib/cabby: the volume takes the
# image directory's ownership (nonroot, 0700), so the server can create its files.
docker volume create "$volume" >/dev/null
cid="$(docker run -d -v "$volume:/var/lib/cabby" "$test_tag" --norpc --public \
  --broker=sqlite --sqlite_path=/var/lib/cabby/cabby.db)"
containers+=("$cid")
wait_healthy "$cid"
files="$(docker run --rm -v "$volume:/data:ro" "$ALPINE" sh -c 'stat -c "%a %u:%g %n" /data /data/cabby.db /data/cabby.db.lock')" ||
  { docker logs "$cid"; fail "the SQLite broker's files are missing on the volume"; }
echo "$files"
grep -qx '700 65532:65532 /data' <<<"$files" || fail "the volume is not nonroot's with mode 0700"
echo "image: SQLite broker on a named volume; healthy; database and lock file created"

multiarch=(docker buildx build --builder "$BUILDER" --platform linux/amd64,linux/arm64 --build-arg "VERSION=$version"
  -t "$IMAGE:$version")
if [ "$mode" = dry-run ]; then
  # Build both platforms, keep nothing: no local image carries a release tag.
  "${multiarch[@]}" --output type=cacheonly "$ctx"
  step "dry run done: $out"
  ls -l "$out"
  echo
  echo "Not published. With --publish this would create a draft release v$version in $RELEASES_REPO"
  echo "from commit $commit with the files below, and push $IMAGE:$version:"
  (cd "$out" && ls cabby-*.tar.gz SHA256SUMS)
  exit 0
fi

step "publish"
flags=(--draft --title "Cabby $version" --notes-file "$out/notes.md")
[ "$prerelease" = true ] && flags+=(--prerelease)
gh release create "v$version" --repo "$RELEASES_REPO" "${flags[@]}" "$out"/cabby-*.tar.gz "$out/SHA256SUMS"
"${multiarch[@]}" --push "$ctx"
echo
echo "Created the draft v$version and pushed $IMAGE:$version. Review the draft, then run:"
echo "  compat/scripts/release.sh --promote"
