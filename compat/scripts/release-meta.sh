#!/usr/bin/env bash
# Release metadata for compat/scripts/release.sh (docs/releasing.md).
#   release-meta.sh check <tag>     verifies that <tag> is v<workspace version> and that
#                                   CHANGELOG.md has a section for it; prints
#                                   version=<v>, prerelease=<true|false> and
#                                   dated=<true|false> (true only for a heading
#                                   `## [<v>] - YYYY-MM-DD` with a real date)
#   release-meta.sh notes <version> prints the CHANGELOG.md section of <version>
# A pre-release (0.2.0-rc.1) uses the section of its release (0.2.0).
set -euo pipefail
cd "$(dirname "$0")/../.."

workspace_version() {
  sed -n '/^\[workspace\.package\]/,/^\[/s/^version = "\(.*\)"$/\1/p' Cargo.toml
}

heading() {
  grep -m1 -F "## [$1]" CHANGELOG.md || true
}

section() {
  awk -v head="## [$1]" '
    index($0, head) == 1 { on = 1; next }
    on && /^## \[/ { exit }
    on { print }
  ' CHANGELOG.md
}

case "${1:-}" in
  check)
    tag="${2:?usage: release-meta.sh check <tag>}"
    version="$(workspace_version)"
    if [ "$tag" != "v$version" ]; then
      echo "tag $tag does not match the workspace version $version (expected v$version)" >&2
      exit 1
    fi
    base="${version%%-*}"
    if [ -z "$(section "$base" | tr -d '[:space:]')" ]; then
      echo "CHANGELOG.md has no section for $base" >&2
      exit 1
    fi
    echo "version=$version"
    if [ "$base" = "$version" ]; then echo "prerelease=false"; else echo "prerelease=true"; fi
    date="$(heading "$base" | sed -n 's/^## \[[^]]*\] - \([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\)$/\1/p')"
    # A real calendar date: it must survive a round trip through date(1).
    if [ -n "$date" ] && [ "$(date -j -f %Y-%m-%d "$date" +%Y-%m-%d 2>/dev/null || date -d "$date" +%Y-%m-%d 2>/dev/null)" = "$date" ]; then
      echo "dated=true"
    else
      echo "dated=false"
    fi
    ;;
  notes)
    version="${2:?usage: release-meta.sh notes <version>}"
    section "${version%%-*}"
    ;;
  *)
    echo "usage: release-meta.sh check <tag> | notes <version>" >&2
    exit 2
    ;;
esac
