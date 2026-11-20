#!/usr/bin/env bash
# Installs Soketi for the Pusher benchmark (no sudo, nothing global): mise's Node of .node-version
# (uWebSockets.js 20.10, which Soketi 1.6 pins, ships binaries for Node 14 to 18 only) and the
# locked packages into bench/soketi/node_modules (gitignored).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
version="$(cat "${here}/.node-version")"
mise install "node@${version}"
node_bin="$(mise where "node@${version}")/bin"
cd "${here}"
PATH="${node_bin}:${PATH}" npm ci --omit=dev --no-audit --no-fund
"${here}/sut" --version
