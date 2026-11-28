# Releasing Cabby

A release is built on a Mac with `compat/scripts/release.sh` and published to two places:

- **[`DaniG2k/cabby-releases`](https://github.com/DaniG2k/cabby-releases)**, which holds only
  release artifacts: one archive per platform and `SHA256SUMS`, with the `CHANGELOG.md` section
  as the release notes. Its README has the install instructions; update it by hand when they
  change.
- **`ghcr.io/danig2k/cabby`**, the multi-arch container image (`linux/amd64`, `linux/arm64`),
  tagged `<version>` and, for a final release, `latest`.

Every release builds the same artifacts:

| Artifact | Contents |
|---|---|
| `cabby-<version>-x86_64-unknown-linux-gnu.tar.gz` | Linux x86_64, glibc 2.28 or newer |
| `cabby-<version>-aarch64-unknown-linux-gnu.tar.gz` | Linux arm64, glibc 2.28 or newer |
| `cabby-<version>-aarch64-apple-darwin.tar.gz` | macOS, Apple silicon |
| `SHA256SUMS` | the archives' checksums |
| the image | `cabby` and `cabby-healthcheck` (Linux builds) on `gcr.io/distroless/cc-debian12:nonroot` |

Each archive holds exactly `cabby`, `LICENSE`, `THIRD_PARTY_LICENSES.md`, `README.md` and
`CHANGELOG.md` at its root, so `tar -xzf` leaves an executable `./cabby`. `LICENSE` is Cabby's
license followed by AnyCable's notice; `README.md` is [`dist/README.md`](../dist/README.md) with the
version filled in. Release builds use `-p cabby-server --no-default-features`, so they contain no
mruby (D-062), and no artifact contains `nats-server`.

## What you need

- A Mac with Apple silicon: the macOS build is native, and the Linux builds are cross-compiled
  with [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) against glibc 2.28.
- [mise](https://mise.jdx.dev). The script runs its pinned tools through it and installs them on
  first use: zig 0.15.2, cargo-zigbuild 0.24.1, cargo-about 0.9.3 (with its `cli` feature) and
  cargo-deny 0.20.2. Rust and `protoc` come from `mise.toml`.
- Docker Desktop with the `desktop-linux` buildx builder and the containerd image store (for
  multi-platform images). The checks run in containers, so Docker must be running.
- For publishing only: `gh` logged in with write access to `DaniG2k/cabby-releases`, and
  `docker login ghcr.io` with a token that can write packages.

## Cutting a release

1. **Gates.** Releases are built and checked locally with `compat/scripts/release.sh`
   (`.github/workflows/ci.yml` remains for CI if it is enabled). On an up-to-date `main`, run:

   ```sh
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo clippy -p cabby-server --all-targets -- -D warnings   # without mruby
   cargo test --workspace --lib --bins
   cargo test -p cabby-server --lib --bins                     # without mruby
   cargo test -p cabby-mruby-sys --tests
   cargo xtask check-inputs
   eval "$(compat/scripts/services.sh start)"
   cargo xtask test --backend rust
   compat/scripts/services.sh stop
   compat/scripts/check-deps.sh
   ```

2. **Version and changelog.** Set `[workspace.package] version` in `Cargo.toml`, run
   `cargo update --workspace`, and replace `Unreleased` with the date in the section heading
   (`## [0.2.0] - 2026-12-01`; the script accepts only a real `YYYY-MM-DD` date). Commit and
   merge.

3. **Dry run** (the default):

   ```sh
   compat/scripts/release.sh
   ```

   It builds and checks everything and publishes nothing. The artifacts land in
   `target/release-dist/<version>/`; look at the archives and `notes.md` (the release notes,
   which name the source commit).

4. **Publish** from `main`:

   ```sh
   compat/scripts/release.sh --publish
   ```

   Before it builds anything, it refuses to continue unless the changelog section is dated, the
   working tree is clean, `HEAD` is `origin/main` (after a `git fetch`), `gh` may push to
   `DaniG2k/cabby-releases`, and neither the release `v<version>` nor the image
   `ghcr.io/danig2k/cabby:<version>` exists yet. Then it runs the whole dry run, creates a
   **draft** release `v<version>` with the archives and `SHA256SUMS`, and pushes the image as
   `:<version>` only. `:latest` doesn't move yet.

5. **Review the draft** at https://github.com/DaniG2k/cabby-releases/releases (and try the
   `:<version>` image if you like), then **promote** it:

   ```sh
   compat/scripts/release.sh --promote
   ```

   It refuses unless the draft and the `:<version>` image exist, publishes the release, and
   points `:latest` at the same image digest (`docker buildx imagetools create`).

6. **Tag the source** at the commit you released:

   ```sh
   git tag -a "v$version" -m "Cabby $version" && git push origin "v$version"
   ```

A pre-release (a version such as `0.2.0-rc.1`) uses the changelog section of its release, is
marked as a pre-release, and `--promote` leaves `latest` where it is.

## What the script checks

- `compat/scripts/check-deps.sh`: every dependency has an allowed license (`deny.toml`).
- `compat/scripts/release-meta.sh`: `CHANGELOG.md` has a section for the version (and, for
  `--publish`, a dated one).
- No binary contains a local path. Builds remap the home directory, the cargo registry and the
  checkout (`RUSTFLAGS="--remap-path-prefix=…"`) to `/home/builder`, `/cargo/registry/src` and
  `/build/cabby`, and `strings` must find no other `/Users/…` or `/home/…` path.
- `objdump -T` (in an Alpine container of the binary's platform) finds no glibc symbol newer
  than `GLIBC_2.28` in the Linux `cabby` and `cabby-healthcheck`.
- Every `cabby` prints `cabby <version> (AnyCable 1.6.22 compatible)`; the Linux ones run in the
  image's base.
- Every archive lists exactly the five files, owned by `root`, with `cabby` executable (bsdtar
  with `--no-mac-metadata --no-xattrs --format ustar` and `COPYFILE_DISABLE=1`), and
  `SHA256SUMS` verifies.
- The image (native platform) prints the version, runs as `nonroot`, turns `healthy` within
  30 s, and, configured as in [`docs/deploy-kamal.md`](deploy-kamal.md), answers an authorized
  `POST /_broadcast` with 201 on port 8090 and 404 on port 8080. With a new named volume at
  `/var/lib/cabby` and `--broker=sqlite --sqlite_path=/var/lib/cabby/cabby.db`, it turns healthy
  and creates the database and its lock file, and the volume is owned by `nonroot` with mode
  0700. Then both platforms are built.

## When something goes wrong

- A failed dry run publishes nothing: fix the cause and run it again.
- `--publish` creates the draft before it pushes the image. If the push fails, delete the draft
  (`gh release delete "v$version" --repo DaniG2k/cabby-releases --yes`) and run `--publish`
  again once the cause is fixed. If the push succeeded but something after it failed, the
  existing `:<version>` image makes `--publish` refuse: promote the draft, or delete both the draft
  and the image tag before trying again.
- A published release is never changed. Fix forward with a new version.
