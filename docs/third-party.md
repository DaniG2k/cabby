# Third-party software

Cabby statically links SQLite (public domain), whose C source the `libsqlite3-sys` crate bundles,
for the SQLite broker; its notice is `compat/licenses/sqlite-PUBLIC-DOMAIN`. With the optional
mruby scripting feature (cargo feature `mruby`, off by default and not in release builds) it also
links mruby 1.2.0 (MIT, © mruby developers) with the mruby-env gem (MIT, © Internet Initiative
Japan Inc.), vendored in [`crates/cabby-mruby-sys`](../crates/cabby-mruby-sys). Their notices are
`compat/licenses/mruby-MITL` and `compat/licenses/mruby-env-LICENSE`; the binding's API is modeled
on go-mruby (see [`docs/mruby.md`](mruby.md)).

Embedded NATS runs the separate `nats-server` binary (Apache-2.0, © The NATS Authors), which
Cabby does not include: release archives and the Docker image contain neither `nats-server` nor
mruby. Every archive and image ships `THIRD_PARTY_LICENSES.md`, the licenses of the Rust crates
in the binary plus the notices above.
