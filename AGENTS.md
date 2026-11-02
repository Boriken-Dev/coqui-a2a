# coqui-a2a — repository orientation

A2A running on a proven `coqui-agentic-peer` link, as an A2A custom protocol binding, and a loopback command so unmodified A2A software can use such a link.

| Shipped header | Covers |
| --- | --- |
| [`crates/coqui-a2a/AGENTS.md`](crates/coqui-a2a/AGENTS.md) | the `coqui-a2a` crate's public API |

## Layout

| Path | Responsibility |
| --- | --- |
| `docs/binding.md` | the binding's specification: usable without this code |
| `docs/loopback.md` | the loopback command |
| `vectors/binding.json` | binding messages as data, which every implementation runs |
| `crates/coqui-a2a/src` | `model` (A2A types), `wire`, `error`, `agent`, `server`, `client`, `loopback`, `main.rs` (the command) |
| `crates/coqui-a2a/tests` | link, pairing, loopback, command, scripted-peer, codec, error-table and layering tests; `support/` is a real proven link and an echo agent |
| `crates/coqui-a2a/examples/echo_server.rs` | an agent on a link, used by the SDK check |
| `scripts/sdk-check.mjs` | the official JavaScript SDK through the command; skips when the SDK is absent |

## Commands

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
mkdocs build --strict
# the SDK check (needs Node and `npm install @a2a-js/sdk` in a directory of your choice)
cargo build --all-features --bins --examples
COQUI_A2A_BIN=target/debug/coqui-a2a COQUI_A2A_ECHO=target/debug/examples/echo_server A2A_JS_SDK_DIR=<dir> node scripts/sdk-check.mjs
```

## Rules

- **This crate holds only the mapping of A2A onto a link.** Proof, pairing, sealing, channels, discovery and the companion are `coqui-agentic-peer`'s; a test (`tests/layering.rs`) fails when the library names one of its own. A need for something from the link crate is a change there.
- **The wire is a contract.** A change to a message, a code or a name changes `docs/binding.md` and `vectors/binding.json` in the same commit, and a change that breaks an existing message takes a new binding identifier.
- **The error table is data.** `ErrorKind::ALL` is the table; `tests/errors.rs` walks it and compares it to the table in `docs/binding.md`.
- **The link crate is pinned by git revision** in `crates/coqui-a2a/Cargo.toml` and in `[patch.crates-io]` of the root manifest. Move the pins together with `coqui-web` and `coqui-peer-webcrypto`.
- **The loopback endpoint is reachable by every program on the machine and by web pages.** It binds loopback only, refuses any `Origin` not listed, what a browser marks as coming from a page, and any `Host` that is not a loopback name, and takes a bearer token unless told by name not to.
- **Only `finish` says an answer is whole.** A server sends it after a whole answer and at no other time; a client that reads the end of a stream asks `finished_by_peer`.
- No push notifications in binding version 1; do not add a webhook sender.
- Files are LF.

## CI and release

`test.yml` (Rust on three systems and the MSRV, plus the SDK check) and `docs.yml` run on demand; `docs.yml` also deploys Pages on a push to `main` that touches the docs. `release.yml` gates a `v*` tag and publishes nothing yet. Pushing a `v*` tag needs the owner's consent for that release.
