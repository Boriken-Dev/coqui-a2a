# coqui-a2a

[![License: MPL-2.0](https://img.shields.io/badge/license-MPL--2.0-blue.svg)](https://github.com/Boriken-Dev/coqui-a2a/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-online-blue.svg)](https://boriken-dev.github.io/coqui-a2a/)
[![CI](https://img.shields.io/github/actions/workflow/status/Boriken-Dev/coqui-a2a/test.yml)](https://github.com/Boriken-Dev/coqui-a2a/actions/workflows/test.yml)

**A2A over a link a peer has already proved, as a custom protocol binding.** A peer paired once for ACP, or anything else on a [`coqui-agentic-peer`](https://github.com/Boriken-Dev/coqui-agentic-peer) link, is granted `a2a/1` and uses A2A on the same connection, with no second pairing. A stream carries A2A's own JSON, and the loopback command lets unmodified A2A software use a link.

| Path | What it is |
| --- | --- |
| `crates/coqui-a2a` | the Rust library and the `coqui-a2a` command |
| `docs/binding.md` | the binding's specification |
| `docs/loopback.md` | the loopback command |
| `vectors/binding.json` | the binding's messages as data |
| `scripts/sdk-check.mjs` | the official JavaScript SDK through the command |

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
```

## License

MPL-2.0; see [LICENSE](https://github.com/Boriken-Dev/coqui-a2a/blob/main/LICENSE).
