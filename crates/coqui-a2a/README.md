# coqui-a2a

[![License: MPL-2.0](https://img.shields.io/badge/license-MPL--2.0-blue.svg)](https://github.com/Boriken-Dev/coqui-a2a/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-online-blue.svg)](https://boriken-dev.github.io/coqui-a2a/)
[![CI](https://img.shields.io/github/actions/workflow/status/Boriken-Dev/coqui-a2a/test.yml)](https://github.com/Boriken-Dev/coqui-a2a/actions/workflows/test.yml)

**A2A runs on a link a peer has already proved, as one more capability, with no second pairing.** The crate is the client and server sides of an [A2A custom protocol binding](https://boriken-dev.github.io/coqui-a2a/binding/) over [`coqui-agentic-peer`](https://github.com/Boriken-Dev/coqui-agentic-peer), and a command that lets unmodified A2A software use such a link through a loopback HTTP endpoint. Documentation is at <https://boriken-dev.github.io/coqui-a2a/>.

## Features

- One stream per operation, carrying A2A's own JSON, so the order of a streamed task is the stream's.
- `SendMessage`, `SendStreamingMessage`, `GetTask`, `ListTasks`, `CancelTask`, `SubscribeToTask`, `GetExtendedAgentCard` and the Agent Card, on both sides. Push notifications are declared unsupported and answer `PushNotificationNotSupportedError`.
- A table of every A2A error type to its wire form, walked by a test.
- The link's proof is the authentication. An agent's own token still travels, in service parameters.
- `coqui-a2a loopback`: a standard HTTP JSON-RPC endpoint on a loopback address, so a stock A2A client or SDK works on a link.
- `TaskStore` (feature `tasks`): task creation, status and artifacts, per-caller ownership, subscribers served the task as it is and then each update, terminal-state rules and every limit a setting.
- `Server::call` and `Server::stream`: a host calls its own agent in process, through the checks a link meets, as a `Caller::local`.

## Installation

Not on crates.io yet:

```sh
cargo add --git https://github.com/Boriken-Dev/coqui-a2a coqui-a2a
```

| Feature | Adds |
| --- | --- |
| `tasks` | the `tasks` module: a `TaskStore` for an agent's tasks, their owners, subscribers and limits (no dependency) |
| `loopback` | the `loopback` module: an HTTP endpoint on a loopback address (`axum`, `hyper`, `tokio`) |
| `cli` | the `coqui-a2a` command (`loopback`, `clap`, the link's identity and TLS pieces) |

A default build depends on `coqui-agentic-peer`, `coqui-transport`, `serde`, `serde_json` and `time`. MSRV is Rust 1.89.

## Quick start

A client, over the mux of a link that has proved its peer:

```rust,no_run
use coqui_a2a::model::{Message, Role, SendMessageRequest};
use coqui_a2a::{Client, ClientError};
use coqui_agentic_peer::mux::Mux;
use coqui_transport::Channel;

fn ask<C: Channel>(mux: &Mux<'_, C>) -> Result<(), ClientError> {
    let client = Client::new(mux);
    let request = SendMessageRequest::new(Message::text(Role::User, "m-1", "hello"));
    let answer = client.send_message(&request)?;
    println!("{answer:?}");
    for item in client.send_streaming_message(&request)? {
        println!("{:?}", item?);
    }
    Ok(())
}
```

A server: implement [`Agent`](https://boriken-dev.github.io/coqui-a2a/rust/coqui_a2a/trait.Agent.html) and hand it to the link as a service.

```rust,no_run
use coqui_a2a::error::A2aError;
use coqui_a2a::model::{AgentCard, SendMessageRequest, SendMessageResponse};
use coqui_a2a::{Agent, Caller, Server};
use coqui_agentic_peer::link::Services;

struct Mute;

impl Agent for Mute {
    fn agent_card(&self, _caller: &Caller<'_>) -> AgentCard {
        AgentCard::default()
    }

    fn send_message(
        &self,
        _caller: &Caller<'_>,
        _request: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2aError> {
        Err(A2aError::standard(coqui_a2a::ErrorKind::UnsupportedOperation))
    }
}

// `a2a/1` is now one of the protocols `link::serve` can open, for a peer granted it.
let _services = Server::new(Mute).serving(Services::default());
```

## Command line

`coqui-a2a loopback` dials a link, proves itself with this user's identity, and serves A2A over HTTP on a loopback address. It prints the endpoint on standard output, then the file holding the bearer token every request has to show, and runs until stopped.

```sh
coqui-a2a loopback --url ws://127.0.0.1:9000/link --peer sha256:<the agent's peer id>
```

| Flag | Environment | Meaning |
| --- | --- | --- |
| `--url` | `COQUI_A2A_URL` | the link to dial |
| `--listen` | `COQUI_A2A_LISTEN` | a loopback address, default `127.0.0.1:0`; anything else is refused |
| `--peer` | | the remote peer's id, pinned; required unless `--any-paired-peer` is given |
| `--any-paired-peer` | | accepts every peer paired in this user's trust store as the agent |
| `--identity` | | this side's key file, whose method (Noise or WebCrypto) it proves by; defaults to this user's |
| `--allow-origin` | | an `Origin` whose page may use the endpoint; by default any request with one is refused |
| `--forward-header` | | a header to pass to the agent as a service parameter |
| `--token-file` | `COQUI_A2A_TOKEN_FILE` | the file holding the bearer token; defaults to this user's, made the first time |
| `--no-token` | | serves with no token, to every program of every user on the machine |
| `--answer-timeout` | | seconds the agent has to say anything to an operation, default 300 |
| `--ca-file` | | roots that must vouch for a `wss://` certificate |

`COQUI_AGENTIC_PEER_HOME` places the identity and trust store. Exit status: 0 stopped, 1 failed (the proof was refused, the endpoint could not start), 2 the invocation was wrong. See the [loopback page](https://boriken-dev.github.io/coqui-a2a/loopback/).

## API overview

| Module | Purpose |
| --- | --- |
| `coqui_a2a` | `PROTOCOL`, `BINDING`, `Agent`, `Caller`, `Server`, `Reply`, `Client` and their errors |
| `model` | the A2A data model as serde types |
| `wire` | requests, responses and service parameters of the binding |
| `error` | every A2A error type, its code and its wire form |
| `tasks` | the in-memory task store (feature `tasks`) |
| `loopback` | the HTTP endpoint (feature `loopback`) |

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
```

`scripts/sdk-check.mjs` runs the official JavaScript SDK through the command and skips, saying so, when the SDK is absent.

### Releasing

Nothing is published yet. A release is a `v*` tag whose version has a `CHANGELOG.md` section, and needs the owner's consent.

## License

[MPL-2.0](https://github.com/Boriken-Dev/coqui-a2a/blob/main/LICENSE).
