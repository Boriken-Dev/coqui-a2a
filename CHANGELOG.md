# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- The binding `https://boriken.dev/a2a/bindings/coqui-agentic-peer/v1` for A2A 1.0, specified in `docs/binding.md`: one `a2a/1` stream per operation, A2A's JSON in JSON-RPC form, service parameters in the request, errors as JSON-RPC errors with `ErrorInfo`, the link's proof as the authentication, and push notifications declared unsupported.
- `Server` and `Agent`: a `link::Handler` for `a2a/1` and the trait a host implements, with capability checks, version and extension checks, and a panic in an agent answered as `InternalError`.
- `Client`: `SendMessage`, `SendStreamingMessage`, `GetTask`, `ListTasks`, `CancelTask`, `SubscribeToTask`, `GetExtendedAgentCard` and the Agent Card over a link's mux, with a stream cut short reported as incomplete.
- `ErrorKind::ALL`: every A2A error type with its code, `ErrorInfo` reason and domain, walked by a test and compared with the table in the binding page.
- `loopback` and `coqui-a2a loopback`: a standard HTTP JSON-RPC and Server-Sent Events endpoint on a loopback address that translates to a link and serves the card at `/.well-known/agent-card.json`. It refuses a non-loopback bind, any `Origin` not listed, what a browser marks as coming from a page, and any `Host` that is not a loopback name. Every request shows a bearer token unless `--no-token` says otherwise, and `--peer` is required unless `--any-paired-peer` says otherwise. Connections, unfinished requests, stalled clients, answers waiting and the time an agent takes to answer are all bounded.
- `vectors/binding.json` and `scripts/sdk-check.mjs`, which runs `@a2a-js/sdk` 1.3.0 through the command.
- `coqui-a2a loopback` proves by its identity's method, Noise or WebCrypto, which coqui-agentic-peer keeps with the identity; a new identity is Noise's.

- `Caller::local` and `Server::call` / `Server::stream`: a host calls its own agent in process and meets the checks a link meets (method, version, required extensions, push notifications, the card's streaming and extended-card capabilities, a panic as `InternalError`), with the same errors. `Server::serve` runs the same checks, in the same order. `Watch::new` and `Watch::close` are public, and `Reply` is the answer of a call.

- Feature `tasks` and `tasks::TaskStore`: the tasks of an agent in memory, for a host that runs work for callers. It creates a task from a message, keeps its status, a bounded history and artifacts, owns each task to the `Caller::peer` that made it (another caller's id is `TaskNotFoundError`, `ListTasks` lists only the caller's), serves a subscriber the task as it is and then each update in order, refuses a message, an update, a subscription and a cancellation after a terminal state, and cuts off a subscriber that falls behind rather than hold up the work. Every limit (tasks, tasks per owner, history, artifacts, artifact bytes, subscribers, queue, page sizes) is a setting of `TaskStore::builder()`, and a cancel hook stops the work.

- `AgentCard::select_skill` and a section of the binding page, "Several agents behind one card": each agent is a `skills` entry whose `id` is its tenant name, and a request chooses one with `tenant`. No tenant is the card's one skill, and an unlisted tenant, no tenant on a card of several and a card of none are `InvalidParamsError` naming only the ids the caller's card lists.

### Changed

- The channel crate is `coqui-transport` (`coqui_transport::{Channel, Failure}`, pinned at `1283116`), replacing `coqui-web-channel`; the command dials with `coqui-transport-web` (`c8df842`, feature `cli`) and coqui-agentic-peer is at `8dad176`, which pins the same. The library's public API is unchanged: its types are generic over `coqui_transport::Channel`.
- The peer family is on `coqui-builder`: coqui-agentic-peer `2966a25`, coqui-peer `984db3d`, coqui-peer-tls `7cbd7ee`, coqui-peer-webcrypto `9146f55` and coqui-acp (tests only) `c15c0ff`, with one copy of each in the graph. The command builds its TLS client with `ClientBuilder::new().server(trust)`, and the tests build a WebCrypto method with `WebCrypto::builder().identity(..)`, whose `build()` can fail with a missing input. No wire, binding message or error changed.
