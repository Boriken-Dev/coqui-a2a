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
