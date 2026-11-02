# The loopback command

`coqui-a2a loopback` makes a link look like an ordinary A2A agent to software that knows nothing of links. It dials a link, proves itself with this user's identity, and serves the **JSON-RPC HTTP binding** of A2A 1.0 on a loopback address.

```sh
coqui-a2a loopback --url ws://127.0.0.1:9000/link --peer sha256:<the agent's peer id>
# http://127.0.0.1:41873/
```

An A2A client is then pointed at that address. Its `GET /.well-known/agent-card.json` returns the card the agent serves over the link, with `supportedInterfaces` replaced by this endpoint (`protocolBinding` `JSONRPC`, version `1.0`) and any `signatures` removed, since the rewritten card is not the one that was signed. `POST /` takes JSON-RPC requests and answers JSON, or Server-Sent Events for `SendStreamingMessage` and `SubscribeToTask`; `GetExtendedAgentCard` has its interfaces replaced the same way.

Each request becomes one stream on the link ([the binding](binding.md)). Headers `A2A-Version`, `A2A-Extensions` and any other `a2a-*`, and `Authorization`, become service parameters; errors come back as the JSON-RPC errors the binding tables. With no link up, the endpoint answers 503 and says so.

## Who can reach it

A loopback port is reachable by every program on the machine, and a web page can send requests to it. The endpoint therefore:

- **binds a loopback address only**, and refuses any other at start;
- **refuses any request with an `Origin`** unless `--allow-origin` lists it, including `null`, and answers no CORS headers, so a page can neither send nor read;
- **refuses a `Host` that is not a loopback name with the bound port** (`127.0.0.1`, `[::1]` or `localhost`), which is what DNS rebinding sends;
- takes only `POST /` with `application/json`, and only bodies up to a limit (1 MiB);
- with `--token-file`, **requires `Authorization: Bearer <token>`** and does not forward that header to the agent, so other programs on a shared machine cannot use the link's grants.

Without a token, every user of the machine can do what the link's peer granted. The link's proof, not this endpoint, is what the remote agent trusts.

## Flags

| Flag | Environment | Meaning |
| --- | --- | --- |
| `--url` | `COQUI_A2A_URL` | the link to dial: `ws://` or `wss://` |
| `--listen` | `COQUI_A2A_LISTEN` | a loopback address, default `127.0.0.1:0` |
| `--peer` | | the remote peer's id, pinned, repeatable; without it the paired peers of this user's trust store are accepted |
| `--identity` | | this side's key file; default this user's |
| `--allow-origin` | | an `Origin` to let through, repeatable |
| `--token-file` | `COQUI_A2A_TOKEN_FILE` | the bearer token every request must present |
| `--ca-file` | | roots that must vouch for a `wss://` certificate; without it the in-band proof alone names the peer |

It runs until stopped, redialing with a growing wait (at most 30 seconds) when the link ends, and exits 1 at once when the remote does not prove the expected key, since that will not mend itself. Exit 0 stopped, 1 failed, 2 the invocation was wrong.

## Checked with an unmodified SDK

`scripts/sdk-check.mjs` starts an agent on a real proven link, the command, and the official `@a2a-js/sdk` client: the SDK reads the card, completes a task, streams one and reads it back. It prints every request the SDK made, and skips with a message when the SDK or the two programs are absent.
