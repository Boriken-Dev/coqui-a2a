# The loopback command

`coqui-a2a loopback` makes a link look like an ordinary A2A agent to software that knows nothing of links. It dials a link, proves itself with this user's identity, and serves the **JSON-RPC HTTP binding** of A2A 1.0 on a loopback address.

```sh
coqui-a2a loopback --url ws://127.0.0.1:9000/link --peer sha256:<the agent's peer id>
# http://127.0.0.1:41873/
# /home/me/.config/coqui-agentic-peer/a2a-loopback.token
```

An A2A client is then pointed at that address, with `Authorization: Bearer <the contents of that file>` on every request. Its `GET /.well-known/agent-card.json` returns the card the agent serves over the link, kept for a minute, and reduced to what says what the agent is:

- `supportedInterfaces` is this endpoint (`protocolBinding` `JSONRPC`, version `1.0`);
- `capabilities.pushNotifications` is `false`, whatever the agent said;
- `signatures` goes, since the card is not the one that was signed, and so does any member A2A 1.0 does not define, such as `url` and `additionalInterfaces` of earlier versions, which would send a client around this endpoint;
- `securitySchemes` keeps API key, HTTP and mutual TLS schemes. OAuth and OpenID Connect schemes go, with the requirements that name them, since a client of a local endpoint would take their URLs for its own. A client that needs one is set up by hand, and sends the token it got in `Authorization` with `--no-token`.

`POST /` takes JSON-RPC requests and answers JSON, or Server-Sent Events for `SendStreamingMessage` and `SubscribeToTask`. `GetExtendedAgentCard` is rewritten the same way, and not kept.

Each request becomes one stream on the link ([the binding](binding.md)). Headers `A2A-Version`, `A2A-Extensions` and any other `a2a-*` become service parameters, several of one name joined with a comma, and so do `Authorization` when this endpoint has no token, and any header `--forward-header` names. Errors come back as the JSON-RPC errors the binding tables. With no link up, the endpoint answers 503 and says so. What the agent sends is carried as the JSON it is, not rewritten, and an answer that is not a response is `InvalidAgentResponseError`.

## Who can reach it

A loopback port is reachable by every program on the machine, and a web page can send requests to it. The endpoint therefore:

- **requires `Authorization: Bearer <token>`** of every request, the card included, and does not forward that header to the agent. By default the token is 32 random bytes in `a2a-loopback.token` in this user's directory, made the first time and the same from then on, and its path is the command's second line of output. The file is mode 600 on Unix. On Windows it takes its directory's access list, which under `%LOCALAPPDATA%` is this user's alone, and this does not check that list, so a `COQUI_AGENTIC_PEER_HOME` or a `--token-file` elsewhere is yours to keep private. `--token-file` names another file. On Unix a file group or others can reach is refused, and on every system so is one that is empty, or holds more than visible ASCII. The comparison takes the same time wherever the token differs, and the length of the token is not secret;
- **binds a loopback address only**, and refuses any other at start;
- **refuses any request with an `Origin`** unless `--allow-origin` lists it, including `null`;
- **refuses what a browser marks as coming from a page**: `Sec-Fetch-Site`, `Sec-Fetch-Dest`, `Sec-Fetch-User`, or a `Sec-Fetch-Mode` other than `cors`, which is how a page's `<img src>` or `no-cors` fetch arrives, with no `Origin`, unless a listed `Origin` came with it. `Sec-Fetch-Mode: cors` alone is not refused, since Node's `fetch` sends it with no page behind it. A browser too old to send these headers meets the token instead; with `--no-token` it can make the endpoint read the card, once for each time a kept card goes, and cannot read the answer;
- **refuses a `Host` that is not a loopback name with the bound port** (`127.0.0.1`, `[::1]` or `localhost`), which is what DNS rebinding sends;
- takes only `POST /` with `application/json`, only bodies up to a limit (1 MiB), and `GET` only of the card.

A listed `--allow-origin` gets the answers its browser asks for: a preflight, which holds no token and reaches no link, then `Access-Control-Allow-Origin` on every answer. The page still has to show the token.

`--no-token` serves with no token. Then every user of the machine can do what the link's peer granted and read the extended card, and `Authorization` goes to the agent. The link's proof, not this endpoint, is what the remote agent trusts.

`--peer` names the agent. Without it, `--any-paired-peer` accepts every peer paired in this user's trust store, any of which can then pose as the agent to whatever uses this endpoint.

## What a client or an agent can hold

- **Connections**: at most 64 at once. One more is answered 503 and closed. Each carries one request.
- **A request**: 10 seconds for its head, and 10 for its body, or the connection is closed. Anything that can reach the port can hold every place for that long, again and again, and no token is asked before the head has arrived.
- **A client that stops reading**: when the socket has taken nothing for 30 seconds while an answer waits, the connection is closed. A stream that ends that way, or because the link did, has no last chunk, which HTTP clients read as cut short. A stream with nothing to send is not held to this.
- **A client that leaves**: its stream on the link is closed within a fraction of a second, which does not cancel the task.
- **Answers waiting**: at most 64 MiB, and for one stream 2 MiB or one answer. Past that the stream on the link is not read, so the agent waits for its window. An agent can still make the command hold one message per stream in the link itself, up to 16 MiB each.
- **An agent that says nothing**: `--answer-timeout`, 300 seconds, then `InternalError`. `SendMessage` waits for the task to settle, so a longer task needs more, or `returnImmediately` and `GetTask`. Once a stream has begun it may be quiet for as long as it likes.

A stream ended by the agent with no `finish` reaches the client as a last JSON-RPC error, not as the end of the stream.

## Flags

| Flag | Environment | Meaning |
| --- | --- | --- |
| `--url` | `COQUI_A2A_URL` | the link to dial: `ws://` or `wss://` |
| `--listen` | `COQUI_A2A_LISTEN` | a loopback address, default `127.0.0.1:0` |
| `--peer` | | the remote peer's id, pinned, repeatable; required unless `--any-paired-peer` is given |
| `--any-paired-peer` | | accepts every peer paired in this user's trust store as the agent |
| `--identity` | | this side's key file, whose method (Noise or WebCrypto) it proves by; default this user's |
| `--allow-origin` | | an `Origin` whose page may use the endpoint, repeatable; refused at start if no browser could send it (`*`, `null`, a path) |
| `--forward-header` | | a header to pass to the agent as a service parameter, repeatable, for a credential its card names |
| `--token-file` | `COQUI_A2A_TOKEN_FILE` | the file holding the bearer token; default this user's, made the first time |
| `--no-token` | | serves with no token |
| `--answer-timeout` | | seconds the agent has to say anything to an operation, default 300 |
| `--ca-file` | | roots that must vouch for a `wss://` certificate; without it the in-band proof alone names the peer |

It prints the endpoint on standard output and, unless `--no-token` is given, the token file's path on the next line. It runs until stopped, redialing with a growing wait (at most 30 seconds) when the link ends, and exits 1 at once when the remote does not prove the expected key, since that will not mend itself. Exit 0 stopped, 1 failed, 2 the invocation was wrong.

## Checked with an unmodified SDK

`scripts/sdk-check.mjs` starts an agent on a real proven link, the command, and the official `@a2a-js/sdk` client: the SDK reads the card, completes a task, streams one and reads it back, showing the token the command made. It prints every request the SDK made, and skips with a message when the SDK or the two programs are absent.
