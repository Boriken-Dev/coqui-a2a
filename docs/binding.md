# The coqui-agentic-peer binding for A2A, version 1

This page specifies an [A2A custom protocol binding](https://a2a-protocol.org/latest/topics/custom-protocol-bindings/) that carries A2A 1.0 over a proven `coqui-agentic-peer` link. It is written to be implemented without reading this repository's code. `vectors/binding.json` holds messages as data, and the table in [Errors](#errors) is checked against the code by a test.

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

## Identification

| | |
| --- | --- |
| Binding identifier (`protocolBinding`) | `https://boriken.dev/a2a/bindings/coqui-agentic-peer/v1` |
| A2A version carried | `1.0` |
| Link stream protocol token | `a2a/1` |

The identifier is a URI as section 5.8 of the A2A specification asks. Nobody fetches it: a client compares it as a string. A breaking change to this page takes a new identifier (`.../v2`) and a new token (`a2a/2`); a change that keeps every message valid does not.

An Agent Card announces the binding with one entry of `supportedInterfaces`:

```json
{
  "url": "wss://agent.example.com/link",
  "protocolBinding": "https://boriken.dev/a2a/bindings/coqui-agentic-peer/v1",
  "protocolVersion": "1.0"
}
```

`url` names the link: the address the link is reached at (`wss://...`), or `coqui-agentic-peer:` followed by the peer's id when the card's publisher does not know one. It is a name and not an instruction: a client MUST NOT dial a `url` it reads from a card to use the binding. The binding is reached only over a link the client has already proved with the peer, so the card is read from that link (see [The Agent Card](#the-agent-card)).

## What a link is

A link is a proven, sealed connection between two peers that carries named, ordered, flow-controlled streams. It is specified, with its frames, in [`docs/link.md` of coqui-agentic-peer](https://boriken-dev.github.io/coqui-agentic-peer/link/). This binding needs only this of it:

- a stream is opened with a protocol token and carries whole messages in order, in each direction;
- either side can stop sending and go on reading (`finish`), which the other side sees as the end of the messages after everything that was sent. Only `finish` says that what was sent is whole: a stream or a link closed in order does not;
- a stream ends in one of six ways, each distinct: closed in order, `refused`, `broken`, `superseded`, `revoked`, or the link was lost;
- a peer that has proved its key may open `link/1`, which answers with the protocol tokens that peer may open. A token is a **grant** on that peer's entry in one trust store, so a peer paired once for another protocol gets `a2a/1` by a grant, without a second pairing.

The side that opens a stream is the **client** of the operation on it, and the side that accepts it is the **server**. Either end of a link can be either, by opening or accepting.

## One stream per operation

A client performs an operation like this:

1. open a stream with the token `a2a/1`;
2. send **one** message, the [request](#messages);
3. `finish` the stream, which says the request is complete;
4. read the [responses](#messages) until the stream ends;
5. close the stream.

A server may answer and close before the client's `finish` goes out. That `finish` then fails, and the answer already read or waiting to be read still stands.

A server, for each stream opened with `a2a/1`:

1. read one message, waiting at most its request timeout (10 seconds by default);
2. answer it with one response, or with a series of them for a [streaming operation](#streaming);
3. `finish` the stream, then close it. It MUST NOT `finish` a stream whose answer it could not send whole.

The server MUST NOT read a second operation from a stream. A message that follows the request is an error: the server closes the stream with the link's `refused` and the reason `one operation per stream`, and the client reads that as the operation being refused. A server MAY notice it late (it looks between the items it sends, and at least every 100 milliseconds on a quiet stream).

An operation that is not granted is not decided by this binding: the link answers the open with `refused` before any request is read. Granting is the link's.

## Messages

Every message is one UTF-8 JSON object, with no byte order mark and no framing of its own, since the link keeps message boundaries.

**Request.** JSON-RPC 2.0 in form:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "SendMessage",
  "params": { "message": { "messageId": "m1", "role": "ROLE_USER", "parts": [{ "text": "hello" }] } },
  "a2a-service-parameters": { "a2a-version": "1.0" }
}
```

- `jsonrpc` MUST be `"2.0"`. `id` is a string, a number or `null`, and is repeated in every response. `method` is one of the [operations](#operations). `params` is a JSON object, and may be absent when the method takes no fields.
- `a2a-service-parameters` is optional, see [Service parameters](#service-parameters).
- A receiver ignores members it does not know, so a later version can add them.

**Response.** A result or an error, never both:

```json
{ "jsonrpc": "2.0", "id": 1, "result": { "task": { "id": "t1", "status": { "state": "TASK_STATE_COMPLETED" } } } }
{ "jsonrpc": "2.0", "id": 1, "error": { "code": -32001, "message": "Task not found", "data": [ ] } }
```

`id` is the request's, or `null` when the server could not read one. A response to a request it could not read is sent all the same, and ends the stream.

## Operations

| A2A operation | `method` | Answer | Notes |
| --- | --- | --- | --- |
| Send message | `SendMessage` | one response, `result` is `{ "task": ... }` or `{ "message": ... }` | blocks until the task settles unless `configuration.returnImmediately` is true |
| Send streaming message | `SendStreamingMessage` | a [stream](#streaming) of `StreamResponse` results | needs `capabilities.streaming` |
| Get task | `GetTask` | one response, a Task | |
| List tasks | `ListTasks` | one response, a ListTasksResponse | `nextPageToken` is always present, empty on the last page |
| Cancel task | `CancelTask` | one response, a Task | |
| Subscribe to task | `SubscribeToTask` | a stream; the first result is the task as it now is | needs `capabilities.streaming`; refused for a task that has ended |
| Create, get, list, delete push notification config | `CreateTaskPushNotificationConfig`, `GetTaskPushNotificationConfig`, `ListTaskPushNotificationConfigs`, `DeleteTaskPushNotificationConfig` | an error | always `PushNotificationNotSupportedError`, see [Push notifications](#push-notifications) |
| Get extended Agent Card | `GetExtendedAgentCard` | one response, an AgentCard | needs `capabilities.extendedAgentCard` |
| (the binding's own) Get Agent Card | `GetAgentCard` | one response, an AgentCard | see [The Agent Card](#the-agent-card) |

Method names are the PascalCase names of A2A's JSON-RPC binding, so that the two bindings share one vocabulary. A server checks, in this order, before an agent sees a request: the method is known; the `a2a-version` is acceptable and every required extension is declared ([Service parameters](#service-parameters)); push notifications are not asked for; streaming and the extended card are declared by the card ([capability validation](https://a2a-protocol.org/latest/specification/), section 3.3.4); the parameters have the shape the operation needs. Each failure is the error [the table](#errors) names.

## Data mapping

The data model is A2A 1.0's, as ProtoJSON writes it, which is the form every A2A JSON binding uses.

| Subject | Rule |
| --- | --- |
| Field names | camelCase: `contextId`, `messageId`, `supportedInterfaces` |
| Enums | the string name of the value: `TASK_STATE_WORKING`, `ROLE_USER`. A name a receiver does not know MUST be kept and forwarded as received, never replaced by a default |
| Timestamps | ISO 8601 in UTC with a `Z` suffix, milliseconds where available: `2026-11-02T10:30:00.123Z`. No other offset |
| Binary data | `Part.raw` is a base64 string (RFC 4648, standard alphabet, with padding). Nothing is added: the binding has no binary framing, and a file larger than the link's message bound is sent by `url`, or in `TaskArtifactUpdateEvent` chunks with `append` |
| `oneof` groups | exactly one member is present: a Part holds one of `text`, `raw`, `url`, `data`; a SendMessageResponse holds `task` or `message`; a StreamResponse holds one of `task`, `message`, `statusUpdate`, `artifactUpdate` |
| Absent and empty | an absent optional field is omitted, never `null`; `ListTasks` with `includeArtifacts` false omits `artifacts` from each task |
| Unknown members | ignored when reading and kept when relaying, except in a card a relay serves ([The Agent Card](#the-agent-card)) |
| `google.protobuf.Struct` and `Any` | any JSON value |
| Required fields | a server SHOULD reject a request that lacks them with `InvalidParamsError` |

## Service parameters

Service parameters are the key-value context A2A defines for headers or metadata (A2A section 3.2.6).

- **Carrier.** The request's top-level member `a2a-service-parameters`, a JSON object whose values are strings. It is part of the request message, so it travels sealed inside the link, and the link's stream `metadata` is not used: that is opaque to the link and is not a credential.
- **Names** are case-insensitive and are lower-cased on receipt. A name is 1 to 64 bytes of `a-z`, `0-9`, `-`, `_` and `.`.
- **Values** are case-sensitive text of at most 8192 bytes with no control character.
- **At most 32** entries. A request outside these bounds is answered `InvalidRequestError`.
- Several values for one name are joined with a comma in one value, as in HTTP.
- A receiver ignores a name it does not know.
- A value MUST NOT be logged or placed in an error: it may hold a credential.

Defined names:

| Name | Meaning |
| --- | --- |
| `a2a-version` | the A2A version the client uses. The server accepts major version 1 (`1`, `1.0`, `1.2`) and otherwise answers `VersionNotSupportedError`; when several values are joined, every one has to be major version 1. Absent means no preference |
| `a2a-extensions` | comma-separated extension URIs the client uses. If the card lists an extension with `required: true` that is not named here the server answers `ExtensionSupportRequiredError` |
| `authorization` | the agent's own credential, if the agent wants one in addition to the link's proof. See [Authentication](#authentication-and-authorization) |
| the name an `apiKeySecurityScheme` gives | that credential, lower-cased as every name is. See [Authentication](#authentication-and-authorization) |

**Reserved by this binding:** the name `a2a-service-parameters` as the request member; the method name `GetAgentCard`; the prefix `coqui-` for service parameters, and `coqui-agentic-peer` anywhere in a name; the protocol tokens `a2a/N`. Names with the prefix `a2a-` belong to A2A.

## Errors

Every error is a JSON-RPC error object: `code` (an integer), `message` (a string for a person) and `data` (an array). The first element of `data` is a `google.rpc.ErrorInfo`: `{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": ..., "domain": ..., "metadata": {...}}`. `metadata` holds strings, such as `taskId`. A receiver that knows a `code` acts on the code; one that does not shows `message` and keeps the code.

| A2A error type | Code | `reason` | Domain | Meaning |
| --- | --- | --- | --- | --- |
| `TaskNotFoundError` | -32001 | `TASK_NOT_FOUND` | `a2a-protocol.org` | no such task, or not this caller's |
| `TaskNotCancelableError` | -32002 | `TASK_NOT_CANCELABLE` | `a2a-protocol.org` | the task is in a state that cannot be canceled |
| `PushNotificationNotSupportedError` | -32003 | `PUSH_NOTIFICATION_NOT_SUPPORTED` | `a2a-protocol.org` | any push-notification operation, or a webhook inside `SendMessage` |
| `UnsupportedOperationError` | -32004 | `UNSUPPORTED_OPERATION` | `a2a-protocol.org` | streaming or the extended card not declared, or the task has ended |
| `ContentTypeNotSupportedError` | -32005 | `CONTENT_TYPE_NOT_SUPPORTED` | `a2a-protocol.org` | a media type in the request is not supported |
| `InvalidAgentResponseError` | -32006 | `INVALID_AGENT_RESPONSE` | `a2a-protocol.org` | the agent's answer does not follow the specification, or is too large for the link |
| `ExtendedAgentCardNotConfiguredError` | -32007 | `EXTENDED_AGENT_CARD_NOT_CONFIGURED` | `a2a-protocol.org` | the card announces an extended card and none is configured |
| `ExtensionSupportRequiredError` | -32008 | `EXTENSION_SUPPORT_REQUIRED` | `a2a-protocol.org` | a required extension is not in `a2a-extensions` |
| `VersionNotSupportedError` | -32009 | `VERSION_NOT_SUPPORTED` | `a2a-protocol.org` | `a2a-version` names another major version |
| `UnauthenticatedError` | -32010 | `UNAUTHENTICATED` | `boriken.dev` | the agent wants its own credential and the request carried none or a wrong one |
| `PermissionDeniedError` | -32011 | `PERMISSION_DENIED` | `boriken.dev` | the caller is known and may not do this |
| `JSONParseError` | -32700 | `JSON_PARSE_ERROR` | `boriken.dev` | the message is not JSON |
| `InvalidRequestError` | -32600 | `INVALID_REQUEST` | `boriken.dev` | not a request this binding accepts: shape, size, a second operation, service parameters |
| `MethodNotFoundError` | -32601 | `METHOD_NOT_FOUND` | `boriken.dev` | no such method |
| `InvalidParamsError` | -32602 | `INVALID_PARAMS` | `boriken.dev` | the parameters are not valid for the method |
| `InternalError` | -32603 | `INTERNAL_ERROR` | `boriken.dev` | the server or the agent failed |

The first nine rows are the types of A2A section 3.3.2, with the codes of its section 5.4 (the JSON-RPC binding's), so a client that knows A2A's JSON-RPC binding reads them unchanged. The gRPC and HTTP columns of that table do not apply. The last seven are JSON-RPC's own five and the two the A2A specification leaves to a binding: A2A names an authentication or authorization error only as "a custom error" of each binding.

`message` is for a person: it never quotes the request, and a server SHOULD NOT put anything a caller is not allowed to know in it (a task that exists but is another caller's is `TaskNotFoundError`, as A2A section 3.3.2 asks).

### What the link says instead

Some outcomes arrive as the end of the stream, because they happen before or beneath any request:

| The stream ended | The client's reading |
| --- | --- |
| `refused` at the open, with a reason | the peer serves no `a2a/1`, or does not serve it to this key: not granted. The reason is for a person |
| `refused`, reason `one operation per stream` | the client sent a second message |
| `revoked`, with a reason | the grant went while the operation ran. Nothing more will come |
| `broken` | a bound was broken, such as a message past the link's size, or a client that left its window full for longer than the server waits: the operation is not complete |
| `superseded` | not used by this binding |
| the link was lost | the operation is not complete |
| closed in order with no `finish`, whatever came before it | not complete (a server finishes before it closes, after a whole answer and only then) |

A client MUST NOT treat any of these as a result.

## Streaming

`SendStreamingMessage` and `SubscribeToTask` are answered with a **series of response messages on the stream**, each with the request's `id` and a `result` holding one `StreamResponse`.

- **Order.** Items are sent in the order the agent generated them, and a stream delivers in order and never skips. A client therefore applies items in the order it reads them. Nothing is ordered across streams: two operations running at once have no order between them.
- **First item.** For `SendStreamingMessage`, a `task` or a `message`. For `SubscribeToTask`, the `task` as it now is.
- **Message-only.** If the first item is a `message`, it is also the last: the server finishes the stream after it.
- **Completion.** The server finishes the stream, then closes it, after the item that puts the task in a terminal state (`TASK_STATE_COMPLETED`, `FAILED`, `CANCELED`, `REJECTED`), or when the agent has no more items. A client reading the end of the messages after a `finish` knows the series is whole, and it has to ask whether that is what it read: the end of the messages reads the same after a stream closed in order. A stream that ends any other way ([above](#what-the-link-says-instead)) is **not** complete, and the client MUST say so and not present what it has as the whole.
- **Errors.** An error before the first item is the stream's one response. An error after items is the last message, and the stream ends after it. A client reads an `error` response as the end of the stream.
- **Backpressure.** Each stream has a window of credit, so a client that reads slowly stalls only its own stream and no other operation or protocol on the link. The server waits for credit, and items are neither dropped nor reordered. A server MAY give up on a client that gives no credit for a time of its choosing (30 seconds by default), and then closes the stream with `broken` and does not `finish` it.
- **Client leaves.** If the client closes its end, the server stops sending and releases the agent. This does **not** cancel the task: `CancelTask` does. A client that wants the rest reconnects as below.
- **Reconnection.** **The link replays nothing.** After a link or stream is lost, the client reads the task with `GetTask` and follows it with `SubscribeToTask`, whose first item is the task's state at that moment, so nothing is lost between the two. A client MUST NOT resend an unacknowledged `SendMessage` or `SendStreamingMessage` on its own: it cannot know whether the agent got it. It MAY resend with the same `messageId` when the agent is known to treat it as idempotent (A2A section 3.3.1). Deciding to reconnect and when is the host's; the binding adds no timer.
- **Silence.** A stream may be quiet for hours. A client has no per-item timeout by default. A link with a heartbeat tells a dead peer from a quiet one.

## Push notifications

Not supported in version 1. A push notification is an HTTP request to a URL the client provides, and a stream a client holds open already delivers events. A server:

- declares `capabilities.pushNotifications` `false` in every card it serves, and
- answers all four push-notification methods, and a `SendMessage` or `SendStreamingMessage` whose `configuration.taskPushNotificationConfig` is present, with `PushNotificationNotSupportedError`.

A later version may carry them, and will take a new identifier.

## The Agent Card

The standard binding serves the card at `/.well-known/agent-card.json`. A link has no such address and a discovery record says only that a peer offers `a2a/1`, so the card is read **from the link, after the proof**, with the method `GetAgentCard`: no parameters, and the result is an AgentCard.

- Any peer that may open `a2a/1` may call it. A peer that may not never reaches it.
- The server puts an interface entry with this binding first in `supportedInterfaces`, and sets `capabilities.pushNotifications` to `false`.
- `GetAgentCard` is not an A2A operation and is not offered over HTTP by a relay; a relay reads the card with it and serves it at the well-known path with its own `supportedInterfaces`. A client takes such a card to be the relay's own, so a relay keeps only what says what the agent is: it drops any member it does not know, such as the `url` and `additionalInterfaces` of earlier versions of A2A, a signature, since the card is not the one signed, and any scheme that names a URL to send a credential to, and it says `capabilities.pushNotifications` is `false`.
- `GetExtendedAgentCard` works as A2A section 3.1.11 says. The caller is the peer the link proved, plus any credential the agent also asks for.

A card MAY carry `securitySchemes` and `securityRequirements`. They describe what the agent asks for **in addition** to the link; the link's proof is never listed there, since A2A's schemes have no way to name an in-band key proof.

## Several agents behind one card

A link has one card, and a host may serve several agents on it. The card says which, and a request chooses one with the member A2A already defines for the purpose, `tenant`.

- **The card.** Each agent is one entry of `skills`, and the entry's `id` is the agent's tenant name. A host that serves one agent needs no more than that agent's card. The card a peer reads is the one made for that peer, so it lists the agents that peer may use and no others.
- **The request.** `tenant` is a member of the request's `params`, in every operation that defines it. `SendMessage` and `SendStreamingMessage` choose the agent that does the work: `tenant` is a skill `id` of the caller's card. With no `tenant`, a card of exactly one skill means that one; a card of several means none was chosen. Either failure, and a `tenant` the card does not list, is `InvalidParamsError`, and its message names the ids the card lists. An agent the peer may not use and an agent that does not exist are answered the same, and the message names only what the peer's card shows, so the refusal reveals nothing the card did not.
- **Tasks.** The other operations name a task by `id`. A `tenant` on them narrows the request to the tasks that were made under it: a task of another tenant is `TaskNotFoundError`, as is one that another peer owns. With no `tenant`, the operation looks at every task of the peer. `ListTasks` with a `tenant` lists the tasks of that tenant only.
- **The id is the name.** A tenant is compared as the exact text of the skill `id`, case included. A host that turns a name of its own into an id (a configured source, a directory) makes sure two names never give one id.

The Rust library does this in two places. `AgentCard::select_skill` is the choice of an agent from a request's `tenant`, and the optional task store keeps each task's tenant for the narrowing above.

## Authentication and authorization

**The link's proof is the authentication.** Before any byte of this binding is read, the link has proved which key each side holds and sealed everything after it. The server learns the peer's id from the proof, never from a request, and the link's grants, kept per peer by the host, decide whether the peer may open `a2a/1` at all and are checked again while a stream runs. The binding has no login, no token exchange and no certificate of its own.

An agent that also wants its own credential declares the scheme in its card's `securitySchemes` as for any binding, and the credential travels in a [service parameter](#service-parameters):

| The card's scheme | The service parameter |
| --- | --- |
| `httpAuthSecurityScheme` | `authorization`, as the HTTP header's value |
| `oauth2SecurityScheme`, `openIdConnectSecurityScheme` | `authorization`, `Bearer` and the access token, which the client got from the scheme's URLs by itself: nothing about getting one travels over the link |
| `apiKeySecurityScheme` | the name the scheme gives, wherever its `location` says, since a link has no query string and no cookies |
| `mtlsSecurityScheme` | none: a link shows no certificate of a client's to the agent, so an agent that insists on one cannot be reached over this binding, and does not offer it |

The binding does not bypass that control: a request without the credential gets `UnauthenticatedError`, whose `ErrorInfo.metadata` may hold a `challenge` string, which is how a challenge reaches a client; an authenticated caller who may not do the thing gets `PermissionDeniedError`. A credential is never put in an error or a log.

**Authorization is the agent's**, per caller. The caller's identity is the proven peer id and the claims the host stored for that peer. `ListTasks` MUST return only the caller's tasks, and a task another caller owns is `TaskNotFoundError`.

## Limits and hostile input

| Bound | Default | What happens past it |
| --- | --- | --- |
| Request size | 1 MiB (a server MUST accept at least 64 KiB) | `InvalidRequestError`, `the request is too large`, nothing parsed |
| Time to the request | 10 seconds | the stream is closed |
| Messages on a stream | one request | `refused`, `one operation per stream` |
| Service parameters | 32 entries, names 64 bytes, values 8192 bytes | `InvalidRequestError` |
| Message the link carries | set by the link (16 MiB) | the link breaks that stream (`broken`) and keeps the link |
| Streams and opens | set by the link (32 per link, 16 on one protocol, 600 opens a minute) | the link refuses the open |
| A client that gives no credit | 30 seconds | `broken` |

### Large payloads

A message has to fit the link's bound, and base64 makes a file a third larger. Past that, the file goes by `Part.url`, or in `TaskArtifactUpdateEvent` chunks with `append`. A request past the server's limit is answered as above. An answer the agent makes past the link's limit is `InvalidAgentResponseError`, and a relay MAY set a lower limit of its own, with the same error. A large message on one stream holds up no other: the link carries it in parts.

### Long-running tasks

A task may outlive any stream. `SendMessage` blocks until the task settles, for as long as the client waits, and the link has no timer of its own. For a task that takes longer than a client will wait, set `configuration.returnImmediately` and follow it with `GetTask` or `SubscribeToTask`. A stream may be quiet for hours, a stream's end cancels nothing, and only `CancelTask` does. See [Streaming](#streaming) for what a client does when a stream is lost.

A server never panics on input: bytes that are not JSON, JSON that is not a request, an unknown method, an `id` that is not a scalar, parameters of the wrong type, a batch (an array) and a second message each get an error response or the end of their own stream, and nothing else on the link is affected. An agent that fails is `InternalError` for that operation.

## Intentional differences from the standard bindings

- There is no `Content-Type`, HTTP status or header. The carriers are the request member `a2a-service-parameters` and the response members above.
- An error before the first item of a stream is that stream's only response, as a JSON-RPC answer.
- `GetAgentCard` exists, because there is no well-known URL.
- The authentication of a request is the link's proof.
- The two error types `UnauthenticatedError` and `PermissionDeniedError` are defined here.
- Push notifications are unsupported.
- Several agents on one card are told apart by `tenant`, which A2A's HTTP bindings put in the path and this one leaves in the parameters ([Several agents behind one card](#several-agents-behind-one-card)).

## An exchange

```text
client opens a2a/1                     (granted: the link answers; refused: the stream ends)
client -> {"jsonrpc":"2.0","id":1,"method":"SendStreamingMessage","params":{...},"a2a-service-parameters":{"a2a-version":"1.0"}}
client    finish
server -> {"jsonrpc":"2.0","id":1,"result":{"task":{"id":"t1","status":{"state":"TASK_STATE_WORKING"}}}}
server -> {"jsonrpc":"2.0","id":1,"result":{"artifactUpdate":{"taskId":"t1","contextId":"c1","artifact":{...},"lastChunk":true}}}
server -> {"jsonrpc":"2.0","id":1,"result":{"statusUpdate":{"taskId":"t1","contextId":"c1","status":{"state":"TASK_STATE_COMPLETED"}}}}
server    finish, close
```

## The checklist of the custom-binding guide

| Area | Where it is answered |
| --- | --- |
| All core operations | [Operations](#operations): every operation of A2A section 3.1, push notifications answered as unsupported (A2A section 3.3.4 requires that answer of an agent that does not support them) |
| Data type mappings | [Data mapping](#data-mapping) |
| Binary data encoding | base64 in `Part.raw`, see [Data mapping](#data-mapping) |
| Enum representation | ProtoJSON string names, unknown names kept |
| Timestamp format | ISO 8601, UTC, `Z`, milliseconds |
| Service parameters: mechanism | the request member `a2a-service-parameters` |
| Service parameters: encoding and size | names `[a-z0-9-_.]{1,64}`, values at most 8192 bytes, at most 32 |
| Service parameters: reserved names | [Service parameters](#service-parameters) |
| Error mapping | [Errors](#errors): every A2A type, a code and an `ErrorInfo` |
| Streaming mechanism | one response message per item on the operation's stream |
| Ordering | generation order, from the stream |
| Reconnection | none by the link; `GetTask` then `SubscribeToTask`, see [Streaming](#streaming) |
| Completion | the server's `finish` after the last item; any other end is not complete |
| Authentication | [Authentication and authorization](#authentication-and-authorization) |
| Credentials declared in the card | a service parameter per scheme, tabled in [Authentication and authorization](#authentication-and-authorization) |
| Authentication challenges | `UnauthenticatedError`, with `challenge` in its `ErrorInfo.metadata` |
| Large payloads | [Large payloads](#large-payloads) |
| Long-running tasks | [Long-running tasks](#long-running-tasks) |
| Intentional deviations | [above](#intentional-differences-from-the-standard-bindings) |
| Sample messages | [An exchange](#an-exchange) and `vectors/binding.json` |
