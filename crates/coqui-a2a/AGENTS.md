# `coqui-a2a` — API header

Header-file-style reference for the `coqui-a2a` crate: every public item with its signature, contract and gotchas, so the crate can be used without reading its source. It ships inside the package and is self-contained. Development documentation lives with the source at <https://github.com/Boriken-Dev/coqui-a2a>.

| Feature | Adds | Default |
| --- | --- | --- |
| `loopback` | module `loopback`: an HTTP endpoint on a loopback address (`axum`, `tokio`, `tokio-stream`, `futures-util`) | off |
| `cli` | the `coqui-a2a` command (`loopback`, `clap`, `coqui-peer-webcrypto`, `coqui-peer-tls`, `rustls`, `url`) | off |

MSRV Rust 1.89. Version `0.0.0-alpha.0`, not published. A default build depends on `coqui-agentic-peer`, `coqui-web`, `serde`, `serde_json` and `time`. No `unsafe` code. The crate holds the mapping of A2A onto a link and no proof, pairing, sealing or discovery: those are `coqui-agentic-peer`'s. The wire is specified in the binding page (<https://boriken-dev.github.io/coqui-a2a/binding/>).

Imports: the root re-exports `PROTOCOL`, `BINDING`, `A2A_VERSION`, `Agent`, `Caller`, `Events`, `Watch`, `Server`, `ServerLimits`, `Client`, `ClientError`, `ClientTimeouts`, `A2aError`, `ErrorKind`. Public modules: `agent`, `client`, `error`, `model`, `server`, `wire`, and `loopback` (feature).

## Constants (`coqui_a2a`)

```rust
pub const PROTOCOL: &str = "a2a/1";      // the link stream token, and the grant a peer needs
pub const BINDING: &str = "https://boriken.dev/a2a/bindings/coqui-agentic-peer/v1";   // an identifier nobody fetches
pub const A2A_VERSION: &str = "1.0";
```

## Server (`coqui_a2a::server`, `coqui_a2a::agent`)

```rust
pub trait Agent: Send + Sync {
    fn agent_card(&self, caller: &Caller<'_>) -> AgentCard;                                                          // required
    fn send_message(&self, caller: &Caller<'_>, request: SendMessageRequest) -> Result<SendMessageResponse, A2aError>; // required
    fn send_streaming_message(&self, caller: &Caller<'_>, request: SendMessageRequest) -> Result<Events, A2aError>;    // default: UnsupportedOperation
    fn get_task(&self, caller: &Caller<'_>, request: GetTaskRequest) -> Result<Task, A2aError>;                        // default: UnsupportedOperation
    fn list_tasks(&self, caller: &Caller<'_>, request: ListTasksRequest) -> Result<ListTasksResponse, A2aError>;       // default: UnsupportedOperation
    fn cancel_task(&self, caller: &Caller<'_>, request: CancelTaskRequest) -> Result<Task, A2aError>;                  // default: UnsupportedOperation
    fn subscribe_to_task(&self, caller: &Caller<'_>, request: SubscribeToTaskRequest) -> Result<Events, A2aError>;     // default: UnsupportedOperation
    fn get_extended_agent_card(&self, caller: &Caller<'_>, request: GetExtendedAgentCardRequest) -> Result<AgentCard, A2aError>;  // default: ExtendedAgentCardNotConfigured
}
pub type Events = Box<dyn Iterator<Item = Result<StreamResponse, A2aError>> + Send>;
pub struct Caller<'a> { pub peer: &'a str, pub claims: &'a BTreeMap<String, String>, pub service_parameters: &'a ServiceParameters }   // Copy
pub fn Caller::is_open(&self) -> bool          // false once the client left, the link went or the operation ended
pub fn Caller::watch(&self) -> Watch           // a handle an Events iterator keeps
pub fn Watch::is_closed(&self) -> bool         // Clone

pub struct Server;                              // Clone
pub fn Server::new(agent: impl Agent + 'static) -> Server
pub fn Server::with_limits(self, limits: ServerLimits) -> Server
pub fn Server::with_url(self, url: impl Into<String>) -> Server        // the `url` of the interface added to the card; default "coqui-agentic-peer:"
pub fn Server::handler(self) -> coqui_agentic_peer::link::Handler
pub fn Server::serving(self, services: Services) -> Services           // adds PROTOCOL
pub fn Server::serve(&self, stream: &dyn LinkStream, accepted: &Accepted)
pub struct ServerLimits { pub max_request: usize, pub request_timeout: Duration }   // non_exhaustive, Default: 1 MiB, 10 s
```

- Each stream runs on a thread of the link's, one operation per stream. A panic in an agent method is `InternalError` for that operation.
- Before an agent method runs the server checks the method, `a2a-version` (major 1), required extensions, streaming and extended-card capabilities against the card, and refuses any push-notification request. The card the server serves has `capabilities.pushNotifications` false and an interface for this binding first.
- A stream of items ends after the item that puts the task in a terminal state, or after a first item that is a message.
- An `Events` iterator runs on a thread of its own. A quiet one must return soon after its `Watch` closes, since the operation waits for it.
- `Caller::peer` is the identity: it comes from the link's proof. Authorize per caller; `ListTasks` returns only the caller's tasks.

## Client (`coqui_a2a::client`)

```rust
pub struct Client<'m, 'c, C: Channel>;
pub fn Client::new(mux: &'m Mux<'c, C>) -> Client            // sends service parameter a2a-version: 1.0
pub fn Client::with_service_parameter(self, name: &str, value: &str) -> Result<Client, A2aError>   // such as authorization
pub fn Client::with_service_parameters(self, parameters: ServiceParameters) -> Client              // replaces all, a2a-version included
pub fn Client::with_timeouts(self, timeouts: ClientTimeouts) -> Client
pub fn Client::agent_card(&self) -> Result<AgentCard, ClientError>
pub fn Client::send_message(&self, request: &SendMessageRequest) -> Result<SendMessageResponse, ClientError>
pub fn Client::send_streaming_message(&self, request: &SendMessageRequest) -> Result<Events<'c, C>, ClientError>
pub fn Client::get_task(&self, request: &GetTaskRequest) -> Result<Task, ClientError>
pub fn Client::list_tasks(&self, request: &ListTasksRequest) -> Result<ListTasksResponse, ClientError>
pub fn Client::cancel_task(&self, request: &CancelTaskRequest) -> Result<Task, ClientError>
pub fn Client::subscribe_to_task(&self, request: &SubscribeToTaskRequest) -> Result<Events<'c, C>, ClientError>
pub fn Client::get_extended_agent_card(&self) -> Result<AgentCard, ClientError>
pub fn Client::call_raw(&self, method: Method, params: Value) -> Result<Value, ClientError>             // the result member, unread
pub fn Client::stream_raw(&self, method: Method, params: Value) -> Result<RawEvents<'c, C>, ClientError>
impl Iterator for Events<'c, C>     { type Item = Result<StreamResponse, ClientError> }
impl Iterator for RawEvents<'c, C>  { type Item = Result<Value, ClientError> }
pub struct ClientTimeouts { pub answer: Duration, pub item: Option<Duration> }   // non_exhaustive, Default: 300 s, None

pub enum ClientError { Remote(A2aError), NotGranted(String), Revoked(String), Interrupted, Timeout, Malformed(&'static str), Link(Failure), TooLarge }  // non_exhaustive
```

- The client reads the first message of a streaming operation before it returns, so an operation refused up front is an `Err` of the call.
- An iterator ends with `None` only when the server finished the stream in order. A stream cut short yields `Err(Interrupted)` (or `Revoked`, `Link`) and then ends: it is never mistaken for complete.
- `NotGranted(reason)`: the peer does not serve `a2a/1` to this key (the link refused the open). Check `link::capabilities(mux, wait)?.offers` for `PROTOCOL` first when a clear message matters.
- Nothing is retried. After a lost link, `GetTask` then `SubscribeToTask`; never resend an unacknowledged send.
- The client holds `&Mux`, which blocks a thread: on Tokio, call it from `spawn_blocking`.

## Data model (`coqui_a2a::model`)

serde types for A2A 1.0 in ProtoJSON: camelCase members, enum names such as `TASK_STATE_WORKING`, omitted optionals, timestamps as ISO 8601 text.

```rust
pub struct Task, TaskStatus, Message, Part, Artifact, TaskStatusUpdateEvent, TaskArtifactUpdateEvent,
           SendMessageRequest, SendMessageConfiguration, GetTaskRequest, ListTasksRequest, ListTasksResponse,
           CancelTaskRequest, SubscribeToTaskRequest, GetExtendedAgentCardRequest,
           AgentCard, AgentInterface, AgentProvider, AgentCapabilities, AgentExtension, AgentSkill;   // Default; pub fields; `extra: Object` holds unknown members
pub enum TaskState, Role { .., Other(String) }     // unknown names are kept
pub enum StreamResponse { Task, Message, StatusUpdate, ArtifactUpdate }           // one member on the wire
pub enum SendMessageResponse { Task, Message }
pub type Object = serde_json::Map<String, Value>;
pub fn Part::text(text) -> Part;  pub fn Part::data(value) -> Part
pub fn Message::text(role: Role, message_id, text) -> Message;  pub fn Message::joined_text(&self) -> String
pub fn SendMessageRequest::new(message: Message) -> SendMessageRequest;  pub fn SendMessageRequest::validate(&self) -> Result<(), A2aError>
pub fn TaskState::is_terminal(&self) -> bool;  pub fn TaskState::is_interrupted(&self) -> bool
pub fn StreamResponse::ends_task(&self) -> bool
pub fn AgentCard::link_interface(&self) -> Option<&AgentInterface>
pub fn TaskStatus::now(state: TaskState) -> TaskStatus
pub fn timestamp_now() -> String                    // 2026-11-02T10:30:00.123Z
pub fn params_as<T: DeserializeOwned>(params: Value) -> Result<T, A2aError>
```

Opaque fields stay `serde_json::Value` or `Object`: `metadata`, `Part::data`, `securitySchemes`, `securityRequirements`, `signatures`, extension `params`, a push-notification configuration. The structs have public fields and are built with `..Default::default()`; a field the schema adds later arrives in `extra` first.

## Wire (`coqui_a2a::wire`)

```rust
pub enum Method { SendMessage, SendStreamingMessage, GetTask, ListTasks, CancelTask, SubscribeToTask, CreateTaskPushNotificationConfig, GetTaskPushNotificationConfig, ListTaskPushNotificationConfigs, DeleteTaskPushNotificationConfig, GetExtendedAgentCard, GetAgentCard }  // non_exhaustive
pub const Method::ALL: &[Method];  pub fn Method::name(self) -> &'static str;  pub fn Method::from_name(&str) -> Option<Method>;  pub fn Method::streams(self) -> bool;  pub fn Method::is_push(self) -> bool
pub struct ServiceParameters;      // names lower-cased; Debug shows names only
pub fn ServiceParameters::set(&mut self, name: &str, value: &str) -> Result<(), A2aError>   // 32 entries, name [a-z0-9-_.]{1,64}, value <= 8192 bytes without control characters
pub fn ServiceParameters::get(&self, name: &str) -> Option<&str>;  pub fn ServiceParameters::iter(&self);  pub fn ServiceParameters::is_empty(&self) -> bool
pub struct Request { pub id: Value, pub method: String, pub params: Value, pub service_parameters: ServiceParameters }
pub fn Request::new(id: Value, method: Method, params: Value) -> Request;  pub fn Request::encode(&self) -> Vec<u8>
pub fn Request::decode(bytes: &[u8]) -> Result<Request, (Value, A2aError)>      // the id to answer with, and the error; no part of the input
pub struct Response { pub id: Value, pub outcome: Result<Value, A2aError> }
pub fn Response::result(id, result) -> Response;  pub fn Response::error(id, error: A2aError) -> Response;  pub fn Response::encode(&self) -> Vec<u8>;  pub fn Response::decode(bytes: &[u8]) -> Option<Response>
pub const MAX_PARAMETERS: usize = 32;  MAX_PARAMETER_NAME: usize = 64;  MAX_PARAMETER_VALUE: usize = 8192;  PARAMETERS_MEMBER: &str = "a2a-service-parameters"
```

## Errors (`coqui_a2a::error`)

```rust
pub enum ErrorKind { TaskNotFound, TaskNotCancelable, PushNotificationNotSupported, UnsupportedOperation, ContentTypeNotSupported, InvalidAgentResponse, ExtendedAgentCardNotConfigured, ExtensionSupportRequired, VersionNotSupported, Unauthenticated, PermissionDenied, ParseError, InvalidRequest, MethodNotFound, InvalidParams, Internal }  // non_exhaustive, Copy
pub const ErrorKind::ALL: &[ErrorKind];                          // the whole table, in order
pub const fn ErrorKind::{code, reason, message, domain, name, is_a2a}(self)
pub fn ErrorKind::from_code(i64) -> Option<ErrorKind>;  pub fn ErrorKind::from_name(&str) -> Option<ErrorKind>     // name: "TaskNotFoundError"
pub struct A2aError { pub code: i64, pub message: String, pub data: Vec<Value> }     // impl Error; the code of a newer peer is kept
pub fn A2aError::new(kind: ErrorKind, message: impl Into<String>) -> A2aError;  pub fn A2aError::standard(kind) -> A2aError;  pub fn A2aError::task_not_found(id: &str) -> A2aError
pub fn A2aError::with_metadata(self, name: &str, value: &str) -> A2aError;  pub fn A2aError::kind(&self) -> Option<ErrorKind>
pub fn A2aError::to_value(&self) -> Value;  pub fn A2aError::from_value(value: &Value) -> Option<A2aError>
```

Each kind has a JSON-RPC `code`, an `ErrorInfo.reason` and a domain; the table is in the binding page and a test keeps the page and `ErrorKind::ALL` equal.

## Loopback (`coqui_a2a::loopback`, feature `loopback`)

```rust
pub struct Config;                                    // Clone
pub fn Config::new(address: SocketAddr) -> Result<Config, LoopbackError>   // refuses an address that is not loopback
pub fn Config::allow_origin(self, origin: impl Into<String>) -> Config      // by default any Origin is refused
pub fn Config::with_token(self, token: impl Into<String>) -> Config         // Authorization: Bearer on every request, not forwarded
pub fn Config::with_max_body(self, bytes: usize) -> Config                  // default 1 MiB
pub struct Hub;                                       // Default
pub fn Hub::new() -> Arc<Hub>;  pub fn Hub::is_up(&self) -> bool
pub fn Hub::attach<C: Channel>(&self, mux: &Mux<'_, C>)      // blocks inside Mux::run's body, until the link ends or close()
pub fn Hub::close(&self)
pub struct Loopback;                                  // a running endpoint; Drop stops it
pub fn Loopback::start(config: Config, hub: Arc<Hub>) -> Result<Loopback, LoopbackError>   // owns a runtime on a thread of its own
pub fn Loopback::local_addr(&self) -> SocketAddr
pub enum LoopbackError { NotLoopback, Io(io::Error) }   // non_exhaustive
```

`POST /` takes one JSON-RPC 2.0 request (`application/json`, at most the body limit) and answers JSON, or Server-Sent Events for `SendStreamingMessage` and `SubscribeToTask`. `GET /.well-known/agent-card.json` answers the link's card with `supportedInterfaces` replaced by this endpoint (and `signatures` dropped). Anything else is 404 or 405. `Host` has to be `127.0.0.1`, `[::1]` or `localhost` with the bound port; any `Origin` not listed is 403. Headers `a2a-*` and `authorization` become service parameters. With no link attached an operation answers 503.

## Command line

`coqui-a2a loopback --url <link> [--listen <loopback addr>] [--peer <id>]... [--identity <file>] [--allow-origin <origin>]... [--token-file <file>] [--ca-file <pem>]`. Prints `http://<addr>/` on standard output, then redials with backoff when the link ends. Exit 0 stopped, 1 failed, 2 usage.

## Environment variables

`COQUI_A2A_URL`, `COQUI_A2A_LISTEN`, `COQUI_A2A_TOKEN_FILE` (flags of `loopback`); `COQUI_AGENTIC_PEER_HOME` (the link crate's identity and trust store directory).

## Gotchas

- A peer must be **granted** `a2a/1` in the link's peer store; being paired for another protocol does not give it. A grant added or removed counts on a running link within its recheck, and a running stream of a removed grant ends as `Revoked`.
- The link replays nothing: a stream cut short is incomplete. Reopen with `GetTask` and `SubscribeToTask`.
- Anything that can reach the loopback port uses what the link's peer granted, including other users of the machine. Use `--token-file` where the machine is shared.
- The endpoint's streaming responses end when the link's stream ends; an HTTP client that leaves closes the link stream, which does not cancel the task.
- `wss://` links: without `--ca-file` the certificate is not checked and the in-band proof alone names the peer.
