//! A standard A2A endpoint on a loopback address, translating to a link.
//!
//! It speaks the JSON-RPC HTTP binding of A2A 1.0, with Server-Sent Events for streams, so A2A
//! software that knows nothing of links runs on one. Anything that can reach a loopback port can
//! use what the link's peer granted, so the endpoint refuses a non-loopback address, any request
//! with an `Origin` unless it is listed, and any `Host` that is not a loopback name (a page can
//! reach loopback, and DNS rebinding makes it look like another host).
//!
//! The link side is [`Hub::attach`], called with the mux of a proven link; the HTTP side is
//! [`Loopback::start`], which owns a runtime of its own.

use core::fmt;
use core::time::Duration;
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request as HttpRequest, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::{get, post};
use coqui_agentic_peer::mux::{LinkState, Mux};
use coqui_web::Channel;
use futures_util::StreamExt as _;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::client::{Client, ClientError};
use crate::error::{A2aError, ErrorKind};
use crate::wire::{Method, ServiceParameters};
use crate::{A2A_VERSION, PROTOCOL};

/// How often an attached link looks for a job and for its own end.
const POLL: Duration = Duration::from_millis(100);
/// Items of one stream waiting for an HTTP client that reads slowly.
const STREAM_BACKLOG: usize = 16;

/// Why the endpoint could not start.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoopbackError {
    /// The address is not a loopback address.
    NotLoopback,
    /// The port could not be bound or the runtime could not start.
    Io(std::io::Error),
}

impl fmt::Display for LoopbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLoopback => f.write_str("the endpoint listens on a loopback address only"),
            Self::Io(error) => write!(f, "the endpoint could not start: {error}"),
        }
    }
}

impl core::error::Error for LoopbackError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::NotLoopback => None,
        }
    }
}

impl From<std::io::Error> for LoopbackError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Settings of the endpoint.
#[derive(Clone)]
pub struct Config {
    address: SocketAddr,
    origins: Vec<String>,
    token: Option<String>,
    max_body: usize,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("address", &self.address)
            .field("origins", &self.origins)
            .field("token", &self.token.as_ref().map(|_| "set"))
            .field("max_body", &self.max_body)
            .finish()
    }
}

impl Config {
    /// Settings for an endpoint at `address`, port 0 for any free port.
    ///
    /// # Errors
    ///
    /// [`LoopbackError::NotLoopback`] unless `address` is a loopback address.
    pub fn new(address: SocketAddr) -> Result<Self, LoopbackError> {
        if !is_loopback(address.ip()) {
            return Err(LoopbackError::NotLoopback);
        }
        Ok(Self {
            address,
            origins: Vec::new(),
            token: None,
            max_body: 1 << 20,
        })
    }

    /// Lets requests from `origin` (such as `http://127.0.0.1:3000`) through. By default every
    /// request that carries an `Origin` is refused.
    #[must_use]
    pub fn allow_origin(mut self, origin: impl Into<String>) -> Self {
        self.origins.push(origin.into());
        self
    }

    /// Requires `Authorization: Bearer <token>` of every request, and does not forward that header
    /// to the agent. Without it, every program of every user on this machine can use the link.
    #[must_use]
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Sets the largest request body, 1 MiB by default.
    #[must_use]
    pub fn with_max_body(mut self, bytes: usize) -> Self {
        self.max_body = bytes;
        self
    }
}

fn is_loopback(ip: IpAddr) -> bool {
    coqui_agentic_peer::forward::is_loopback(ip)
}

/// One operation waiting for the link.
enum Job {
    Call {
        method: Method,
        params: Value,
        parameters: ServiceParameters,
        reply: oneshot::Sender<Result<Value, A2aError>>,
    },
    Stream {
        method: Method,
        params: Value,
        parameters: ServiceParameters,
        items: mpsc::Sender<Result<Value, A2aError>>,
    },
}

/// Where the endpoint's operations go: to whichever link is attached.
#[derive(Default)]
pub struct Hub {
    jobs: Mutex<Option<std_mpsc::Sender<Job>>>,
    closed: AtomicBool,
}

impl fmt::Debug for Hub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hub").field("up", &self.is_up()).finish()
    }
}

impl Hub {
    /// A hub with no link.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Whether a link is attached.
    #[must_use]
    pub fn is_up(&self) -> bool {
        self.jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    fn submit(&self, job: Job) -> Result<(), Job> {
        let jobs = self.jobs.lock().unwrap_or_else(PoisonError::into_inner);
        match jobs.as_ref() {
            Some(sender) => sender.send(job).map_err(|rejected| rejected.0),
            None => Err(job),
        }
    }

    /// Ends the hub for good: a running [`Hub::attach`] returns, a later one returns at once, and
    /// operations are refused. The link itself is left to whoever owns it.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    /// Serves operations over `mux` until the link ends or [`Hub::close`] is called, then
    /// detaches. Blocks: call it inside `Mux::run`'s body. A second link attached meanwhile takes
    /// over new operations.
    pub fn attach<C: Channel>(&self, mux: &Mux<'_, C>) {
        let (sender, jobs) = std_mpsc::channel();
        *self.jobs.lock().unwrap_or_else(PoisonError::into_inner) = Some(sender);
        thread::scope(|scope| {
            loop {
                match jobs.recv_timeout(POLL) {
                    Ok(job) => {
                        scope.spawn(move || run_job(mux, job));
                    }
                    Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
                }
                if self.closed.load(Ordering::SeqCst) || mux.link_state() != LinkState::Up {
                    break;
                }
            }
        });
        // Operations that arrived while the link was ending are answered, not left waiting.
        *self.jobs.lock().unwrap_or_else(PoisonError::into_inner) = None;
        while let Ok(job) = jobs.try_recv() {
            refuse(job);
        }
    }
}

fn refuse(job: Job) {
    let error = A2aError::new(ErrorKind::Internal, "the link is down");
    match job {
        Job::Call { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Job::Stream { items, .. } => {
            let _ = items.blocking_send(Err(error));
        }
    }
}

fn run_job<C: Channel>(mux: &Mux<'_, C>, job: Job) {
    match job {
        Job::Call {
            method,
            params,
            parameters,
            reply,
        } => {
            let client = Client::new(mux).with_service_parameters(parameters);
            let _ = reply.send(client.call_raw(method, params).map_err(into_error));
        }
        Job::Stream {
            method,
            params,
            parameters,
            items,
        } => {
            let client = Client::new(mux).with_service_parameters(parameters);
            match client.stream_raw(method, params) {
                Ok(events) => {
                    for item in events {
                        let failed = item.is_err();
                        // The HTTP client leaving closes the channel, which closes the stream.
                        if items.blocking_send(item.map_err(into_error)).is_err() || failed {
                            break;
                        }
                    }
                }
                Err(error) => {
                    let _ = items.blocking_send(Err(into_error(error)));
                }
            }
        }
    }
}

/// How a failure of the link reads to an A2A client.
fn into_error(error: ClientError) -> A2aError {
    match error {
        ClientError::Remote(error) => error,
        ClientError::NotGranted(_) | ClientError::Revoked(_) => A2aError::new(
            ErrorKind::PermissionDenied,
            format!("the peer does not serve {PROTOCOL} to this link"),
        ),
        ClientError::Interrupted => A2aError::new(
            ErrorKind::Internal,
            "the stream ended before the operation was complete",
        ),
        ClientError::Timeout => {
            A2aError::new(ErrorKind::Internal, "the peer did not answer in time")
        }
        ClientError::Malformed(_) => A2aError::new(
            ErrorKind::InvalidAgentResponse,
            "the peer's answer is malformed",
        ),
        ClientError::TooLarge => {
            A2aError::new(ErrorKind::InvalidRequest, "the request is too large")
        }
        _ => A2aError::new(ErrorKind::Internal, "the link failed"),
    }
}

/// A running endpoint. Dropping it stops it.
pub struct Loopback {
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl fmt::Debug for Loopback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Loopback")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct Shared {
    hub: Arc<Hub>,
    origins: Arc<Vec<String>>,
    token: Option<Arc<String>>,
    address: SocketAddr,
}

impl Loopback {
    /// Binds the address and serves it on a runtime of its own.
    ///
    /// # Errors
    ///
    /// [`LoopbackError::Io`] when the port is taken or the runtime cannot start.
    pub fn start(config: Config, hub: Arc<Hub>) -> Result<Self, LoopbackError> {
        let listener = TcpListener::bind(config.address)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let state = Shared {
            hub,
            origins: Arc::new(config.origins),
            token: config.token.map(Arc::new),
            address,
        };
        let app = Router::new()
            .route("/", post(rpc))
            .route("/.well-known/agent-card.json", get(card))
            .layer(DefaultBodyLimit::max(config.max_body))
            .layer(middleware::from_fn_with_state(state.clone(), guard))
            .with_state(state);
        let (stop, stopped) = oneshot::channel::<()>();
        let thread = thread::Builder::new()
            .name("coqui-a2a-loopback".to_owned())
            .spawn(move || {
                let serving = async {
                    let Ok(listener) = tokio::net::TcpListener::from_std(listener) else {
                        return;
                    };
                    let serve = axum::serve(listener, app).into_future();
                    futures_util::pin_mut!(serve);
                    let _ = futures_util::future::select(serve, stopped).await;
                };
                runtime.block_on(serving);
                runtime.shutdown_timeout(Duration::from_millis(500));
            })?;
        Ok(Self {
            address,
            stop: Some(stop),
            thread: Some(thread),
        })
    }

    /// The address it listens on, with the port that was bound.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for Loopback {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn plain(status: StatusCode, text: &'static str) -> HttpResponse {
    (status, text).into_response()
}

/// Refuses what a web page or a rebinding attack would send, before anything else looks at it.
async fn guard(State(state): State<Shared>, request: HttpRequest, next: Next) -> HttpResponse {
    let headers = request.headers();
    if headers.contains_key(header::ORIGIN) {
        let allowed = headers
            .get(header::ORIGIN)
            .and_then(|origin| origin.to_str().ok())
            .is_some_and(|origin| state.origins.iter().any(|allowed| allowed == origin));
        if !allowed {
            return plain(StatusCode::FORBIDDEN, "an Origin is not accepted here");
        }
    }
    let host_ok = headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
        .is_some_and(|host| host_is_loopback(host, state.address.port()));
    if !host_ok {
        return plain(StatusCode::FORBIDDEN, "the Host is not a loopback name");
    }
    if let Some(token) = &state.token {
        let given = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if given != Some(token.as_str()) {
            return plain(StatusCode::UNAUTHORIZED, "a bearer token is required");
        }
    }
    next.run(request).await
}

/// `127.0.0.1:port`, `[::1]:port` or `localhost:port`, with the port this endpoint listens on.
fn host_is_loopback(host: &str, port: u16) -> bool {
    let Some((name, given)) = host.rsplit_once(':') else {
        return false;
    };
    if given.parse::<u16>() != Ok(port) {
        return false;
    }
    if name.eq_ignore_ascii_case("localhost") {
        return true;
    }
    let literal = name
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(name);
    literal.parse::<IpAddr>().is_ok_and(is_loopback)
}

/// The service parameters a request's headers give: `a2a-*`, and `authorization` unless this
/// endpoint spent it.
fn parameters_of(
    headers: &HeaderMap,
    spent_authorization: bool,
) -> Result<ServiceParameters, A2aError> {
    let mut parameters = ServiceParameters::new();
    for (name, value) in headers {
        let name = name.as_str();
        let wanted = name.starts_with("a2a-") || (name == "authorization" && !spent_authorization);
        if !wanted {
            continue;
        }
        let value = value
            .to_str()
            .map_err(|_| A2aError::new(ErrorKind::InvalidRequest, "a header is not text"))?;
        parameters.set(name, value)?;
    }
    Ok(parameters)
}

fn envelope(id: &Value, outcome: Result<Value, A2aError>) -> Value {
    match outcome {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err(error) => json!({ "jsonrpc": "2.0", "id": id, "error": error.to_value() }),
    }
}

fn json_response(status: StatusCode, body: &Value) -> HttpResponse {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

fn rpc_error(status: StatusCode, id: &Value, error: A2aError) -> HttpResponse {
    json_response(status, &envelope(id, Err(error)))
}

async fn rpc(State(state): State<Shared>, headers: HeaderMap, body: Bytes) -> HttpResponse {
    let json_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("application/json")
        });
    if !json_type {
        return plain(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "application/json is required",
        );
    }
    let Ok(Value::Object(mut request)) = serde_json::from_slice::<Value>(&body) else {
        let malformed = serde_json::from_slice::<Value>(&body).is_err();
        let kind = if malformed {
            ErrorKind::ParseError
        } else {
            ErrorKind::InvalidRequest
        };
        return rpc_error(StatusCode::OK, &Value::Null, A2aError::standard(kind));
    };
    let id = request.remove("id").unwrap_or(Value::Null);
    if !(id.is_string() || id.is_number() || id.is_null()) {
        return rpc_error(
            StatusCode::OK,
            &Value::Null,
            A2aError::new(
                ErrorKind::InvalidRequest,
                "the id is a string, a number or null",
            ),
        );
    }
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .and_then(Method::from_name);
    let params = request.remove("params").unwrap_or(Value::Null);
    // GetAgentCard belongs to the link binding: HTTP clients read the card at its well-known path.
    let Some(method) = method.filter(|method| *method != Method::GetAgentCard) else {
        return rpc_error(
            StatusCode::OK,
            &id,
            A2aError::standard(ErrorKind::MethodNotFound),
        );
    };
    if request.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || !(params.is_null() || params.is_object())
    {
        return rpc_error(
            StatusCode::OK,
            &id,
            A2aError::new(ErrorKind::InvalidRequest, "not a JSON-RPC 2.0 request"),
        );
    }
    let parameters = match parameters_of(&headers, state.token.is_some()) {
        Ok(parameters) => parameters,
        Err(error) => return rpc_error(StatusCode::OK, &id, error),
    };
    if method.streams() {
        let (items, mut receiver) = mpsc::channel(STREAM_BACKLOG);
        let job = Job::Stream {
            method,
            params,
            parameters,
            items,
        };
        if state.hub.submit(job).is_err() {
            return down(&id);
        }
        // An error before the first item is an ordinary JSON answer, as a client expects.
        let first = match receiver.recv().await {
            Some(Ok(first)) => first,
            Some(Err(error)) => return rpc_error(StatusCode::OK, &id, error),
            None => return down(&id),
        };
        let rest = ReceiverStream::new(receiver);
        let events = futures_util::stream::once(async move { Ok(first) })
            .chain(rest)
            .map(move |item: Result<Value, A2aError>| {
                let message = envelope(&id, item);
                Ok::<_, core::convert::Infallible>(Event::default().data(message.to_string()))
            });
        return Sse::new(events).into_response();
    }
    let (reply, answer) = oneshot::channel();
    let job = Job::Call {
        method,
        params,
        parameters,
        reply,
    };
    if state.hub.submit(job).is_err() {
        return down(&id);
    }
    match answer.await {
        Ok(mut outcome) => {
            // The extended card names the link too, which a client of this endpoint cannot use.
            if method == Method::GetExtendedAgentCard
                && let Ok(card) = &mut outcome
            {
                rewrite_card(card, state.address);
            }
            json_response(StatusCode::OK, &envelope(&id, outcome))
        }
        Err(_) => down(&id),
    }
}

fn down(id: &Value) -> HttpResponse {
    rpc_error(
        StatusCode::SERVICE_UNAVAILABLE,
        id,
        A2aError::new(ErrorKind::Internal, "the link is down"),
    )
}

/// The card the link's agent serves, with `supportedInterfaces` naming this endpoint.
async fn card(State(state): State<Shared>, headers: HeaderMap) -> HttpResponse {
    let parameters = match parameters_of(&headers, state.token.is_some()) {
        Ok(parameters) => parameters,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, &Value::Null, error),
    };
    let (reply, answer) = oneshot::channel();
    let job = Job::Call {
        method: Method::GetAgentCard,
        params: Value::Null,
        parameters,
        reply,
    };
    if state.hub.submit(job).is_err() {
        return down(&Value::Null);
    }
    let Ok(outcome) = answer.await else {
        return down(&Value::Null);
    };
    match outcome {
        Ok(mut card) => {
            rewrite_card(&mut card, state.address);
            json_response(StatusCode::OK, &card)
        }
        Err(error) => rpc_error(StatusCode::BAD_GATEWAY, &Value::Null, error),
    }
}

/// Points the card at this endpoint. A signature covered the original interfaces, so it goes.
fn rewrite_card(card: &mut Value, address: SocketAddr) {
    let Some(object) = card.as_object_mut() else {
        return;
    };
    object.insert(
        "supportedInterfaces".to_owned(),
        json!([{
            "url": format!("http://{address}/"),
            "protocolBinding": "JSONRPC",
            "protocolVersion": A2A_VERSION,
        }]),
    );
    object.remove("signatures");
}
