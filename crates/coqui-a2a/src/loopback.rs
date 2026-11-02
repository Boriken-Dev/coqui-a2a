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
//!
//! What a slow client or a hostile agent can hold is bounded: connections by a cap, an unfinished
//! request by a timeout, a client that stopped reading by a stall limit on the socket, and the
//! answers waiting to be written by a budget in bytes, which the agent feels as the stream's window.

use core::fmt;
use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll, ready};
use core::time::Duration;
use std::io;
use std::net::{IpAddr, SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::Instant;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{FromRequest as _, Request as HttpRequest, State};
use axum::http::{HeaderMap, HeaderValue, Method as Method_, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::{get, post};
use coqui_agentic_peer::mux::{CloseReason, LinkState, Mux};
use coqui_web::{Channel, Failure};
use serde_json::{Value, json};
use subtle::ConstantTimeEq as _;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::{Sleep, sleep, timeout};

use crate::client::{Client, ClientError, Polled, RawEvents};
use crate::error::{A2aError, ErrorKind};
use crate::wire::{Method, RawResponse, ServiceParameters};
use crate::{A2A_VERSION, PROTOCOL};

/// How often an attached link looks for a job and for its own end, and a job for its client.
const POLL: Duration = Duration::from_millis(100);
/// How often a job tries for room to put an answer.
const FULL_POLL: Duration = Duration::from_millis(2);
/// Answers of one stream waiting for an HTTP client that reads slowly.
const STREAM_BACKLOG: usize = 16;
/// The bytes one stream may have waiting, unless a single answer is larger: so one client that stopped
/// reading holds up its own stream and not the budget every stream shares.
const STREAM_BYTES: usize = 2 << 20;
/// The largest card this endpoint reads, since a card is the one answer it has to parse.
const MAX_CARD: usize = 1 << 20;
/// The largest error this endpoint reads. One is a code and a short message.
const MAX_ERROR: usize = 64 << 10;
/// What a client past the connection cap is told, before anything it sent is read.
const BUSY: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";

/// Why the endpoint could not start.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoopbackError {
    /// The address is not a loopback address.
    NotLoopback,
    /// Neither [`Config::with_token`] nor [`Config::without_token`] was called.
    TokenUndecided,
    /// The token is empty, or holds what a header cannot carry.
    BadToken,
    /// A listed origin is not one a browser would send: a lower-case `http` or `https` scheme and a
    /// host, an optional port and nothing else, which leaves out `*` and `null`.
    BadOrigin(String),
    /// The port could not be bound or the runtime could not start.
    Io(std::io::Error),
}

impl fmt::Display for LoopbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLoopback => f.write_str("the endpoint listens on a loopback address only"),
            Self::TokenUndecided => {
                f.write_str("the endpoint needs a token, or to be told to go without one")
            }
            Self::BadToken => f.write_str("a token is one or more visible ASCII characters"),
            Self::BadOrigin(origin) => write!(f, "{origin:?} is not an origin a browser sends"),
            Self::Io(error) => write!(f, "the endpoint could not start: {error}"),
        }
    }
}

impl core::error::Error for LoopbackError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::NotLoopback | Self::TokenUndecided | Self::BadToken | Self::BadOrigin(_) => None,
        }
    }
}

impl From<std::io::Error> for LoopbackError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// What a request has to show.
#[derive(Clone)]
enum Token {
    Undecided,
    Required(String),
    None,
}

/// Settings of the endpoint.
#[derive(Clone)]
pub struct Config {
    address: SocketAddr,
    origins: Vec<String>,
    forwarded: Vec<String>,
    token: Token,
    max_body: usize,
    max_connections: usize,
    request_timeout: Duration,
    write_stall: Duration,
    answer_timeout: Duration,
    max_buffered: usize,
    card_cache: Duration,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("address", &self.address)
            .field("origins", &self.origins)
            .field("forwarded", &self.forwarded)
            .field(
                "token",
                match &self.token {
                    Token::Undecided => &"undecided",
                    Token::Required(_) => &"set",
                    Token::None => &"none",
                },
            )
            .field("max_body", &self.max_body)
            .field("max_connections", &self.max_connections)
            .field("request_timeout", &self.request_timeout)
            .field("write_stall", &self.write_stall)
            .field("answer_timeout", &self.answer_timeout)
            .field("max_buffered", &self.max_buffered)
            .field("card_cache", &self.card_cache)
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
            forwarded: Vec::new(),
            token: Token::Undecided,
            max_body: 1 << 20,
            max_connections: 64,
            request_timeout: Duration::from_secs(10),
            write_stall: Duration::from_secs(30),
            answer_timeout: Duration::from_secs(300),
            max_buffered: 64 << 20,
            card_cache: Duration::from_secs(60),
        })
    }

    /// Lets a page from `origin` (such as `http://127.0.0.1:3000`) use the endpoint, with the CORS
    /// answers a browser asks for. By default every request that carries an `Origin` is refused.
    #[must_use]
    pub fn allow_origin(mut self, origin: impl Into<String>) -> Self {
        self.origins.push(origin.into());
        self
    }

    /// Passes the header `name` to the agent as a service parameter, as well as `a2a-*` and
    /// `Authorization`: for a credential whose scheme on the agent's card names another header.
    #[must_use]
    pub fn forward_header(mut self, name: impl AsRef<str>) -> Self {
        self.forwarded.push(name.as_ref().to_ascii_lowercase());
        self
    }

    /// Requires `Authorization: Bearer <token>` of every request, and does not forward that header
    /// to the agent. [`Loopback::start`] refuses an endpoint until this or
    /// [`Config::without_token`] was called.
    #[must_use]
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Token::Required(token.into());
        self
    }

    /// Serves every request with no token: every program of every user on this machine can then do
    /// what the link's peer granted, and read the extended card. `Authorization` goes to the agent.
    #[must_use]
    pub fn without_token(mut self) -> Self {
        self.token = Token::None;
        self
    }

    /// Sets the largest request body, 1 MiB by default.
    #[must_use]
    pub fn with_max_body(mut self, bytes: usize) -> Self {
        self.max_body = bytes;
        self
    }

    /// Sets how many connections are open at once, 64 by default. Past it a connection is answered
    /// 503 and closed. Each connection carries one request.
    #[must_use]
    pub fn with_max_connections(mut self, connections: usize) -> Self {
        self.max_connections = connections;
        self
    }

    /// Sets how long a client has to send its request once connected, head and body each, 10 seconds
    /// by default. Past it the connection is closed.
    #[must_use]
    pub fn with_request_timeout(mut self, limit: Duration) -> Self {
        self.request_timeout = limit;
        self
    }

    /// Sets how long a client may leave the socket full while an answer waits, 30 seconds by default.
    /// Past it the connection is closed with the answer unfinished, and the stream on the link with it.
    #[must_use]
    pub fn with_write_stall(mut self, limit: Duration) -> Self {
        self.write_stall = limit;
        self
    }

    /// Sets how long the agent has to say anything to an operation, 300 seconds by default. `SendMessage`
    /// blocks until the task settles, so a longer task needs more, or `returnImmediately` and `GetTask`.
    /// A stream that has begun may then be quiet for as long as it likes.
    #[must_use]
    pub fn with_answer_timeout(mut self, limit: Duration) -> Self {
        self.answer_timeout = limit;
        self
    }

    /// Sets how many bytes of answers may wait for HTTP clients, 64 MiB by default, across every
    /// connection. An answer larger than this is `InvalidAgentResponseError`.
    #[must_use]
    pub fn with_max_buffered(mut self, bytes: usize) -> Self {
        self.max_buffered = bytes;
        self
    }

    /// Sets how long the card is served without asking the agent again, 60 seconds by default, so
    /// reading it does not spend the link's allowance of streams. Zero asks every time.
    #[must_use]
    pub fn with_card_cache(mut self, limit: Duration) -> Self {
        self.card_cache = limit;
        self
    }
}

fn is_loopback(ip: IpAddr) -> bool {
    coqui_agentic_peer::forward::is_loopback(ip)
}

/// An answer on its way to an HTTP client, and its share of what may wait.
struct Relayed {
    /// JSON: a whole JSON-RPC response, or for [`Form::Bare`] a result's own value.
    text: String,
    is_error: bool,
    shares: Shares,
}

/// Given back when the answer has been handed to the socket.
struct Shares {
    _budget: Option<OwnedSemaphorePermit>,
    _stream: Option<OwnedSemaphorePermit>,
}

impl Shares {
    const NONE: Self = Self {
        _budget: None,
        _stream: None,
    };
}

/// What a stream's queue holds. Only [`Item::End`] ends the HTTP response in order: a queue closed
/// without it is a stream given up on, whose client must not read it as whole.
enum Item {
    Answer(Relayed),
    End,
}

/// What a job's result is wrapped in.
enum Form {
    /// As a response with this id, as JSON text.
    Response(String),
    /// Not at all, for a card this endpoint still has to rewrite. An error is still a response, with
    /// this id.
    Bare(String),
}

/// Where a job's answers go.
enum Reply {
    One(oneshot::Sender<Relayed>),
    Many(mpsc::Sender<Item>),
}

/// One operation waiting for the link.
struct Job {
    method: Method,
    params: Value,
    parameters: ServiceParameters,
    answer_timeout: Duration,
    budget: Arc<Semaphore>,
    max_buffered: usize,
    form: Form,
    reply: Reply,
}

/// What came of waiting for the next answer.
enum Waited {
    Answer(Relayed),
    End,
    Failed(A2aError),
    /// Nobody is there to tell, or the queue cannot be reached before the link's end.
    GaveUp,
}

impl Job {
    fn id_text(&self) -> &str {
        match &self.form {
            Form::Response(id) | Form::Bare(id) => id,
        }
    }

    /// Answers `error`, with nothing else to come.
    fn refuse(self, error: &A2aError) {
        let answer = Relayed {
            text: error_text(self.id_text(), error),
            is_error: true,
            shares: Shares::NONE,
        };
        match self.reply {
            Reply::One(reply) => {
                let _ = reply.send(answer);
            }
            Reply::Many(items) => {
                // A queue with nothing in it yet, so there is room for both.
                let _ = items.try_send(Item::Answer(answer));
                let _ = items.try_send(Item::End);
            }
        }
    }

    fn run<C: Channel>(self, hub: &Hub, mux: &Mux<'_, C>) {
        let client = Client::new(mux).with_service_parameters(self.parameters.clone());
        let events = match client.begin(self.method, self.params.clone()) {
            Ok(events) => events,
            Err(error) => return self.refuse(&into_error(error)),
        };
        let deadline = Instant::now() + self.answer_timeout;
        match &self.reply {
            Reply::One(reply) => {
                let gone = || reply.is_closed();
                let answer = match self.next_answer(&events, hub, mux, &gone, None, Some(deadline))
                {
                    Waited::Answer(answer) => answer,
                    Waited::End => self.failure(&into_error(ClientError::Interrupted)),
                    Waited::Failed(error) => self.failure(&error),
                    Waited::GaveUp => return,
                };
                let Reply::One(reply) = self.reply else {
                    return;
                };
                let _ = reply.send(answer);
            }
            Reply::Many(items) => {
                let own = Arc::new(Semaphore::new(STREAM_BYTES));
                let gone = || items.is_closed();
                let mut deadline = Some(deadline);
                loop {
                    match self.next_answer(&events, hub, mux, &gone, Some(&own), deadline) {
                        Waited::Answer(answer) => {
                            deadline = None;
                            if !deliver(hub, mux, items, Item::Answer(answer)) {
                                return;
                            }
                        }
                        Waited::End if deadline.is_none() => {
                            deliver(hub, mux, items, Item::End);
                            return;
                        }
                        Waited::End => {
                            let failure = self.failure(&into_error(ClientError::Interrupted));
                            deliver(hub, mux, items, Item::Answer(failure));
                            deliver(hub, mux, items, Item::End);
                            return;
                        }
                        Waited::Failed(error) => {
                            deliver(hub, mux, items, Item::Answer(self.failure(&error)));
                            deliver(hub, mux, items, Item::End);
                            return;
                        }
                        Waited::GaveUp => return,
                    }
                }
            }
        }
    }

    fn failure(&self, error: &A2aError) -> Relayed {
        Relayed {
            text: error_text(self.id_text(), error),
            is_error: true,
            shares: Shares::NONE,
        }
    }

    /// Waits for the next answer while watching the client: a quiet stream must not hide that nobody
    /// is reading it, since a stream left open counts against the link's limit.
    fn next_answer<C: Channel>(
        &self,
        events: &RawEvents<'_, C>,
        hub: &Hub,
        mux: &Mux<'_, C>,
        gone: &dyn Fn() -> bool,
        own: Option<&Arc<Semaphore>>,
        deadline: Option<Instant>,
    ) -> Waited {
        loop {
            if gone() || hub.is_closed() {
                return Waited::GaveUp;
            }
            // With nothing of this stream's share left, a message read now could only wait in memory.
            if own.is_some_and(|own| own.available_permits() == 0) {
                if mux.link_state() != LinkState::Up {
                    return Waited::GaveUp;
                }
                thread::sleep(FULL_POLL);
                continue;
            }
            match events.poll(Some(POLL)) {
                Polled::Quiet => {
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        return Waited::Failed(into_error(ClientError::Timeout));
                    }
                }
                Polled::Finished => return Waited::End,
                Polled::Failed(error) => return Waited::Failed(into_error(error)),
                Polled::Message(bytes) => {
                    return match self.render(&events.request_id(), &bytes) {
                        Ok(text) => self.reserve(text, hub, mux, gone, own),
                        Err(error) => Waited::Failed(error),
                    };
                }
            }
        }
    }

    /// The text of a message from the peer, or the error it holds.
    fn render(&self, request_id: &str, bytes: &[u8]) -> Result<String, A2aError> {
        let malformed = || into_error(ClientError::Malformed("not a response"));
        let response = RawResponse::decode(bytes).ok_or_else(malformed)?;
        // A server answers with the request's id, or null when it could not read one.
        if response.id.is_some_and(|id| id.get() != request_id) {
            return Err(malformed());
        }
        match (response.result, response.error) {
            (Some(result), None) => {
                let result = result.get();
                let text = match &self.form {
                    Form::Response(id) => {
                        format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{result}}}"#)
                    }
                    Form::Bare(_) if result.len() > MAX_CARD => return Err(malformed()),
                    Form::Bare(_) => result.to_owned(),
                };
                // Outside a string these are only spacing, and inside one JSON does not allow them. An
                // event's data holds neither.
                if text.contains(['\r', '\n']) {
                    return Ok(text.replace(['\r', '\n'], " "));
                }
                Ok(text)
            }
            (None, Some(error)) if error.get().len() <= MAX_ERROR => {
                let error = serde_json::from_str(error.get())
                    .ok()
                    .and_then(|error| A2aError::from_value(&error))
                    .ok_or_else(malformed)?;
                Err(error)
            }
            _ => Err(malformed()),
        }
    }

    /// Takes a share for `text`, waiting for room while the client is there. Until then this stream is
    /// not read, so it is the agent's send that waits, for the stream's window.
    fn reserve<C: Channel>(
        &self,
        text: String,
        hub: &Hub,
        mux: &Mux<'_, C>,
        gone: &dyn Fn() -> bool,
        own: Option<&Arc<Semaphore>>,
    ) -> Waited {
        let Ok(cost) = u32::try_from(text.len()) else {
            return Waited::Failed(too_large());
        };
        if text.len() > self.max_buffered {
            return Waited::Failed(too_large());
        }
        let own_cost = cost.min(u32::try_from(STREAM_BYTES).unwrap_or(u32::MAX));
        loop {
            let stream = match own {
                Some(own) => Arc::clone(own).try_acquire_many_owned(own_cost).map(Some),
                None => Ok(None),
            };
            if let Ok(stream) = stream
                && let Ok(budget) = Arc::clone(&self.budget).try_acquire_many_owned(cost)
            {
                return Waited::Answer(Relayed {
                    text,
                    is_error: false,
                    shares: Shares {
                        _budget: Some(budget),
                        _stream: stream,
                    },
                });
            }
            if gone() || hub.is_closed() || mux.link_state() != LinkState::Up {
                return Waited::GaveUp;
            }
            thread::sleep(FULL_POLL);
        }
    }
}

/// Puts `item` on the queue, unless nobody is there or the link's end will not wait for room: a
/// client that stopped reading must not keep [`Hub::attach`] from returning. Whoever reads the queue
/// then sees it close with no [`Item::End`].
fn deliver<C: Channel>(
    hub: &Hub,
    mux: &Mux<'_, C>,
    items: &mpsc::Sender<Item>,
    item: Item,
) -> bool {
    let mut item = item;
    loop {
        match items.try_send(item) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            Err(mpsc::error::TrySendError::Full(back)) => {
                if hub.is_closed() || mux.link_state() != LinkState::Up {
                    return false;
                }
                item = back;
                thread::sleep(FULL_POLL);
            }
        }
    }
}

fn too_large() -> A2aError {
    A2aError::new(
        ErrorKind::InvalidAgentResponse,
        "the answer is too large for this endpoint",
    )
}

fn error_text(id: &str, error: &A2aError) -> String {
    let error = error.to_value();
    format!(r#"{{"jsonrpc":"2.0","id":{id},"error":{error}}}"#)
}

/// Where the endpoint's operations go: to whichever link is attached.
#[derive(Default)]
pub struct Hub {
    jobs: Mutex<Option<(u64, std_mpsc::Sender<Job>)>>,
    links: AtomicU64,
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

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Queues `job`. `false` when no link is attached.
    fn submit(&self, job: Job) -> bool {
        let jobs = self.jobs.lock().unwrap_or_else(PoisonError::into_inner);
        jobs.as_ref()
            .is_some_and(|(_, sender)| sender.send(job).is_ok())
    }

    /// Which link is attached, if any: a number no other link of this hub has had.
    fn link(&self) -> Option<u64> {
        self.jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|(link, _)| *link)
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
        let link = self.links.fetch_add(1, Ordering::SeqCst);
        *self.jobs.lock().unwrap_or_else(PoisonError::into_inner) = Some((link, sender));
        thread::scope(|scope| {
            loop {
                match jobs.recv_timeout(POLL) {
                    Ok(job) => {
                        scope.spawn(move || Job::run(job, self, mux));
                    }
                    Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
                }
                // Nothing here serves a stream the peer opens, and one left waiting holds its bytes.
                while let Ok(Some(incoming)) = mux.accept(Some(Duration::ZERO)) {
                    scope.spawn(move || {
                        incoming
                            .stream
                            .close_with(CloseReason::Refused, "this side serves nothing");
                    });
                }
                if self.is_closed() || mux.link_state() != LinkState::Up {
                    break;
                }
            }
        });
        // A link attached since has taken the place, and this one's end is not that one's.
        {
            let mut attached = self.jobs.lock().unwrap_or_else(PoisonError::into_inner);
            if attached.as_ref().is_some_and(|(held, _)| *held == link) {
                *attached = None;
            }
        }
        // Operations that arrived while the link was ending are answered, not left waiting.
        while let Ok(job) = jobs.try_recv() {
            job.refuse(&A2aError::new(ErrorKind::Internal, "the link is down"));
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
        // What `Mux::open` answers past the link's limit on streams.
        ClientError::Link(Failure::NoProgress) => A2aError::new(
            ErrorKind::Internal,
            "too many operations are running on the link",
        ),
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
    forwarded: Arc<Vec<String>>,
    token: Option<Arc<String>>,
    address: SocketAddr,
    request_timeout: Duration,
    answer_timeout: Duration,
    budget: Arc<Semaphore>,
    max_buffered: usize,
    card_cache: Duration,
    // Held across asking the agent, so those waiting take what that brings rather than asking too.
    card: Arc<tokio::sync::Mutex<Option<CachedCard>>>,
}

/// A card as served, and what it may be served for again.
struct CachedCard {
    link: u64,
    parameters: ServiceParameters,
    taken: Instant,
    card: Value,
}

impl Loopback {
    /// Binds the address and serves it on a runtime of its own.
    ///
    /// # Errors
    ///
    /// [`LoopbackError::TokenUndecided`] or [`LoopbackError::BadToken`] for the token,
    /// [`LoopbackError::BadOrigin`] for a listed origin, and [`LoopbackError::Io`] when the port is
    /// taken or the runtime cannot start.
    pub fn start(config: Config, hub: Arc<Hub>) -> Result<Self, LoopbackError> {
        if let Some(origin) = config.origins.iter().find(|origin| !valid_origin(origin)) {
            return Err(LoopbackError::BadOrigin(origin.clone()));
        }
        let token = match config.token {
            Token::Undecided => return Err(LoopbackError::TokenUndecided),
            Token::Required(token)
                if token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()) =>
            {
                return Err(LoopbackError::BadToken);
            }
            Token::Required(token) => Some(Arc::new(token)),
            Token::None => None,
        };
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
            forwarded: Arc::new(config.forwarded),
            token,
            address,
            request_timeout: config.request_timeout,
            answer_timeout: config.answer_timeout,
            budget: Arc::new(Semaphore::new(
                config.max_buffered.min(Semaphore::MAX_PERMITS),
            )),
            max_buffered: config.max_buffered,
            card_cache: config.card_cache,
            card: Arc::default(),
        };
        let app = Router::new()
            .route("/", post(rpc))
            .route("/.well-known/agent-card.json", get(card))
            .layer(axum::extract::DefaultBodyLimit::max(config.max_body))
            .layer(middleware::from_fn_with_state(state.clone(), guard))
            .with_state(state);
        let limits = Connections {
            request_timeout: config.request_timeout,
            write_stall: config.write_stall,
        };
        let slots = Arc::new(Semaphore::new(config.max_connections));
        let (stop, stopped) = oneshot::channel::<()>();
        let thread = thread::Builder::new()
            .name("coqui-a2a-loopback".to_owned())
            .spawn(move || {
                let serving = async {
                    let Ok(listener) = tokio::net::TcpListener::from_std(listener) else {
                        return;
                    };
                    let accepting = pin!(accept(listener, app, limits, slots));
                    let _ = futures_util::future::select(accepting, stopped).await;
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

/// What holds a connection to account.
#[derive(Clone, Copy)]
struct Connections {
    request_timeout: Duration,
    write_stall: Duration,
}

async fn accept(
    listener: tokio::net::TcpListener,
    app: Router,
    limits: Connections,
    slots: Arc<Semaphore>,
) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            // Out of descriptors, say: the same again at once would only spin.
            sleep(Duration::from_millis(50)).await;
            continue;
        };
        let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
            let _ = socket.try_write(BUSY);
            continue;
        };
        let app = app.clone();
        tokio::spawn(async move {
            let _slot = slot;
            connection(socket, app, limits).await;
        });
    }
}

/// Serves one request. A connection is never kept for another, so all a connection can hold on to is
/// the time to send its request and the time its answer takes.
async fn connection(socket: TcpStream, app: Router, limits: Connections) {
    let _ = socket.set_nodelay(true);
    let requested = Arc::new(Notify::new());
    let heard = Arc::clone(&requested);
    let service = hyper_util::service::TowerToHyperService::new(app);
    let serving = hyper::server::conn::http1::Builder::new()
        .keep_alive(false)
        .serve_connection(
            hyper_util::rt::TokioIo::new(Stalling::new(socket, limits.write_stall)),
            hyper::service::service_fn(move |request| {
                heard.notify_one();
                hyper::service::Service::call(&service, request)
            }),
        );
    let head = async {
        if timeout(limits.request_timeout, requested.notified())
            .await
            .is_ok()
        {
            core::future::pending::<()>().await;
        }
    };
    let _ = futures_util::future::select(pin!(serving), pin!(head)).await;
}

/// A socket whose writes fail when the other side has taken nothing for a while, so a client that
/// stopped reading loses its connection, and with it its place on the link.
struct Stalling {
    socket: TcpStream,
    limit: Duration,
    since: Option<Pin<Box<Sleep>>>,
}

impl Stalling {
    const fn new(socket: TcpStream, limit: Duration) -> Self {
        Self {
            socket,
            limit,
            since: None,
        }
    }

    fn watched(
        &mut self,
        cx: &mut Context<'_>,
        written: Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        if written.is_ready() {
            self.since = None;
            return written;
        }
        let limit = self.limit;
        let since = self.since.get_or_insert_with(|| Box::pin(sleep(limit)));
        ready!(since.as_mut().poll(cx));
        Poll::Ready(Err(io::ErrorKind::TimedOut.into()))
    }
}

impl AsyncRead for Stalling {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_read(cx, buf)
    }
}

impl AsyncWrite for Stalling {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = Pin::new(&mut this.socket).poll_write(cx, buf);
        this.watched(cx, written)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = Pin::new(&mut this.socket).poll_write_vectored(cx, bufs);
        this.watched(cx, written)
    }

    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_shutdown(cx)
    }
}

fn plain(status: StatusCode, text: &'static str) -> HttpResponse {
    (status, text).into_response()
}

/// Refuses what a web page or a rebinding attack would send, before anything else looks at it, and
/// gives a page from a listed origin the answers its browser needs.
async fn guard(State(state): State<Shared>, request: HttpRequest, next: Next) -> HttpResponse {
    let headers = request.headers();
    let origin = match headers.get(header::ORIGIN) {
        Some(origin) => {
            let listed = origin
                .to_str()
                .is_ok_and(|origin| state.origins.iter().any(|allowed| allowed == origin));
            if !listed {
                return plain(StatusCode::FORBIDDEN, "an Origin is not accepted here");
            }
            Some(origin.clone())
        }
        None if from_a_page(headers) => {
            return plain(StatusCode::FORBIDDEN, "a web page is not accepted here");
        }
        None => None,
    };
    let host_ok = headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
        .is_some_and(|host| host_is_loopback(host, state.address.port()));
    if !host_ok {
        return plain(StatusCode::FORBIDDEN, "the Host is not a loopback name");
    }
    // A browser's preflight holds no credential, so it comes before the token, and reaches no link.
    if let Some(origin) = &origin
        && request.method() == Method_::OPTIONS
        && headers.contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
    {
        return preflight(headers, origin);
    }
    let authorized = state
        .token
        .as_ref()
        .is_none_or(|token| shows_token(headers, token));
    let mut response = if authorized {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            "a bearer token is required",
        )
            .into_response()
    };
    if let Some(origin) = origin {
        let answer = response.headers_mut();
        answer.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        answer.insert(header::VARY, HeaderValue::from_static("Origin"));
    }
    response
}

/// The answer to a listed origin's preflight. It may send whatever headers it asks to: it is listed.
fn preflight(headers: &HeaderMap, origin: &HeaderValue) -> HttpResponse {
    let mut response = StatusCode::NO_CONTENT.into_response();
    let answer = response.headers_mut();
    answer.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
    answer.insert(header::VARY, HeaderValue::from_static("Origin"));
    answer.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST"),
    );
    if let Some(asked) = headers.get(header::ACCESS_CONTROL_REQUEST_HEADERS) {
        answer.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, asked.clone());
    }
    // What a browser asks of a public page reaching a loopback address.
    if headers.contains_key("access-control-request-private-network") {
        answer.insert(
            "access-control-allow-private-network",
            HeaderValue::from_static("true"),
        );
    }
    response
}

/// Whether a browser could send `origin`: what `Origin` compares with is exact, so anything else here
/// is a setting that never matches, or in the case of `*` and `null` one that should not.
fn valid_origin(origin: &str) -> bool {
    let Some((scheme, host)) = origin.split_once("://") else {
        return false;
    };
    matches!(scheme, "http" | "https")
        && !host.is_empty()
        && host.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".-:[]".contains(&byte)
        })
}

/// Whether the request holds one `Authorization` header, of the scheme `Bearer`, with `token`. A byte
/// that differs takes as long to find as any other; the length of a token is not a secret.
fn shows_token(headers: &HeaderMap, token: &str) -> bool {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false;
    };
    let Some((scheme, given)) = value.as_bytes().split_at_checked(6) else {
        return false;
    };
    scheme.eq_ignore_ascii_case(b"Bearer")
        && given
            .strip_prefix(b" ")
            .is_some_and(|given| bool::from(given.ct_eq(token.as_bytes())))
}

/// Whether a browser sent this for a page, as a page's GET does with no `Origin`: `<img src>`, a
/// `no-cors` fetch or a navigation. A browser's `Sec-Fetch-*` cannot be set by script, and it sends them
/// to a loopback address. Node's `fetch` sends `Sec-Fetch-Mode: cors` and nothing else of them, which
/// says nothing about a page.
fn from_a_page(headers: &HeaderMap) -> bool {
    ["sec-fetch-site", "sec-fetch-dest", "sec-fetch-user"]
        .iter()
        .any(|name| headers.contains_key(*name))
        || headers
            .get("sec-fetch-mode")
            .is_some_and(|mode| mode != "cors")
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

/// The service parameters a request's headers give: `a2a-*`, those named to be forwarded, and
/// `authorization` unless this endpoint spent it. Several values for a name are one list to HTTP, and
/// the binding says the same, so a later one does not quietly stand in for the first.
fn parameters_of(state: &Shared, headers: &HeaderMap) -> Result<ServiceParameters, A2aError> {
    let mut parameters = ServiceParameters::new();
    for name in headers.keys() {
        let name = name.as_str();
        let wanted = if name == "authorization" {
            state.token.is_none()
        } else {
            name.starts_with("a2a-") || state.forwarded.iter().any(|named| named == name)
        };
        if !wanted {
            continue;
        }
        let mut joined = String::new();
        for value in headers.get_all(name) {
            let value = value
                .to_str()
                .map_err(|_| A2aError::new(ErrorKind::InvalidRequest, "a header is not text"))?;
            if !joined.is_empty() {
                joined.push_str(", ");
            }
            joined.push_str(value);
        }
        parameters
            .set(name, &joined)
            .map_err(|error| A2aError::new(ErrorKind::InvalidRequest, error.message))?;
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

impl Shared {
    fn job(
        &self,
        method: Method,
        params: Value,
        parameters: ServiceParameters,
        form: Form,
        reply: Reply,
    ) -> Job {
        Job {
            method,
            params,
            parameters,
            answer_timeout: self.answer_timeout,
            budget: Arc::clone(&self.budget),
            max_buffered: self.max_buffered,
            form,
            reply,
        }
    }
}

async fn rpc(
    State(state): State<Shared>,
    headers: HeaderMap,
    request: HttpRequest,
) -> HttpResponse {
    let body = match timeout(state.request_timeout, Bytes::from_request(request, &())).await {
        Ok(Ok(body)) => body,
        Ok(Err(rejection)) => return rejection.into_response(),
        Err(_) => {
            return plain(
                StatusCode::REQUEST_TIMEOUT,
                "the request took too long to arrive",
            );
        }
    };
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
    let parameters = match parameters_of(&state, &headers) {
        Ok(parameters) => parameters,
        Err(error) => return rpc_error(StatusCode::OK, &id, error),
    };
    if method.streams() {
        let (items, mut receiver) = mpsc::channel(STREAM_BACKLOG);
        let form = Form::Response(id.to_string());
        let job = state.job(method, params, parameters, form, Reply::Many(items));
        if !state.hub.submit(job) {
            return down(&id);
        }
        // An error before the first item is an ordinary JSON answer, as a client expects.
        let first = match receiver.recv().await {
            Some(Item::Answer(first)) => first,
            Some(Item::End) | None => return down(&id),
        };
        if first.is_error {
            return answered(StatusCode::OK, first);
        }
        return Sse::new(events(first, receiver)).into_response();
    }
    // The extended card names the link too, which a client of this endpoint cannot use.
    let form = if method == Method::GetExtendedAgentCard {
        Form::Bare(id.to_string())
    } else {
        Form::Response(id.to_string())
    };
    let bare = matches!(form, Form::Bare(_));
    let (reply, answer) = oneshot::channel();
    let job = state.job(method, params, parameters, form, Reply::One(reply));
    if !state.hub.submit(job) {
        return down(&id);
    }
    let Ok(answer) = answer.await else {
        return down(&id);
    };
    if !bare || answer.is_error {
        return answered(StatusCode::OK, answer);
    }
    let card = rewritten(&answer.text, state.address);
    json_response(StatusCode::OK, &envelope(&id, card))
}

/// One answer as an HTTP response. Its share goes when the socket has taken it, or the connection has
/// gone, since until then its bytes are still held here.
fn answered(status: StatusCode, answer: Relayed) -> HttpResponse {
    let Relayed { text, shares, .. } = answer;
    let length = text.len();
    let mut chunk = Some(Bytes::from(text));
    let mut shares = Some(shares);
    let body = Body::from_stream(futures_util::stream::poll_fn(move |_| {
        if let Some(chunk) = chunk.take() {
            return Poll::Ready(Some(Ok::<_, core::convert::Infallible>(chunk)));
        }
        drop(shares.take());
        Poll::Ready(None)
    }));
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json".to_owned()),
            (header::CONTENT_LENGTH, length.to_string()),
        ],
        body,
    )
        .into_response()
}

/// The events of a stream. Each answer's share goes when the next is asked for, by which time the
/// server has written all but a small part of it. Ending with an error rather than in order closes
/// the connection with the response unfinished, which is how HTTP says it was cut short.
fn events(
    first: Relayed,
    mut receiver: mpsc::Receiver<Item>,
) -> impl futures_util::Stream<Item = io::Result<Event>> + Send + 'static {
    let mut next = Some(first);
    let mut held = None;
    futures_util::stream::poll_fn(move |cx| {
        drop(held.take());
        let answer = match next.take() {
            Some(first) => first,
            None => match ready!(receiver.poll_recv(cx)) {
                Some(Item::Answer(answer)) => answer,
                Some(Item::End) => return Poll::Ready(None),
                None => return Poll::Ready(Some(Err(io::ErrorKind::ConnectionAborted.into()))),
            },
        };
        let Relayed { text, shares, .. } = answer;
        held = Some(shares);
        Poll::Ready(Some(Ok(Event::default().data(text))))
    })
}

fn down(id: &Value) -> HttpResponse {
    rpc_error(
        StatusCode::SERVICE_UNAVAILABLE,
        id,
        A2aError::new(ErrorKind::Internal, "the link is down"),
    )
}

/// The card the link's agent serves, with `supportedInterfaces` naming this endpoint. It is kept for a
/// while for as long as the link lasts, unless what it was asked with could be a credential: then it may
/// differ per caller, and it is not for the next one.
async fn card(State(state): State<Shared>, headers: HeaderMap) -> HttpResponse {
    let parameters = match parameters_of(&state, &headers) {
        Ok(parameters) => parameters,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, &Value::Null, error),
    };
    if !parameters.iter().all(|(name, _)| name.starts_with("a2a-")) {
        return match asked_card(&state, parameters).await {
            Ok(card) => json_response(StatusCode::OK, &card),
            Err(refused) => *refused,
        };
    }
    let mut cached = state.card.lock().await;
    let Some(link) = state.hub.link() else {
        return down(&Value::Null);
    };
    if let Some(kept) = cached.as_ref().filter(|kept| {
        kept.link == link
            && kept.parameters == parameters
            && kept.taken.elapsed() < state.card_cache
    }) {
        return json_response(StatusCode::OK, &kept.card);
    }
    match asked_card(&state, parameters.clone()).await {
        Ok(card) => {
            let response = json_response(StatusCode::OK, &card);
            *cached = Some(CachedCard {
                link,
                parameters,
                taken: Instant::now(),
                card,
            });
            response
        }
        Err(refused) => *refused,
    }
}

/// Asks the agent for its card. `Err` is the HTTP answer to a failure.
async fn asked_card(
    state: &Shared,
    parameters: ServiceParameters,
) -> Result<Value, Box<HttpResponse>> {
    let (reply, answer) = oneshot::channel();
    let job = state.job(
        Method::GetAgentCard,
        Value::Null,
        parameters,
        Form::Bare(Value::Null.to_string()),
        Reply::One(reply),
    );
    if !state.hub.submit(job) {
        return Err(Box::new(down(&Value::Null)));
    }
    let Ok(answer) = answer.await else {
        return Err(Box::new(down(&Value::Null)));
    };
    if answer.is_error {
        return Err(Box::new(answered(StatusCode::BAD_GATEWAY, answer)));
    }
    rewritten(&answer.text, state.address)
        .map_err(|error| Box::new(rpc_error(StatusCode::BAD_GATEWAY, &Value::Null, error)))
}

/// The members of an A2A 1.0 card that say what the agent is, not where to go. What is not listed goes:
/// `supportedInterfaces` is this endpoint's to fill, a signature covered the card as it was, and a client
/// that reads a member of an older or later version, such as `url` or `additionalInterfaces`, must not
/// be sent around this endpoint by it.
const CARD_MEMBERS: [&str; 9] = [
    "name",
    "description",
    "provider",
    "version",
    "documentationUrl",
    "defaultInputModes",
    "defaultOutputModes",
    "skills",
    "iconUrl",
];
/// The schemes with nowhere to send a credential. An OAuth or OpenID Connect scheme names URLs that a
/// client of a local endpoint would take for its own, so a client needing one is set up by hand.
const LOCAL_SCHEMES: [&str; 3] = [
    "apiKeySecurityScheme",
    "httpAuthSecurityScheme",
    "mtlsSecurityScheme",
];

/// A card as this endpoint serves it, from the JSON the agent sent.
fn rewritten(text: &str, address: SocketAddr) -> Result<Value, A2aError> {
    let Ok(Value::Object(mut card)) = serde_json::from_str::<Value>(text) else {
        return Err(into_error(ClientError::Malformed("not a card")));
    };
    let schemes = kept_schemes(card.get("securitySchemes"));
    let mut served = serde_json::Map::new();
    if card.contains_key("securitySchemes") {
        served.insert("securitySchemes".to_owned(), Value::Object(schemes.clone()));
    }
    for name in CARD_MEMBERS {
        if let Some(value) = card.remove(name) {
            served.insert(name.to_owned(), value);
        }
    }
    if let Some(requirements) = card.get("securityRequirements") {
        served.insert(
            "securityRequirements".to_owned(),
            kept_requirements(requirements, &schemes),
        );
    }
    if let Some(Value::Array(skills)) = served.get_mut("skills") {
        for skill in skills.iter_mut().filter_map(Value::as_object_mut) {
            if let Some(requirements) = skill.get_mut("securityRequirements") {
                *requirements = kept_requirements(requirements, &schemes);
            }
        }
    }
    let mut capabilities = serde_json::Map::new();
    if let Some(Value::Object(given)) = card.get("capabilities") {
        for name in ["streaming", "extensions", "extendedAgentCard"] {
            if let Some(value) = given.get(name) {
                capabilities.insert(name.to_owned(), value.clone());
            }
        }
    }
    capabilities.insert("pushNotifications".to_owned(), Value::Bool(false));
    served.insert("capabilities".to_owned(), Value::Object(capabilities));
    served.insert(
        "supportedInterfaces".to_owned(),
        json!([{
            "url": format!("http://{address}/"),
            "protocolBinding": "JSONRPC",
            "protocolVersion": A2A_VERSION,
        }]),
    );
    Ok(Value::Object(served))
}

/// The security schemes of a card that are one of [`LOCAL_SCHEMES`] and nothing else.
fn kept_schemes(schemes: Option<&Value>) -> serde_json::Map<String, Value> {
    let Some(Value::Object(schemes)) = schemes else {
        return serde_json::Map::new();
    };
    schemes
        .iter()
        .filter(|(_, scheme)| {
            scheme.as_object().is_some_and(|scheme| {
                scheme.len() == 1 && LOCAL_SCHEMES.iter().any(|name| scheme.contains_key(*name))
            })
        })
        .map(|(name, scheme)| (name.clone(), scheme.clone()))
        .collect()
}

/// The requirements that name only schemes still on the card, in the form `{"schemes": {name: ...}}`.
fn kept_requirements(requirements: &Value, schemes: &serde_json::Map<String, Value>) -> Value {
    let Value::Array(requirements) = requirements else {
        return json!([]);
    };
    requirements
        .iter()
        .filter(|requirement| {
            requirement
                .get("schemes")
                .and_then(Value::as_object)
                .is_some_and(|named| named.keys().all(|name| schemes.contains_key(name)))
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use coqui_agentic_peer::mux::{Frame, Limits, Side};

    use super::*;

    /// A peer that answers the first request with as many items as the stream's window lets it, and
    /// counts what the client has read by the credit it gives back.
    #[derive(Default)]
    struct Flood {
        item: Vec<u8>,
        stream: Mutex<Option<u32>>,
        sent: AtomicUsize,
        read: AtomicUsize,
    }

    impl Channel for Flood {
        fn send(&self, message: &[u8]) -> Result<(), Failure> {
            match Frame::decode(message) {
                Some(Frame::Data { stream, .. }) => {
                    *self.stream.lock().unwrap() = Some(stream);
                }
                Some(Frame::Credit { .. }) => {
                    self.read.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
            Ok(())
        }

        fn receive(&self, _: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
            thread::sleep(Duration::from_millis(1));
            let Some(stream) = *self.stream.lock().unwrap() else {
                return Err(Failure::Timeout);
            };
            let unread = self.sent.load(Ordering::SeqCst) - self.read.load(Ordering::SeqCst);
            if unread * self.item.len() >= 1 << 20 {
                return Err(Failure::Timeout);
            }
            self.sent.fetch_add(1, Ordering::SeqCst);
            Ok(Some(
                Frame::Data {
                    stream,
                    body: self.item.clone(),
                }
                .encode(),
            ))
        }

        fn close(&self) {}
    }

    fn flood_of(bytes: usize) -> Flood {
        let text = "a".repeat(bytes);
        Flood {
            item: format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{text}"}}"#).into_bytes(),
            ..Flood::default()
        }
    }

    /// Runs a stream against `flood`, with nobody reading its queue, until the relay stops reading.
    fn held(flood: &Flood, budget: usize) -> (usize, usize, Vec<Item>) {
        let hub = Hub::new();
        let shares = Arc::new(Semaphore::new(budget));
        let (items, mut receiver) = mpsc::channel(STREAM_BACKLOG);
        let job = Job {
            method: Method::SubscribeToTask,
            params: json!({ "id": "x" }),
            parameters: ServiceParameters::new(),
            answer_timeout: Duration::from_secs(10),
            budget: Arc::clone(&shares),
            max_buffered: budget,
            form: Form::Response("1".to_owned()),
            reply: Reply::Many(items),
        };
        Mux::run(flood, Side::Dialed, Limits::default(), None, |mux| {
            thread::scope(|scope| {
                scope.spawn(|| hub.attach(mux));
                while !hub.is_up() {
                    thread::sleep(Duration::from_millis(1));
                }
                assert!(hub.submit(job));
                let mut read = 0;
                let mut quiet = 0;
                // A second with nothing read: a relay that is only slow is not what this is about.
                while quiet < 50 {
                    thread::sleep(Duration::from_millis(20));
                    let now = flood.read.load(Ordering::SeqCst);
                    quiet = if now == read { quiet + 1 } else { 0 };
                    read = now;
                }
                let free = shares.available_permits();
                hub.close();
                let mut queued = Vec::new();
                while let Ok(item) = receiver.try_recv() {
                    queued.push(item);
                }
                (read, free, queued)
            })
        })
    }

    const ITEM: usize = 256 << 10;

    #[test]
    fn what_waits_for_a_stopped_client_stays_within_the_budget() {
        let flood = flood_of(ITEM);
        let (read, free, queued) = held(&flood, 1 << 20);
        // Three fit, as each is a little past 256 KiB with its envelope, and one more is read and waits.
        assert_eq!(queued.len(), 3);
        assert_eq!(read, 4, "the relay kept reading past what it may hold");
        assert!(free < ITEM, "the budget should be spent, with {free} free");
        // And the agent is held by the window, which is the point of not reading.
        assert!(flood.sent.load(Ordering::SeqCst) * ITEM <= (1 << 20) + read * ITEM + ITEM);
    }

    #[test]
    fn one_stopped_client_cannot_spend_the_whole_budget() {
        let (read, free, queued) = held(&flood_of(ITEM), 64 << 20);
        // Seven fit in the bytes one stream may have waiting.
        assert_eq!(queued.len(), 7);
        assert_eq!(read, 8);
        assert!(free > 60 << 20, "only {free} left");
    }

    #[test]
    fn a_stream_with_no_share_left_is_not_read() {
        // One answer spends all a stream may have waiting, so the next stays in the link's window.
        let (read, _, queued) = held(&flood_of(3 << 20), 64 << 20);
        assert_eq!(queued.len(), 1);
        assert_eq!(read, 1);
    }

    #[test]
    fn an_answer_past_the_whole_budget_is_an_error_and_not_a_wait() {
        let (_, _, queued) = held(&flood_of(2 * ITEM), ITEM);
        let Some(Item::Answer(answer)) = queued.first() else {
            panic!("no answer");
        };
        assert!(answer.is_error);
        assert!(answer.text.contains("-32006"), "{}", answer.text);
    }

    /// A peer that opens one stream toward the command, and keeps what it hears about it.
    #[derive(Default)]
    struct Opener {
        opened: AtomicBool,
        answers: Mutex<Vec<Vec<u8>>>,
    }

    impl Channel for Opener {
        fn send(&self, message: &[u8]) -> Result<(), Failure> {
            self.answers.lock().unwrap().push(message.to_vec());
            Ok(())
        }

        fn receive(&self, _: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
            if self.opened.swap(true, Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
                return Err(Failure::Timeout);
            }
            Ok(Some(
                Frame::Open {
                    stream: 2,
                    protocol: "a2a/1".to_owned(),
                    metadata: Vec::new(),
                }
                .encode(),
            ))
        }

        fn close(&self) {}
    }

    #[test]
    fn a_stream_the_peer_opens_is_refused_and_holds_nothing() {
        // Closes the hub when the test ends, a failing assertion included, so the scope can end.
        struct Closing<'a>(&'a Hub);

        impl Drop for Closing<'_> {
            fn drop(&mut self) {
                self.0.close();
            }
        }

        let opener = Opener::default();
        let hub = Hub::new();
        Mux::run(&opener, Side::Dialed, Limits::default(), None, |mux| {
            thread::scope(|scope| {
                let _closing = Closing(&hub);
                scope.spawn(|| hub.attach(mux));
                let until = Instant::now() + Duration::from_secs(10);
                let refused = |answer: &Vec<u8>| {
                    matches!(
                        Frame::decode(answer),
                        Some(Frame::Close {
                            stream: 2,
                            reason: CloseReason::Refused,
                            ..
                        })
                    )
                };
                while !opener.answers.lock().unwrap().iter().any(refused) {
                    assert!(
                        Instant::now() < until,
                        "a stream the peer opened was left open"
                    );
                    thread::sleep(Duration::from_millis(5));
                }
            });
        });
    }

    /// A peer that answers every request with a card naming it, until told the link is over.
    struct CardPeer {
        name: &'static str,
        outgoing: Mutex<std::collections::VecDeque<Vec<u8>>>,
        over: AtomicBool,
        asked: AtomicUsize,
    }

    impl CardPeer {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                outgoing: Mutex::default(),
                over: AtomicBool::new(false),
                asked: AtomicUsize::new(0),
            }
        }
    }

    impl Channel for CardPeer {
        fn send(&self, message: &[u8]) -> Result<(), Failure> {
            if let Some(Frame::Data { stream, .. }) = Frame::decode(message) {
                self.asked.fetch_add(1, Ordering::SeqCst);
                let answer = json!({ "jsonrpc": "2.0", "id": 1, "result": { "name": self.name } });
                let mut outgoing = self.outgoing.lock().unwrap();
                outgoing.push_back(
                    Frame::Data {
                        stream,
                        body: answer.to_string().into_bytes(),
                    }
                    .encode(),
                );
                outgoing.push_back(Frame::Finish { stream }.encode());
            }
            Ok(())
        }

        fn receive(&self, _: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
            if let Some(frame) = self.outgoing.lock().unwrap().pop_front() {
                return Ok(Some(frame));
            }
            if self.over.load(Ordering::SeqCst) {
                return Ok(None);
            }
            thread::sleep(Duration::from_millis(1));
            Err(Failure::Timeout)
        }

        fn close(&self) {}
    }

    fn card_name(address: SocketAddr) -> Option<String> {
        use std::io::{Read as _, Write as _};
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        write!(
            stream,
            "GET /.well-known/agent-card.json HTTP/1.0\r\nHost: {address}\r\n\r\n"
        )
        .unwrap();
        let mut text = String::new();
        stream.read_to_string(&mut text).unwrap();
        let (_, body) = text.split_once("\r\n\r\n")?;
        serde_json::from_str::<Value>(body).ok()?["name"]
            .as_str()
            .map(str::to_owned)
    }

    /// Ends every link when a test does, a failing assertion included, so the scope can end.
    struct Ends<'a>(&'a [&'a CardPeer]);

    impl Drop for Ends<'_> {
        fn drop(&mut self) {
            for peer in self.0 {
                peer.over.store(true, Ordering::SeqCst);
            }
        }
    }

    fn attached(hub: &Hub, link: u64) {
        let until = Instant::now() + Duration::from_secs(10);
        while hub.link() != Some(link) {
            assert!(
                Instant::now() < until,
                "link {link} is not the one attached"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn a_card_lasts_as_long_as_its_link_and_a_links_end_is_only_its_own() {
        let hub = Hub::new();
        let config = Config::new("127.0.0.1:0".parse().unwrap()).unwrap();
        let endpoint = Loopback::start(config.without_token(), Arc::clone(&hub)).unwrap();
        let address = endpoint.local_addr();
        let (first, second, third) = (
            CardPeer::new("first"),
            CardPeer::new("second"),
            CardPeer::new("third"),
        );
        let limits = Limits::default();
        let peers = [&first, &second, &third];
        thread::scope(|scope| {
            let _ends = Ends(&peers);
            scope.spawn(|| Mux::run(&first, Side::Dialed, limits, None, |mux| hub.attach(mux)));
            attached(&hub, 0);
            for _ in 0..5 {
                assert_eq!(card_name(address).as_deref(), Some("first"));
            }
            assert_eq!(first.asked.load(Ordering::SeqCst), 1);

            // A link that takes over serves its own card, and outlives the end of the one before.
            scope.spawn(|| Mux::run(&second, Side::Dialed, limits, None, |mux| hub.attach(mux)));
            attached(&hub, 1);
            assert_eq!(card_name(address).as_deref(), Some("second"));
            first.over.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(300));
            assert!(hub.is_up(), "the end of the first link detached the second");
            assert_eq!(card_name(address).as_deref(), Some("second"));
            assert_eq!(second.asked.load(Ordering::SeqCst), 1);

            // With no link there is no card, however recently one was read.
            second.over.store(true, Ordering::SeqCst);
            while hub.is_up() {
                thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(card_name(address), None);

            scope.spawn(|| Mux::run(&third, Side::Dialed, limits, None, |mux| hub.attach(mux)));
            attached(&hub, 2);
            assert_eq!(card_name(address).as_deref(), Some("third"));
        });
    }
}
