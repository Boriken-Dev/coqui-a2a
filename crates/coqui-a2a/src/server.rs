//! The server side: a [`Handler`] that answers `a2a/1` streams with an [`Agent`].

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coqui_agentic_peer::link::{Accepted, Handler, Services};
use coqui_agentic_peer::mux::{CloseReason, LinkStream, StreamLife};
use coqui_transport::Failure;
use serde::Serialize;
use serde_json::Value;

use crate::agent::{Agent, Caller, Events, Watch};
use crate::error::{A2aError, ErrorKind};
use crate::model::{
    AgentCard, AgentInterface, CancelTaskRequest, GetExtendedAgentCardRequest, GetTaskRequest,
    ListTasksRequest, ListTasksResponse, SendMessageRequest, SendMessageResponse, StreamResponse,
    SubscribeToTaskRequest, Task, params_as,
};
use crate::wire::{Method, Request, Response};
use crate::{A2A_VERSION, BINDING, PROTOCOL};

/// What a server holds a peer to.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ServerLimits {
    /// The largest request, in bytes. A larger one is answered `InvalidRequestError`.
    pub max_request: usize,
    /// How long a stream may stay open before it sends its request.
    pub request_timeout: Duration,
    /// How long a client may leave its window full. Past this the stream is closed as `broken`, so
    /// the client reads it as cut short, and the agent is released.
    pub reader_stall: Duration,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_request: 1 << 20,
            request_timeout: Duration::from_secs(10),
            reader_stall: Duration::from_secs(30),
        }
    }
}

/// What a call that does not stream answers.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Reply {
    /// `GetAgentCard` and `GetExtendedAgentCard`.
    Card(AgentCard),
    /// `SendMessage`.
    Message(SendMessageResponse),
    /// `GetTask` and `CancelTask`.
    Task(Task),
    /// `ListTasks`.
    Tasks(ListTasksResponse),
}

/// Serves an [`Agent`] over `a2a/1` streams.
#[derive(Clone)]
pub struct Server {
    agent: Arc<dyn Agent>,
    limits: ServerLimits,
    url: String,
}

impl core::fmt::Debug for Server {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Server")
            .field("limits", &self.limits)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl Server {
    /// A server for `agent`.
    #[must_use]
    pub fn new(agent: impl Agent + 'static) -> Self {
        Self {
            agent: Arc::new(agent),
            limits: ServerLimits::default(),
            url: String::from("coqui-agentic-peer:"),
        }
    }

    /// Replaces the limits.
    #[must_use]
    pub fn with_limits(mut self, limits: ServerLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Sets the `url` of the interface this server adds to the card: the address the link is
    /// reached at, or `coqui-agentic-peer:` and the peer id.
    #[must_use]
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// The handler for [`PROTOCOL`], for [`Services::with`].
    pub fn handler(self) -> Handler {
        Box::new(move |stream: &dyn LinkStream, accepted: &Accepted| self.serve(stream, accepted))
    }

    /// `services` and `a2a/1`, served by this server.
    #[must_use]
    pub fn serving(self, services: Services) -> Services {
        services.with(PROTOCOL, self.handler())
    }

    /// Calls the agent in process, as a host reaches its own agent, with the checks a link meets:
    /// the method, `a2a-version`, required extensions, the card's capabilities and push
    /// notifications, and a panic in the agent is `InternalError`. `params` is the request's
    /// `params` member.
    ///
    /// Only operations that answer once go through here; a streaming one is [`Server::stream`].
    /// The card answered has no webhooks and, since the caller is not on a link, no interface for
    /// one. The authorization of the caller is the agent's, by [`Caller::peer`].
    ///
    /// # Errors
    ///
    /// The error a link would answer the same request with.
    pub fn call(
        &self,
        caller: &Caller<'_>,
        method: Method,
        params: Value,
    ) -> Result<Reply, A2aError> {
        let card = self.admit(caller, method)?;
        if method.streams() {
            return Err(form_error(method, true));
        }
        match self.invoke(caller, method, card, params)? {
            Outcome::Reply(Reply::Card(card)) => Ok(Reply::Card(without_webhooks(card))),
            Outcome::Reply(reply) => Ok(reply),
            Outcome::Events(_) => Err(A2aError::standard(ErrorKind::Internal)),
        }
    }

    /// As [`Server::call`], for `SendStreamingMessage` and `SubscribeToTask`.
    ///
    /// The iterator ends after the item that puts the task in a terminal state, after a first item
    /// that is a message, and after an error, as a link's stream does. Nothing closes the caller's
    /// [`Watch`] when the iterator is dropped: the host does when it stops reading, since only it
    /// knows whether the watch is shared. A quiet iterator returns soon after the watch closes.
    ///
    /// # Errors
    ///
    /// As [`Server::call`].
    pub fn stream(
        &self,
        caller: &Caller<'_>,
        method: Method,
        params: Value,
    ) -> Result<Events, A2aError> {
        let card = self.admit(caller, method)?;
        if !method.streams() {
            return Err(form_error(method, false));
        }
        match self.invoke(caller, method, card, params)? {
            Outcome::Events(events) => Ok(Box::new(Bounded {
                events,
                first: true,
                done: false,
            })),
            Outcome::Reply(_) => Err(A2aError::standard(ErrorKind::Internal)),
        }
    }

    /// Answers one stream: reads its request, runs it, and ends it. Returns when the operation is
    /// over or the client is gone.
    pub fn serve(&self, stream: &dyn LinkStream, accepted: &Accepted) {
        let Some(request) = self.read_request(stream) else {
            return;
        };
        let id = request.id.clone();
        let sent = match self.run(stream, accepted, request) {
            Ok(sent) => sent,
            Err(error) => self.send(stream, &Response::error(id, error)),
        };
        // `finish` tells the client the answer is whole, so nothing else may lead to it.
        if sent == Sent::Yes {
            let _ = stream.finish();
        }
    }

    fn read_request(&self, stream: &dyn LinkStream) -> Option<Request> {
        let bytes = match stream.receive(Some(self.limits.request_timeout)) {
            Ok(Some(bytes)) => bytes,
            Ok(None) | Err(_) => return None,
        };
        let refuse = |id: Value, error: A2aError| {
            if self.send(stream, &Response::error(id, error)) == Sent::Yes {
                let _ = stream.finish();
            }
        };
        if bytes.len() > self.limits.max_request {
            refuse(
                Value::Null,
                A2aError::new(ErrorKind::InvalidRequest, "the request is too large"),
            );
            return None;
        }
        match Request::decode(&bytes) {
            Ok(request) => Some(request),
            Err((id, error)) => {
                refuse(id, error);
                None
            }
        }
    }

    /// Runs a request. `Err` is sent as the stream's one response; `Ok` has sent its own, and says
    /// how that went.
    fn run(
        &self,
        stream: &dyn LinkStream,
        accepted: &Accepted,
        request: Request,
    ) -> Result<Sent, A2aError> {
        let Request {
            id,
            method,
            params,
            service_parameters,
        } = request;
        let method = Method::from_name(&method)
            .ok_or_else(|| A2aError::standard(ErrorKind::MethodNotFound))?;
        let watch = Watch::default();
        let _closes = CloseOnDrop(watch.clone());
        let caller = Caller::new(
            &accepted.peer,
            &accepted.claims,
            &service_parameters,
            stream,
            &watch,
        );
        let card = self.admit(&caller, method)?;
        if let Some(extra) = excess(stream) {
            return Err(extra);
        }
        match self.invoke(&caller, method, card, params)? {
            Outcome::Reply(reply) => {
                let reply = match reply {
                    Reply::Card(card) => Reply::Card(self.link_card(card)),
                    other => other,
                };
                self.unary(stream, id, Ok(reply))
            }
            Outcome::Events(events) => self.pump(stream, &id, events, &watch),
        }
    }

    /// The checks before any agent method but the card, shared by a link and a call in process so
    /// that both meet them in this order: the version and extensions, push notifications, and the
    /// capabilities the card declares.
    fn admit(&self, caller: &Caller<'_>, method: Method) -> Result<AgentCard, A2aError> {
        let card = guarded_value(|| self.agent.agent_card(caller))?;
        if method != Method::GetAgentCard {
            check_version(caller.service_parameters)?;
            check_extensions(&card, caller.service_parameters)?;
        }
        if method.is_push() {
            return Err(A2aError::standard(ErrorKind::PushNotificationNotSupported));
        }
        if method.streams() && card.capabilities.streaming != Some(true) {
            return Err(A2aError::new(
                ErrorKind::UnsupportedOperation,
                "this agent does not stream",
            ));
        }
        if method == Method::GetExtendedAgentCard
            && card.capabilities.extended_agent_card != Some(true)
        {
            return Err(A2aError::new(
                ErrorKind::UnsupportedOperation,
                "this agent has no extended card",
            ));
        }
        Ok(card)
    }

    /// Reads the parameters and runs the agent's method. A card is as the agent made it.
    fn invoke(
        &self,
        caller: &Caller<'_>,
        method: Method,
        card: AgentCard,
        params: Value,
    ) -> Result<Outcome, A2aError> {
        let agent = &*self.agent;
        let reply = match method {
            Method::GetAgentCard => Reply::Card(card),
            Method::SendMessage => {
                let request = send_request(params)?;
                Reply::Message(guarded(|| agent.send_message(caller, request))?)
            }
            Method::SendStreamingMessage => {
                let request = send_request(params)?;
                let events = guarded(|| agent.send_streaming_message(caller, request))?;
                return Ok(Outcome::Events(events));
            }
            Method::GetTask => {
                let request: GetTaskRequest = params_as(params)?;
                Reply::Task(guarded(|| agent.get_task(caller, request))?)
            }
            Method::ListTasks => {
                let request: ListTasksRequest = params_as(params)?;
                Reply::Tasks(guarded(|| agent.list_tasks(caller, request))?)
            }
            Method::CancelTask => {
                let request: CancelTaskRequest = params_as(params)?;
                Reply::Task(guarded(|| agent.cancel_task(caller, request))?)
            }
            Method::SubscribeToTask => {
                let request: SubscribeToTaskRequest = params_as(params)?;
                let events = guarded(|| agent.subscribe_to_task(caller, request))?;
                return Ok(Outcome::Events(events));
            }
            Method::GetExtendedAgentCard => {
                let request: GetExtendedAgentCardRequest = params_as(params)?;
                Reply::Card(guarded(|| agent.get_extended_agent_card(caller, request))?)
            }
            // The push operations are refused by `admit` and never reach an agent.
            _ => return Err(A2aError::standard(ErrorKind::MethodNotFound)),
        };
        Ok(Outcome::Reply(reply))
    }

    /// The card as this binding serves it: the interface that names the link first, and no
    /// webhooks.
    fn link_card(&self, mut card: AgentCard) -> AgentCard {
        card.capabilities.push_notifications = Some(false);
        card.supported_interfaces
            .retain(|interface| interface.protocol_binding != BINDING);
        card.supported_interfaces.insert(
            0,
            AgentInterface {
                url: self.url.clone(),
                protocol_binding: BINDING.to_owned(),
                protocol_version: A2A_VERSION.to_owned(),
                ..AgentInterface::default()
            },
        );
        card
    }

    /// Sends one response, or returns the error to be the stream's answer instead.
    fn unary<T: Serialize>(
        &self,
        stream: &dyn LinkStream,
        id: Value,
        outcome: Result<T, A2aError>,
    ) -> Result<Sent, A2aError> {
        let result = to_result(&outcome?)?;
        match self.send(stream, &Response::result(id, result)) {
            Sent::TooLarge => Err(too_large()),
            sent => Ok(sent),
        }
    }

    /// Sends a stream of items in order, and stops after the one that ends the task. The agent's
    /// iterator runs on a thread of its own, so a quiet one cannot hide a client that left.
    fn pump(
        &self,
        stream: &dyn LinkStream,
        id: &Value,
        events: Events,
        watch: &Watch,
    ) -> Result<Sent, A2aError> {
        let (items, received) = std::sync::mpsc::sync_channel(1);
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let mut events = events;
                loop {
                    let item = match guarded_value(|| events.next()) {
                        Ok(Some(item)) => item,
                        Ok(None) => break,
                        Err(error) => Err(error),
                    };
                    let failed = item.is_err();
                    if items.send(item).is_err() || failed {
                        break;
                    }
                }
            });
            // Moved in, so it goes before the scope joins: that is what frees a thread blocked on a
            // full channel. The watch closing is what frees one waiting on the agent's own events.
            let received = received;
            let _closes = CloseOnDrop(watch.clone());
            let mut first = true;
            loop {
                let item = match received.recv_timeout(QUIET_POLL) {
                    Ok(item) => item?,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if !stream_open(stream) {
                            return Ok(Sent::Gone);
                        }
                        match excess(stream) {
                            Some(extra) => return Err(extra),
                            None => continue,
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Ok(Sent::Yes),
                };
                // A message-only stream holds exactly one message; a task stream closes at a terminal state.
                let last =
                    item.ends_task() || (first && matches!(item, StreamResponse::Message(_)));
                match self.send(stream, &Response::result(id.clone(), to_result(&item)?)) {
                    Sent::Yes => {}
                    Sent::TooLarge => return Err(too_large()),
                    ended => return Ok(ended),
                }
                if last {
                    return Ok(Sent::Yes);
                }
                if !stream_open(stream) {
                    return Ok(Sent::Gone);
                }
                first = false;
                if let Some(extra) = excess(stream) {
                    return Err(extra);
                }
            }
        })
    }

    /// Sends one response, waiting for a slow reader's credit. A reader that gives none within
    /// [`ServerLimits::reader_stall`] loses the stream as `broken`.
    fn send(&self, stream: &dyn LinkStream, response: &Response) -> Sent {
        let bytes = response.encode();
        let until = Instant::now() + self.limits.reader_stall;
        loop {
            match stream.send(&bytes) {
                Ok(()) => return Sent::Yes,
                // Full, either this stream's window or the link. Nothing was sent, so it can be sent again.
                Err(Failure::NoProgress) if Instant::now() < until => {
                    std::thread::sleep(FULL_POLL);
                }
                Err(Failure::NoProgress) => {
                    stream.close_with(CloseReason::Broken, "the client stopped reading");
                    return Sent::Stalled;
                }
                // The same failure for a message past the bound and for a stream already over.
                Err(Failure::Protocol) if stream_open(stream) => return Sent::TooLarge,
                Err(_) => return Sent::Gone,
            }
        }
    }
}

fn send_request(params: Value) -> Result<SendMessageRequest, A2aError> {
    let request: SendMessageRequest = params_as(params)?;
    request.validate()?;
    if request
        .configuration
        .as_ref()
        .is_some_and(|configuration| configuration.task_push_notification_config.is_some())
    {
        return Err(A2aError::standard(ErrorKind::PushNotificationNotSupported));
    }
    Ok(request)
}

/// The major version in `a2a-version` has to be 1; no value means the client has no preference.
/// Headers joined into one value by a relay all count: a version the agent cannot speak is not hidden
/// by another beside it.
fn check_version(parameters: &crate::wire::ServiceParameters) -> Result<(), A2aError> {
    let Some(versions) = parameters.get("a2a-version") else {
        return Ok(());
    };
    let major_one = |version: &str| {
        version
            .trim()
            .split('.')
            .next()
            .and_then(|major| major.parse::<u32>().ok())
            == Some(1)
    };
    if versions.split(',').all(major_one) {
        Ok(())
    } else {
        Err(A2aError::new(
            ErrorKind::VersionNotSupported,
            format!("this binding carries A2A {A2A_VERSION}"),
        )
        .with_metadata("supported", A2A_VERSION))
    }
}

/// An extension the card marks required has to be named in `a2a-extensions`.
fn check_extensions(
    card: &AgentCard,
    parameters: &crate::wire::ServiceParameters,
) -> Result<(), A2aError> {
    let declared: Vec<&str> = parameters
        .get("a2a-extensions")
        .map(|list| list.split(',').map(str::trim).collect())
        .unwrap_or_default();
    for extension in card.capabilities.extensions.iter().flatten() {
        if extension.required == Some(true)
            && let Some(uri) = &extension.uri
            && !declared.contains(&uri.as_str())
        {
            return Err(A2aError::standard(ErrorKind::ExtensionSupportRequired)
                .with_metadata("extension", uri));
        }
    }
    Ok(())
}

/// A message the client sent after its request, which a stream does not allow.
fn excess(stream: &dyn LinkStream) -> Option<A2aError> {
    match stream.receive(Some(Duration::ZERO)) {
        Ok(Some(_)) => {
            stream.close_with(CloseReason::Refused, "one operation per stream");
            Some(A2aError::new(
                ErrorKind::InvalidRequest,
                "one operation per stream",
            ))
        }
        _ => None,
    }
}

/// Runs the agent's code, turning a panic in it into an internal error rather than a lost link.
fn guarded_value<T>(call: impl FnOnce() -> T) -> Result<T, A2aError> {
    catch_unwind(AssertUnwindSafe(call))
        .map_err(|_| A2aError::new(ErrorKind::Internal, "the agent failed"))
}

/// As [`guarded_value`], for a call that has an error of its own.
fn guarded<T>(call: impl FnOnce() -> Result<T, A2aError>) -> Result<T, A2aError> {
    guarded_value(call)?
}

fn to_result<T: Serialize>(value: &T) -> Result<Value, A2aError> {
    serde_json::to_value(value).map_err(|_| {
        A2aError::new(
            ErrorKind::InvalidAgentResponse,
            "the agent's answer is not JSON",
        )
    })
}

/// Closes a [`Watch`] when the operation is over, however it ends.
struct CloseOnDrop(Watch);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// How often a quiet stream looks for a client that left or spoke twice.
const QUIET_POLL: Duration = Duration::from_millis(100);
/// How often a send tries a link that is full, which answers at once and not after a wait.
const FULL_POLL: Duration = Duration::from_millis(5);

fn too_large() -> A2aError {
    A2aError::new(
        ErrorKind::InvalidAgentResponse,
        "the answer is too large to send",
    )
}

fn stream_open(stream: &dyn LinkStream) -> bool {
    stream.life() == StreamLife::Open
}

/// What a method of the agent answered.
#[expect(
    clippy::large_enum_variant,
    reason = "an outcome is built once and moved at once"
)]
enum Outcome {
    Reply(Reply),
    Events(Events),
}

/// The refusal of an operation asked through the form that does not carry it.
fn form_error(method: Method, called: bool) -> A2aError {
    let (what, instead) = if called {
        ("streams", "Server::stream")
    } else {
        ("answers once", "Server::call")
    };
    A2aError::new(
        ErrorKind::InvalidRequest,
        format!("{} {what}: use {instead}", method.name()),
    )
}

/// A card as a caller that is not on a link is shown it: no webhooks.
fn without_webhooks(mut card: AgentCard) -> AgentCard {
    card.capabilities.push_notifications = Some(false);
    card
}

/// An agent's events for a caller in process, held to the rules of a link's stream: they end
/// after the item that ends the task, after a first message and after an error, and a panic in the
/// agent is an error.
struct Bounded {
    events: Events,
    first: bool,
    done: bool,
}

impl Iterator for Bounded {
    type Item = Result<StreamResponse, A2aError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let item = match guarded_value(|| self.events.next()) {
            Ok(Some(item)) => item,
            Ok(None) => {
                self.done = true;
                return None;
            }
            Err(error) => Err(error),
        };
        self.done = match &item {
            Ok(item) => {
                item.ends_task() || (self.first && matches!(item, StreamResponse::Message(_)))
            }
            Err(_) => true,
        };
        self.first = false;
        Some(item)
    }
}

/// How a response went.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Sent {
    Yes,
    TooLarge,
    /// The stream ended some other way.
    Gone,
    /// The reader stopped, and this side closed the stream as `broken`.
    Stalled,
}
