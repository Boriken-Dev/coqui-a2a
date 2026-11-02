//! The server side: a [`Handler`] that answers `a2a/1` streams with an [`Agent`].

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use coqui_agentic_peer::link::{Accepted, Handler, Services};
use coqui_agentic_peer::mux::{CloseReason, LinkStream, StreamLife};
use coqui_web::Failure;
use serde::Serialize;
use serde_json::Value;

use crate::agent::{Agent, Caller, Events, Watch};
use crate::error::{A2aError, ErrorKind};
use crate::model::{
    AgentCard, AgentInterface, CancelTaskRequest, GetExtendedAgentCardRequest, GetTaskRequest,
    ListTasksRequest, SendMessageRequest, StreamResponse, SubscribeToTaskRequest, params_as,
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
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_request: 1 << 20,
            request_timeout: Duration::from_secs(10),
        }
    }
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

    /// Answers one stream: reads its request, runs it, and ends it. Returns when the operation is
    /// over or the client is gone.
    pub fn serve(&self, stream: &dyn LinkStream, accepted: &Accepted) {
        let Some(request) = self.read_request(stream) else {
            return;
        };
        let id = request.id.clone();
        let outcome = self.run(stream, accepted, request);
        if let Err(error) = outcome {
            let _ = send(stream, &Response::error(id, error));
        }
        let _ = stream.finish();
    }

    fn read_request(&self, stream: &dyn LinkStream) -> Option<Request> {
        let bytes = match stream.receive(Some(self.limits.request_timeout)) {
            Ok(Some(bytes)) => bytes,
            Ok(None) | Err(_) => return None,
        };
        let refuse = |id: Value, error: A2aError| {
            let _ = send(stream, &Response::error(id, error));
            let _ = stream.finish();
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

    /// Runs a request. `Err` is sent as the stream's one response; success has sent its own.
    fn run(
        &self,
        stream: &dyn LinkStream,
        accepted: &Accepted,
        request: Request,
    ) -> Result<(), A2aError> {
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
        let card = guarded_value(|| self.agent.agent_card(&caller))?;
        if method != Method::GetAgentCard {
            check_version(&service_parameters)?;
            check_extensions(&card, &service_parameters)?;
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
        if let Some(extra) = excess(stream) {
            return Err(extra);
        }
        let agent = &*self.agent;
        match method {
            Method::GetAgentCard => unary(stream, id, Ok(self.link_card(card))),
            Method::SendMessage => {
                let request = send_request(params)?;
                unary(stream, id, guarded(|| agent.send_message(&caller, request)))
            }
            Method::SendStreamingMessage => {
                let request = send_request(params)?;
                let events = guarded(|| agent.send_streaming_message(&caller, request))?;
                pump(stream, &id, events, &watch)
            }
            Method::GetTask => {
                let request: GetTaskRequest = params_as(params)?;
                unary(stream, id, guarded(|| agent.get_task(&caller, request)))
            }
            Method::ListTasks => {
                let request: ListTasksRequest = params_as(params)?;
                unary(stream, id, guarded(|| agent.list_tasks(&caller, request)))
            }
            Method::CancelTask => {
                let request: CancelTaskRequest = params_as(params)?;
                unary(stream, id, guarded(|| agent.cancel_task(&caller, request)))
            }
            Method::SubscribeToTask => {
                let request: SubscribeToTaskRequest = params_as(params)?;
                let events = guarded(|| agent.subscribe_to_task(&caller, request))?;
                pump(stream, &id, events, &watch)
            }
            Method::GetExtendedAgentCard => {
                let request: GetExtendedAgentCardRequest = params_as(params)?;
                let card = guarded(|| agent.get_extended_agent_card(&caller, request))?;
                unary(stream, id, Ok(self.link_card(card)))
            }
            // Handled above: the push operations never reach an agent.
            _ => Err(A2aError::standard(ErrorKind::MethodNotFound)),
        }
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
fn check_version(parameters: &crate::wire::ServiceParameters) -> Result<(), A2aError> {
    let Some(version) = parameters.get("a2a-version") else {
        return Ok(());
    };
    let major = version
        .split('.')
        .next()
        .and_then(|major| major.parse::<u32>().ok());
    if major == Some(1) {
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

/// Sends one response, or returns the error to be the stream's answer instead.
fn unary<T: Serialize>(
    stream: &dyn LinkStream,
    id: Value,
    outcome: Result<T, A2aError>,
) -> Result<(), A2aError> {
    let result = to_result(&outcome?)?;
    match send(stream, &Response::result(id, result)) {
        Sent::TooLarge => Err(too_large()),
        Sent::Yes | Sent::Gone => Ok(()),
    }
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

/// Sends a stream of items in order, and stops after the one that ends the task. The agent's
/// iterator runs on a thread of its own, so a quiet one cannot hide a client that left.
fn pump(
    stream: &dyn LinkStream,
    id: &Value,
    events: Events,
    watch: &Watch,
) -> Result<(), A2aError> {
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
        let _closes = CloseOnDrop(watch.clone());
        let mut first = true;
        loop {
            let item = match received.recv_timeout(QUIET_POLL) {
                Ok(item) => item?,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if !stream_open(stream) {
                        return Ok(());
                    }
                    match excess(stream) {
                        Some(extra) => return Err(extra),
                        None => continue,
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            };
            // A message-only stream holds exactly one message; a task stream closes at a terminal state.
            let last = item.ends_task() || (first && matches!(item, StreamResponse::Message(_)));
            match send(stream, &Response::result(id.clone(), to_result(&item)?)) {
                Sent::Yes => {}
                Sent::TooLarge => return Err(too_large()),
                Sent::Gone => return Ok(()),
            }
            if last || !stream_open(stream) {
                return Ok(());
            }
            first = false;
            if let Some(extra) = excess(stream) {
                return Err(extra);
            }
        }
    })
}

fn too_large() -> A2aError {
    A2aError::new(
        ErrorKind::InvalidAgentResponse,
        "the answer is too large to send",
    )
}

fn stream_open(stream: &dyn LinkStream) -> bool {
    stream.life() == StreamLife::Open
}

enum Sent {
    Yes,
    TooLarge,
    Gone,
}

fn send(stream: &dyn LinkStream, response: &Response) -> Sent {
    match stream.send(&response.encode()) {
        Ok(()) => Sent::Yes,
        Err(Failure::Protocol) => Sent::TooLarge,
        Err(_) => Sent::Gone,
    }
}
