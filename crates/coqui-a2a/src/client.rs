//! The client side: an operation is a stream on a link.

use core::fmt;
use core::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};

use coqui_agentic_peer::mux::{Mux, Stream, StreamEnd};
use coqui_web_channel::{Channel, Failure};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::A2aError;
use crate::model::{
    AgentCard, CancelTaskRequest, GetExtendedAgentCardRequest, GetTaskRequest, ListTasksRequest,
    ListTasksResponse, SendMessageRequest, SendMessageResponse, StreamResponse,
    SubscribeToTaskRequest, Task,
};
use crate::wire::{Method, Request, Response, ServiceParameters};
use crate::{A2A_VERSION, PROTOCOL};

/// Why an operation did not give its result.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ClientError {
    /// The agent answered with an A2A error.
    Remote(A2aError),
    /// The peer does not serve `a2a/1` to this key, saying why.
    NotGranted(String),
    /// The grant was taken away while the operation ran, saying why.
    Revoked(String),
    /// The stream ended before the operation was complete: a streamed result is cut short, and the
    /// link replays nothing. Read the task with `GetTask` or follow it with `SubscribeToTask`.
    Interrupted,
    /// Nothing came within the time allowed.
    Timeout,
    /// The peer's answer is not what the binding says.
    Malformed(&'static str),
    /// The link failed in another way.
    Link(Failure),
    /// The request does not fit the link's bounds.
    TooLarge,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Remote(error) => write!(f, "the agent answered with {error}"),
            Self::NotGranted(why) => write!(f, "the peer does not serve a2a/1 to this key: {why}"),
            Self::Revoked(why) => write!(f, "the grant was taken away: {why}"),
            Self::Interrupted => f.write_str("the stream ended before the operation was complete"),
            Self::Timeout => f.write_str("nothing came in time"),
            Self::Malformed(why) => write!(f, "the peer's answer is malformed: {why}"),
            Self::Link(failure) => write!(f, "the link failed: {failure}"),
            Self::TooLarge => f.write_str("the request is too large for the link"),
        }
    }
}

impl core::error::Error for ClientError {}

impl From<A2aError> for ClientError {
    fn from(error: A2aError) -> Self {
        Self::Remote(error)
    }
}

/// How a client waits.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ClientTimeouts {
    /// For the answer to a single-answer operation. `SendMessage` blocks until the task settles, so
    /// this is long.
    pub answer: Duration,
    /// For the next item of a stream, or `None` to wait as long as the link lives. A stream may be
    /// silent for hours, and a link with a heartbeat tells a dead peer from a quiet one.
    pub item: Option<Duration>,
}

impl Default for ClientTimeouts {
    fn default() -> Self {
        Self {
            answer: Duration::from_secs(300),
            item: None,
        }
    }
}

/// Calls an agent over a link's mux.
pub struct Client<'m, 'c, C: Channel> {
    mux: &'m Mux<'c, C>,
    parameters: ServiceParameters,
    timeouts: ClientTimeouts,
    next_id: AtomicU64,
}

impl<C: Channel> fmt::Debug for Client<'_, '_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("parameters", &self.parameters)
            .field("timeouts", &self.timeouts)
            .finish_non_exhaustive()
    }
}

impl<'m, 'c, C: Channel> Client<'m, 'c, C> {
    /// A client over `mux`, announcing A2A version 1.0 in each request.
    #[must_use]
    pub fn new(mux: &'m Mux<'c, C>) -> Self {
        let mut parameters = ServiceParameters::new();
        // A constant within the bounds.
        let _ = parameters.set("a2a-version", A2A_VERSION);
        Self {
            mux,
            parameters,
            timeouts: ClientTimeouts::default(),
            next_id: AtomicU64::new(1),
        }
    }

    /// Sets a service parameter sent with every request, such as the agent's own `authorization`.
    ///
    /// # Errors
    ///
    /// When the name or value is outside the bounds.
    pub fn with_service_parameter(mut self, name: &str, value: &str) -> Result<Self, A2aError> {
        self.parameters.set(name, value)?;
        Ok(self)
    }

    /// Replaces the service parameters sent with every request, including `a2a-version`: a relay
    /// forwards the ones its own caller gave.
    #[must_use]
    pub fn with_service_parameters(mut self, parameters: ServiceParameters) -> Self {
        self.parameters = parameters;
        self
    }

    /// Replaces the timeouts.
    #[must_use]
    pub fn with_timeouts(mut self, timeouts: ClientTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    fn open(&self, method: Method, params: Value) -> Result<(Stream<'c, C>, Value), ClientError> {
        let id = Value::from(self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut request = Request::new(id.clone(), method, params);
        request.service_parameters = self.parameters.clone();
        let stream = self.mux.open(PROTOCOL).map_err(ClientError::Link)?;
        match stream.send(&request.encode()) {
            Ok(()) => {}
            Err(Failure::Protocol) => return Err(ClientError::TooLarge),
            Err(failure) => return Err(why(&stream, failure)),
        }
        // Half-close: the request is complete, and the answer comes back on this stream. It fails when
        // the stream has already ended, answered or not, which the read that follows says better.
        let _ = stream.finish();
        Ok((stream, id))
    }

    /// Performs a single-answer operation with raw JSON, for a relay that must not rewrite what it
    /// carries.
    ///
    /// # Errors
    ///
    /// [`ClientError::Remote`] for the agent's error, and the others for the link.
    pub fn call_raw(&self, method: Method, params: Value) -> Result<Value, ClientError> {
        let (stream, id) = self.open(method, params)?;
        let response = read_response(&stream, &id, Some(self.timeouts.answer))?;
        response.outcome.map_err(ClientError::Remote)
    }

    /// Starts a streaming operation with raw JSON.
    ///
    /// # Errors
    ///
    /// As [`Client::call_raw`], for the first item: an operation refused up front is an `Err`
    /// here.
    pub fn stream_raw(
        &self,
        method: Method,
        params: Value,
    ) -> Result<RawEvents<'c, C>, ClientError> {
        let (stream, id) = self.open(method, params)?;
        // The first message is read here, so an operation refused up front is an `Err` of this call
        // and not the first item of the iterator.
        let first = read_response(&stream, &id, Some(self.timeouts.answer))?
            .outcome
            .map_err(ClientError::Remote)?;
        Ok(RawEvents {
            stream,
            id,
            idle: self.timeouts.item,
            first: Some(first),
            done: false,
        })
    }

    /// Starts an operation and reads nothing of its answer, for a relay that waits on something
    /// else as well.
    #[cfg(feature = "loopback")]
    pub(crate) fn begin(
        &self,
        method: Method,
        params: Value,
    ) -> Result<RawEvents<'c, C>, ClientError> {
        let (stream, id) = self.open(method, params)?;
        Ok(RawEvents {
            stream,
            id,
            idle: None,
            first: None,
            done: false,
        })
    }

    fn call<P: Serialize, R: DeserializeOwned>(
        &self,
        method: Method,
        params: &P,
    ) -> Result<R, ClientError> {
        let params = serde_json::to_value(params).map_err(|_| ClientError::Malformed("request"))?;
        let result = self.call_raw(method, params)?;
        serde_json::from_value(result).map_err(|_| ClientError::Malformed("result"))
    }

    /// The agent's card, over the link: the only way to get it, since a link has no well-known
    /// address.
    ///
    /// # Errors
    ///
    /// As [`Client::call_raw`].
    pub fn agent_card(&self) -> Result<AgentCard, ClientError> {
        self.call(Method::GetAgentCard, &Value::Null)
    }

    /// `SendMessage`.
    ///
    /// # Errors
    ///
    /// As [`Client::call_raw`].
    pub fn send_message(
        &self,
        request: &SendMessageRequest,
    ) -> Result<SendMessageResponse, ClientError> {
        self.call(Method::SendMessage, request)
    }

    /// `SendStreamingMessage`: items in the order the agent made them, until its stream ends.
    ///
    /// # Errors
    ///
    /// As [`Client::stream_raw`].
    pub fn send_streaming_message(
        &self,
        request: &SendMessageRequest,
    ) -> Result<Events<'c, C>, ClientError> {
        let params =
            serde_json::to_value(request).map_err(|_| ClientError::Malformed("request"))?;
        Ok(Events(
            self.stream_raw(Method::SendStreamingMessage, params)?,
        ))
    }

    /// `GetTask`.
    ///
    /// # Errors
    ///
    /// As [`Client::call_raw`].
    pub fn get_task(&self, request: &GetTaskRequest) -> Result<Task, ClientError> {
        self.call(Method::GetTask, request)
    }

    /// `ListTasks`.
    ///
    /// # Errors
    ///
    /// As [`Client::call_raw`].
    pub fn list_tasks(&self, request: &ListTasksRequest) -> Result<ListTasksResponse, ClientError> {
        self.call(Method::ListTasks, request)
    }

    /// `CancelTask`.
    ///
    /// # Errors
    ///
    /// As [`Client::call_raw`].
    pub fn cancel_task(&self, request: &CancelTaskRequest) -> Result<Task, ClientError> {
        self.call(Method::CancelTask, request)
    }

    /// `SubscribeToTask`: the task as it is, then what follows, until it ends.
    ///
    /// # Errors
    ///
    /// As [`Client::stream_raw`].
    pub fn subscribe_to_task(
        &self,
        request: &SubscribeToTaskRequest,
    ) -> Result<Events<'c, C>, ClientError> {
        let params =
            serde_json::to_value(request).map_err(|_| ClientError::Malformed("request"))?;
        Ok(Events(self.stream_raw(Method::SubscribeToTask, params)?))
    }

    /// `GetExtendedAgentCard`.
    ///
    /// # Errors
    ///
    /// As [`Client::call_raw`].
    pub fn get_extended_agent_card(&self) -> Result<AgentCard, ClientError> {
        self.call(
            Method::GetExtendedAgentCard,
            &GetExtendedAgentCardRequest::default(),
        )
    }
}

/// What a stream's end means for the operation.
fn why<C: Channel>(stream: &Stream<'_, C>, failure: Failure) -> ClientError {
    match failure {
        Failure::Timeout => ClientError::Timeout,
        Failure::Permission | Failure::Protocol | Failure::PathLost => match stream.end() {
            Some(StreamEnd::Refused(reason)) => ClientError::NotGranted(reason),
            Some(StreamEnd::Revoked(reason)) => ClientError::Revoked(reason),
            Some(StreamEnd::Superseded(_) | StreamEnd::Broken | StreamEnd::PeerClosed) => {
                ClientError::Interrupted
            }
            Some(StreamEnd::LinkLost(failure)) => ClientError::Link(failure),
            Some(_) | None => ClientError::Link(failure),
        },
        other => ClientError::Link(other),
    }
}

fn read_message<C: Channel>(
    stream: &Stream<'_, C>,
    wait: Option<Duration>,
) -> Result<Option<Vec<u8>>, ClientError> {
    stream.receive(wait).map_err(|failure| why(stream, failure))
}

fn read_response<C: Channel>(
    stream: &Stream<'_, C>,
    id: &Value,
    wait: Option<Duration>,
) -> Result<Response, ClientError> {
    let bytes = read_message(stream, wait)?.ok_or(ClientError::Interrupted)?;
    let response = Response::decode(&bytes).ok_or(ClientError::Malformed("not a response"))?;
    // A server answers with the request's id, or null when it could not read one.
    if response.id != *id && !response.id.is_null() {
        return Err(ClientError::Malformed("the id is not the request's"));
    }
    Ok(response)
}

/// What one look at a stream found.
pub(crate) enum Polled {
    /// A whole message.
    Message(Vec<u8>),
    /// Nothing within the time allowed. The stream goes on.
    Quiet,
    /// The peer finished it: what it sent is whole.
    Finished,
    /// It ended any other way.
    Failed(ClientError),
}

/// The items of a streaming operation as JSON, until the stream finishes.
pub struct RawEvents<'c, C: Channel> {
    stream: Stream<'c, C>,
    id: Value,
    idle: Option<Duration>,
    first: Option<Value>,
    done: bool,
}

impl<C: Channel> fmt::Debug for RawEvents<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawEvents")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<C: Channel> RawEvents<'_, C> {
    /// The next message, waiting at most `wait`.
    pub(crate) fn poll(&self, wait: Option<Duration>) -> Polled {
        match self.stream.receive(wait) {
            Ok(Some(bytes)) => Polled::Message(bytes),
            // An orderly close says nothing of the answer being whole: only the peer's finish does.
            Ok(None) if self.stream.finished_by_peer() => Polled::Finished,
            Ok(None) => Polled::Failed(ClientError::Interrupted),
            Err(Failure::Timeout) => Polled::Quiet,
            Err(failure) => Polled::Failed(why(&self.stream, failure)),
        }
    }

    /// The id the request went out with, which a response repeats, as its JSON text.
    #[cfg(feature = "loopback")]
    pub(crate) fn request_id(&self) -> String {
        self.id.to_string()
    }
}

impl<C: Channel> Iterator for RawEvents<'_, C> {
    type Item = Result<Value, ClientError>;

    /// The next result, an error as `Err` (after which the iterator ends), or `None` when the
    /// stream finished in order. A stream cut short is `Err(Interrupted)`, never `None`.
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(first) = self.first.take() {
            return Some(Ok(first));
        }
        let message = match self.poll(self.idle) {
            Polled::Message(bytes) => bytes,
            Polled::Finished => {
                self.done = true;
                return None;
            }
            Polled::Quiet => {
                self.done = true;
                return Some(Err(ClientError::Timeout));
            }
            Polled::Failed(error) => {
                self.done = true;
                return Some(Err(error));
            }
        };
        let item = Response::decode(&message)
            .ok_or(ClientError::Malformed("not a response"))
            .and_then(|response| {
                if response.id != self.id && !response.id.is_null() {
                    return Err(ClientError::Malformed("the id is not the request's"));
                }
                response.outcome.map_err(ClientError::Remote)
            });
        if item.is_err() {
            self.done = true;
        }
        Some(item)
    }
}

/// The items of a streaming operation, typed.
pub struct Events<'c, C: Channel>(RawEvents<'c, C>);

impl<C: Channel> fmt::Debug for Events<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Events").field(&self.0).finish()
    }
}

impl<C: Channel> Iterator for Events<'_, C> {
    type Item = Result<StreamResponse, ClientError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.0.next()?;
        Some(item.and_then(|value| {
            serde_json::from_value(value).map_err(|_| ClientError::Malformed("stream item"))
        }))
    }
}
