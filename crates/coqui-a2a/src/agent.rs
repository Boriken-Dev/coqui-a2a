//! What a host implements to be an A2A agent on a link.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use coqui_agentic_peer::mux::{LinkStream, StreamLife};

use crate::error::{A2aError, ErrorKind};
use crate::model::{
    AgentCard, CancelTaskRequest, GetExtendedAgentCardRequest, GetTaskRequest, ListTasksRequest,
    ListTasksResponse, SendMessageRequest, SendMessageResponse, StreamResponse,
    SubscribeToTaskRequest, Task,
};
use crate::wire::ServiceParameters;

/// A stream of items from an agent, in the order they were generated. The server sends each as it
/// is produced and stops after the one that ends the task. Dropping it tells the agent nobody is
/// listening.
pub type Events = Box<dyn Iterator<Item = Result<StreamResponse, A2aError>> + Send>;

/// Becomes closed when nobody is listening to an operation any more: the client left, the link
/// went, or the operation ended. An [`Events`] iterator that waits holds one, and returns soon
/// after it closes.
#[derive(Debug, Clone, Default)]
pub struct Watch(Arc<Flag>);

#[derive(Debug, Default)]
struct Flag {
    closed: AtomicBool,
    /// Closing this one closes the watch that was made from it, not the other way.
    parent: Option<Watch>,
}

impl Watch {
    /// An open watch.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether nobody is listening.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::SeqCst) || self.0.parent.as_ref().is_some_and(Watch::is_closed)
    }

    /// A watch that closes when this one does, and can be closed alone. [`Server::stream`] gives
    /// each streaming call one, so that dropping the stream releases what waits on it without
    /// ending the caller's other operations.
    ///
    /// [`Server::stream`]: crate::Server::stream
    #[must_use]
    pub fn child(&self) -> Watch {
        Watch(Arc::new(Flag {
            closed: AtomicBool::new(false),
            parent: Some(self.clone()),
        }))
    }

    /// Says nobody is listening. Every clone sees it, and it cannot be undone. A host that calls
    /// its own agent through [`Caller::local`] closes the watch to stop an operation it started.
    pub fn close(&self) {
        self.0.closed.store(true, Ordering::SeqCst);
    }
}

/// Who is calling: the peer a link proved, or the host itself, calling in process.
#[derive(Clone, Copy)]
pub struct Caller<'a> {
    /// The peer, as its proof showed. This is the authentication.
    pub peer: &'a str,
    /// What the host set about that peer when it admitted it.
    pub claims: &'a BTreeMap<String, String>,
    /// The request's service parameters, which hold an agent's own token if it wants one.
    pub service_parameters: &'a ServiceParameters,
    stream: Option<&'a dyn LinkStream>,
    watch: &'a Watch,
}

impl core::fmt::Debug for Caller<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Caller")
            .field("peer", &self.peer)
            .field("service_parameters", &self.service_parameters)
            .finish_non_exhaustive()
    }
}

impl<'a> Caller<'a> {
    pub(crate) fn new(
        peer: &'a str,
        claims: &'a BTreeMap<String, String>,
        service_parameters: &'a ServiceParameters,
        stream: &'a dyn LinkStream,
        watch: &'a Watch,
    ) -> Self {
        Self {
            peer,
            claims,
            service_parameters,
            stream: Some(stream),
            watch,
        }
    }

    /// A caller that is not on a link: a host calling its own agent through [`Server::call`] or
    /// [`Server::stream`](crate::Server::stream).
    ///
    /// `peer` is the identity the host answers to the agent, which authorizes by it exactly as
    /// for a peer a link proved; nothing here proves it, so it is as true as the host's own code.
    /// The caller is open until `watch` is closed.
    ///
    /// Name a local caller in a namespace no proof can produce: a proved peer id is the link
    /// crate's hash form (`sha256:` and digits), so `local:` followed by a name of the host's own
    /// never equals one, and an agent or a store keyed by `peer` cannot confuse the two. Use one
    /// local peer for each party whose tasks must not reach another's, such as `local:` and a
    /// session id for each parent session, since a store gives a task to the peer that made it.
    /// [`Caller::is_local`] tells the two kinds apart in an agent.
    ///
    /// [`Server::call`]: crate::Server::call
    #[must_use]
    pub fn local(
        peer: &'a str,
        claims: &'a BTreeMap<String, String>,
        service_parameters: &'a ServiceParameters,
        watch: &'a Watch,
    ) -> Self {
        Self {
            peer,
            claims,
            service_parameters,
            stream: None,
            watch,
        }
    }

    /// Whether the client is still there. A long operation checks it between steps and stops when
    /// it is not.
    #[must_use]
    pub fn is_open(&self) -> bool {
        !self.watch.is_closed()
            && self
                .stream
                .is_none_or(|stream| stream.life() == StreamLife::Open)
    }

    /// This caller with another watch, which is the one it is open until.
    pub(crate) fn watched<'b>(self, watch: &'b Watch) -> Caller<'b>
    where
        'a: 'b,
    {
        Caller { watch, ..self }
    }

    /// Whether the caller is the host itself, in process, and not a peer a link proved.
    #[must_use]
    pub fn is_local(&self) -> bool {
        self.stream.is_none()
    }

    /// A handle an [`Events`] iterator keeps to learn when to stop.
    #[must_use]
    pub fn watch(&self) -> Watch {
        self.watch.clone()
    }
}

/// An A2A agent. One value serves every link, so it is shared between threads.
///
/// Each method runs on a thread of its own and may take as long as the operation does. Only
/// [`Agent::agent_card`] and [`Agent::send_message`] are required: the rest answer
/// `UnsupportedOperationError` until implemented.
///
/// The server checks, before a method runs: the A2A version, required extensions, that streaming
/// and the extended card are declared by the card, and that no push-notification configuration is
/// asked for.
pub trait Agent: Send + Sync {
    /// The card this agent shows a caller. Its `capabilities.streaming` and
    /// `capabilities.extendedAgentCard` decide which operations the server lets through.
    fn agent_card(&self, caller: &Caller<'_>) -> AgentCard;

    /// `SendMessage`.
    ///
    /// # Errors
    ///
    /// Any A2A error, such as `ContentTypeNotSupportedError` or `TaskNotFoundError`.
    fn send_message(
        &self,
        caller: &Caller<'_>,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2aError>;

    /// `SendStreamingMessage`. The first item is the task or the message.
    ///
    /// # Errors
    ///
    /// As [`Agent::send_message`].
    fn send_streaming_message(
        &self,
        caller: &Caller<'_>,
        request: SendMessageRequest,
    ) -> Result<Events, A2aError> {
        let _ = (caller, request);
        Err(A2aError::standard(ErrorKind::UnsupportedOperation))
    }

    /// `GetTask`.
    ///
    /// # Errors
    ///
    /// `TaskNotFoundError` for a task that does not exist or is not this caller's.
    fn get_task(&self, caller: &Caller<'_>, request: GetTaskRequest) -> Result<Task, A2aError> {
        let _ = (caller, request);
        Err(A2aError::standard(ErrorKind::UnsupportedOperation))
    }

    /// `ListTasks`: only this caller's tasks, most recently updated first.
    ///
    /// # Errors
    ///
    /// Any A2A error.
    fn list_tasks(
        &self,
        caller: &Caller<'_>,
        request: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2aError> {
        let _ = (caller, request);
        Err(A2aError::standard(ErrorKind::UnsupportedOperation))
    }

    /// `CancelTask`.
    ///
    /// # Errors
    ///
    /// `TaskNotCancelableError` or `TaskNotFoundError`.
    fn cancel_task(
        &self,
        caller: &Caller<'_>,
        request: CancelTaskRequest,
    ) -> Result<Task, A2aError> {
        let _ = (caller, request);
        Err(A2aError::standard(ErrorKind::UnsupportedOperation))
    }

    /// `SubscribeToTask`. The first item is the task as it is now.
    ///
    /// # Errors
    ///
    /// `TaskNotFoundError`, or `UnsupportedOperationError` for a task that has ended.
    fn subscribe_to_task(
        &self,
        caller: &Caller<'_>,
        request: SubscribeToTaskRequest,
    ) -> Result<Events, A2aError> {
        let _ = (caller, request);
        Err(A2aError::standard(ErrorKind::UnsupportedOperation))
    }

    /// `GetExtendedAgentCard`, reached only when the card declares one.
    ///
    /// # Errors
    ///
    /// `ExtendedAgentCardNotConfiguredError` by default.
    fn get_extended_agent_card(
        &self,
        caller: &Caller<'_>,
        request: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2aError> {
        let _ = (caller, request);
        Err(A2aError::standard(
            ErrorKind::ExtendedAgentCardNotConfigured,
        ))
    }
}
