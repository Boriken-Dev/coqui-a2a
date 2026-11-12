//! A host calling its own agent in process meets the checks a link meets, and gets the same errors.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod support;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use coqui_a2a::error::{A2aError, ErrorKind};
use coqui_a2a::model::{
    AgentCapabilities, AgentCard, AgentExtension, Message, Role, SendMessageConfiguration,
    SendMessageRequest, SendMessageResponse, StreamResponse, Task, TaskState, TaskStatus,
    TaskStatusUpdateEvent,
};
use coqui_a2a::server::Reply;
use coqui_a2a::wire::{Method, ServiceParameters};
use coqui_a2a::{Agent, Caller, Client, ClientError, Events, PROTOCOL, Server, Watch};
use coqui_agentic_peer::link::Services;
use coqui_agentic_peer::peers::{MemoryPeers, PeerRecord, PeerStore};
use coqui_peer_webcrypto::{Identity, Trust};
use serde_json::{Value, json};
use support::link::{Host, Setup, over_link};

const EXTENSION: &str = "https://example.com/needed";

/// An agent whose card can be made to decline what the server checks for.
#[derive(Clone, Default)]
struct Plain {
    streams: bool,
    extended: bool,
    needs_extension: bool,
    /// Times a method reached the agent.
    reached: Arc<AtomicUsize>,
    /// What `Caller::is_local` said, and the watch the agent was given, at each streaming call.
    seen: Arc<Mutex<Vec<(bool, Watch)>>>,
}

impl Plain {
    fn task(&self, state: TaskState) -> Task {
        Task {
            id: "task-1".to_owned(),
            status: TaskStatus::now(state),
            ..Task::default()
        }
    }
}

impl Agent for Plain {
    fn agent_card(&self, _caller: &Caller<'_>) -> AgentCard {
        let mut capabilities = AgentCapabilities {
            streaming: Some(self.streams),
            extended_agent_card: Some(self.extended),
            // The server forces this off; an agent that says otherwise is not believed.
            push_notifications: Some(true),
            ..AgentCapabilities::default()
        };
        if self.needs_extension {
            capabilities.extensions = Some(vec![AgentExtension {
                uri: Some(EXTENSION.to_owned()),
                required: Some(true),
                ..AgentExtension::default()
            }]);
        }
        AgentCard {
            name: "plain".to_owned(),
            capabilities,
            ..AgentCard::default()
        }
    }

    fn send_message(
        &self,
        _caller: &Caller<'_>,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2aError> {
        self.reached.fetch_add(1, Ordering::SeqCst);
        let text = request.message.joined_text();
        assert!(text != "panic", "the agent was told to panic");
        Ok(SendMessageResponse::Task(self.task(TaskState::Completed)))
    }

    fn send_streaming_message(
        &self,
        caller: &Caller<'_>,
        request: SendMessageRequest,
    ) -> Result<Events, A2aError> {
        self.reached.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap()
            .push((caller.is_local(), caller.watch()));
        let finished = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
            task_id: "task-1".to_owned(),
            status: TaskStatus::now(TaskState::Completed),
            ..TaskStatusUpdateEvent::default()
        });
        let items = match request.message.joined_text().as_str() {
            "panic" => panic!("the agent was told to panic"),
            // An agent that goes on after a message, which is not a stream of one task.
            "message" => vec![
                StreamResponse::Message(Message::text(Role::Agent, "m", "hi")),
                finished,
            ],
            // An agent that goes on after the task has ended.
            _ => vec![
                StreamResponse::Task(self.task(TaskState::Working)),
                finished.clone(),
                finished,
            ],
        };
        Ok(Box::new(items.into_iter().map(Ok)))
    }
}

fn user(text: &str) -> Value {
    serde_json::to_value(SendMessageRequest::new(Message::text(
        Role::User,
        "m1",
        text,
    )))
    .unwrap()
}

fn parameters(pairs: &[(&str, &str)]) -> ServiceParameters {
    let mut parameters = ServiceParameters::default();
    for (name, value) in pairs {
        parameters.set(name, value).unwrap();
    }
    parameters
}

/// The error of `method` called in process, `None` when it was accepted.
fn locally(
    agent: &Plain,
    parameters: &ServiceParameters,
    method: Method,
    params: &Value,
) -> Option<A2aError> {
    let server = Server::new(agent.clone());
    let (claims, watch) = (BTreeMap::new(), Watch::new());
    let caller = Caller::local("host", &claims, parameters, &watch);
    if method.streams() {
        server.stream(&caller, method, params.clone()).err()
    } else {
        server.call(&caller, method, params.clone()).err()
    }
}

/// The error of `method` over a real link, `None` when it was accepted.
fn on_a_link(
    agent: &Plain,
    parameters: &ServiceParameters,
    method: Method,
    params: &Value,
) -> Option<A2aError> {
    let client = Identity::generate().unwrap();
    let store = Arc::new(MemoryPeers::default());
    store
        .put(&PeerRecord::new(&client.peer_id().to_string()).granting(PROTOCOL))
        .unwrap();
    let services = Server::new(agent.clone()).serving(Services::default());
    let host = Host::start(Setup::new(
        Identity::generate().unwrap(),
        Trust::pinned([client.peer_id()]),
        store,
        services,
    ));
    over_link(&host, client, |mux| {
        let client = Client::new(mux).with_service_parameters(parameters.clone());
        let outcome = if method.streams() {
            client.stream_raw(method, params.clone()).map(|_| ())
        } else {
            client.call_raw(method, params.clone()).map(|_| ())
        };
        match outcome {
            Ok(()) => None,
            Err(ClientError::Remote(error)) => Some(error),
            Err(other) => panic!("{method}: {other}"),
        }
    })
}

fn same_on_both(
    what: &str,
    agent: &Plain,
    pairs: &[(&str, &str)],
    method: Method,
    params: &Value,
    kind: ErrorKind,
) {
    let parameters = parameters(pairs);
    let local = locally(agent, &parameters, method, params);
    let link = on_a_link(agent, &parameters, method, params);
    assert_eq!(
        local.as_ref().and_then(A2aError::kind),
        Some(kind),
        "{what}: in process: {local:?}"
    );
    assert_eq!(local, link, "{what}: a link says something else");
}

#[test]
fn a_wrong_version_is_refused_as_a_link_refuses_it() {
    let agent = Plain {
        streams: true,
        ..Plain::default()
    };
    for (what, version) in [
        ("2.0", "2.0"),
        ("a list with one", "1.0, 2.0"),
        ("text", "x"),
    ] {
        for method in [
            Method::SendMessage,
            Method::GetTask,
            Method::SendStreamingMessage,
        ] {
            same_on_both(
                what,
                &agent,
                &[("a2a-version", version)],
                method,
                &user("hi"),
                ErrorKind::VersionNotSupported,
            );
        }
    }
    assert_eq!(agent.reached.load(Ordering::SeqCst), 0);
}

#[test]
fn a_streaming_call_of_an_agent_that_does_not_stream_is_refused_as_a_link_refuses_it() {
    let agent = Plain::default();
    for method in [Method::SendStreamingMessage, Method::SubscribeToTask] {
        same_on_both(
            "no streaming",
            &agent,
            &[],
            method,
            &json!({ "id": "task-1", "message": { "messageId": "m", "role": "ROLE_USER", "parts": [{ "text": "hi" }] } }),
            ErrorKind::UnsupportedOperation,
        );
    }
    assert_eq!(agent.reached.load(Ordering::SeqCst), 0);
}

#[test]
fn an_extended_card_the_card_does_not_declare_is_refused_as_a_link_refuses_it() {
    same_on_both(
        "no extended card",
        &Plain::default(),
        &[],
        Method::GetExtendedAgentCard,
        &json!({}),
        ErrorKind::UnsupportedOperation,
    );
}

#[test]
fn a_required_extension_that_is_not_declared_is_refused_as_a_link_refuses_it() {
    let agent = Plain {
        needs_extension: true,
        ..Plain::default()
    };
    same_on_both(
        "no extension",
        &agent,
        &[],
        Method::SendMessage,
        &user("hi"),
        ErrorKind::ExtensionSupportRequired,
    );
    let declared = parameters(&[("a2a-extensions", EXTENSION)]);
    assert!(locally(&agent, &declared, Method::SendMessage, &user("hi")).is_none());
}

#[test]
fn every_push_request_is_refused_as_a_link_refuses_it() {
    let agent = Plain {
        streams: true,
        ..Plain::default()
    };
    for method in Method::ALL.iter().filter(|method| method.is_push()) {
        same_on_both(
            method.name(),
            &agent,
            &[],
            *method,
            &json!({ "id": "task-1" }),
            ErrorKind::PushNotificationNotSupported,
        );
    }
    let mut request = SendMessageRequest::new(Message::text(Role::User, "m", "hi"));
    request.configuration = Some(SendMessageConfiguration {
        task_push_notification_config: Some(json!({ "url": "https://example.com/hook" })),
        ..SendMessageConfiguration::default()
    });
    let params = serde_json::to_value(request).unwrap();
    for method in [Method::SendMessage, Method::SendStreamingMessage] {
        same_on_both(
            "a webhook in the configuration",
            &agent,
            &[],
            method,
            &params,
            ErrorKind::PushNotificationNotSupported,
        );
    }
    assert_eq!(agent.reached.load(Ordering::SeqCst), 0);
}

#[test]
fn a_request_that_is_not_one_and_an_agent_that_panics_are_answered_as_a_link_answers_them() {
    let agent = Plain {
        streams: true,
        ..Plain::default()
    };
    same_on_both(
        "no message",
        &agent,
        &[],
        Method::SendMessage,
        &json!({}),
        ErrorKind::InvalidParams,
    );
    for method in [Method::SendMessage, Method::SendStreamingMessage] {
        same_on_both(
            "a panic",
            &agent,
            &[],
            method,
            &user("panic"),
            ErrorKind::Internal,
        );
    }
}

#[test]
fn an_operation_is_not_run_through_the_other_form() {
    let agent = Plain {
        streams: true,
        ..Plain::default()
    };
    let (claims, watch, none) = (BTreeMap::new(), Watch::new(), ServiceParameters::default());
    let caller = Caller::local("host", &claims, &none, &watch);
    let server = Server::new(agent.clone());
    let error = server
        .call(&caller, Method::SendStreamingMessage, user("hi"))
        .unwrap_err();
    assert_eq!(error.kind(), Some(ErrorKind::InvalidRequest));
    let error = server
        .stream(&caller, Method::SendMessage, user("hi"))
        .map(|_| ())
        .unwrap_err();
    assert_eq!(error.kind(), Some(ErrorKind::InvalidRequest));
    assert_eq!(agent.reached.load(Ordering::SeqCst), 0);
}

#[test]
fn an_accepted_call_answers_what_the_agent_answered() {
    let agent = Plain {
        streams: true,
        ..Plain::default()
    };
    let (claims, watch, none) = (BTreeMap::new(), Watch::new(), ServiceParameters::default());
    let caller = Caller::local("host", &claims, &none, &watch);
    let server = Server::new(agent);
    let Reply::Message(SendMessageResponse::Task(task)) = server
        .call(&caller, Method::SendMessage, user("hi"))
        .unwrap()
    else {
        panic!("a task was expected");
    };
    assert_eq!(task.status.state, TaskState::Completed);

    // The card is the agent's, with no webhooks, and no interface for a link the caller is not on.
    let Reply::Card(card) = server
        .call(&caller, Method::GetAgentCard, json!({}))
        .unwrap()
    else {
        panic!("a card was expected");
    };
    assert_eq!(card.name, "plain");
    assert_eq!(card.capabilities.push_notifications, Some(false));
    assert!(card.link_interface().is_none());
}

#[test]
fn a_stream_ends_after_the_item_that_ends_the_task_and_after_a_first_message() {
    let agent = Plain {
        streams: true,
        ..Plain::default()
    };
    let (claims, watch, none) = (BTreeMap::new(), Watch::new(), ServiceParameters::default());
    let caller = Caller::local("host", &claims, &none, &watch);
    let server = Server::new(agent);

    let items: Vec<_> = server
        .stream(&caller, Method::SendStreamingMessage, user("hi"))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 2, "{items:?}");
    assert!(items[1].as_ref().unwrap().ends_task());

    let items: Vec<_> = server
        .stream(&caller, Method::SendStreamingMessage, user("message"))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 1, "{items:?}");
    assert!(matches!(items[0], Ok(StreamResponse::Message(_))));
}

#[test]
fn a_local_caller_is_open_until_its_watch_is_closed() {
    let (claims, watch, none) = (BTreeMap::new(), Watch::new(), ServiceParameters::default());
    let caller = Caller::local("host", &claims, &none, &watch);
    assert_eq!(caller.peer, "host");
    assert!(caller.is_open());
    let kept = caller.watch();
    assert!(!kept.is_closed());
    watch.close();
    assert!(!caller.is_open());
    assert!(kept.is_closed());
}

#[test]
fn a_dropped_stream_closes_its_own_watch_and_not_the_callers() {
    let agent = Plain {
        streams: true,
        ..Plain::default()
    };
    let (claims, watch, none) = (BTreeMap::new(), Watch::new(), ServiceParameters::default());
    let caller = Caller::local("local:host", &claims, &none, &watch);
    let server = Server::new(agent.clone());
    let events = server
        .stream(&caller, Method::SendStreamingMessage, user("hi"))
        .unwrap();
    let (local, given) = agent.seen.lock().unwrap()[0].clone();
    assert!(local);
    assert!(!given.is_closed());
    drop(events);
    // What the agent was waiting on is released, and the host's own watch is still open for the
    // next operation.
    assert!(given.is_closed());
    assert!(!watch.is_closed());

    // Closing the host's watch ends every operation it started.
    let events = server
        .stream(&caller, Method::SendStreamingMessage, user("hi"))
        .unwrap();
    let given = agent.seen.lock().unwrap()[1].1.clone();
    watch.close();
    assert!(given.is_closed());
    drop(events);
}

#[test]
fn a_caller_on_a_link_is_not_local_and_no_proved_id_is_in_the_local_namespace() {
    let agent = Plain {
        streams: true,
        ..Plain::default()
    };
    let none = ServiceParameters::default();
    let events = on_a_link_streaming(&agent, &none);
    assert!(events);
    assert!(!agent.seen.lock().unwrap()[0].0);
    // A proved peer id is the hash form the link crate makes; nothing in it starts with `local:`.
    for _ in 0..32 {
        let id = Identity::generate().unwrap().peer_id().to_string();
        assert!(!id.starts_with("local:"), "{id}");
    }
}

/// Opens `SendStreamingMessage` over a real link, for the agent to see its caller.
fn on_a_link_streaming(agent: &Plain, parameters: &ServiceParameters) -> bool {
    on_a_link(agent, parameters, Method::SendStreamingMessage, &user("hi")).is_none()
}
