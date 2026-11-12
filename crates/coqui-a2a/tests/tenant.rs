//! Several agents behind one card: the card lists them as skills and a request's `tenant` chooses.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use coqui_a2a::error::{A2aError, ErrorKind};
use coqui_a2a::model::{
    AgentCard, AgentSkill, Message, Role, SendMessageRequest, SendMessageResponse, Task, TaskState,
    TaskStatus,
};
use coqui_a2a::server::Reply;
use coqui_a2a::wire::{Method, ServiceParameters};
use coqui_a2a::{Agent, Caller, Client, ClientError, PROTOCOL, Server, Watch};
use coqui_agentic_peer::link::Services;
use coqui_agentic_peer::peers::{MemoryPeers, PeerRecord, PeerStore};
use coqui_peer_webcrypto::{Identity, Trust};
use support::link::{Host, Setup, over_link};

/// A host with two agents, `alpha` and `beta`. Whoever is not `"owner"` may use only `alpha`.
struct Several;

fn skill(id: &str) -> AgentSkill {
    AgentSkill {
        id: id.to_owned(),
        name: format!("the {id} agent"),
        ..AgentSkill::default()
    }
}

impl Agent for Several {
    fn agent_card(&self, caller: &Caller<'_>) -> AgentCard {
        let mut card = AgentCard {
            name: "host".to_owned(),
            skills: vec![skill("alpha")],
            ..AgentCard::default()
        };
        if caller.peer == "owner" {
            card.skills.push(skill("beta"));
        }
        card
    }

    fn send_message(
        &self,
        caller: &Caller<'_>,
        request: coqui_a2a::model::SendMessageRequest,
    ) -> Result<SendMessageResponse, A2aError> {
        // The agent chooses by the caller's own card, so what the caller may not name is not found.
        let card = self.agent_card(caller);
        let chosen = card.select_skill(request.tenant.as_deref())?;
        Ok(SendMessageResponse::Task(Task {
            id: format!("task-of-{}", chosen.id),
            status: TaskStatus::now(TaskState::Completed),
            ..Task::default()
        }))
    }
}

fn request(tenant: Option<&str>) -> SendMessageRequest {
    let mut request = SendMessageRequest::new(Message::text(Role::User, "m", "hi"));
    request.tenant = tenant.map(str::to_owned);
    request
}

fn agent_of(response: &SendMessageResponse) -> &str {
    let SendMessageResponse::Task(task) = response else {
        panic!("a task was expected");
    };
    &task.id
}

fn in_process(peer: &str, tenant: Option<&str>) -> Result<SendMessageResponse, A2aError> {
    let (claims, parameters, watch) = (BTreeMap::new(), ServiceParameters::default(), Watch::new());
    let caller = Caller::local(peer, &claims, &parameters, &watch);
    let params = serde_json::to_value(request(tenant)).unwrap();
    match Server::new(Several).call(&caller, Method::SendMessage, params)? {
        Reply::Message(response) => Ok(response),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_tenant_chooses_the_agent_from_the_callers_card() {
    let both = in_process("owner", Some("beta")).unwrap();
    assert_eq!(agent_of(&both), "task-of-beta");
    assert_eq!(
        agent_of(&in_process("owner", Some("alpha")).unwrap()),
        "task-of-alpha"
    );
    // A card of one agent is that agent when no tenant is named.
    assert_eq!(
        agent_of(&in_process("guest", None).unwrap()),
        "task-of-alpha"
    );
}

#[test]
fn no_tenant_on_a_card_of_several_and_an_unlisted_tenant_are_refused_naming_what_is_listed() {
    let none = in_process("owner", None).unwrap_err();
    assert_eq!(none.kind(), Some(ErrorKind::InvalidParams));
    assert!(none.message.contains("alpha, beta"), "{}", none.message);

    let unknown = in_process("owner", Some("gamma")).unwrap_err();
    assert_eq!(unknown.kind(), Some(ErrorKind::InvalidParams));
    assert!(
        unknown.message.contains("alpha, beta"),
        "{}",
        unknown.message
    );

    // An agent the caller may not use answers as one that does not exist, and the message shows
    // only what this caller's card shows.
    let forbidden = in_process("guest", Some("beta")).unwrap_err();
    let absent = in_process("guest", Some("gamma")).unwrap_err();
    assert_eq!(forbidden.kind(), Some(ErrorKind::InvalidParams));
    assert!(!forbidden.message.contains("alpha, beta"));
    assert_eq!(
        forbidden.message.replace("\"beta\"", "X"),
        absent.message.replace("\"gamma\"", "X")
    );

    // The tenant is the skill id as text, case included.
    assert!(in_process("owner", Some("Alpha")).is_err());
}

#[test]
fn a_card_with_no_skill_names_no_agent() {
    let card = AgentCard::default();
    for tenant in [None, Some("alpha")] {
        let error = card.select_skill(tenant).unwrap_err();
        assert_eq!(error.kind(), Some(ErrorKind::InvalidParams));
    }
}

#[test]
fn the_choice_is_the_same_over_a_link() {
    let client = Identity::generate().unwrap();
    let store = Arc::new(MemoryPeers::default());
    store
        .put(&PeerRecord::new(&client.peer_id().to_string()).granting(PROTOCOL))
        .unwrap();
    let host = Host::start(Setup::new(
        Identity::generate().unwrap(),
        Trust::pinned([client.peer_id()]),
        store,
        Server::new(Several).serving(Services::default()),
    ));
    // The peer a link proved is not "owner", so it has the card of one agent.
    over_link(&host, client, |mux| {
        let client = Client::new(mux);
        let alpha = client.send_message(&request(Some("alpha"))).unwrap();
        assert_eq!(agent_of(&alpha), "task-of-alpha");
        assert_eq!(
            agent_of(&client.send_message(&request(None)).unwrap()),
            "task-of-alpha"
        );
        let Err(ClientError::Remote(error)) = client.send_message(&request(Some("beta"))) else {
            panic!("beta is not this peer's");
        };
        assert_eq!(error.kind(), Some(ErrorKind::InvalidParams));
        assert!(error.message.contains("alpha"), "{}", error.message);
        assert!(!error.message.contains("alpha, beta"));
    });
}
