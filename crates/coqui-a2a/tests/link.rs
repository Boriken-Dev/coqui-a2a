//! A2A over a real proven link: a WebSocket on loopback, the in-band proof, a sealed channel, and
//! the link's streams, with a second protocol on the same link.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod support;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use coqui_a2a::error::ErrorKind;
use coqui_a2a::model::{
    CancelTaskRequest, GetTaskRequest, ListTasksRequest, SendMessageResponse, StreamResponse,
    SubscribeToTaskRequest, TaskState,
};
use coqui_a2a::wire::Method;
use coqui_a2a::{BINDING, Client, ClientError, PROTOCOL, Server, ServerLimits};
use coqui_agentic_peer::link::{Services, capabilities};
use coqui_agentic_peer::mux::{LinkStream, StreamEnd};
use coqui_agentic_peer::peers::{MemoryPeers, PeerRecord, PeerStore};
use coqui_peer_webcrypto::{Identity, Trust};
use coqui_web::Channel as _;
use serde_json::{Value, json};
use support::agent::{Echo, user};
use support::link::{Host, Setup, WAIT, Watchdog, over_link};

const ECHO: &str = "echo/1";

/// A second protocol on the same link: it sends back what it is sent.
fn echo_service(services: Services) -> Services {
    services.with(
        ECHO,
        Box::new(|stream: &dyn LinkStream, _| {
            while let Ok(Some(message)) = stream.receive(Some(WAIT)) {
                if stream.send(&message).is_err() {
                    break;
                }
            }
        }),
    )
}

struct World {
    host: Host,
    client: Identity,
    store: Arc<MemoryPeers>,
    agent: Echo,
}

/// A host serving the echo agent and a second protocol to one client, granted both.
fn world(grants: &[&str]) -> World {
    let agent = Echo::default();
    let client = Identity::generate().unwrap();
    let store = Arc::new(MemoryPeers::default());
    let mut record = PeerRecord::new(&client.peer_id().to_string());
    for token in grants {
        record = record.granting(token);
    }
    store.put(&record).unwrap();
    let services = echo_service(Server::new(agent.clone()).serving(Services::default()));
    let setup = Setup::new(
        Identity::generate().unwrap(),
        Trust::pinned([client.peer_id()]),
        store.clone(),
        services,
    );
    World {
        host: Host::start(setup),
        client,
        store,
        agent,
    }
}

/// A host serving `services`, with its one client granted `a2a/1`.
fn hosting(services: Services) -> (Host, Identity) {
    let client = Identity::generate().unwrap();
    let store = Arc::new(MemoryPeers::default());
    store
        .put(&PeerRecord::new(&client.peer_id().to_string()).granting(PROTOCOL))
        .unwrap();
    let host = Host::start(Setup::new(
        Identity::generate().unwrap(),
        Trust::pinned([client.peer_id()]),
        store,
        services,
    ));
    (host, client)
}

/// Waits for `count` iterators of `agent` to have been dropped, which frees the operations they
/// belonged to.
fn released(agent: &Echo, count: usize, what: &str) {
    let started = Instant::now();
    while agent.ended.load(Ordering::SeqCst) < count {
        assert!(started.elapsed() < WAIT, "{what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn artifact_text(response: &SendMessageResponse) -> String {
    let SendMessageResponse::Task(task) = response else {
        panic!("a task was expected");
    };
    task.artifacts.as_ref().unwrap()[0].parts[0]
        .text
        .clone()
        .unwrap()
}

#[test]
fn a_task_completes_over_a_proven_link_that_also_carries_a_second_protocol() {
    let world = world(&[PROTOCOL, ECHO]);
    over_link(&world.host, world.client.clone(), |mux| {
        let offers = capabilities(mux, WAIT).unwrap().offers;
        assert_eq!(offers, [PROTOCOL, ECHO]);

        // The second protocol is open while the task runs.
        let other = mux.open(ECHO).unwrap();
        other.send(b"ping").unwrap();

        let client = Client::new(mux);
        let response = client.send_message(&user("hello")).unwrap();
        assert_eq!(artifact_text(&response), "echo: hello");
        let SendMessageResponse::Task(task) = response else {
            unreachable!()
        };
        assert_eq!(task.status.state, TaskState::Completed);

        assert_eq!(other.receive(Some(WAIT)).unwrap().unwrap(), b"ping");
        let again = client
            .get_task(&GetTaskRequest {
                id: task.id.clone(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(again.id, task.id);
    });
}

#[test]
fn a_streamed_task_arrives_in_generation_order_and_ends_with_its_stream() {
    let world = world(&[PROTOCOL, ECHO]);
    over_link(&world.host, world.client.clone(), |mux| {
        let client = Client::new(mux);
        let items: Vec<StreamResponse> = client
            .send_streaming_message(&user("stream"))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let kinds: Vec<&str> = items
            .iter()
            .map(|item| match item {
                StreamResponse::Task(_) => "task",
                StreamResponse::StatusUpdate(_) => "status",
                StreamResponse::ArtifactUpdate(_) => "artifact",
                _ => "other",
            })
            .collect();
        assert_eq!(kinds, ["task", "artifact", "artifact", "status"]);
        let texts: Vec<String> = items
            .iter()
            .filter_map(|item| match item {
                StreamResponse::ArtifactUpdate(event) => event.artifact.parts[0].text.clone(),
                _ => None,
            })
            .collect();
        assert_eq!(texts, ["echo: stream", " (and again)"]);
        assert!(items.last().unwrap().ends_task());
    });
}

#[test]
fn the_other_operations_work_and_the_card_names_the_link() {
    let world = world(&[PROTOCOL]);
    over_link(&world.host, world.client.clone(), |mux| {
        let client = Client::new(mux);
        let card = client.agent_card().unwrap();
        let interface = &card.supported_interfaces[0];
        assert_eq!(interface.protocol_binding, BINDING);
        assert_eq!(interface.protocol_version, "1.0");
        assert_eq!(card.capabilities.push_notifications, Some(false));
        assert!(
            client
                .get_extended_agent_card()
                .unwrap()
                .description
                .contains("extended")
        );

        let SendMessageResponse::Task(held) = client.send_message(&user("hold")).unwrap() else {
            panic!("a task was expected");
        };
        assert_eq!(held.status.state, TaskState::Working);
        let listed = client
            .list_tasks(&ListTasksRequest {
                status: Some(TaskState::Working),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(listed.total_size, 1);
        assert_eq!(listed.next_page_token, "");

        // Subscribing gives the task as it is, then its end once it is canceled.
        let mut events = client
            .subscribe_to_task(&SubscribeToTaskRequest {
                id: held.id.clone(),
                ..Default::default()
            })
            .unwrap();
        assert!(matches!(
            events.next().unwrap().unwrap(),
            StreamResponse::Task(_)
        ));
        let canceled = client
            .cancel_task(&CancelTaskRequest {
                id: held.id.clone(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(canceled.status.state, TaskState::Canceled);
        let StreamResponse::StatusUpdate(update) = events.next().unwrap().unwrap() else {
            panic!("a status update was expected");
        };
        assert_eq!(update.status.state, TaskState::Canceled);
        assert!(
            events.next().is_none(),
            "the stream ends at a terminal state"
        );

        let again = client.cancel_task(&CancelTaskRequest {
            id: held.id.clone(),
            ..Default::default()
        });
        assert!(
            matches!(&again, Err(ClientError::Remote(error)) if error.kind() == Some(ErrorKind::TaskNotCancelable)),
            "{again:?}"
        );
        let ended = client.subscribe_to_task(&SubscribeToTaskRequest {
            id: held.id,
            ..Default::default()
        });
        assert!(
            matches!(&ended, Err(ClientError::Remote(error)) if error.kind() == Some(ErrorKind::UnsupportedOperation)),
            "{ended:?}"
        );
        let missing = client.get_task(&GetTaskRequest {
            id: "nope".into(),
            ..Default::default()
        });
        assert!(
            matches!(&missing, Err(ClientError::Remote(error)) if error.kind() == Some(ErrorKind::TaskNotFound))
        );
    });
}

#[test]
fn push_notification_operations_answer_the_error_for_it() {
    let world = world(&[PROTOCOL]);
    over_link(&world.host, world.client.clone(), |mux| {
        let client = Client::new(mux);
        for method in Method::ALL.iter().filter(|method| method.is_push()) {
            let error = client
                .call_raw(*method, json!({ "id": "task-1" }))
                .unwrap_err();
            assert!(
                matches!(&error, ClientError::Remote(e) if e.kind() == Some(ErrorKind::PushNotificationNotSupported)),
                "{method}: {error}"
            );
        }
        // A webhook asked for inside SendMessage is refused the same way.
        let mut request = user("hello");
        request.configuration = Some(coqui_a2a::model::SendMessageConfiguration {
            task_push_notification_config: Some(json!({ "url": "https://example.com/hook" })),
            ..Default::default()
        });
        let error = client.send_message(&request).unwrap_err();
        assert!(
            matches!(&error, ClientError::Remote(e) if e.kind() == Some(ErrorKind::PushNotificationNotSupported))
        );
    });
}

#[test]
fn every_error_type_crosses_a_real_link_as_the_table_says() {
    let world = world(&[PROTOCOL]);
    over_link(&world.host, world.client.clone(), |mux| {
        let client = Client::new(mux);
        for kind in ErrorKind::ALL {
            let error = client
                .send_message(&user(&format!("fail:{}", kind.name())))
                .unwrap_err();
            let ClientError::Remote(error) = error else {
                panic!("{kind}: {error}");
            };
            assert_eq!(error.kind(), Some(*kind), "{kind}");
            assert_eq!(error.code, kind.code());
            assert_eq!(error.data[0]["reason"], kind.reason());
            assert_eq!(error.data[0]["domain"], kind.domain());
        }
    });
}

#[test]
fn a_peer_listed_for_one_protocol_gets_the_other_by_a_grant_on_the_same_link() {
    let world = world(&[ECHO]);
    over_link(&world.host, world.client.clone(), |mux| {
        assert_eq!(capabilities(mux, WAIT).unwrap().offers, [ECHO]);
        let client = Client::new(mux);
        let refused = client.send_message(&user("hello")).unwrap_err();
        assert!(matches!(refused, ClientError::NotGranted(_)), "{refused}");
        // The first protocol is untouched by the refusal.
        let other = mux.open(ECHO).unwrap();
        other.send(b"still here").unwrap();
        assert_eq!(other.receive(Some(WAIT)).unwrap().unwrap(), b"still here");

        // A grant on the same entry, with no second proof and no second link.
        let peer = world.client.peer_id().to_string();
        world
            .store
            .put(&PeerRecord::new(&peer).granting(ECHO).granting(PROTOCOL))
            .unwrap();
        assert_eq!(capabilities(mux, WAIT).unwrap().offers, [PROTOCOL, ECHO]);
        assert!(client.send_message(&user("hello")).is_ok());
    });
}

#[test]
fn a_peer_not_listed_for_the_protocol_is_refused_and_keeps_the_other() {
    let world = world(&[PROTOCOL, ECHO]);
    let stranger = Identity::generate().unwrap();
    // The stranger is let into the link by its proof, and granted only the echo protocol.
    world
        .store
        .put(&PeerRecord::new(&stranger.peer_id().to_string()).granting(ECHO))
        .unwrap();
    let host = {
        let services = echo_service(Server::new(world.agent.clone()).serving(Services::default()));
        Host::start(Setup::new(
            Identity::generate().unwrap(),
            Trust::pinned([stranger.peer_id()]),
            world.store.clone(),
            services,
        ))
    };
    over_link(&host, stranger, |mux| {
        let refused = Client::new(mux).send_message(&user("hello")).unwrap_err();
        assert!(matches!(&refused, ClientError::NotGranted(_)), "{refused}");
        let other = mux.open(ECHO).unwrap();
        other.send(b"kept").unwrap();
        assert_eq!(other.receive(Some(WAIT)).unwrap().unwrap(), b"kept");
    });
}

#[test]
fn a_running_stream_ends_as_revoked_when_the_grant_goes() {
    let world = world(&[PROTOCOL, ECHO]);
    over_link(&world.host, world.client.clone(), |mux| {
        let client = Client::new(mux);
        let mut events = client.send_streaming_message(&user("hold")).unwrap();
        assert!(matches!(
            events.next().unwrap().unwrap(),
            StreamResponse::Task(_)
        ));

        let peer = world.client.peer_id().to_string();
        world
            .store
            .put(&PeerRecord::new(&peer).granting(ECHO))
            .unwrap();
        let started = Instant::now();
        let ended = events.next().unwrap();
        assert!(matches!(ended, Err(ClientError::Revoked(_))), "{ended:?}");
        assert!(
            started.elapsed() < WAIT,
            "the recheck is a fraction of a second here"
        );
        assert!(events.next().is_none());

        // The other protocol keeps running on the same link.
        let other = mux.open(ECHO).unwrap();
        other.send(b"alive").unwrap();
        assert_eq!(other.receive(Some(WAIT)).unwrap().unwrap(), b"alive");
    });
}

fn raw_request(
    mux: &coqui_agentic_peer::mux::Mux<'_, impl coqui_web::Channel>,
    bytes: &[u8],
) -> Value {
    let stream = mux.open(PROTOCOL).unwrap();
    stream.send(bytes).unwrap_or_else(|error| {
        panic!(
            "{error:?} state {:?} end {:?}",
            mux.link_state(),
            stream.end()
        )
    });
    stream.finish().unwrap();
    let answer = stream.receive(Some(WAIT)).unwrap().unwrap();
    serde_json::from_slice(&answer).unwrap()
}

#[test]
fn hostile_requests_are_answered_with_errors_and_the_link_goes_on() {
    let world = world(&[PROTOCOL]);
    over_link(&world.host, world.client.clone(), |mux| {
        let code = |value: &Value| value["error"]["code"].as_i64().unwrap();

        // Not JSON, not an object, not JSON-RPC, no method, an unknown method, bad parameters.
        assert_eq!(code(&raw_request(mux, b"{not json")), -32700);
        assert_eq!(code(&raw_request(mux, b"[1,2,3]")), -32600);
        assert_eq!(
            code(&raw_request(mux, br#"{"id":1,"method":"GetTask"}"#)),
            -32600
        );
        assert_eq!(
            code(&raw_request(mux, br#"{"jsonrpc":"2.0","id":1}"#)),
            -32600
        );
        let unknown = raw_request(mux, br#"{"jsonrpc":"2.0","id":7,"method":"Frobnicate"}"#);
        assert_eq!(code(&unknown), -32601);
        assert_eq!(unknown["id"], 7);
        assert_eq!(
            code(&raw_request(
                mux,
                br#"{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":5}}"#
            )),
            -32602
        );
        assert_eq!(code(&raw_request(mux, br#"{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{}]}}}"#)), -32602);
        let version = raw_request(mux, br#"{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":"x"},"a2a-service-parameters":{"A2A-Version":"0.3"}}"#);
        assert_eq!(code(&version), -32009);

        // Bad service parameters, and an id that is not a scalar.
        assert_eq!(
            code(&raw_request(
                mux,
                br#"{"jsonrpc":"2.0","id":1,"method":"GetTask","a2a-service-parameters":{"a":5}}"#
            )),
            -32600
        );
        assert_eq!(
            code(&raw_request(
                mux,
                br#"{"jsonrpc":"2.0","id":{"a":1},"method":"GetTask"}"#
            )),
            -32600
        );

        // An oversized request is refused by size, not parsed.
        let mut big = br#"{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"x":""#.to_vec();
        big.extend(std::iter::repeat_n(b'a', 2 << 20));
        big.extend_from_slice(b"\"}}");
        assert_eq!(code(&raw_request(mux, &big)), -32600);

        // A stream that never sends its request is let go; one that finishes without one ends.
        let silent = mux.open(PROTOCOL).unwrap();
        silent.finish().unwrap();
        assert_eq!(silent.receive(Some(WAIT)).unwrap(), None);

        assert!(
            Client::new(mux).send_message(&user("after")).is_ok(),
            "the link survived"
        );
    });
}

#[test]
fn a_stream_carrying_an_operation_twice_is_refused() {
    let world = world(&[PROTOCOL]);
    over_link(&world.host, world.client.clone(), |mux| {
        let request = br#"{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":"x"}}"#;
        let stream = mux.open(PROTOCOL).unwrap();
        stream.send(request).unwrap();
        stream.send(request).unwrap();
        let mut answers = 0;
        while let Ok(Some(_)) = stream.receive(Some(WAIT)) {
            answers += 1;
        }
        // The server sees the second message before it answers, and refuses, or only after: then it
        // has answered the first and said so with `finish`. The `Close` that follows a `finish` is a
        // frame of its own, so `end` can still be `None` here.
        match stream.end() {
            Some(StreamEnd::Refused(reason)) => {
                assert_eq!(reason, "one operation per stream");
                assert_eq!(answers, 0, "a refused stream answers nothing");
            }
            end => {
                assert!(stream.finished_by_peer(), "{end:?}");
                assert_eq!(answers, 1, "the second operation is never run");
            }
        }

        // Mid-stream, on a quiet subscription, a second message ends it within the poll.
        let SendMessageResponse::Task(held) = Client::new(mux).send_message(&user("hold")).unwrap()
        else {
            panic!("a task was expected");
        };
        let sub = mux.open(PROTOCOL).unwrap();
        let subscribe =
            json!({"jsonrpc":"2.0","id":2,"method":"SubscribeToTask","params":{"id":held.id}})
                .to_string();
        sub.send(subscribe.as_bytes()).unwrap();
        assert!(
            sub.receive(Some(WAIT)).unwrap().is_some(),
            "the task snapshot"
        );
        sub.send(subscribe.as_bytes()).unwrap();
        let cut = sub.receive(Some(WAIT));
        assert!(cut.is_err(), "{cut:?}");
        assert!(matches!(sub.end(), Some(StreamEnd::Refused(_))));
        drop(sub);
        let started = Instant::now();
        while world.agent.ended.load(Ordering::SeqCst) < 1 {
            assert!(started.elapsed() < WAIT, "the subscription was not let go");
            std::thread::sleep(Duration::from_millis(20));
        }
    });
}

#[test]
fn a_client_that_leaves_mid_stream_frees_the_agent() {
    let world = world(&[PROTOCOL]);
    over_link(&world.host, world.client.clone(), |mux| {
        let client = Client::new(mux);
        let mut events = client.send_streaming_message(&user("hold")).unwrap();
        assert!(events.next().unwrap().is_ok());
        drop(events);
        let started = Instant::now();
        while world.agent.ended.load(Ordering::SeqCst) < 1 {
            assert!(started.elapsed() < WAIT, "the agent kept serving nobody");
            std::thread::sleep(Duration::from_millis(20));
        }
        let next = client.send_message(&user("next"));
        assert!(next.is_ok(), "{next:?}");
    });
}

#[test]
fn an_agent_that_panics_costs_one_operation_not_the_link() {
    let world = world(&[PROTOCOL]);
    over_link(&world.host, world.client.clone(), |mux| {
        let client = Client::new(mux);
        let error = client.send_message(&user("panic")).unwrap_err();
        assert!(
            matches!(&error, ClientError::Remote(e) if e.kind() == Some(ErrorKind::Internal)),
            "{error}"
        );
        assert!(client.send_message(&user("fine")).is_ok());
    });
}

#[test]
fn a_message_past_the_links_own_bound_breaks_that_stream_and_keeps_the_link() {
    let agent = Echo::default();
    let client = Identity::generate().unwrap();
    let store = Arc::new(MemoryPeers::default());
    store
        .put(&PeerRecord::new(&client.peer_id().to_string()).granting(PROTOCOL))
        .unwrap();
    let mut setup = Setup::new(
        Identity::generate().unwrap(),
        Trust::pinned([client.peer_id()]),
        store,
        Server::new(agent).serving(Services::default()),
    );
    setup.options.limits.max_message = 64 * 1024;
    let host = Host::start(setup);
    over_link(&host, client, |mux| {
        let client = Client::new(mux);
        let mut request = user("big");
        request.message.parts[0].text = Some("a".repeat(100 * 1024));
        let error = client.send_message(&request).unwrap_err();
        assert!(
            matches!(error, ClientError::Interrupted | ClientError::Link(_)),
            "{error}"
        );
        assert!(
            client.send_message(&user("small")).is_ok(),
            "the link and the next stream are fine"
        );
    });
}

#[test]
fn an_agent_still_producing_is_released_however_the_operation_ends() {
    let _hang = Watchdog::start(
        Duration::from_secs(60),
        "an operation never let its agent go",
    );
    let world = world(&[PROTOCOL]);
    over_link(&world.host, world.client.clone(), |mux| {
        // The client leaves while the agent has more ready than the channel holds.
        let client = Client::new(mux);
        let mut events = client.send_streaming_message(&user("flood:64")).unwrap();
        assert!(events.next().unwrap().is_ok());
        drop(events);
        released(
            &world.agent,
            1,
            "the client left and the agent kept producing",
        );

        // A second message on the stream ends it the same way.
        let stream = mux.open(PROTOCOL).unwrap();
        let request = json!({
            "jsonrpc": "2.0", "id": 1, "method": "SendStreamingMessage",
            "params": { "message": { "messageId": "m", "role": "ROLE_USER", "parts": [{ "text": "flood:64" }] } },
        })
        .to_string();
        stream.send(request.as_bytes()).unwrap();
        assert!(stream.receive(Some(WAIT)).unwrap().is_some());
        stream.send(request.as_bytes()).unwrap();
        released(&world.agent, 2, "a refused stream kept its agent producing");

        let next = client.send_message(&user("next"));
        assert!(next.is_ok(), "{next:?}");
    });
}

/// The window is 1 MiB and a send waits 250 ms for credit by default, so a client that reads nothing
/// for longer than that, with more than the window on its way, is what this needs.
const CHUNK: usize = 256 * 1024;

#[test]
fn a_slow_reader_gets_all_of_it_and_the_end_says_so() {
    let world = world(&[PROTOCOL]);
    over_link(&world.host, world.client.clone(), |mux| {
        let client = Client::new(mux);
        let mut events = client
            .send_streaming_message(&user(&format!("big:12:{CHUNK}")))
            .unwrap();
        let first = events.next().unwrap().unwrap();
        assert!(matches!(first, StreamResponse::Task(_)));
        std::thread::sleep(Duration::from_millis(1200));
        let rest: Vec<StreamResponse> = events.collect::<Result<_, _>>().unwrap();
        assert_eq!(rest.len(), 13, "twelve chunks and the task's end");
        assert!(rest.last().unwrap().ends_task());
    });
}

#[test]
fn a_reader_that_stops_loses_the_stream_and_is_told_it_is_cut_short() {
    let agent = Echo::default();
    let mut limits = ServerLimits::default();
    limits.reader_stall = Duration::from_millis(300);
    let services = Server::new(agent.clone())
        .with_limits(limits)
        .serving(Services::default());
    let (host, client) = hosting(services);
    over_link(&host, client, |mux| {
        let request = json!({
            "jsonrpc": "2.0", "id": 1, "method": "SendStreamingMessage",
            "params": { "message": { "messageId": "m", "role": "ROLE_USER", "parts": [{ "text": format!("big:40:{CHUNK}") }] } },
        })
        .to_string();
        let stream = mux.open(PROTOCOL).unwrap();
        stream.send(request.as_bytes()).unwrap();
        stream.finish().unwrap();
        assert!(stream.receive(Some(WAIT)).unwrap().is_some());
        released(
            &agent,
            1,
            "the agent was not released when the reader stopped",
        );

        // What was already on its way can be read, and then the end is no finish.
        let mut read = 1;
        let end = loop {
            match stream.receive(Some(WAIT)) {
                Ok(Some(_)) => read += 1,
                other => break other,
            }
        };
        assert!(
            read < 41,
            "the whole task did not get through a stopped reader"
        );
        assert!(end.is_err(), "{end:?}");
        assert_eq!(stream.end(), Some(StreamEnd::Broken));
        assert!(!stream.finished_by_peer());
    });
}

#[test]
fn a_stream_closed_in_order_with_no_finish_is_not_a_whole_answer() {
    // A peer that says two things, and goes without `finish`: what a crash or a bad server does.
    let services = Services::default().with(
        PROTOCOL,
        Box::new(|stream: &dyn LinkStream, _| {
            let request: Value =
                serde_json::from_slice(&stream.receive(Some(WAIT)).unwrap().unwrap()).unwrap();
            for _ in 0..2 {
                let item = json!({ "jsonrpc": "2.0", "id": request["id"], "result": { "statusUpdate": { "taskId": "t", "contextId": "c", "status": { "state": "TASK_STATE_WORKING" } } } });
                stream.send(item.to_string().as_bytes()).unwrap();
            }
        }),
    );
    let (host, client) = hosting(services);
    over_link(&host, client, |mux| {
        let client = Client::new(mux);
        let mut events = client.send_streaming_message(&user("x")).unwrap();
        assert!(events.next().unwrap().is_ok());
        assert!(events.next().unwrap().is_ok());
        let end = events.next();
        assert!(
            matches!(end, Some(Err(ClientError::Interrupted))),
            "{end:?}"
        );
        assert!(events.next().is_none());
    });

    // With no item at all, a single-answer operation and a stream both say so.
    let services = Services::default().with(
        PROTOCOL,
        Box::new(|stream: &dyn LinkStream, _| {
            let _ = stream.receive(Some(WAIT));
        }),
    );
    let (host, client) = hosting(services);
    over_link(&host, client, |mux| {
        let client = Client::new(mux);
        let call = client.send_message(&user("x"));
        assert!(matches!(call, Err(ClientError::Interrupted)), "{call:?}");
        let stream = client.send_streaming_message(&user("x"));
        assert!(
            matches!(stream, Err(ClientError::Interrupted)),
            "{stream:?}"
        );
    });
}
