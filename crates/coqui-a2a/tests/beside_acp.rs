//! A2A beside a real ACP client on one proven link: a peer granted only ACP is refused A2A and goes
//! on, then a grant adds A2A with no second proof, and a streamed task finishes in the middle of an
//! ACP turn. The ACP agent is scripted; the client is `coqui-acp`, speaking ACP's own messages.

#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod support;

use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use coqui_a2a::model::StreamResponse;
use coqui_a2a::{Client, ClientError, PROTOCOL, Server};
use coqui_acp::Event;
use coqui_acp::lines::{DEFAULT_MAX_LINE, over_io};
use coqui_agentic_peer::link::{Services, capabilities};
use coqui_agentic_peer::mux::LinkStream;
use coqui_agentic_peer::peers::{MemoryPeers, PeerRecord, PeerStore};
use coqui_peer_webcrypto::{Identity, Trust};
use coqui_web::Channel as _;
use serde_json::{Value, json};
use support::agent::{Echo, user};
use support::link::{Host, Setup, WAIT, Watchdog, over_link};

const ACP: &str = "acp/1";

fn chunk(text: &str) -> Vec<u8> {
    json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": "s1",
            "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": text}},
        },
    })
    .to_string()
    .into_bytes()
}

/// An ACP agent on one stream. Its turn says so on `in_turn` and then stays open until `release`.
fn acp_agent(
    stream: &dyn LinkStream,
    in_turn: &mpsc::SyncSender<()>,
    release: &Mutex<mpsc::Receiver<()>>,
) {
    while let Ok(Some(message)) = stream.receive(Some(WAIT)) {
        let request: Value = serde_json::from_slice(&message).unwrap();
        let Some(id) = request.get("id") else {
            continue;
        };
        let result = match request["method"].as_str() {
            Some("initialize") => json!({"protocolVersion": 1, "agentCapabilities": {}}),
            Some("session/new") => json!({"sessionId": "s1"}),
            Some("session/prompt") => {
                stream.send(&chunk("first")).unwrap();
                in_turn.send(()).unwrap();
                release.lock().unwrap().recv_timeout(WAIT).unwrap();
                stream.send(&chunk(" second")).unwrap();
                json!({"stopReason": "end_turn"})
            }
            _ => json!({}),
        };
        let answer = json!({"jsonrpc": "2.0", "id": id, "result": result});
        if stream.send(answer.to_string().as_bytes()).is_err() {
            break;
        }
    }
}

/// What the ACP client reads, one message per line.
struct Incoming {
    messages: mpsc::Receiver<Vec<u8>>,
    rest: Vec<u8>,
}

impl Read for Incoming {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.rest.is_empty() {
            let Ok(message) = self.messages.recv() else {
                return Ok(0);
            };
            self.rest = message;
        }
        let count = buffer.len().min(self.rest.len());
        buffer[..count].copy_from_slice(&self.rest[..count]);
        self.rest.drain(..count);
        Ok(count)
    }
}

/// What the ACP client writes, cut back into one message per line.
struct Outgoing {
    messages: mpsc::Sender<Vec<u8>>,
    line: Vec<u8>,
}

impl Write for Outgoing {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.line.extend_from_slice(data);
        while let Some(end) = self.line.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = self.line.drain(..=end).collect();
            line.pop();
            self.messages
                .send(line)
                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_peer_with_acp_gets_a2a_by_a_grant_and_a_streamed_task_runs_inside_an_acp_turn() {
    let _watchdog = Watchdog::start(Duration::from_secs(60), "a2a beside acp");
    let client_identity = Identity::generate().unwrap();
    let peer = client_identity.peer_id().to_string();
    let store = Arc::new(MemoryPeers::default());
    store.put(&PeerRecord::new(&peer).granting(ACP)).unwrap();

    let (in_turn, turn_began) = mpsc::sync_channel(1);
    let (release, released) = mpsc::channel();
    let released = Arc::new(Mutex::new(released));
    let services = Server::new(Echo::default())
        .serving(Services::default())
        .with(
            ACP,
            Box::new(move |stream: &dyn LinkStream, _| acp_agent(stream, &in_turn, &released)),
        );
    let host = Host::start(Setup::new(
        Identity::generate().unwrap(),
        Trust::pinned([client_identity.peer_id()]),
        store.clone(),
        services,
    ));

    over_link(&host, client_identity, |mux| {
        assert_eq!(capabilities(mux, WAIT).unwrap().offers, [ACP]);
        let acp_stream = mux.open(ACP).unwrap();
        let (to_agent, for_agent) = mpsc::channel::<Vec<u8>>();
        let (to_client, for_client) = mpsc::channel::<Vec<u8>>();
        let (transport, _) = over_io(
            Incoming {
                messages: for_client,
                rest: Vec::new(),
            },
            Outgoing {
                messages: to_agent,
                line: Vec::new(),
            },
            0,
            DEFAULT_MAX_LINE,
        )
        .unwrap();

        thread::scope(|scope| {
            let stream = &acp_stream;
            scope.spawn(move || {
                for line in for_agent {
                    if stream.send(&line).is_err() {
                        break;
                    }
                }
            });
            scope.spawn(move || {
                while let Ok(Some(mut message)) = stream.receive(Some(WAIT)) {
                    message.push(b'\n');
                    if to_client.send(message).is_err() {
                        break;
                    }
                }
            });

            let session = coqui_acp::Client::builder("/w")
                .prompt("hello")
                .connect(transport)
                .unwrap();
            turn_began.recv_timeout(WAIT).unwrap();

            // Refused while the turn runs, and the turn is untouched by it.
            let a2a = Client::new(mux);
            let refused = a2a.send_message(&user("hello")).unwrap_err();
            assert!(matches!(refused, ClientError::NotGranted(_)), "{refused}");

            // A grant on the same entry, with no second proof and no second link.
            store
                .put(&PeerRecord::new(&peer).granting(ACP).granting(PROTOCOL))
                .unwrap();
            assert_eq!(capabilities(mux, WAIT).unwrap().offers, [PROTOCOL, ACP]);
            let items: Vec<StreamResponse> = a2a
                .send_streaming_message(&user("stream"))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert!(items.last().unwrap().ends_task());
            assert!(
                session.next_event(Duration::from_millis(100)).is_none(),
                "the ACP turn is still open"
            );

            release.send(()).unwrap();
            let mut text = String::new();
            let stop = loop {
                match session.next_event(WAIT) {
                    Some(Event::Text(part)) => text.push_str(&part),
                    Some(Event::Completed { stop }) => break stop,
                    Some(Event::Failed(why)) => panic!("the turn failed: {why}"),
                    Some(_) => {}
                    None => panic!("the turn did not finish"),
                }
            };
            assert_eq!(text, "first second");
            assert_eq!(stop, "end_turn");

            session.close();
            acp_stream.close();
        });
    });
}
