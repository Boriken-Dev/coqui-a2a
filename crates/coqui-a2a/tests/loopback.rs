//! The loopback endpoint, reached with plain HTTP over a real link to a real agent.
#![cfg(feature = "loopback")]
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod support;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

use coqui_a2a::loopback::{Config, Hub, Loopback};
use coqui_a2a::{PROTOCOL, Server};
use coqui_agentic_peer::link::Services;
use coqui_agentic_peer::mux::LinkStream;
use coqui_agentic_peer::peers::{MemoryPeers, PeerRecord, PeerStore};
use coqui_peer_webcrypto::{Identity, Trust};
use serde_json::{Value, json};
use support::agent::Echo;
use support::link::{Host, Setup, WAIT, Watchdog, over_link};

struct Reply {
    status: u16,
    headers: String,
    body: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|_| panic!("not JSON: {}", self.body))
    }
}

/// One HTTP/1.0 exchange: no chunking, and the end of the body is the end of the connection.
fn http(address: SocketAddr, head: &str, body: &[u8]) -> Reply {
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    // In one write: a body arriving after a refusal has closed the connection resets it, which loses the refusal.
    let mut request = head.as_bytes().to_vec();
    request.extend_from_slice(body);
    stream.write_all(&request).unwrap();
    let mut bytes = Vec::new();
    let _ = stream.read_to_end(&mut bytes);
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    Reply {
        status,
        headers: head.to_ascii_lowercase(),
        body: body.to_owned(),
    }
}

fn post(address: SocketAddr, extra: &str, body: &Value) -> Reply {
    let body = body.to_string();
    post_raw(
        address,
        &format!("Host: {address}\r\n{extra}"),
        "application/json",
        body.as_bytes(),
    )
}

fn post_raw(address: SocketAddr, headers: &str, content_type: &str, body: &[u8]) -> Reply {
    let head = format!(
        "POST / HTTP/1.0\r\n{headers}Content-Type: {content_type}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    http(address, &head, body)
}

fn rpc(id: &str, method: &str, params: &Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

fn message(text: &str) -> Value {
    json!({ "message": { "messageId": format!("m-{text}"), "role": "ROLE_USER", "parts": [{ "text": text }] } })
}

/// Closes the hub when a test ends, a failing assertion included, so the link's thread can end.
struct CloseHub(Arc<Hub>);

impl Drop for CloseHub {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// A running endpoint and what a test can see of the other end.
struct Endpoint {
    address: SocketAddr,
    hub: Arc<Hub>,
    agent: Echo,
}

impl Endpoint {
    /// Waits for `count` of the agent's streams to have been let go.
    fn released(&self, count: usize, what: &str) {
        let until = Instant::now() + WAIT;
        while self.agent.ended.load(Ordering::SeqCst) < count {
            assert!(Instant::now() < until, "{what}");
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Waits for the agent to stop producing: everything between it and the client is full.
    fn settled(&self) -> usize {
        let mut last = self.agent.produced.load(Ordering::SeqCst);
        let until = Instant::now() + WAIT;
        loop {
            thread::sleep(Duration::from_millis(400));
            let now = self.agent.produced.load(Ordering::SeqCst);
            if now == last && now > 0 {
                return now;
            }
            assert!(Instant::now() < until, "the agent never stopped producing");
            last = now;
        }
    }
}

/// Runs `body` with a loopback endpoint attached to a real link to `services`.
fn with_services(
    services: Services,
    agent: Echo,
    config: impl FnOnce(Config) -> Config,
    body: impl FnOnce(&Endpoint),
) {
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
    let hub = Hub::new();
    let endpoint = Loopback::start(
        config(Config::new("127.0.0.1:0".parse().unwrap()).unwrap()),
        Arc::clone(&hub),
    )
    .unwrap();
    over_link(&host, client, |mux| {
        thread::scope(|scope| {
            let _closes = CloseHub(Arc::clone(&hub));
            scope.spawn(|| hub.attach(mux));
            // Until then the endpoint rightly answers that it has no link, which a busy machine shows.
            let until = Instant::now() + WAIT;
            while !hub.is_up() {
                assert!(Instant::now() < until, "the link never attached");
                thread::sleep(Duration::from_millis(5));
            }
            body(&Endpoint {
                address: endpoint.local_addr(),
                hub: Arc::clone(&hub),
                agent,
            });
        });
    });
}

/// Runs `body` with an endpoint in front of the echo agent, and a look at that agent.
fn with_echo(config: impl FnOnce(Config) -> Config, body: impl FnOnce(&Endpoint)) {
    let agent = Echo::default();
    let services = Server::new(agent.clone()).serving(Services::default());
    with_services(services, agent, config, body);
}

/// As [`with_echo`], for a test that only needs the address.
fn with_endpoint(config: impl FnOnce(Config) -> Config, body: impl FnOnce(SocketAddr)) {
    with_echo(config, |endpoint| body(endpoint.address));
}

/// Starts an operation over HTTP/1.1 and reads none of the answer.
fn started(address: SocketAddr, method: &str, text: &str) -> TcpStream {
    let body = rpc("1", method, &message(text)).to_string();
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    write!(
        stream,
        "POST / HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    stream
}

/// Reads until a whole first event has come, and returns what it read.
fn first_event(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut part = [0_u8; 4096];
    loop {
        let text = String::from_utf8_lossy(&bytes);
        if text.contains("data:") && text.contains("\n\n") {
            return text.into_owned();
        }
        match stream.read(&mut part) {
            Ok(read) if read > 0 => bytes.extend_from_slice(&part[..read]),
            _ => return text.into_owned(),
        }
    }
}

/// A peer that reads the request and sends each of `messages`, then goes with no `finish`.
fn saying(messages: Vec<&'static [u8]>) -> Services {
    Services::default().with(
        PROTOCOL,
        Box::new(move |stream: &dyn LinkStream, _| {
            let _ = stream.receive(Some(WAIT));
            for message in &messages {
                stream.send(message).unwrap();
            }
        }),
    )
}

/// The JSON of each event of a whole response.
fn sse_events(body: &str) -> Vec<Value> {
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|data| serde_json::from_str(data.trim()).unwrap())
        .collect()
}

#[test]
fn the_card_is_served_at_the_well_known_path_and_names_the_endpoint() {
    with_endpoint(
        |config| config,
        |address| {
            let reply = http(
                address,
                &format!("GET /.well-known/agent-card.json HTTP/1.0\r\nHost: {address}\r\n\r\n"),
                b"",
            );
            assert_eq!(reply.status, 200);
            let card = reply.json();
            assert_eq!(card["name"], "Echo");
            assert_eq!(
                card["supportedInterfaces"],
                json!([{ "url": format!("http://{address}/"), "protocolBinding": "JSONRPC", "protocolVersion": "1.0" }])
            );
            assert_eq!(card["capabilities"]["pushNotifications"], false);
            assert!(
                !card.to_string().contains("coqui-agentic-peer"),
                "the link's own interface is not shown"
            );
        },
    );
}

#[test]
fn a_standard_json_rpc_client_completes_a_task() {
    with_endpoint(
        |config| config,
        |address| {
            let reply = post(
                address,
                "A2A-Version: 1.0\r\n",
                &rpc("req-1", "SendMessage", &message("hello")),
            );
            assert_eq!(reply.status, 200);
            assert!(reply.headers.contains("application/json"));
            let answer = reply.json();
            assert_eq!(answer["id"], "req-1");
            assert_eq!(
                answer["result"]["task"]["status"]["state"],
                "TASK_STATE_COMPLETED"
            );
            assert_eq!(
                answer["result"]["task"]["artifacts"][0]["parts"][0]["text"],
                "echo: hello"
            );

            let id = answer["result"]["task"]["id"].clone();
            let got = post(address, "", &rpc("req-2", "GetTask", &json!({ "id": id }))).json();
            assert_eq!(got["result"]["id"], id);
        },
    );
}

#[test]
fn a_streamed_task_is_server_sent_events_in_order() {
    with_endpoint(
        |config| config,
        |address| {
            let reply = post(
                address,
                "",
                &rpc("s-1", "SendStreamingMessage", &message("stream")),
            );
            assert_eq!(reply.status, 200);
            assert!(
                reply.headers.contains("text/event-stream"),
                "{}",
                reply.headers
            );
            let events = sse_events(&reply.body);
            let kinds: Vec<&str> = events
                .iter()
                .map(|event| {
                    ["task", "artifactUpdate", "statusUpdate"]
                        .into_iter()
                        .find(|key| event["result"].get(key).is_some())
                        .unwrap()
                })
                .collect();
            assert_eq!(
                kinds,
                ["task", "artifactUpdate", "artifactUpdate", "statusUpdate"]
            );
            assert!(events.iter().all(|event| event["id"] == "s-1"));
        },
    );
}

#[test]
fn errors_cross_as_json_rpc_errors_and_the_endpoint_adds_none_of_its_own() {
    with_endpoint(
        |config| config,
        |address| {
            let code = |reply: Reply| reply.json()["error"]["code"].as_i64().unwrap();
            assert_eq!(
                code(post(
                    address,
                    "",
                    &rpc("1", "GetTask", &json!({ "id": "nope" }))
                )),
                -32001
            );
            assert_eq!(
                code(post(
                    address,
                    "A2A-Version: 0.3\r\n",
                    &rpc("1", "GetTask", &json!({ "id": "x" }))
                )),
                -32009
            );
            assert_eq!(
                code(post(address, "", &rpc("1", "Frobnicate", &json!({})))),
                -32601
            );
            assert_eq!(
                code(post(address, "", &rpc("1", "GetAgentCard", &json!({})))),
                -32601
            );
            assert_eq!(
                code(post(
                    address,
                    "",
                    &rpc(
                        "1",
                        "CreateTaskPushNotificationConfig",
                        &json!({ "taskId": "x" })
                    )
                )),
                -32003
            );
            assert_eq!(
                code(post(
                    address,
                    "",
                    &rpc("1", "SendMessage", &json!({ "message": {} }))
                )),
                -32602
            );
            assert_eq!(
                code(post(
                    address,
                    "",
                    &rpc("1", "SendMessage", &message("fail:TaskNotCancelableError"))
                )),
                -32002
            );
            let unreadable = post_raw(
                address,
                &format!("Host: {address}\r\n"),
                "application/json",
                b"{nope",
            );
            assert_eq!(code(unreadable), -32700);
            let batch = post(address, "", &json!([rpc("1", "GetTask", &json!({}))]));
            assert_eq!(code(batch), -32600);
            // A stream refused up front is an ordinary JSON answer, not an empty event stream.
            let refused = post(
                address,
                "",
                &rpc("1", "SubscribeToTask", &json!({ "id": "nope" })),
            );
            assert!(refused.headers.contains("application/json"));
            assert_eq!(code(refused), -32001);
        },
    );
}

#[test]
fn a_web_page_and_a_rebinding_host_are_refused() {
    with_endpoint(
        |config| config,
        |address| {
            let body = rpc("1", "GetTask", &json!({ "id": "x" }));
            // Any Origin, including one that looks local, and including a null one.
            for origin in ["https://evil.example", "http://127.0.0.1:3000", "null"] {
                let reply = post(address, &format!("Origin: {origin}\r\n"), &body);
                assert_eq!(reply.status, 403, "{origin}");
            }
            let preflight = http(
                address,
                &format!(
                    "OPTIONS / HTTP/1.0\r\nHost: {address}\r\nOrigin: https://evil.example\r\nAccess-Control-Request-Method: POST\r\n\r\n"
                ),
                b"",
            );
            assert_eq!(preflight.status, 403);
            assert!(!preflight.headers.contains("access-control-allow"));

            // A Host that is not the loopback address, or not this port, or absent.
            let port = address.port();
            for host in [
                format!("evil.example:{port}"),
                "evil.example".to_owned(),
                format!("127.0.0.1:{}", port.wrapping_add(1)),
                format!("10.0.0.1:{port}"),
                format!("127.0.0.1.evil.example:{port}"),
            ] {
                let reply = post_raw(
                    address,
                    &format!("Host: {host}\r\n"),
                    "application/json",
                    body.to_string().as_bytes(),
                );
                assert_eq!(reply.status, 403, "{host}");
            }
            let hostless = http(
                address,
                "POST / HTTP/1.0\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
                b"",
            );
            assert_eq!(hostless.status, 403);

            // The loopback names are fine.
            let local = post_raw(
                address,
                &format!("Host: localhost:{port}\r\n"),
                "application/json",
                body.to_string().as_bytes(),
            );
            assert_eq!(local.status, 200);
        },
    );
}

#[test]
fn a_listed_origin_is_let_through_and_nothing_else_is() {
    with_endpoint(
        |config| config.allow_origin("http://127.0.0.1:3000"),
        |address| {
            let body = rpc("1", "GetTask", &json!({ "id": "x" }));
            assert_eq!(
                post(address, "Origin: http://127.0.0.1:3000\r\n", &body).status,
                200
            );
            assert_eq!(
                post(address, "Origin: http://127.0.0.1:3001\r\n", &body).status,
                403
            );
        },
    );
}

#[test]
fn only_json_and_only_post_and_only_a_bounded_body() {
    with_endpoint(
        |config| config.with_max_body(4096),
        |address| {
            let ok = rpc("1", "GetTask", &json!({ "id": "x" })).to_string();
            let head = format!("Host: {address}\r\n");
            assert_eq!(
                post_raw(address, &head, "text/plain", ok.as_bytes()).status,
                415
            );
            assert_eq!(
                post_raw(
                    address,
                    &head,
                    "application/x-www-form-urlencoded",
                    ok.as_bytes()
                )
                .status,
                415
            );
            assert_eq!(
                post_raw(address, &head, "application/json", &vec![b' '; 100_000]).status,
                413
            );
            assert_eq!(
                http(
                    address,
                    &format!("GET / HTTP/1.0\r\nHost: {address}\r\n\r\n"),
                    b""
                )
                .status,
                405
            );
            assert_eq!(
                http(
                    address,
                    &format!("GET /elsewhere HTTP/1.0\r\nHost: {address}\r\n\r\n"),
                    b""
                )
                .status,
                404
            );
            assert_eq!(
                post_raw(address, &head, "application/json", ok.as_bytes()).status,
                200
            );
        },
    );
}

#[test]
fn a_bearer_token_can_be_required_of_every_request() {
    with_endpoint(
        |config| config.with_token("s3cret"),
        |address| {
            let body = rpc("1", "GetTask", &json!({ "id": "x" }));
            assert_eq!(post(address, "", &body).status, 401);
            assert_eq!(
                post(address, "Authorization: Bearer wrong\r\n", &body).status,
                401
            );
            assert_eq!(
                post(address, "Authorization: Bearer s3cret\r\n", &body).status,
                200
            );
            let card = http(
                address,
                &format!("GET /.well-known/agent-card.json HTTP/1.0\r\nHost: {address}\r\n\r\n"),
                b"",
            );
            assert_eq!(card.status, 401);
        },
    );
}

#[test]
fn an_address_that_is_not_loopback_is_refused() {
    for address in ["0.0.0.0:0", "192.168.1.5:80", "[::]:0", "172.16.0.1:0"] {
        assert!(Config::new(address.parse().unwrap()).is_err(), "{address}");
    }
    for address in ["127.0.0.1:0", "127.5.5.5:0", "[::1]:0"] {
        assert!(Config::new(address.parse().unwrap()).is_ok(), "{address}");
    }
}

#[test]
fn with_no_link_attached_every_operation_says_so() {
    let hub = Hub::new();
    let endpoint =
        Loopback::start(Config::new("127.0.0.1:0".parse().unwrap()).unwrap(), hub).unwrap();
    let address = endpoint.local_addr();
    let reply = post(address, "", &rpc("1", "SendMessage", &message("hello")));
    assert_eq!(reply.status, 503);
    assert_eq!(reply.json()["error"]["code"], -32603);
    thread::sleep(Duration::from_millis(10));
}

#[test]
fn http_clients_that_leave_let_go_of_their_streams() {
    with_echo(
        |config| config,
        |endpoint| {
            // More than the link's limit of streams on one protocol, each quiet after its first item.
            for round in 1..=20 {
                let mut stream = started(endpoint.address, "SendStreamingMessage", "hold");
                let event = first_event(&mut stream);
                assert!(event.contains("\"task\""), "round {round}: {event}");
                drop(stream);
                endpoint.released(round, "a client left and its stream stayed open");
            }
            // A single answer that has not come yet goes the same way.
            for round in 21..=23 {
                let stream = started(endpoint.address, "SendMessage", "wait");
                thread::sleep(Duration::from_millis(200));
                drop(stream);
                endpoint.released(round, "a client left and its call stayed open");
            }
            let after = post(
                endpoint.address,
                "",
                &rpc("1", "SendMessage", &message("after")),
            );
            assert_eq!(after.status, 200);
            assert!(after.body.contains("echo: after"), "{}", after.body);
        },
    );
}

#[test]
fn a_client_that_stops_reading_holds_up_nothing_else_and_never_reads_a_clean_end() {
    let _hang = Watchdog::start(
        Duration::from_secs(60),
        "a client that stopped reading held up the endpoint",
    );
    with_echo(
        |config| config,
        |endpoint| {
            let mut stalled = started(endpoint.address, "SendStreamingMessage", "flood:262144");
            endpoint.settled();
            let other = post(
                endpoint.address,
                "",
                &rpc("1", "SendMessage", &message("other")),
            );
            assert_eq!(other.status, 200, "{}", other.body);

            // The link's end does not wait for it.
            let closing = Instant::now();
            endpoint.hub.close();
            while endpoint.hub.is_up() {
                assert!(
                    closing.elapsed() < WAIT,
                    "the hub waited for a stalled client"
                );
                thread::sleep(Duration::from_millis(20));
            }

            let mut bytes = Vec::new();
            let _ = stalled.read_to_end(&mut bytes);
            assert!(
                !bytes.ends_with(b"0\r\n\r\n"),
                "a stream given up on ended as if it were whole"
            );
        },
    );
}

#[test]
fn a_client_that_stops_reading_loses_its_connection_and_the_link_its_stream() {
    let _hang = Watchdog::start(
        Duration::from_secs(60),
        "a client that stopped reading kept its stream",
    );
    with_echo(
        |config| config.with_write_stall(Duration::from_millis(300)),
        |endpoint| {
            let _stalled = started(endpoint.address, "SendStreamingMessage", "flood:262144");
            endpoint.released(1, "the connection of a stalled client was never closed");
            let after = post(
                endpoint.address,
                "",
                &rpc("1", "SendMessage", &message("after")),
            );
            assert_eq!(after.status, 200);
        },
    );
}

/// Waits for the server to close the connection, within `limit`.
fn closed_within(stream: &mut TcpStream, limit: Duration) -> bool {
    stream.set_read_timeout(Some(limit)).unwrap();
    let mut part = [0_u8; 256];
    loop {
        match stream.read(&mut part) {
            Ok(0) => return true,
            Ok(_) => {}
            Err(error) => {
                return !matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                );
            }
        }
    }
}

#[test]
fn connections_are_capped_and_one_that_sends_nothing_is_let_go() {
    with_endpoint(
        |config| {
            config
                .with_max_connections(3)
                .with_request_timeout(Duration::from_millis(500))
        },
        |address| {
            let body = rpc("1", "GetTask", &json!({ "id": "x" })).to_string();
            // Nothing at all, half a head, and a whole head with half a body.
            let mut silent = TcpStream::connect(address).unwrap();
            let mut half_head = TcpStream::connect(address).unwrap();
            half_head.write_all(b"POST / HTTP/1.1\r\nHost:").unwrap();
            let mut half_body = TcpStream::connect(address).unwrap();
            write!(
                half_body,
                "POST / HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{{",
                body.len()
            )
            .unwrap();
            thread::sleep(Duration::from_millis(100));

            // The cap: told at once, and nothing it sends reaches the link.
            let refused = post(address, "", &json!({}));
            assert_ne!(refused.status, 200, "{}", refused.body);
            assert!(refused.status == 503 || refused.status == 0);

            for (name, stream) in [
                ("silent", &mut silent),
                ("half a head", &mut half_head),
                ("half a body", &mut half_body),
            ] {
                assert!(closed_within(stream, WAIT), "{name} was kept");
            }

            // And the places are free again.
            let reply = post(address, "", &rpc("1", "GetTask", &json!({ "id": "x" })));
            assert_eq!(reply.status, 200, "{}", reply.body);
        },
    );
}

#[test]
fn a_connection_carries_one_request() {
    with_endpoint(
        |config| config.with_max_connections(1),
        |address| {
            for _ in 0..5 {
                let mut stream = started(address, "SendMessage", "again");
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).unwrap();
                let text = String::from_utf8_lossy(&bytes);
                assert!(text.starts_with("HTTP/1.1 200"), "{text}");
                assert!(text.contains("echo: again"), "{text}");
            }
        },
    );
}

#[test]
fn a_stream_the_agent_never_finished_is_an_error_to_the_http_client() {
    let item: &'static [u8] = br#"{"jsonrpc":"2.0","id":1,"result":{"statusUpdate":{"taskId":"t","contextId":"c","status":{"state":"TASK_STATE_WORKING"}}}}"#;
    with_services(
        saying(vec![item]),
        Echo::default(),
        |config| config,
        |endpoint| {
            let reply = post(
                endpoint.address,
                "",
                &rpc("s", "SendStreamingMessage", &message("x")),
            );
            let events = sse_events(&reply.body);
            assert_eq!(events.len(), 2, "{}", reply.body);
            assert_eq!(events[0]["id"], "s");
            assert_eq!(events[1]["id"], "s");
            assert_eq!(events[1]["error"]["code"], -32603, "{}", reply.body);
        },
    );
}

#[test]
fn what_a_hostile_agent_sends_stays_one_answer_or_becomes_an_error() {
    // Spacing an event's data cannot hold, which JSON allows between tokens.
    let spaced: &'static [u8] =
        b"{\"jsonrpc\":\"2.0\",\r\n\"id\":1,\n\"result\":{\r\n\"statusUpdate\":{\"taskId\":\"t\"}}}";
    with_services(
        saying(vec![spaced, spaced]),
        Echo::default(),
        |config| config,
        |endpoint| {
            let reply = post(
                endpoint.address,
                "",
                &rpc("s", "SendStreamingMessage", &message("x")),
            );
            let events = sse_events(&reply.body);
            assert_eq!(events.len(), 3, "{}", reply.body);
            assert_eq!(events[0]["result"]["statusUpdate"]["taskId"], "t");
        },
    );
    for hostile in [
        &b"not json"[..],
        b"[1,{},null]",
        b"5",
        br#"{"jsonrpc":"2.0","id":1}"#,
        br#"{"jsonrpc":"2.0","id":2,"result":{}}"#,
        br#"{"jsonrpc":"2.0","id":1,"result":{},"result":{}}"#,
        br#"{"jsonrpc":"2.0","id":1,"result":{},"error":{"code":1,"message":"x"}}"#,
        br#"{"jsonrpc":"2.0","id":1,"error":"not an error"}"#,
    ] {
        with_services(
            saying(vec![hostile]),
            Echo::default(),
            |config| config,
            |endpoint| {
                let shown = String::from_utf8_lossy(hostile);
                let reply = post(endpoint.address, "", &rpc("1", "GetTask", &json!({})));
                assert_eq!(reply.json()["error"]["code"], -32006, "{shown}");
                assert!(!reply.body.contains("not an error"), "{shown}");
                let reply = post(
                    endpoint.address,
                    "",
                    &rpc("1", "SubscribeToTask", &json!({ "id": "x" })),
                );
                assert_eq!(reply.json()["error"]["code"], -32006, "{shown}");
            },
        );
    }
}

#[test]
fn an_agent_that_says_nothing_in_time_is_an_error_and_its_stream_is_let_go() {
    let _hang = Watchdog::start(Duration::from_secs(60), "a call with a deadline hung");
    with_echo(
        |config| config.with_answer_timeout(Duration::from_millis(300)),
        |endpoint| {
            let started = Instant::now();
            let reply = post(
                endpoint.address,
                "",
                &rpc("1", "SendMessage", &message("wait")),
            );
            assert_eq!(reply.json()["error"]["code"], -32603);
            assert!(started.elapsed() < WAIT);
            endpoint.released(1, "the stream of a call past its deadline stayed open");
        },
    );
}
