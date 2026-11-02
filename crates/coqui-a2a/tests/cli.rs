//! The built `coqui-a2a` command, run with real arguments against a real link.
#![cfg(feature = "cli")]
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod support;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coqui_a2a::{PROTOCOL, Server};
use coqui_agentic_peer::link::Services;
use coqui_agentic_peer::peers::{MemoryPeers, PeerRecord, PeerStore};
use coqui_peer_webcrypto::{Identity, Trust};
use support::agent::Echo;
use support::link::{Host, Setup, WAIT};

const COMMAND: &str = env!("CARGO_BIN_EXE_coqui-a2a");

struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn http_post(address: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    write!(
        stream,
        "POST / HTTP/1.0\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut text = String::new();
    let _ = stream.read_to_string(&mut text);
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    (
        status,
        text.split_once("\r\n\r\n")
            .map(|(_, body)| body.to_owned())
            .unwrap_or_default(),
    )
}

#[test]
fn the_command_serves_a_standard_endpoint_for_a_peer_over_a_real_link() {
    let dir = std::env::temp_dir().join(format!("coqui-a2a-cli-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = dir.join("client.pem");
    let client = Identity::open_or_create(&key).unwrap();
    let store = Arc::new(MemoryPeers::default());
    store
        .put(&PeerRecord::new(&client.peer_id().to_string()).granting(PROTOCOL))
        .unwrap();
    let host = Host::start(Setup::new(
        Identity::generate().unwrap(),
        Trust::pinned([client.peer_id()]),
        store,
        Server::new(Echo::default()).serving(Services::default()),
    ));

    let mut child = Command::new(COMMAND)
        .args([
            "loopback",
            "--url",
            host.url().as_str(),
            "--peer",
            &host.id.to_string(),
        ])
        .arg("--identity")
        .arg(&key)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let _running = Running(child);
    let endpoint = line
        .trim()
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_owned();

    let request = r#"{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"via the command"}]}}}"#;
    let until = Instant::now() + WAIT;
    let (status, body) = loop {
        let (status, body) = http_post(&endpoint, request);
        // 503 until the command has dialed and proved itself.
        if status != 503 || Instant::now() > until {
            break (status, body);
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("echo: via the command"), "{body}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_missing_url_is_a_usage_error_and_a_non_loopback_address_is_refused() {
    let usage = Command::new(COMMAND).arg("loopback").output().unwrap();
    assert_eq!(usage.status.code(), Some(2));
    let refused = Command::new(COMMAND)
        .args([
            "loopback",
            "--url",
            "ws://127.0.0.1:1/x",
            "--listen",
            "0.0.0.0:0",
            "--peer",
            "sha256:00",
        ])
        .output()
        .unwrap();
    assert_ne!(refused.status.code(), Some(0));
    assert!(refused.stdout.is_empty(), "nothing is served");
}

#[test]
fn a_peer_the_link_cannot_prove_ends_the_command_with_a_failure() {
    let dir = std::env::temp_dir().join(format!("coqui-a2a-cli-bad-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = dir.join("client.pem");
    let client = Identity::open_or_create(&key).unwrap();
    let store = Arc::new(MemoryPeers::default());
    let host = Host::start(Setup::new(
        Identity::generate().unwrap(),
        Trust::pinned([client.peer_id()]),
        store,
        Server::new(Echo::default()).serving(Services::default()),
    ));
    // Pinned to a key the host does not hold.
    let wrong = Identity::generate().unwrap().peer_id().to_string();
    let mut child = Command::new(COMMAND)
        .args(["loopback", "--url", host.url().as_str(), "--peer", &wrong])
        .arg("--identity")
        .arg(&key)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let until = Instant::now() + WAIT;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < until, "the command kept running");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(status.code(), Some(1));
    let _ = std::fs::remove_dir_all(&dir);
}
