//! The built `coqui-a2a` command, run with real arguments against a real link.
#![cfg(feature = "cli")]
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod support;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
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

const REQUEST: &str = r#"{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m","role":"ROLE_USER","parts":[{"text":"via the command"}]}}}"#;

fn http_post(address: &str, extra: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    write!(
        stream,
        "POST / HTTP/1.0\r\nHost: {address}\r\n{extra}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
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

/// A directory of a test's own, for an identity and a token.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("coqui-a2a-cli-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A host serving the echo agent to the identity in `key`.
fn host_for(key: &Path) -> Host {
    let client = Identity::open_or_create(key).unwrap();
    let store = Arc::new(MemoryPeers::default());
    store
        .put(&PeerRecord::new(&client.peer_id().to_string()).granting(PROTOCOL))
        .unwrap();
    Host::start(Setup::new(
        Identity::generate().unwrap(),
        Trust::pinned([client.peer_id()]),
        store,
        Server::new(Echo::default()).serving(Services::default()),
    ))
}

/// The command with this test's home and identity, pinned to `host`, and nothing from the environment.
fn command(scratch: &Scratch, host: &Host) -> Command {
    let mut command = Command::new(COMMAND);
    command
        .env("COQUI_AGENTIC_PEER_HOME", &scratch.0)
        .env_remove("COQUI_A2A_TOKEN_FILE")
        .env_remove("COQUI_A2A_URL")
        .env_remove("COQUI_A2A_LISTEN")
        .args(["loopback", "--url", host.url().as_str()])
        .args(["--peer", &host.id.to_string()])
        .arg("--identity")
        .arg(scratch.0.join("client.pem"))
        .stdin(Stdio::null())
        .stderr(Stdio::null());
    command
}

/// Starts it, and returns the running command, its address, and the rest of what it prints.
fn started(mut command: Command) -> (Running, String, BufReader<ChildStdout>) {
    let mut child = command.stdout(Stdio::piped()).spawn().unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    out.read_line(&mut line).unwrap();
    let endpoint = line
        .trim()
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_owned();
    (Running(child), endpoint, out)
}

/// As [`started`], for a command that names its token file on its second line.
fn started_with_token(command: Command) -> (Running, String, PathBuf) {
    let (running, endpoint, mut out) = started(command);
    let mut line = String::new();
    out.read_line(&mut line).unwrap();
    (running, endpoint, PathBuf::from(line.trim()))
}

/// Posts until the command has dialed and proved itself, which is 503 until then.
fn answered(endpoint: &str, extra: &str) -> (u16, String) {
    let until = Instant::now() + WAIT;
    loop {
        let (status, body) = http_post(endpoint, extra, REQUEST);
        if status != 503 || Instant::now() > until {
            return (status, body);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn the_command_makes_a_token_of_its_own_and_serves_only_who_shows_it() {
    let scratch = Scratch::new("default");
    let host = host_for(&scratch.0.join("client.pem"));
    let (running, endpoint, file) = started_with_token(command(&scratch, &host));
    assert!(file.starts_with(&scratch.0), "{}", file.display());
    let token = std::fs::read_to_string(&file).unwrap();
    assert_eq!(token.len(), 64, "32 bytes in hex");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&file).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    assert_eq!(http_post(&endpoint, "", REQUEST).0, 401);
    let bearer = format!("Authorization: Bearer {token}\r\n");
    let (status, body) = answered(&endpoint, &bearer);
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("echo: via the command"), "{body}");
    drop(running);

    // The next start uses the same one, so a client set up once stays set up.
    let (_running, endpoint, again) = started_with_token(command(&scratch, &host));
    assert_eq!(again, file);
    assert_eq!(std::fs::read_to_string(&again).unwrap(), token);
    assert_eq!(answered(&endpoint, &bearer).0, 200);
}

#[test]
fn starts_racing_to_make_the_token_agree_on_one() {
    let scratch = Scratch::new("race");
    let host = host_for(&scratch.0.join("client.pem"));
    let tokens: Vec<String> = std::thread::scope(|scope| {
        let starts: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    let (_running, _, file) = started_with_token(command(&scratch, &host));
                    std::fs::read_to_string(file).unwrap()
                })
            })
            .collect();
        starts
            .into_iter()
            .map(|start| start.join().unwrap())
            .collect()
    });
    assert_eq!(tokens[0].len(), 64);
    assert!(tokens.iter().all(|token| *token == tokens[0]));
}

#[test]
fn a_named_token_file_is_used_as_it_is() {
    let scratch = Scratch::new("named");
    let host = host_for(&scratch.0.join("client.pem"));
    let file = scratch.0.join("named.token");
    write_private(&file, "  named-token\n");
    let mut named = command(&scratch, &host);
    named.arg("--token-file").arg(&file);
    let (_running, endpoint, shown) = started_with_token(named);
    assert_eq!(shown, file);
    assert_eq!(http_post(&endpoint, "", REQUEST).0, 401);
    let (status, body) = answered(&endpoint, "Authorization: Bearer named-token\r\n");
    assert_eq!(status, 200, "{body}");
}

fn write_private(file: &Path, text: &str) {
    std::fs::write(file, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[test]
fn a_token_file_that_will_not_do_stops_the_command_before_it_serves() {
    let scratch = Scratch::new("bad");
    let host = host_for(&scratch.0.join("client.pem"));
    let refused = |file: &Path, what: &str| {
        let output = command(&scratch, &host)
            .arg("--token-file")
            .arg(file)
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{what}: {stderr}");
        assert!(output.stdout.is_empty(), "{what}: nothing is served");
        assert!(stderr.contains("token"), "{what}: {stderr}");
    };
    for (name, text) in [
        ("empty", ""),
        ("spaces", "   "),
        ("lines", "\r\n\n"),
        ("words", "two words"),
        ("control", "a\u{1}b"),
    ] {
        let file = scratch.0.join(name);
        write_private(&file, text);
        refused(&file, name);
    }
    let huge = scratch.0.join("huge");
    write_private(&huge, &"a".repeat(1 << 16));
    refused(&huge, "past what a token file holds");

    // A file named and missing is a mistake, and not made.
    let missing = scratch.0.join("missing.token");
    refused(&missing, "missing");
    assert!(!missing.exists());
    refused(&scratch.0, "a directory");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        for mode in [0o640, 0o604, 0o660, 0o644, 0o666, 0o610, 0o601] {
            let file = scratch.0.join(format!("mode-{mode:o}"));
            std::fs::write(&file, "fine").unwrap();
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
            refused(&file, &format!("mode {mode:o}"));
        }
    }
}

#[test]
fn no_token_is_asked_for_by_name_and_then_serves_anyone() {
    let scratch = Scratch::new("open");
    let host = host_for(&scratch.0.join("client.pem"));
    let mut open = command(&scratch, &host);
    open.arg("--no-token");
    let (_running, endpoint, _) = started(open);
    let (status, body) = answered(&endpoint, "");
    assert_eq!(status, 200, "{body}");
    assert!(!scratch.0.join("a2a-loopback.token").exists());
}

#[test]
fn any_paired_peer_is_asked_for_by_name_and_goes_to_the_trust_store() {
    let scratch = Scratch::new("paired");
    let host = host_for(&scratch.0.join("client.pem"));
    let missing = Command::new(COMMAND)
        .env("COQUI_AGENTIC_PEER_HOME", &scratch.0)
        .env_remove("COQUI_A2A_TOKEN_FILE")
        .args(["loopback", "--no-token", "--url", host.url().as_str()])
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(2));
    assert!(missing.stdout.is_empty());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("--peer"));

    // A store with nobody paired: the host's key is not accepted, which ends the command.
    let status = Command::new(COMMAND)
        .env("COQUI_AGENTIC_PEER_HOME", &scratch.0)
        .env_remove("COQUI_A2A_TOKEN_FILE")
        .args(["loopback", "--no-token", "--any-paired-peer"])
        .args(["--url", host.url().as_str()])
        .arg("--identity")
        .arg(scratch.0.join("client.pem"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut running = Running(status);
    let until = Instant::now() + WAIT;
    let code = loop {
        if let Some(done) = running.0.try_wait().unwrap() {
            break done.code();
        }
        assert!(
            Instant::now() < until,
            "a peer nobody paired was kept trying"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(code, Some(1));
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
    let scratch = Scratch::new("wrong");
    let host = host_for(&scratch.0.join("client.pem"));
    // Pinned to a key the host does not hold.
    let wrong = Identity::generate().unwrap().peer_id().to_string();
    let mut child = Command::new(COMMAND)
        .env("COQUI_AGENTIC_PEER_HOME", &scratch.0)
        .env_remove("COQUI_A2A_TOKEN_FILE")
        .args(["loopback", "--url", host.url().as_str(), "--peer", &wrong])
        .arg("--identity")
        .arg(scratch.0.join("client.pem"))
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
}

#[test]
fn the_flags_of_the_relay_reach_it() {
    let scratch = Scratch::new("flags");
    let host = host_for(&scratch.0.join("client.pem"));

    // An origin no browser sends is a mistake to say at once.
    let output = command(&scratch, &host)
        .args(["--no-token", "--allow-origin", "*"])
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("origin"));

    let mut relay = command(&scratch, &host);
    relay.args(["--no-token", "--allow-origin", "http://127.0.0.1:3000"]);
    relay.args(["--answer-timeout", "1"]);
    let (_running, endpoint, _) = started(relay);
    assert_eq!(answered(&endpoint, "").0, 200);
    let listed = "Origin: http://127.0.0.1:3000\r\n";
    assert_eq!(http_post(&endpoint, listed, REQUEST).0, 200);
    // The agent does not answer this until the client goes, so only the flag's second ends it.
    let waited = Instant::now();
    let wait = REQUEST.replace("via the command", "wait");
    let (status, body) = http_post(&endpoint, "", &wait);
    assert_eq!(status, 200);
    assert!(body.contains("-32603"), "{body}");
    assert!(waited.elapsed() < WAIT, "{:?}", waited.elapsed());
}
