//! The `coqui-a2a` command: an A2A HTTP endpoint on loopback that talks to a link.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

use std::io::{self, Read as _};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use clap::{Parser, Subcommand};
use coqui_a2a::loopback::{Config, Hub, Loopback};
use coqui_agentic_peer::auth::{self, Proof, Sealed, Trust};
use coqui_agentic_peer::mux::{Heartbeat, Limits, Mux, Side};
use coqui_agentic_peer::{Error, home};
use coqui_peer_tls as tls;
use coqui_peer_webcrypto::TrustStore;
use coqui_web_channel::websocket::{Dialer, Frames};
use coqui_web_channel::{Channel as _, Dialer as _, Failure, Kind};
use url::Url;

/// The largest message on the link, which A2A results and cards fit within.
const MAX_MESSAGE: usize = 16 * 1024 * 1024;
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const PROOF_TIMEOUT: Duration = Duration::from_secs(15);
const LONGEST_WAIT: Duration = Duration::from_secs(30);
/// The file under this user's directory that holds the token, unless another is named.
const TOKEN_FILE: &str = "a2a-loopback.token";
const TOKEN_BYTES: usize = 32;
/// More than any token, so a file named by mistake is refused rather than read.
const MAX_TOKEN_FILE: u64 = 4096;

/// A2A over a coqui-agentic-peer link.
#[derive(Debug, Parser)]
#[command(
    name = "coqui-a2a",
    version,
    after_help = "Environment: COQUI_A2A_URL, COQUI_A2A_LISTEN, COQUI_A2A_TOKEN_FILE (flags above), \
                  and COQUI_AGENTIC_PEER_HOME for the identity and trust store."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve a standard A2A HTTP endpoint on a loopback address and translate it to a link.
    ///
    /// Runs until it is stopped. Exit status: 0 stopped, 1 the link refused the proof or the
    /// endpoint could not start, 2 the invocation was wrong.
    Loopback(Loopback_),
}

#[derive(Debug, clap::Args)]
struct Loopback_ {
    /// The link to dial: ws:// (loopback only is sensible) or wss://.
    #[arg(long, env = "COQUI_A2A_URL")]
    url: Url,
    /// Where to listen: a loopback address. Port 0 picks a free port, printed on standard output.
    #[arg(long, env = "COQUI_A2A_LISTEN", default_value = "127.0.0.1:0")]
    listen: SocketAddr,
    /// The remote peer's id, pinned, repeatable. Required unless --any-paired-peer is given.
    #[arg(long = "peer", required_unless_present = "any_paired_peer")]
    peers: Vec<coqui_peer_webcrypto::PeerId>,
    /// Accepts every peer paired in this user's trust store as the agent, so any of them can pose
    /// as it to whatever uses this endpoint.
    #[arg(long, conflicts_with = "peers")]
    any_paired_peer: bool,
    /// This side's identity file, whose key's method is the one it proves by. Defaults to this user's.
    #[arg(long)]
    identity: Option<PathBuf>,
    /// Lets a page from this Origin use the endpoint, repeatable; by default any request with an
    /// Origin is refused.
    #[arg(long = "allow-origin")]
    origins: Vec<String>,
    /// Passes this header to the agent as a service parameter, repeatable: for a credential the
    /// agent's card names in a header other than Authorization.
    #[arg(long = "forward-header", value_name = "NAME")]
    forwarded: Vec<String>,
    /// How long the agent has to say anything to an operation. SendMessage waits for the task to
    /// settle, so a longer task needs more, or returnImmediately and GetTask.
    #[arg(long, value_name = "SECONDS", default_value_t = 300)]
    answer_timeout: u64,
    /// A PEM bundle of roots that must vouch for a wss:// link's certificate. Without it the
    /// certificate is not checked and the in-band proof alone decides who the peer is.
    #[arg(long)]
    ca_file: Option<PathBuf>,
    /// A file holding the bearer token every request must present, which only this user may read.
    /// By default this user's own, made the first time. The header is not forwarded to the agent.
    #[arg(long, env = "COQUI_A2A_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    /// Serves with no token: every program of every user on this machine can then do what the
    /// link's peer granted, and read the extended card.
    #[arg(long, conflicts_with = "token_file")]
    no_token: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Loopback(args) => loopback(&args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(why) => {
            eprintln!("coqui-a2a: {why}");
            ExitCode::FAILURE
        }
    }
}

fn loopback(args: &Loopback_) -> Result<(), String> {
    let mut config = Config::new(args.listen).map_err(|error| error.to_string())?;
    for origin in &args.origins {
        config = config.allow_origin(origin.clone());
    }
    for name in &args.forwarded {
        config = config.forward_header(name);
    }
    config = config.with_answer_timeout(Duration::from_secs(args.answer_timeout));
    let token_file = if args.no_token {
        config = config.without_token();
        None
    } else {
        let file = match &args.token_file {
            Some(file) => file.clone(),
            None => default_token_file()?,
        };
        config = config.with_token(token_from(&file, args.token_file.is_none())?);
        Some(file)
    };
    let identity =
        home::identity(args.identity.as_deref(), None).map_err(|error| error.to_string())?;
    let (trust, store) = if args.peers.is_empty() {
        let store = Arc::new(home::store(None).map_err(|error| error.to_string())?);
        (Trust::Store(TrustStore::clone(&store)), Some(store))
    } else {
        (Trust::pinned(args.peers.iter().copied()), None)
    };
    let method = Proof::new(identity).with_responders(trust);

    let hub = Hub::new();
    let endpoint = Loopback::start(config, Arc::clone(&hub)).map_err(|error| error.to_string())?;
    println!("http://{}/", endpoint.local_addr());
    if let Some(file) = &token_file {
        println!("{}", file.display());
    }

    let mut dialer = Dialer::new().with_frames(Frames::Binary);
    if args.url.scheme() == "wss" {
        dialer = dialer.with_tls(tls_client(args.ca_file.as_ref())?);
    }
    let mut wait = Duration::from_secs(1);
    loop {
        match dialer.dial(Kind::WebSocket, &args.url, DIAL_TIMEOUT) {
            Ok(channel) => {
                let proved = auth::prove_out(
                    &method,
                    &channel,
                    PROOF_TIMEOUT,
                    method.method().record_length(MAX_MESSAGE),
                );
                match proved {
                    Ok(established) => {
                        wait = Duration::from_secs(1);
                        let peer = established.peer;
                        eprintln!("connected to {peer}");
                        let sealed = Sealed::new(&channel, established.session);
                        let sealed = match &store {
                            Some(store) => {
                                let store = Arc::clone(store);
                                sealed.with_check(move || store.is_accepted(&peer))
                            }
                            None => sealed,
                        };
                        let mut limits = Limits::default();
                        limits.max_message = MAX_MESSAGE;
                        limits.heartbeat = Some(Heartbeat::new(Duration::from_secs(15), 3));
                        Mux::run(&sealed, Side::Dialed, limits, None, |mux| hub.attach(mux));
                        sealed.close();
                        eprintln!("the link ended");
                    }
                    // A peer that proved the wrong key will not fix itself.
                    Err(Error::Proof(why)) => {
                        return Err(format!("the remote did not prove itself: {why}"));
                    }
                    Err(error) => eprintln!("the proof did not finish: {error}"),
                }
                channel.close();
            }
            Err(Failure::Certificate) => {
                return Err("the remote certificate is not accepted".to_owned());
            }
            Err(failure) => eprintln!("could not connect: {failure}"),
        }
        thread::sleep(wait);
        wait = (wait * 2).min(LONGEST_WAIT);
    }
}

fn default_token_file() -> Result<PathBuf, String> {
    home::dir()
        .map(|dir| dir.join(TOKEN_FILE))
        .ok_or_else(|| "no home directory for this user to keep a token in".to_owned())
}

/// The token in `file`. When `make` says so a missing file is made with a new token, and when two
/// starts race, the one that loses reads what the other wrote.
fn token_from(file: &Path, make: bool) -> Result<String, String> {
    let shown = file.display();
    let mut opened = std::fs::File::open(file);
    if make
        && let Err(error) = &opened
        && error.kind() == io::ErrorKind::NotFound
    {
        match new_token().and_then(|token| write_token(file, &token)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(format!("the token file {shown} could not be made: {error}")),
        }
        opened = std::fs::File::open(file);
    }
    let mut opened =
        opened.map_err(|error| format!("the token file {shown} could not be read: {error}"))?;
    private_enough(&opened).map_err(|why| format!("the token file {shown} {why}"))?;
    let mut text = String::new();
    opened
        .read_to_string(&mut text)
        .map_err(|error| format!("the token file {shown} could not be read: {error}"))?;
    let token = text.trim();
    if token.is_empty() {
        return Err(format!("the token file {shown} is empty"));
    }
    Ok(token.to_owned())
}

fn new_token() -> io::Result<String> {
    let mut bytes = [0_u8; TOKEN_BYTES];
    // A guessable token lets anyone on this machine in, so no randomness means no endpoint.
    coqui_peer_webcrypto::random()
        .fill(&mut bytes)
        .map_err(|_| io::Error::other("this machine has no randomness to make one with"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn write_token(file: &Path, token: &str) -> io::Result<()> {
    coqui_peer::file::write_new(file, token.as_bytes(), coqui_peer::file::Access::OwnerOnly)
}

/// Refuses a file others could read the token from. On Windows a new file takes its directory's
/// access list, which is this user's alone under `%LOCALAPPDATA%`, and this does not read it.
fn private_enough(file: &std::fs::File) -> Result<(), String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("could not be read: {error}"))?;
    if !metadata.is_file() {
        return Err("is not a file".to_owned());
    }
    if metadata.len() > MAX_TOKEN_FILE {
        return Err("is too large to hold a token".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(format!(
                "can be reached by other users (mode {:o}): make it 600",
                mode & 0o777
            ));
        }
    }
    Ok(())
}

fn tls_client(ca_file: Option<&PathBuf>) -> Result<Arc<rustls::ClientConfig>, String> {
    let trust = match ca_file {
        Some(file) => tls::Trust::Roots(tls::Roots::bundle_file(file)),
        None => tls::Trust::AnyWithKeyProof,
    };
    let config =
        tls::client_config_without_identity(&trust, &[]).map_err(|error| error.to_string())?;
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory as _;

    #[test]
    fn the_command_line_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn a_missing_url_is_a_usage_error() {
        let error = Cli::try_parse_from(["coqui-a2a", "loopback"]).unwrap_err();
        assert_eq!(error.exit_code(), 2);
    }

    #[test]
    fn the_peer_is_named_or_explicitly_left_open() {
        let parse = |extra: &[&str]| {
            let mut line = vec!["coqui-a2a", "loopback", "--url", "ws://127.0.0.1:1/x"];
            line.extend_from_slice(extra);
            Cli::try_parse_from(line)
        };
        let error = parse(&[]).unwrap_err();
        assert_eq!(error.exit_code(), 2);
        assert!(error.to_string().contains("--peer"), "{error}");
        assert!(parse(&["--any-paired-peer"]).is_ok());
        let id = coqui_peer_webcrypto::Identity::generate()
            .unwrap()
            .peer_id()
            .to_string();
        let both = parse(&["--any-paired-peer", "--peer", &id]);
        assert_eq!(both.map(|_| ()).unwrap_err().exit_code(), 2);
    }

    #[test]
    fn a_token_is_given_or_explicitly_left_out_not_both() {
        let error = Cli::try_parse_from([
            "coqui-a2a",
            "loopback",
            "--url",
            "ws://127.0.0.1:1/x",
            "--any-paired-peer",
            "--no-token",
            "--token-file",
            "x",
        ])
        .unwrap_err();
        assert_eq!(error.exit_code(), 2);
    }

    #[test]
    fn a_non_loopback_listen_address_parses_and_the_endpoint_refuses_it() {
        let cli = Cli::try_parse_from([
            "coqui-a2a",
            "loopback",
            "--url",
            "ws://127.0.0.1:1/x",
            "--listen",
            "0.0.0.0:80",
            "--no-token",
            "--any-paired-peer",
        ])
        .unwrap();
        let Command::Loopback(args) = cli.command;
        assert!(Config::new(args.listen).is_err());
    }
}
