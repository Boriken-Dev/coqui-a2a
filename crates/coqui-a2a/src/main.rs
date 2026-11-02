//! The `coqui-a2a` command: an A2A HTTP endpoint on loopback that talks to a link.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use clap::{Parser, Subcommand};
use coqui_a2a::loopback::{Config, Hub, Loopback};
use coqui_agentic_peer::auth::{self, Sealed};
use coqui_agentic_peer::mux::{Heartbeat, Limits, Mux, Side};
use coqui_agentic_peer::{Error, home};
use coqui_peer_tls as tls;
use coqui_peer_webcrypto::{Trust, TrustStore, WebCrypto};
use coqui_web::websocket::{Dialer, Frames};
use coqui_web::{Channel as _, Dialer as _, Failure, Kind};
use url::Url;

/// The largest message on the link, which A2A results and cards fit within.
const MAX_MESSAGE: usize = 16 * 1024 * 1024;
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
const PROOF_TIMEOUT: Duration = Duration::from_secs(15);
const LONGEST_WAIT: Duration = Duration::from_secs(30);

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
    /// The remote peer's id, pinned. Without it the paired peers in this user's trust store are
    /// accepted.
    #[arg(long = "peer")]
    peers: Vec<coqui_peer_webcrypto::PeerId>,
    /// This side's identity file. Defaults to this user's.
    #[arg(long)]
    identity: Option<PathBuf>,
    /// Lets requests from this Origin through; by default any request with an Origin is refused.
    #[arg(long = "allow-origin")]
    origins: Vec<String>,
    /// A PEM bundle of roots that must vouch for a wss:// link's certificate. Without it the
    /// certificate is not checked and the in-band proof alone decides who the peer is.
    #[arg(long)]
    ca_file: Option<PathBuf>,
    /// A file holding a bearer token every request must present, so other programs on this
    /// machine cannot use the link. The header is then not forwarded to the agent.
    #[arg(long, env = "COQUI_A2A_TOKEN_FILE")]
    token_file: Option<PathBuf>,
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
    if let Some(file) = &args.token_file {
        let token = std::fs::read_to_string(file)
            .map_err(|error| format!("the token file could not be read: {error}"))?;
        config = config.with_token(token.trim());
    }
    let identity = home::identity(args.identity.as_deref()).map_err(|error| error.to_string())?;
    let (trust, store) = if args.peers.is_empty() {
        let store = Arc::new(home::store(None).map_err(|error| error.to_string())?);
        (Trust::Store(TrustStore::clone(&store)), Some(store))
    } else {
        (Trust::pinned(args.peers.iter().copied()), None)
    };
    let method = WebCrypto::new(identity).with_responders(trust);

    let hub = Hub::new();
    let endpoint = Loopback::start(config, Arc::clone(&hub)).map_err(|error| error.to_string())?;
    println!("http://{}/", endpoint.local_addr());

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
                    MAX_MESSAGE + auth::RECORD_OVERHEAD,
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
    fn a_non_loopback_listen_address_parses_and_the_endpoint_refuses_it() {
        let cli = Cli::try_parse_from([
            "coqui-a2a",
            "loopback",
            "--url",
            "ws://127.0.0.1:1/x",
            "--listen",
            "0.0.0.0:80",
        ])
        .unwrap();
        let Command::Loopback(args) = cli.command;
        assert!(Config::new(args.listen).is_err());
    }
}
