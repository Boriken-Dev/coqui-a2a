//! An A2A agent on a link, for trying the loopback command with a stock A2A client.
//!
//! `echo_server --identity SERVER_KEY --allow-identity CLIENT_KEY` serves the echo agent to the one
//! peer whose key is in `CLIENT_KEY` (made if missing), prints the port and its own peer id, and
//! runs until standard input closes.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

#[path = "../tests/support/agent.rs"]
mod agent;
#[path = "../tests/support/link.rs"]
mod link;

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use coqui_a2a::{PROTOCOL, Server};
use coqui_agentic_peer::link::Services;
use coqui_agentic_peer::peers::{MemoryPeers, PeerRecord, PeerStore};
use coqui_peer_webcrypto::{Identity, Trust};

fn main() -> Result<(), String> {
    let mut identity: Option<PathBuf> = None;
    let mut allowed: Option<PathBuf> = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or("a flag needs a value")?;
        match flag.as_str() {
            "--identity" => identity = Some(value.into()),
            "--allow-identity" => allowed = Some(value.into()),
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    let identity = Identity::open_or_create(&identity.ok_or("--identity is needed")?)
        .map_err(|error| error.to_string())?;
    let client = Identity::open_or_create(&allowed.ok_or("--allow-identity is needed")?)
        .map_err(|error| error.to_string())?;

    let store = Arc::new(MemoryPeers::default());
    store
        .put(&PeerRecord::new(&client.peer_id().to_string()).granting(PROTOCOL))
        .map_err(|error| error.to_string())?;
    let setup = link::Setup::new(
        identity,
        Trust::pinned([client.peer_id()]),
        store,
        Server::new(agent::Echo::default()).serving(Services::default()),
    );
    let host = link::Host::start(setup);
    println!("{}", host.address.port());
    println!("{}", host.id);
    std::io::stdout()
        .flush()
        .map_err(|error| error.to_string())?;

    // The caller ends this by closing standard input.
    let mut sink = Vec::new();
    let _ = std::io::stdin().read_to_end(&mut sink);
    drop(host);
    Ok(())
}
