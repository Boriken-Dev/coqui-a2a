//! One pairing, then a protocol per grant: the trust store and the grants are the link crate's,
//! and this crate only asks to be granted.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod support;

use std::sync::Arc;
use std::thread;

use coqui_a2a::{Client, ClientError, PROTOCOL, Server};
use coqui_agentic_peer::auth;
use coqui_agentic_peer::link::{Services, capabilities};
use coqui_agentic_peer::mux::LinkStream;
use coqui_agentic_peer::pairing;
use coqui_agentic_peer::peers::{FilePeers, PeerRecord, PeerStore};
use coqui_peer_webcrypto::pair::{CodeAlphabet, Pin};
use coqui_peer_webcrypto::{Identity, Trust, TrustStore};
use coqui_web::websocket::{Dialer, Frames, Listener};
use coqui_web::{Channel as _, Dialer as _, Kind};
use support::agent::{Echo, user};
use support::link::{Host, Setup, WAIT, over_link_with};

const OTHER: &str = "other/1";

fn temp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("coqui-a2a-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn one_pairing_serves_the_first_protocol_and_a_grant_adds_a2a_with_no_second_pairing() {
    let dir = temp("paired");
    let server_identity = Identity::open_or_create(&dir.join("server-identity")).unwrap();
    let client_identity = Identity::open_or_create(&dir.join("client-identity")).unwrap();
    let server_store = TrustStore::open(&dir.join("server-trust")).unwrap();
    let client_store = TrustStore::open(&dir.join("client-trust")).unwrap();

    // The one pairing, with a PIN, over a real WebSocket.
    let listener = Listener::bind("127.0.0.1:0".parse().unwrap(), None)
        .unwrap()
        .with_frames(Frames::Binary)
        .with_limits(coqui_web::Limits::default().with_max_message(auth::MAX_PROOF_MESSAGE));
    let address = listener.local_addr().unwrap();
    let desk = pairing::desk().unwrap();
    let pin = desk.show_pin().unwrap().pin;
    let responder = {
        let (identity, store, desk) = (server_identity.clone(), server_store.clone(), desk.clone());
        thread::spawn(move || {
            let channel = listener.next(Some(WAIT)).unwrap().handshake().unwrap();
            let paired = pairing::respond(&channel, &identity.into(), &store, &desk, None, WAIT);
            channel.close();
            paired.map(|paired| paired.peer.id().to_string())
        })
    };
    let channel = Dialer::new()
        .with_frames(Frames::Binary)
        .dial(
            Kind::WebSocket,
            &format!("ws://{address}/pair").parse().unwrap(),
            WAIT,
        )
        .unwrap();
    let pin = Pin::parse(&CodeAlphabet::pin(), pin.as_str()).unwrap();
    pairing::join(
        &channel,
        &client_identity.clone().into(),
        pin,
        &client_store,
        WAIT,
    )
    .unwrap();
    channel.close();
    let paired_peer = responder.join().unwrap().unwrap();
    assert_eq!(paired_peer, client_identity.peer_id().to_string());

    // What pairing gave the peer: one protocol. The grants are the link crate's own file store.
    let grants = FilePeers::new(dir.join("grants.json"));
    grants
        .put(&PeerRecord::new(&paired_peer).granting(OTHER))
        .unwrap();
    let services = Server::new(Echo::default())
        .serving(Services::default())
        .with(
            OTHER,
            Box::new(|stream: &dyn LinkStream, _| {
                while let Ok(Some(message)) = stream.receive(Some(WAIT)) {
                    if stream.send(&message).is_err() {
                        break;
                    }
                }
            }),
        );
    let host = Host::start(Setup::new(
        server_identity,
        Trust::Store(server_store),
        Arc::new(FilePeers::new(dir.join("grants.json"))),
        services,
    ));
    over_link_with(
        host.url(),
        host.id,
        client_identity,
        coqui_agentic_peer::mux::Limits::default(),
        |mux| {
            // Proved by the pairing alone: no second pairing and no new key.
            assert_eq!(capabilities(mux, WAIT).unwrap().offers, [OTHER]);
            let client = Client::new(mux);
            assert!(matches!(
                client.send_message(&user("hello")),
                Err(ClientError::NotGranted(_))
            ));

            // A grant, as the link crate's `grant` does it: the same entry gets another token.
            grants
                .put(
                    &PeerRecord::new(&paired_peer)
                        .granting(OTHER)
                        .granting(PROTOCOL),
                )
                .unwrap();
            assert!(client.send_message(&user("hello")).is_ok());
            assert_eq!(capabilities(mux, WAIT).unwrap().offers, [PROTOCOL, OTHER]);

            // And taking it away ends a running operation within the recheck.
            let mut events = client.send_streaming_message(&user("hold")).unwrap();
            assert!(events.next().unwrap().is_ok());
            grants
                .put(&PeerRecord::new(&paired_peer).granting(OTHER))
                .unwrap();
            let ended = events.next().unwrap();
            assert!(matches!(ended, Err(ClientError::Revoked(_))), "{ended:?}");
        },
    );
    let _ = std::fs::remove_dir_all(&dir);
}
