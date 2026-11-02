//! A real proven link on loopback: a WebSocket listener, the in-band proof, a sealed channel and
//! the link's mux, with nothing mocked.
#![allow(dead_code)]

use std::cell::Cell;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use coqui_agentic_peer::auth::{self, Sealed, prove_out};
use coqui_agentic_peer::link::{self, Host as LinkHost, Options, Services};
use coqui_agentic_peer::mux::{Limits, Mux, Side};
use coqui_agentic_peer::peers::{NoEvents, PeerStore};
use coqui_peer_webcrypto::{Identity, PeerId, Trust, WebCrypto};
use coqui_web::websocket::{Dialer as WsDialer, Frames, Listener};
use coqui_web::{Channel as _, Dialer as _, Kind};

pub const WAIT: Duration = Duration::from_secs(10);
/// The channel limit once a peer is proven.
pub const MAX_LINE: usize = 16 * 1024 * 1024;

/// What a server end of a link is built from.
pub struct Setup {
    pub identity: Identity,
    pub initiators: Trust,
    pub store: Arc<dyn PeerStore>,
    pub services: Services,
    pub options: Options,
}

impl Setup {
    pub fn new(
        identity: Identity,
        initiators: Trust,
        store: Arc<dyn PeerStore>,
        services: Services,
    ) -> Self {
        let mut options = Options::default();
        options.limits.max_message = MAX_LINE;
        options.recheck = Duration::from_millis(100);
        Self {
            identity,
            initiators,
            store,
            services,
            options,
        }
    }
}

/// One test sets up links at a time. Thirteen proofs started within a millisecond of each other lose
/// a connection about one run in seven on the machine this was written on, so the tests take turns.
static TURN: Mutex<()> = Mutex::new(());

thread_local! {
    static HOLDING: Cell<bool> = const { Cell::new(false) };
}

/// A test's turn, held for as long as its hosts live. A thread that already has it keeps it.
struct Turn(Option<MutexGuard<'static, ()>>);

impl Turn {
    fn take() -> Self {
        if HOLDING.with(Cell::get) {
            return Self(None);
        }
        let guard = TURN.lock().unwrap_or_else(PoisonError::into_inner);
        HOLDING.with(|holding| holding.set(true));
        Self(Some(guard))
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        if self.0.is_some() {
            HOLDING.with(|holding| holding.set(false));
        }
    }
}

/// The answering end: accepts connections, proves each peer, and serves its link.
pub struct Host {
    pub address: SocketAddr,
    pub id: PeerId,
    stop: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
    _turn: Turn,
}

impl Host {
    pub fn start(setup: Setup) -> Self {
        let turn = Turn::take();
        let id = setup.identity.peer_id();
        let listener = Listener::bind("127.0.0.1:0".parse().unwrap(), None)
            .unwrap()
            .with_limits(coqui_web::Limits::default().with_max_message(auth::MAX_PROOF_MESSAGE))
            .with_frames(Frames::Binary);
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let method = Arc::new(WebCrypto::new(setup.identity).with_initiators(setup.initiators));
        let (store, services, options) = (setup.store, Arc::new(setup.services), setup.options);
        let flag = Arc::clone(&stop);
        let accept = thread::spawn(move || {
            let mut links: Vec<JoinHandle<()>> = Vec::new();
            while !flag.load(Ordering::SeqCst) {
                let Ok(pending) = listener.next(Some(Duration::from_millis(50))) else {
                    continue;
                };
                let (method, store, services) = (
                    Arc::clone(&method),
                    Arc::clone(&store),
                    Arc::clone(&services),
                );
                links.push(thread::spawn(move || {
                    let Ok(channel) = pending.handshake() else {
                        return;
                    };
                    if let Ok(established) =
                        auth::prove(&*method, &channel, WAIT, MAX_LINE + auth::RECORD_OVERHEAD)
                    {
                        let peer = established.peer.to_string();
                        let sealed = Sealed::new(&channel, established.session);
                        let host = LinkHost {
                            store: &*store,
                            services: &services,
                            events: &NoEvents,
                        };
                        link::serve(&sealed, Side::Answered, options, None, &peer, &host);
                        sealed.close();
                    }
                    channel.close();
                }));
            }
            for link in links {
                let _ = link.join();
            }
        });
        Self {
            address,
            id,
            stop,
            accept: Some(accept),
            _turn: turn,
        }
    }

    pub fn url(&self) -> url::Url {
        format!("ws://{}/link", self.address).parse().unwrap()
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
    }
}

/// Dials, proves this identity to `host`, and runs `body` over the link's mux.
pub fn over_link<R>(
    host: &Host,
    client: Identity,
    body: impl FnOnce(&Mux<'_, Sealed<'_, coqui_web::websocket::Channel>>) -> R,
) -> R {
    over_link_with(host.url(), host.id, client, Limits::default(), body)
}

pub fn over_link_with<R>(
    url: url::Url,
    server: PeerId,
    client: Identity,
    limits: Limits,
    body: impl FnOnce(&Mux<'_, Sealed<'_, coqui_web::websocket::Channel>>) -> R,
) -> R {
    let method = WebCrypto::new(client).with_responders(Trust::pinned([server]));
    let channel = WsDialer::new()
        .with_frames(Frames::Binary)
        .dial(Kind::WebSocket, &url, WAIT)
        .unwrap();
    let established = prove_out(&method, &channel, WAIT, MAX_LINE + auth::RECORD_OVERHEAD).unwrap();
    let sealed = Sealed::new(&channel, established.session);
    let result = Mux::run(&sealed, Side::Dialed, limits, None, body);
    sealed.close();
    channel.close();
    result
}
