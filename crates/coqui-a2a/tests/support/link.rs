//! A real proven link on loopback: a WebSocket listener, the in-band proof, a sealed channel and
//! the link's mux, with nothing mocked.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use coqui_agentic_peer::auth::{self, Sealed, prove_out};
use coqui_agentic_peer::link::{self, Host as LinkHost, Options, Services};
use coqui_agentic_peer::mux::{Limits, Mux, Side};
use coqui_agentic_peer::peers::{NoEvents, PeerStore};
use coqui_peer_webcrypto::{Identity, PeerId, Trust, WebCrypto};
use coqui_web_channel::websocket::{Dialer as WsDialer, Frames, Listener};
use coqui_web_channel::{Channel as _, Dialer as _, Kind};

pub const WAIT: Duration = Duration::from_secs(10);
/// The channel limit once a peer is proven.
pub const MAX_LINE: usize = 16 * 1024 * 1024;

/// Ends the whole test program when it is still alive after `limit`, naming what hung: the thing
/// a test holds it for is a deadlock, which would otherwise stop the run rather than fail it.
pub struct Watchdog(Arc<AtomicBool>);

impl Watchdog {
    pub fn start(limit: Duration, what: &'static str) -> Self {
        let disarmed = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&disarmed);
        let until = Instant::now() + limit;
        thread::spawn(move || {
            while !flag.load(Ordering::SeqCst) {
                if Instant::now() > until {
                    eprintln!("hung: {what}");
                    std::process::exit(101);
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        Self(disarmed)
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

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

/// The answering end: accepts connections, proves each peer, and serves its link.
pub struct Host {
    pub address: SocketAddr,
    pub id: PeerId,
    stop: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl Host {
    pub fn start(setup: Setup) -> Self {
        let id = setup.identity.peer_id();
        let listener = Listener::bind("127.0.0.1:0".parse().unwrap(), None)
            .unwrap()
            .with_limits(
                coqui_web_channel::Limits::default().with_max_message(auth::MAX_PROOF_MESSAGE),
            )
            .with_frames(Frames::Binary);
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let method = Arc::new(
            WebCrypto::builder()
                .identity(setup.identity)
                .initiators(setup.initiators)
                .build()
                .unwrap(),
        );
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
                    if let Ok(established) = auth::prove(
                        &*method,
                        &channel,
                        WAIT,
                        auth::Method::WebCrypto.record_length(MAX_LINE),
                    ) {
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
    body: impl FnOnce(&Mux<'_, Sealed<'_, coqui_web_channel::websocket::Channel>>) -> R,
) -> R {
    over_link_with(host.url(), host.id, client, Limits::default(), body)
}

pub fn over_link_with<R>(
    url: url::Url,
    server: PeerId,
    client: Identity,
    limits: Limits,
    body: impl FnOnce(&Mux<'_, Sealed<'_, coqui_web_channel::websocket::Channel>>) -> R,
) -> R {
    let method = WebCrypto::builder()
        .identity(client)
        .responders(Trust::pinned([server]))
        .build()
        .unwrap();
    let channel = WsDialer::new()
        .with_frames(Frames::Binary)
        .dial(Kind::WebSocket, &url, WAIT)
        .unwrap();
    let established = prove_out(
        &method,
        &channel,
        WAIT,
        auth::Method::WebCrypto.record_length(MAX_LINE),
    )
    .unwrap();
    let sealed = Sealed::new(&channel, established.session);
    let result = Mux::run(&sealed, Side::Dialed, limits, None, body);
    sealed.close();
    channel.close();
    result
}
