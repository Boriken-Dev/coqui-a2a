#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod agent;
pub mod client;
pub mod error;
#[cfg(feature = "loopback")]
#[cfg_attr(docsrs, doc(cfg(feature = "loopback")))]
pub mod loopback;
pub mod model;
pub mod server;
pub mod wire;

pub use agent::{Agent, Caller, Events, Watch};
pub use client::{Client, ClientError, ClientTimeouts};
pub use error::{A2aError, ErrorKind};
pub use server::{Server, ServerLimits};

/// The token of the stream protocol, for [`coqui_agentic_peer::link::Services::with`] and a grant.
pub const PROTOCOL: &str = "a2a/1";

/// The `protocolBinding` of the Agent Card interface that names a link. An identifier nobody
/// fetches; a breaking change of the binding takes a new one.
pub const BINDING: &str = "https://boriken.dev/a2a/bindings/coqui-agentic-peer/v1";

/// The A2A version this binding carries.
pub const A2A_VERSION: &str = "1.0";
