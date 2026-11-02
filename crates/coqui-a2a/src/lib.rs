//! The A2A data model, its error table and the messages of the coqui-agentic-peer binding.
#![forbid(unsafe_code)]
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

pub mod error;
pub mod model;
pub mod wire;

pub use error::{A2aError, ErrorKind};

/// The token of the stream protocol, and the grant a peer needs.
pub const PROTOCOL: &str = "a2a/1";

/// The `protocolBinding` of the Agent Card interface that names a link. An identifier nobody
/// fetches; a breaking change of the binding takes a new one.
pub const BINDING: &str = "https://boriken.dev/a2a/bindings/coqui-agentic-peer/v1";

/// The A2A version this binding carries.
pub const A2A_VERSION: &str = "1.0";
