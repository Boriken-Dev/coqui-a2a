# coqui-a2a

**A2A over a link a peer has already proved.** A peer paired once on a [coqui-agentic-peer](https://boriken-dev.github.io/coqui-agentic-peer/) link is granted `a2a/1` like any other capability, and uses A2A on the same connection: no second pairing, no URL, no certificate of its own. A stream carries A2A's own messages.

- [The binding](binding.md): the A2A custom protocol binding, specified so it can be implemented without this code.
- [The loopback command](loopback.md): a standard HTTP A2A endpoint on a loopback address, so a stock A2A client or SDK works on a link.
- The Rust API reference is under `rust/` on this site.

Three rules hold the design together:

- **Standards stay standard.** A2A's messages cross the link unchanged, and a client with none of this code keeps working through the loopback command.
- **One pairing per peer.** `a2a/1` is a grant on the entry pairing made.
- **One implementation of each thing.** Proof, pairing, sealing and discovery live in `coqui-agentic-peer`; this crate holds only the mapping of A2A.
