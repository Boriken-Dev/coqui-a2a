//! This crate holds the mapping of A2A onto a link and nothing the link crate already holds: no
//! proof, pairing, sealing or discovery of its own.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

use std::fs;
use std::path::{Path, PathBuf};

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn sources() -> Vec<(PathBuf, String)> {
    let mut found = Vec::new();
    let mut pending = vec![crate_dir().join("src")];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let text = fs::read_to_string(&path).unwrap();
                found.push((path, text));
            }
        }
    }
    found
}

fn name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}

/// What the manifest asks for, by section.
fn dependencies(section: &str) -> Vec<String> {
    let manifest = fs::read_to_string(crate_dir().join("Cargo.toml")).unwrap();
    let mut inside = false;
    let mut names = Vec::new();
    for line in manifest.lines() {
        if line.starts_with('[') {
            inside = line.trim() == format!("[{section}]");
        } else if inside && let Some((key, _)) = line.split_once('=') {
            names.push(key.trim().to_owned());
        }
    }
    names
}

#[test]
fn the_library_depends_on_the_link_crate_and_no_primitive_of_its_own() {
    let allowed = [
        "coqui-agentic-peer",
        "coqui-web",
        "serde",
        "serde_json",
        "time",
        // The loopback endpoint's HTTP server.
        "axum",
        "tokio",
        "tokio-stream",
        "futures-util",
        // The command line only.
        "clap",
        "coqui-peer-webcrypto",
        "coqui-peer-tls",
        "rustls",
        "url",
    ];
    for dependency in dependencies("dependencies") {
        assert!(
            allowed.contains(&dependency.as_str()),
            "{dependency} is not on the list of what this crate may depend on: if it is a proof, \
             sealing, pairing or discovery primitive, it belongs in coqui-agentic-peer"
        );
    }
    // No hash, cipher, curve or multicast-DNS crate is named here, directly.
    for forbidden in [
        "sha2", "ring", "p256", "aes-gcm", "hkdf", "spake2", "mdns-sd", "socket2",
    ] {
        assert!(
            !dependencies("dependencies")
                .iter()
                .any(|name| name == forbidden),
            "{forbidden}"
        );
    }
}

#[test]
fn the_library_source_names_no_proof_pairing_or_discovery_item() {
    let forbidden = [
        "coqui_peer",
        "coqui_agentic_peer::auth",
        "coqui_agentic_peer::pairing",
        "coqui_agentic_peer::discovery",
        "coqui_agentic_peer::companion",
        "Sealed",
        "TrustStore",
        "Identity",
        "mdns",
    ];
    for (path, text) in sources() {
        // The command wires a proof from the link crate's pieces; the library does not.
        if name(&path) == "main.rs" {
            continue;
        }
        for item in forbidden {
            assert!(!text.contains(item), "{} names {item}", path.display());
        }
    }
}

#[test]
fn the_command_reaches_the_proof_only_through_the_link_crate() {
    let main = sources()
        .into_iter()
        .find(|(path, _)| name(path) == "main.rs")
        .unwrap()
        .1;
    // The proof, the sealing, the identity and the trust store are the link crate's calls.
    for call in [
        "auth::prove_out",
        "Sealed::new",
        "home::identity",
        "home::store",
    ] {
        assert!(main.contains(call), "{call}");
    }
    // It builds no handshake, key or cipher of its own.
    for own in [
        "sha2",
        "hkdf",
        "Handshake",
        "spake",
        "encrypt",
        "decrypt",
        "mdns",
    ] {
        assert!(!main.contains(own), "{own}");
    }
}
