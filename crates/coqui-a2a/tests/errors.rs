//! Every A2A error type has a wire form, and the specification's table is the code's table.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

use std::collections::BTreeSet;

use coqui_a2a::error::{A2aError, ERROR_INFO, ErrorKind};
use coqui_a2a::wire::Response;
use serde_json::Value;

/// The nine types of A2A 1.0 section 3.3.2 with the JSON-RPC codes of section 5.4. A type the
/// specification adds is added here, and this test then fails until the table maps it.
const SPECIFICATION: [(&str, i64); 9] = [
    ("TaskNotFoundError", -32001),
    ("TaskNotCancelableError", -32002),
    ("PushNotificationNotSupportedError", -32003),
    ("UnsupportedOperationError", -32004),
    ("ContentTypeNotSupportedError", -32005),
    ("InvalidAgentResponseError", -32006),
    ("ExtendedAgentCardNotConfiguredError", -32007),
    ("ExtensionSupportRequiredError", -32008),
    ("VersionNotSupportedError", -32009),
];

#[test]
fn every_type_the_specification_names_is_in_the_table_with_its_code() {
    for (name, code) in SPECIFICATION {
        let kind = ErrorKind::from_name(name).unwrap_or_else(|| panic!("{name} has no mapping"));
        assert_eq!(kind.code(), code, "{name}");
        assert!(kind.is_a2a(), "{name}");
    }
    let a2a = ErrorKind::ALL.iter().filter(|kind| kind.is_a2a()).count();
    assert_eq!(
        a2a,
        SPECIFICATION.len(),
        "a type the specification does not list"
    );
}

#[test]
fn the_table_walks_whole_and_each_row_round_trips() {
    let mut codes = BTreeSet::new();
    let mut reasons = BTreeSet::new();
    let mut names = BTreeSet::new();
    for kind in ErrorKind::ALL {
        assert!(codes.insert(kind.code()), "{kind}: a code used twice");
        assert!(reasons.insert(kind.reason()), "{kind}: a reason used twice");
        assert!(names.insert(kind.name()), "{kind}: a name used twice");
        assert!(
            kind.reason()
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b == b'_'),
            "{kind}"
        );
        assert!(!kind.message().is_empty());
        assert_eq!(ErrorKind::from_code(kind.code()), Some(*kind));
        assert_eq!(ErrorKind::from_name(kind.name()), Some(*kind));

        let error = A2aError::new(*kind, "a message").with_metadata("taskId", "t1");
        let wire = error.to_value();
        assert_eq!(wire["code"], kind.code());
        assert_eq!(wire["data"][0]["@type"], ERROR_INFO);
        assert_eq!(wire["data"][0]["reason"], kind.reason());
        assert_eq!(wire["data"][0]["domain"], kind.domain());
        assert_eq!(wire["data"][0]["metadata"]["taskId"], "t1");
        let read = A2aError::from_value(&wire).unwrap();
        assert_eq!(read, error);
        assert_eq!(read.kind(), Some(*kind));

        let response = Response::error(Value::from(3), error.clone());
        assert_eq!(Response::decode(&response.encode()).unwrap(), response);
    }
    // JSON-RPC codes stay where JSON-RPC puts them, and the rest inside the range it leaves.
    for kind in ErrorKind::ALL {
        let code = kind.code();
        assert!(
            (-32099..=-32000).contains(&code)
                || [-32700, -32600, -32601, -32602, -32603].contains(&code),
            "{kind}: {code}"
        );
    }
}

/// The rows of the table in `docs/binding.md`: name, code, reason and domain.
fn documented() -> Option<Vec<(String, i64, String, String)>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/binding.md");
    let text = std::fs::read_to_string(path).ok()?;
    let cells = |line: &str| -> Vec<String> {
        line.trim()
            .trim_matches('|')
            .split('|')
            .map(|cell| cell.trim().trim_matches('`').to_owned())
            .collect()
    };
    Some(
        text.lines()
            .filter(|line| line.starts_with("| `") && line.contains("Error`"))
            .filter_map(|line| {
                let cells = cells(line);
                Some((
                    cells.first()?.clone(),
                    cells.get(1)?.parse().ok()?,
                    cells.get(2)?.clone(),
                    cells.get(3)?.clone(),
                ))
            })
            .collect(),
    )
}

#[test]
fn the_documented_table_is_the_table_in_the_code() {
    let Some(rows) = documented() else {
        eprintln!("skipped: docs/binding.md is not beside this crate");
        return;
    };
    let code: Vec<_> = ErrorKind::ALL
        .iter()
        .map(|kind| {
            (
                kind.name().to_owned(),
                kind.code(),
                kind.reason().to_owned(),
                kind.domain().to_owned(),
            )
        })
        .collect();
    assert_eq!(rows, code);
}
