//! The binding's messages: the shared vectors, and arbitrary bytes never panicking a decoder.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

use coqui_a2a::error::{A2aError, ErrorKind};
use coqui_a2a::model::{AgentCard, Task};
use coqui_a2a::wire::{Method, Request, Response, ServiceParameters};
use proptest::prelude::*;
use serde_json::{Value, json};

fn vectors() -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vectors/binding.json");
    match std::fs::read_to_string(&path) {
        Ok(text) => serde_json::from_str(&text).unwrap(),
        // A packaged crate has no repository around it; the repository's own run covers this.
        Err(_) => {
            eprintln!("skipped: vectors/binding.json is not beside this crate");
            Value::Null
        }
    }
}

#[test]
fn every_request_vector_decodes_or_fails_as_recorded() {
    let vectors = vectors();
    let Some(cases) = vectors["requests"].as_array() else {
        return;
    };
    for case in cases {
        let input = case["input"].as_str().unwrap().as_bytes();
        let outcome = &case["outcome"];
        let name = case["name"].as_str().unwrap();
        match Request::decode(input) {
            Ok(request) => {
                assert!(
                    outcome.get("error").is_none(),
                    "{name}: decoded, expected an error"
                );
                assert_eq!(request.id, outcome["id"], "{name}");
                assert_eq!(
                    request.method,
                    outcome["method"].as_str().unwrap(),
                    "{name}"
                );
                let expected = outcome["service_parameters"].as_object().unwrap();
                assert_eq!(
                    request.service_parameters.iter().count(),
                    expected.len(),
                    "{name}"
                );
                for (key, value) in expected {
                    assert_eq!(
                        request.service_parameters.get(key),
                        value.as_str(),
                        "{name}"
                    );
                }
                // What is encoded decodes to the same request.
                assert_eq!(
                    Request::decode(&request.encode()).unwrap(),
                    request,
                    "{name}"
                );
            }
            Err((id, error)) => {
                assert_eq!(error.code, outcome["error"].as_i64().unwrap(), "{name}");
                assert_eq!(id, outcome["id"], "{name}");
            }
        }
    }
}

#[test]
fn every_error_vector_reads_as_recorded() {
    let vectors = vectors();
    let Some(cases) = vectors["errors"].as_array() else {
        return;
    };
    for case in cases {
        let response = Response::decode(case["response"].as_str().unwrap().as_bytes()).unwrap();
        let error = response.outcome.unwrap_err();
        match case["kind"].as_str() {
            Some(name) => assert_eq!(error.kind(), ErrorKind::from_name(name)),
            None => assert_eq!(error.kind(), None),
        }
    }
}

#[test]
fn unknown_members_and_enum_names_survive_a_round_trip() {
    let task: Task = serde_json::from_str(
        r#"{"id":"t","status":{"state":"TASK_STATE_FUTURE","later":1},"extra":{"a":[1,2]}}"#,
    )
    .unwrap();
    let again: Value = serde_json::to_value(&task).unwrap();
    assert_eq!(again["status"]["state"], "TASK_STATE_FUTURE");
    assert_eq!(again["status"]["later"], 1);
    assert_eq!(again["extra"]["a"][1], 2);
    assert!(
        again.get("artifacts").is_none(),
        "absent stays absent, not null"
    );
}

proptest! {
    #[test]
    fn arbitrary_bytes_never_panic_a_decoder(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let _ = Request::decode(&bytes);
        let _ = Response::decode(&bytes);
        let _ = serde_json::from_slice::<AgentCard>(&bytes);
        let _ = serde_json::from_slice::<Task>(&bytes);
    }

    #[test]
    fn arbitrary_text_in_every_member_never_panics_a_decoder(text in "\\PC{0,200}", id in any::<i64>()) {
        let quoted = serde_json::to_string(&text).unwrap();
        let wrapped = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":{quoted},\"params\":{{\"x\":{quoted}}},\"a2a-service-parameters\":{{{quoted}:{quoted}}}}}"
        );
        let _ = Request::decode(wrapped.as_bytes());
    }

    #[test]
    fn a_request_that_encodes_decodes_to_itself(
        id in any::<u32>(),
        names in proptest::collection::btree_map("[a-z][a-z0-9-]{0,20}", "[ -~]{0,40}", 0..5),
    ) {
        let mut request = Request::new(Value::from(id), Method::GetTask, json!({"id": "t"}));
        let mut parameters = ServiceParameters::new();
        for (name, value) in &names {
            parameters.set(name, value).unwrap();
        }
        request.service_parameters = parameters;
        prop_assert_eq!(Request::decode(&request.encode()).unwrap(), request);
    }

    #[test]
    fn an_error_that_is_sent_is_the_error_that_is_read(code in -32100_i64..-32000, message in "\\PC{0,60}") {
        let error = A2aError { code, message, data: Vec::new() };
        let response = Response::error(Value::Null, error.clone());
        prop_assert_eq!(Response::decode(&response.encode()).unwrap().outcome.unwrap_err(), error);
    }
}
