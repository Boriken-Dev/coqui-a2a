//! The task store: ownership, terminal states, subscribers and limits.
#![cfg(feature = "tasks")]
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use coqui_a2a::error::{A2aError, ErrorKind};
use coqui_a2a::model::{
    AgentCapabilities, AgentCard, Artifact, CancelTaskRequest, GetTaskRequest, ListTasksRequest,
    ListTasksResponse, Message, Part, Role, SendMessageRequest, SendMessageResponse,
    StreamResponse, SubscribeToTaskRequest, Task, TaskState,
};
use coqui_a2a::tasks::TaskStore;
use coqui_a2a::wire::{Method, ServiceParameters};
use coqui_a2a::{Agent, Caller, Events, Reply, Server, Watch};
use serde_json::{Value, json};

/// A caller in process, which owns what its borrowed parts point at.
struct Who {
    peer: &'static str,
    claims: BTreeMap<String, String>,
    parameters: ServiceParameters,
    watch: Watch,
}

impl Who {
    fn new(peer: &'static str) -> Self {
        Self {
            peer,
            claims: BTreeMap::new(),
            parameters: ServiceParameters::default(),
            watch: Watch::new(),
        }
    }

    fn caller(&self) -> Caller<'_> {
        Caller::local(self.peer, &self.claims, &self.parameters, &self.watch)
    }
}

fn message(text: &str) -> Message {
    Message::text(Role::User, format!("m-{text}"), text)
}

fn request(text: &str) -> SendMessageRequest {
    SendMessageRequest::new(message(text))
}

fn follow_up(task: &Task, text: &str) -> SendMessageRequest {
    let mut message = message(text);
    message.task_id = Some(task.id.clone());
    SendMessageRequest::new(message)
}

fn id(task: &str) -> GetTaskRequest {
    GetTaskRequest {
        id: task.to_owned(),
        ..GetTaskRequest::default()
    }
}

fn artifact(name: &str, text: &str) -> Artifact {
    Artifact {
        artifact_id: name.to_owned(),
        parts: vec![Part::text(text)],
        ..Artifact::default()
    }
}

fn kind(error: &A2aError) -> Option<ErrorKind> {
    error.kind()
}

fn next(events: &mut Events) -> StreamResponse {
    events.next().expect("an item").expect("not an error")
}

fn state_of(item: &StreamResponse) -> TaskState {
    match item {
        StreamResponse::Task(task) => task.status.state.clone(),
        StreamResponse::StatusUpdate(event) => event.status.state.clone(),
        other => panic!("not a state: {other:?}"),
    }
}

#[test]
fn a_new_task_is_submitted_with_its_message_and_a_context() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    assert!(!task.id.is_empty());
    assert!(task.context_id.as_deref().is_some_and(|c| !c.is_empty()));
    assert_eq!(task.status.state, TaskState::Submitted);
    assert!(task.status.timestamp.is_some());
    let history = task.history.as_ref().unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].task_id.as_deref(), Some(task.id.as_str()));
    assert_eq!(history[0].context_id, task.context_id);

    // A context the sender named is kept; two tasks never share an id.
    let mut named = request("again");
    named.message.context_id = Some("ctx".to_owned());
    let second = store.create(&me.caller(), &named).unwrap();
    assert_eq!(second.context_id.as_deref(), Some("ctx"));
    assert_ne!(second.id, task.id);

    // A new task names no task of its own.
    let error = store
        .create(&me.caller(), &follow_up(&task, "x"))
        .unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::InvalidParams));
}

#[test]
fn a_store_refuses_a_message_after_a_terminal_state() {
    for ending in [
        TaskState::Completed,
        TaskState::Failed,
        TaskState::Canceled,
        TaskState::Rejected,
    ] {
        let store = TaskStore::builder().build();
        let me = Who::new("a");
        let task = store.create(&me.caller(), &request("hi")).unwrap();
        store
            .update_status(&task.id, TaskState::Working, None)
            .unwrap();
        // A follow-up is accepted while the task lives.
        store
            .append(&me.caller(), &follow_up(&task, "more"))
            .unwrap();
        store.update_status(&task.id, ending.clone(), None).unwrap();
        let before = store.snapshot(&task.id).unwrap();

        let error = store
            .append(&me.caller(), &follow_up(&task, "late"))
            .unwrap_err();
        assert_eq!(
            kind(&error),
            Some(ErrorKind::UnsupportedOperation),
            "{ending}"
        );
        // The task has ended for the agent too: no state, no artifact, no way back.
        let error = store
            .update_status(&task.id, TaskState::Working, None)
            .unwrap_err();
        assert_eq!(
            kind(&error),
            Some(ErrorKind::UnsupportedOperation),
            "{ending}"
        );
        let error = store
            .add_artifact(&task.id, artifact("late", "x"), false, true)
            .unwrap_err();
        assert_eq!(
            kind(&error),
            Some(ErrorKind::UnsupportedOperation),
            "{ending}"
        );
        // A task that has ended is not subscribed to, and is not canceled again.
        let error = store
            .subscribe(
                &me.caller(),
                &SubscribeToTaskRequest {
                    id: task.id.clone(),
                    ..SubscribeToTaskRequest::default()
                },
            )
            .map(|_| ())
            .unwrap_err();
        assert_eq!(
            kind(&error),
            Some(ErrorKind::UnsupportedOperation),
            "{ending}"
        );
        let error = store
            .cancel(
                &me.caller(),
                &CancelTaskRequest {
                    id: task.id.clone(),
                    ..CancelTaskRequest::default()
                },
            )
            .unwrap_err();
        assert_eq!(kind(&error), Some(ErrorKind::TaskNotCancelable), "{ending}");
        assert_eq!(store.snapshot(&task.id).unwrap(), before, "{ending}");
    }
}

#[test]
fn a_task_does_not_go_back_to_submitted_or_to_a_state_nobody_named() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    store
        .update_status(&task.id, TaskState::Working, None)
        .unwrap();
    for state in [
        TaskState::Submitted,
        TaskState::Unspecified,
        TaskState::Other("TASK_STATE_NEW".to_owned()),
    ] {
        let error = store.update_status(&task.id, state, None).unwrap_err();
        assert_eq!(kind(&error), Some(ErrorKind::InvalidParams));
    }
    // Waiting for the person and working again is allowed, as often as it happens.
    for state in [
        TaskState::InputRequired,
        TaskState::Working,
        TaskState::AuthRequired,
        TaskState::Working,
    ] {
        store.update_status(&task.id, state, None).unwrap();
    }
    assert_eq!(
        store
            .update_status("nobody", TaskState::Working, None)
            .map(|_| ())
            .unwrap_err()
            .kind(),
        Some(ErrorKind::TaskNotFound)
    );
}

#[test]
fn a_second_subscriber_is_answered_the_current_task_then_the_updates() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    let subscribe = |who: &Who| {
        store
            .subscribe(
                &who.caller(),
                &SubscribeToTaskRequest {
                    id: task.id.clone(),
                    ..SubscribeToTaskRequest::default()
                },
            )
            .unwrap()
    };

    let mut first = subscribe(&me);
    assert_eq!(state_of(&next(&mut first)), TaskState::Submitted);
    store
        .update_status(&task.id, TaskState::Working, None)
        .unwrap();
    store
        .add_artifact(&task.id, artifact("out", "part one"), false, false)
        .unwrap();

    // Joining late, it is told where the task is, with what it has so far, before any update.
    let mut second = subscribe(&me);
    let StreamResponse::Task(now) = next(&mut second) else {
        panic!("the task as it is now comes first");
    };
    assert_eq!(now.status.state, TaskState::Working);
    assert_eq!(
        now.artifacts.as_ref().unwrap()[0].parts[0].text.as_deref(),
        Some("part one")
    );

    store
        .update_status(&task.id, TaskState::Completed, None)
        .unwrap();
    // Both are told the same, in the same order, and both end after the terminal state.
    for events in [&mut first, &mut second] {
        let mut seen = Vec::new();
        for item in events.by_ref() {
            seen.push(item.unwrap());
        }
        let last = seen.last().expect("the terminal update");
        assert!(last.ends_task());
        assert_eq!(state_of(last), TaskState::Completed);
    }
}

#[test]
fn updates_reach_a_subscriber_in_the_order_they_were_made() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    let mut events = store
        .subscribe(
            &me.caller(),
            &SubscribeToTaskRequest {
                id: task.id.clone(),
                ..SubscribeToTaskRequest::default()
            },
        )
        .unwrap();
    next(&mut events);
    store
        .update_status(&task.id, TaskState::Working, None)
        .unwrap();
    store
        .add_artifact(&task.id, artifact("out", "a"), false, false)
        .unwrap();
    store
        .add_artifact(&task.id, artifact("out", "b"), true, true)
        .unwrap();
    store
        .update_status(&task.id, TaskState::InputRequired, Some(message("which?")))
        .unwrap();
    store
        .update_status(&task.id, TaskState::Completed, None)
        .unwrap();
    let shapes: Vec<String> = events
        .map(|item| match item.unwrap() {
            StreamResponse::StatusUpdate(event) => event.status.state.to_string(),
            StreamResponse::ArtifactUpdate(event) => format!(
                "artifact {} append={:?} last={:?}",
                event.artifact.parts[0].text.clone().unwrap(),
                event.append,
                event.last_chunk
            ),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        shapes,
        [
            "TASK_STATE_WORKING",
            "artifact a append=Some(false) last=Some(false)",
            "artifact b append=Some(true) last=Some(true)",
            "TASK_STATE_INPUT_REQUIRED",
            "TASK_STATE_COMPLETED",
        ]
    );
    // An appended chunk joins the artifact; the status message joins the history.
    let task = store.snapshot(&task.id).unwrap();
    let parts = &task.artifacts.unwrap()[0].parts;
    assert_eq!(parts.len(), 2);
    assert_eq!(task.history.unwrap().len(), 2);
}

#[test]
fn a_store_lists_only_the_callers_tasks_and_answers_anothers_id_as_not_found() {
    let store = TaskStore::builder().build();
    let (alice, bob) = (Who::new("alice"), Who::new("bob"));
    let mine = store.create(&alice.caller(), &request("one")).unwrap();
    let also_mine = store.create(&alice.caller(), &request("two")).unwrap();
    let theirs = store.create(&bob.caller(), &request("three")).unwrap();

    let listed = |who: &Who| -> Vec<String> {
        store
            .list(&who.caller(), &ListTasksRequest::default())
            .unwrap()
            .tasks
            .into_iter()
            .map(|task| task.id)
            .collect()
    };
    // Most recently updated first.
    assert_eq!(listed(&alice), [also_mine.id.clone(), mine.id.clone()]);
    assert_eq!(listed(&bob), vec![theirs.id.clone()]);
    assert!(listed(&Who::new("carol")).is_empty());

    // Every operation on another caller's task answers as for a task that does not exist, with
    // nothing to tell the two apart.
    let missing = store.get(&alice.caller(), &id("no-such-task")).unwrap_err();
    let foreign = store.get(&alice.caller(), &id(&theirs.id)).unwrap_err();
    assert_eq!(kind(&foreign), Some(ErrorKind::TaskNotFound));
    assert_eq!(foreign.code, missing.code);
    assert!(!foreign.message.contains(&bob.peer.to_owned()));
    let subscribe = store
        .subscribe(
            &alice.caller(),
            &SubscribeToTaskRequest {
                id: theirs.id.clone(),
                ..SubscribeToTaskRequest::default()
            },
        )
        .map(|_| ())
        .unwrap_err();
    assert_eq!(kind(&subscribe), Some(ErrorKind::TaskNotFound));
    let cancel = store
        .cancel(
            &alice.caller(),
            &CancelTaskRequest {
                id: theirs.id.clone(),
                ..CancelTaskRequest::default()
            },
        )
        .unwrap_err();
    assert_eq!(kind(&cancel), Some(ErrorKind::TaskNotFound));
    let append = store
        .append(&alice.caller(), &follow_up(&theirs, "mine now"))
        .unwrap_err();
    assert_eq!(kind(&append), Some(ErrorKind::TaskNotFound));

    // None of it changed bob's task.
    let task = store.snapshot(&theirs.id).unwrap();
    assert_eq!(task.status.state, TaskState::Submitted);
    assert_eq!(task.history.unwrap().len(), 1);
}

#[test]
fn a_list_filters_and_pages() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    let mut made = Vec::new();
    for n in 0..5 {
        let mut asking = request(&format!("task {n}"));
        asking.message.context_id = Some(if n % 2 == 0 { "even" } else { "odd" }.to_owned());
        made.push(store.create(&me.caller(), &asking).unwrap());
    }
    store
        .update_status(&made[0].id, TaskState::Working, None)
        .unwrap();
    store
        .add_artifact(&made[0].id, artifact("out", "x"), false, true)
        .unwrap();

    let list = |request: ListTasksRequest| store.list(&me.caller(), &request).unwrap();
    let by_context = list(ListTasksRequest {
        context_id: Some("even".to_owned()),
        ..ListTasksRequest::default()
    });
    assert_eq!(by_context.total_size, 3);
    let by_state = list(ListTasksRequest {
        status: Some(TaskState::Working),
        ..ListTasksRequest::default()
    });
    assert_eq!(by_state.tasks.len(), 1);
    assert_eq!(by_state.tasks[0].id, made[0].id);
    // Artifacts and history are left out unless asked for.
    assert!(by_state.tasks[0].artifacts.is_none());
    let with = list(ListTasksRequest {
        status: Some(TaskState::Working),
        include_artifacts: Some(true),
        history_length: Some(1),
        ..ListTasksRequest::default()
    });
    assert_eq!(with.tasks[0].artifacts.as_ref().unwrap().len(), 1);
    assert_eq!(with.tasks[0].history.as_ref().unwrap().len(), 1);

    // Pages are walked by their token, newest first, and the last one has none.
    let mut seen = Vec::new();
    let mut token = None;
    loop {
        let page = list(ListTasksRequest {
            page_size: Some(2),
            page_token: token.clone(),
            ..ListTasksRequest::default()
        });
        assert_eq!(page.total_size, 5);
        seen.extend(page.tasks.iter().map(|task| task.id.clone()));
        if page.next_page_token.is_empty() {
            break;
        }
        token = Some(page.next_page_token);
    }
    // Task 0 was updated last, so it leads; the others follow, newest first.
    let expected: Vec<String> = [0, 4, 3, 2, 1]
        .iter()
        .map(|n| made[*n].id.clone())
        .collect();
    assert_eq!(seen, expected);

    let error = store
        .list(
            &me.caller(),
            &ListTasksRequest {
                page_token: Some("not a token".to_owned()),
                ..ListTasksRequest::default()
            },
        )
        .unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::InvalidParams));
}

#[test]
fn a_tenant_on_a_request_narrows_it_and_a_request_without_one_sees_the_callers_tasks() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    let mut for_x = request("x");
    for_x.tenant = Some("x".to_owned());
    let mut for_y = request("y");
    for_y.tenant = Some("y".to_owned());
    let x = store.create(&me.caller(), &for_x).unwrap();
    let y = store.create(&me.caller(), &for_y).unwrap();

    let tenant = |name: &str| ListTasksRequest {
        tenant: Some(name.to_owned()),
        ..ListTasksRequest::default()
    };
    let only_x = store.list(&me.caller(), &tenant("x")).unwrap();
    assert_eq!(only_x.tasks.len(), 1);
    assert_eq!(only_x.tasks[0].id, x.id);
    assert_eq!(
        store
            .list(&me.caller(), &ListTasksRequest::default())
            .unwrap()
            .tasks
            .len(),
        2
    );
    assert!(
        store
            .list(&me.caller(), &tenant("z"))
            .unwrap()
            .tasks
            .is_empty()
    );

    // A task of another tenant is not found through this one, and is through its own or none.
    let through = |tenant: Option<&str>, task: &Task| {
        store.get(
            &me.caller(),
            &GetTaskRequest {
                tenant: tenant.map(str::to_owned),
                id: task.id.clone(),
                ..GetTaskRequest::default()
            },
        )
    };
    assert_eq!(
        kind(&through(Some("x"), &y).unwrap_err()),
        Some(ErrorKind::TaskNotFound)
    );
    assert!(through(Some("y"), &y).is_ok());
    assert!(through(None, &y).is_ok());
}

#[test]
fn history_artifacts_and_bytes_are_bounded() {
    let store = TaskStore::builder()
        .max_history(3)
        .max_artifacts(2)
        .max_artifact_bytes(400)
        .build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("m0")).unwrap();
    for n in 1..=5 {
        store
            .append(&me.caller(), &follow_up(&task, &format!("m{n}")))
            .unwrap();
    }
    // The oldest go; the newest stay in order.
    let history = store.snapshot(&task.id).unwrap().history.unwrap();
    let texts: Vec<String> = history.iter().map(Message::joined_text).collect();
    assert_eq!(texts, ["m3", "m4", "m5"]);
    // A reader can ask for fewer.
    let two = store
        .get(
            &me.caller(),
            &GetTaskRequest {
                history_length: Some(2),
                ..id(&task.id)
            },
        )
        .unwrap();
    assert_eq!(two.history.unwrap().len(), 2);
    let none = store
        .get(
            &me.caller(),
            &GetTaskRequest {
                history_length: Some(0),
                ..id(&task.id)
            },
        )
        .unwrap();
    assert!(none.history.is_none_or(|history| history.is_empty()));

    // Two artifacts, not a third; replacing one is not adding one.
    store
        .add_artifact(&task.id, artifact("a", "1"), false, true)
        .unwrap();
    store
        .add_artifact(&task.id, artifact("b", "2"), false, true)
        .unwrap();
    let error = store
        .add_artifact(&task.id, artifact("c", "3"), false, true)
        .unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::InvalidAgentResponse));
    store
        .add_artifact(&task.id, artifact("a", "replaced"), false, true)
        .unwrap();
    let artifacts = store.snapshot(&task.id).unwrap().artifacts.unwrap();
    assert_eq!(artifacts.len(), 2);
    assert_eq!(artifacts[0].parts[0].text.as_deref(), Some("replaced"));

    // Appended chunks count against the bytes, and the one that does not fit changes nothing.
    let before = store.snapshot(&task.id).unwrap();
    let error = store
        .add_artifact(&task.id, artifact("b", &"x".repeat(1000)), true, false)
        .unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::InvalidAgentResponse));
    assert_eq!(store.snapshot(&task.id).unwrap(), before);
}

#[test]
fn a_subscriber_that_falls_behind_is_told_so_and_never_mistaken_for_whole() {
    let store = TaskStore::builder().subscriber_queue(2).build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    let mut slow = store
        .subscribe(
            &me.caller(),
            &SubscribeToTaskRequest {
                id: task.id.clone(),
                ..SubscribeToTaskRequest::default()
            },
        )
        .unwrap();
    for n in 0..6 {
        store
            .add_artifact(&task.id, artifact("out", &n.to_string()), n > 0, false)
            .unwrap();
    }
    store
        .update_status(&task.id, TaskState::Completed, None)
        .unwrap();
    let items: Vec<_> = slow.by_ref().collect();
    assert!(items.last().unwrap().is_err(), "{items:?}");
    assert!(
        items
            .iter()
            .all(|item| !matches!(item, Ok(item) if item.ends_task()))
    );
    // The producer was never held up, and the task is whole.
    assert_eq!(
        store.snapshot(&task.id).unwrap().status.state,
        TaskState::Completed
    );
}

#[test]
fn subscribers_are_bounded_and_a_dropped_one_makes_room() {
    let store = TaskStore::builder().max_subscribers(2).build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    let subscribe = || {
        store.subscribe(
            &me.caller(),
            &SubscribeToTaskRequest {
                id: task.id.clone(),
                ..SubscribeToTaskRequest::default()
            },
        )
    };
    let one = subscribe().unwrap();
    let _two = subscribe().unwrap();
    assert_eq!(
        kind(&subscribe().map(|_| ()).unwrap_err()),
        Some(ErrorKind::InvalidRequest)
    );
    drop(one);
    assert!(subscribe().is_ok());
}

#[test]
fn a_quiet_subscription_ends_when_its_watch_closes() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    let mut events = store
        .subscribe(
            &me.caller(),
            &SubscribeToTaskRequest {
                id: task.id,
                ..SubscribeToTaskRequest::default()
            },
        )
        .unwrap();
    next(&mut events);
    let watch = me.watch.clone();
    let closer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        watch.close();
    });
    // Nothing is made to happen to the task: only the watch ends the wait.
    assert!(events.next().is_none());
    closer.join().unwrap();
}

#[test]
fn cancel_runs_the_hook_then_ends_the_task_and_a_hook_that_refuses_changes_nothing() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let refuse = Arc::new(AtomicUsize::new(1));
    let store = TaskStore::builder()
        .cancel_hook({
            let (seen, refuse) = (Arc::clone(&seen), Arc::clone(&refuse));
            move |task: &Task| {
                seen.lock().unwrap().push(task.id.clone());
                if refuse.load(Ordering::SeqCst) == 1 {
                    return Err(A2aError::standard(ErrorKind::TaskNotCancelable));
                }
                Ok(())
            }
        })
        .build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    let cancel = CancelTaskRequest {
        id: task.id.clone(),
        ..CancelTaskRequest::default()
    };
    let mut events = store
        .subscribe(
            &me.caller(),
            &SubscribeToTaskRequest {
                id: task.id.clone(),
                ..SubscribeToTaskRequest::default()
            },
        )
        .unwrap();
    next(&mut events);

    let error = store.cancel(&me.caller(), &cancel).unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::TaskNotCancelable));
    assert_eq!(
        store.snapshot(&task.id).unwrap().status.state,
        TaskState::Submitted
    );

    refuse.store(0, Ordering::SeqCst);
    let canceled = store.cancel(&me.caller(), &cancel).unwrap();
    assert_eq!(canceled.status.state, TaskState::Canceled);
    assert_eq!(*seen.lock().unwrap(), [task.id.clone(), task.id.clone()]);
    // The subscriber is told, and the stream ends.
    let last = events.last().unwrap().unwrap();
    assert_eq!(state_of(&last), TaskState::Canceled);
}

#[test]
fn without_a_hook_cancel_just_ends_the_task() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    let canceled = store
        .cancel(
            &me.caller(),
            &CancelTaskRequest {
                id: task.id,
                ..CancelTaskRequest::default()
            },
        )
        .unwrap();
    assert_eq!(canceled.status.state, TaskState::Canceled);
}

/// An agent over a store, as a host writes one: the store keeps the tasks and answers the
/// operations about them; the agent only starts the work.
struct Stored {
    store: TaskStore,
}

impl Stored {
    /// Starts the work. A streaming caller subscribes before this, or a quick task is over first.
    fn work(&self, id: &str) {
        let (store, id) = (self.store.clone(), id.to_owned());
        std::thread::spawn(move || {
            let _ = store.update_status(&id, TaskState::Working, None);
            let _ = store.add_artifact(&id, artifact("result", "done"), false, true);
            let _ = store.update_status(&id, TaskState::Completed, None);
        });
    }
}

impl Agent for Stored {
    fn agent_card(&self, _caller: &Caller<'_>) -> AgentCard {
        AgentCard {
            capabilities: AgentCapabilities {
                streaming: Some(true),
                ..AgentCapabilities::default()
            },
            ..AgentCard::default()
        }
    }

    fn send_message(
        &self,
        caller: &Caller<'_>,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2aError> {
        let task = self.store.create(caller, &request)?;
        self.work(&task.id);
        Ok(SendMessageResponse::Task(task))
    }

    fn send_streaming_message(
        &self,
        caller: &Caller<'_>,
        request: SendMessageRequest,
    ) -> Result<Events, A2aError> {
        let task = self.store.create(caller, &request)?;
        let events = self.store.subscribe(
            caller,
            &SubscribeToTaskRequest {
                id: task.id.clone(),
                ..SubscribeToTaskRequest::default()
            },
        )?;
        self.work(&task.id);
        Ok(events)
    }

    fn get_task(&self, caller: &Caller<'_>, request: GetTaskRequest) -> Result<Task, A2aError> {
        self.store.get(caller, &request)
    }

    fn list_tasks(
        &self,
        caller: &Caller<'_>,
        request: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2aError> {
        self.store.list(caller, &request)
    }

    fn cancel_task(
        &self,
        caller: &Caller<'_>,
        request: CancelTaskRequest,
    ) -> Result<Task, A2aError> {
        self.store.cancel(caller, &request)
    }

    fn subscribe_to_task(
        &self,
        caller: &Caller<'_>,
        request: SubscribeToTaskRequest,
    ) -> Result<Events, A2aError> {
        self.store.subscribe(caller, &request)
    }
}

#[test]
fn an_agent_over_a_store_serves_two_local_callers_each_their_own_tasks() {
    let server = Server::new(Stored {
        store: TaskStore::builder().build(),
    });
    let (alice, bob) = (Who::new("alice"), Who::new("bob"));
    let call =
        |who: &Who, method: Method, params: Value| server.call(&who.caller(), method, params);

    // Alice's streamed task runs to its end through the same checks a link has.
    let items: Vec<_> = server
        .stream(
            &alice.caller(),
            Method::SendStreamingMessage,
            serde_json::to_value(request("go")).unwrap(),
        )
        .unwrap()
        .collect();
    let last = items.last().unwrap().as_ref().unwrap();
    assert_eq!(state_of(last), TaskState::Completed);
    assert!(
        items
            .iter()
            .any(|item| matches!(item, Ok(StreamResponse::ArtifactUpdate(_))))
    );
    let StreamResponse::Task(first) = items[0].as_ref().unwrap() else {
        panic!("the task comes first");
    };
    let task_id = first.id.clone();

    let Reply::Tasks(listed) = call(&alice, Method::ListTasks, json!({})).unwrap() else {
        panic!("tasks");
    };
    assert_eq!(listed.tasks.len(), 1);
    let Reply::Tasks(listed) = call(&bob, Method::ListTasks, json!({})).unwrap() else {
        panic!("tasks");
    };
    assert!(listed.tasks.is_empty());
    let error = call(&bob, Method::GetTask, json!({ "id": task_id })).unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::TaskNotFound));
    let error = call(&bob, Method::CancelTask, json!({ "id": task_id })).unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::TaskNotFound));
    let Reply::Task(mine) = call(&alice, Method::GetTask, json!({ "id": task_id })).unwrap() else {
        panic!("a task");
    };
    assert_eq!(mine.status.state, TaskState::Completed);
}

fn limit_of(error: &A2aError) -> Option<&str> {
    error.data[0]["metadata"]["limit"].as_str()
}

#[test]
fn a_task_that_ends_on_its_own_while_the_hook_stops_it_is_canceled_not_failed() {
    let cell: Arc<OnceLock<TaskStore>> = Arc::new(OnceLock::new());
    let store = TaskStore::builder()
        .cancel_hook({
            let cell = Arc::clone(&cell);
            move |task: &Task| {
                // The hook kills the work; the waiter on the work reports it as failed.
                let late = cell
                    .get()
                    .unwrap()
                    .update_status(&task.id, TaskState::Failed, None);
                assert_eq!(late.unwrap().status.state, TaskState::Canceled);
                Ok(())
            }
        })
        .build();
    cell.set(store.clone()).unwrap();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    store
        .update_status(&task.id, TaskState::Working, None)
        .unwrap();
    let mut events = store
        .subscribe(
            &me.caller(),
            &SubscribeToTaskRequest {
                id: task.id.clone(),
                ..SubscribeToTaskRequest::default()
            },
        )
        .unwrap();
    next(&mut events);
    let canceled = store
        .cancel(
            &me.caller(),
            &CancelTaskRequest {
                id: task.id.clone(),
                ..CancelTaskRequest::default()
            },
        )
        .unwrap();
    assert_eq!(canceled.status.state, TaskState::Canceled);
    let states: Vec<TaskState> = events.map(|item| state_of(&item.unwrap())).collect();
    assert_eq!(states, [TaskState::Canceled]);
    assert_eq!(
        store.snapshot(&task.id).unwrap().status.state,
        TaskState::Canceled
    );
}

#[test]
fn two_cancels_at_once_run_the_hook_once() {
    let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let released = Mutex::new(released);
    let runs = Arc::new(AtomicUsize::new(0));
    let store = TaskStore::builder()
        .cancel_hook({
            let runs = Arc::clone(&runs);
            let entered_tx = Mutex::new(entered_tx);
            move |_: &Task| {
                runs.fetch_add(1, Ordering::SeqCst);
                entered_tx.lock().unwrap().send(()).unwrap();
                released.lock().unwrap().recv().unwrap();
                Ok(())
            }
        })
        .build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    let cancel = CancelTaskRequest {
        id: task.id.clone(),
        ..CancelTaskRequest::default()
    };
    std::thread::scope(|scope| {
        let first = scope.spawn(|| store.cancel(&me.caller(), &cancel));
        entered.recv().unwrap();
        // The second is answered at once with the task as it is, and runs nothing.
        let second = store.cancel(&me.caller(), &cancel).unwrap();
        assert_ne!(second.status.state, TaskState::Canceled);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        let first = first.join().unwrap().unwrap();
        assert_eq!(first.status.state, TaskState::Canceled);
    });
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[test]
fn a_hook_that_refuses_leaves_the_task_open_to_another_cancel() {
    let store = TaskStore::builder()
        .cancel_hook(|_: &Task| Err(A2aError::standard(ErrorKind::TaskNotCancelable)))
        .build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("hi")).unwrap();
    let cancel = CancelTaskRequest {
        id: task.id.clone(),
        ..CancelTaskRequest::default()
    };
    for _ in 0..2 {
        let error = store.cancel(&me.caller(), &cancel).unwrap_err();
        assert_eq!(kind(&error), Some(ErrorKind::TaskNotCancelable));
    }
    // It ends on its own as usual.
    store
        .update_status(&task.id, TaskState::Completed, None)
        .unwrap();
}

#[test]
fn history_and_metadata_are_bounded_in_bytes() {
    let store = TaskStore::builder()
        .max_history_bytes(4000)
        .max_metadata_bytes(100)
        .build();
    let me = Who::new("a");
    let task = store.create(&me.caller(), &request("m0")).unwrap();
    for n in 1..=12 {
        let text = format!("{n:02}{}", "x".repeat(500));
        store
            .append(&me.caller(), &follow_up(&task, &text))
            .unwrap();
    }
    let history = store.snapshot(&task.id).unwrap().history.unwrap();
    let bytes: usize = history
        .iter()
        .map(|message| serde_json::to_vec(message).unwrap().len())
        .sum();
    assert!(bytes <= 4000, "{bytes}");
    assert!(history.len() >= 2);
    // The newest are the ones kept.
    assert!(history.last().unwrap().joined_text().starts_with("12"));

    let error = store
        .append(&me.caller(), &follow_up(&task, &"y".repeat(5000)))
        .unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::InvalidRequest));
    assert_eq!(limit_of(&error), Some("max_history_bytes"));
    // A status message is held to the same.
    let error = store
        .update_status(
            &task.id,
            TaskState::Working,
            Some(message(&"z".repeat(5000))),
        )
        .unwrap_err();
    assert_eq!(limit_of(&error), Some("max_history_bytes"));
    assert_eq!(
        store.snapshot(&task.id).unwrap().status.state,
        TaskState::Submitted
    );

    let mut big = request("meta");
    big.metadata = Some(serde_json::from_value(json!({ "blob": "m".repeat(500) })).unwrap());
    let error = store.create(&me.caller(), &big).unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::InvalidRequest));
    assert_eq!(limit_of(&error), Some("max_metadata_bytes"));
}

#[test]
fn one_owner_never_evicts_or_locks_out_anothers_tasks() {
    let store = TaskStore::builder()
        .max_tasks(3)
        .max_tasks_per_owner(2)
        .build();
    let (alice, bob) = (Who::new("alice"), Who::new("bob"));
    let a1 = store.create(&alice.caller(), &request("a1")).unwrap();
    let a2 = store.create(&alice.caller(), &request("a2")).unwrap();
    store
        .update_status(&a1.id, TaskState::Completed, None)
        .unwrap();
    store
        .update_status(&a2.id, TaskState::Completed, None)
        .unwrap();
    store.create(&bob.caller(), &request("b1")).unwrap();

    // The store is full. Bob has nothing ended of his own, and alice's ended tasks are hers.
    let error = store.create(&bob.caller(), &request("b2")).unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::InvalidRequest));
    assert_eq!(limit_of(&error), Some("max_tasks"));
    assert!(store.snapshot(&a1.id).is_some());
    assert!(store.snapshot(&a2.id).is_some());

    // Alice makes room for herself by dropping her own oldest ended task, and only that one.
    let a3 = store.create(&alice.caller(), &request("a3")).unwrap();
    assert!(store.snapshot(&a1.id).is_none());
    assert!(store.snapshot(&a2.id).is_some());
    assert!(store.snapshot(&a3.id).is_some());

    // Her next one drops her other ended task, and then she is at her cap with nothing ended: she
    // is refused, and bob's task is untouched.
    let a4 = store.create(&alice.caller(), &request("a4")).unwrap();
    assert!(store.snapshot(&a2.id).is_none());
    assert!(store.snapshot(&a3.id).is_some() && store.snapshot(&a4.id).is_some());
    let error = store.create(&alice.caller(), &request("a5")).unwrap_err();
    assert_eq!(limit_of(&error), Some("max_tasks_per_owner"));
    assert_eq!(
        store
            .list(&bob.caller(), &ListTasksRequest::default())
            .unwrap()
            .tasks
            .len(),
        1
    );
}

#[test]
fn an_inspected_task_names_its_owner_and_tenant() {
    let store = TaskStore::builder().build();
    let me = Who::new("local:session-1");
    let mut asking = request("hi");
    asking.tenant = Some("x".to_owned());
    let task = store.create(&me.caller(), &asking).unwrap();
    let record = store.inspect(&task.id).unwrap();
    assert_eq!(record.owner, "local:session-1");
    assert_eq!(record.tenant.as_deref(), Some("x"));
    assert_eq!(record.task, task);
    assert!(store.inspect("nobody").is_none());
}

#[test]
fn an_empty_tenant_and_a_page_size_of_zero_are_unset() {
    let store = TaskStore::builder().default_page_size(3).build();
    let me = Who::new("a");
    let mut empty = request("one");
    empty.tenant = Some(String::new());
    empty.message.context_id = Some(String::new());
    let first = store.create(&me.caller(), &empty).unwrap();
    assert!(first.context_id.as_deref().is_some_and(|c| !c.is_empty()));
    assert_eq!(store.inspect(&first.id).unwrap().tenant, None);
    for n in 0..4 {
        store
            .create(&me.caller(), &request(&format!("t{n}")))
            .unwrap();
    }
    let listed = store
        .list(
            &me.caller(),
            &ListTasksRequest {
                tenant: Some(String::new()),
                page_size: Some(0),
                ..ListTasksRequest::default()
            },
        )
        .unwrap();
    assert_eq!(listed.total_size, 5);
    // Zero is the default size, not one.
    assert_eq!(listed.tasks.len(), 3);
    // An empty tenant on a lookup narrows nothing.
    let got = store
        .get(
            &me.caller(),
            &GetTaskRequest {
                tenant: Some(String::new()),
                ..id(&first.id)
            },
        )
        .unwrap();
    assert_eq!(got.id, first.id);
}

#[test]
fn status_timestamp_after_is_read_as_a_time() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    // Some millisecond other than .000, so that a second without a fraction falls just before it.
    let task = loop {
        let task = store.create(&me.caller(), &request("t")).unwrap();
        if !task.status.timestamp.as_ref().unwrap().ends_with(".000Z") {
            break task;
        }
    };
    let stamp = task.status.timestamp.clone().unwrap();
    let list = |after: &str| {
        store.list(
            &me.caller(),
            &ListTasksRequest {
                status_timestamp_after: Some(after.to_owned()),
                ..ListTasksRequest::default()
            },
        )
    };
    let whole_second = format!("{}Z", &stamp[..19]);
    let mut hits = list(&whole_second).unwrap().tasks.into_iter();
    assert!(
        hits.any(|hit| hit.id == task.id),
        "{whole_second} vs {stamp}"
    );
    assert!(list("2000-01-01T00:00:00Z").unwrap().total_size >= 1);
    assert!(list("2999-01-01T00:00:00Z").unwrap().tasks.is_empty());
    // An offset names the same instant: a time far in the past in another zone is still past.
    assert!(!list("2000-01-01T05:00:00+05:00").unwrap().tasks.is_empty());
    assert!(list("2999-01-01T00:00:00-12:00").unwrap().tasks.is_empty());
    for bad in ["yesterday", "2026-11-12", "2026-11-12 10:00:00", "1"] {
        let error = list(bad).unwrap_err();
        assert_eq!(kind(&error), Some(ErrorKind::InvalidParams), "{bad}");
    }
}

#[test]
fn ids_have_no_counter_and_a_page_token_is_opaque() {
    let store = TaskStore::builder().build();
    let me = Who::new("a");
    for n in 0..3 {
        let task = store
            .create(&me.caller(), &request(&format!("t{n}")))
            .unwrap();
        let (kind_of, digits) = task.id.split_once('-').unwrap();
        assert_eq!(kind_of, "task");
        assert_eq!(digits.len(), 16, "{}", task.id);
        assert!(digits.chars().all(|c| c.is_ascii_hexdigit()));
    }
    let page = store
        .list(
            &me.caller(),
            &ListTasksRequest {
                page_size: Some(1),
                ..ListTasksRequest::default()
            },
        )
        .unwrap();
    let token = page.next_page_token;
    assert!(!token.is_empty());
    assert!(!token.chars().all(|c| c.is_ascii_digit()), "{token}");
    // It works as a token, and a number does not.
    let next = store
        .list(
            &me.caller(),
            &ListTasksRequest {
                page_size: Some(1),
                page_token: Some(token),
                ..ListTasksRequest::default()
            },
        )
        .unwrap();
    assert_eq!(next.tasks.len(), 1);
    let error = store
        .list(
            &me.caller(),
            &ListTasksRequest {
                page_token: Some("2".to_owned()),
                ..ListTasksRequest::default()
            },
        )
        .unwrap_err();
    assert_eq!(kind(&error), Some(ErrorKind::InvalidParams));
}
