//! A small agent for the tests and the example server: it echoes, streams, holds and fails on
//! request, and keeps its tasks in memory.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::Duration;

use coqui_a2a::error::{A2aError, ErrorKind};
use coqui_a2a::model::{
    AgentCapabilities, AgentCard, AgentSkill, Artifact, CancelTaskRequest,
    GetExtendedAgentCardRequest, GetTaskRequest, ListTasksRequest, ListTasksResponse, Message,
    Part, Role, SendMessageRequest, SendMessageResponse, StreamResponse, SubscribeToTaskRequest,
    Task, TaskArtifactUpdateEvent, TaskState, TaskStatus, TaskStatusUpdateEvent,
};
use coqui_a2a::{Agent, Caller, Events, Watch};

/// An in-memory agent. Text "hold" makes a task that stays working until it is canceled,
/// "fail:<Name>" answers that error, "panic" panics, anything else completes at once.
#[derive(Clone, Default)]
pub struct Echo {
    tasks: Arc<Mutex<BTreeMap<String, Task>>>,
    counter: Arc<AtomicUsize>,
    /// Subscriptions whose iterator has been dropped.
    pub ended: Arc<AtomicUsize>,
}

impl Echo {
    fn next_id(&self) -> String {
        format!("task-{}", self.counter.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn put(&self, task: &Task) {
        self.tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(task.id.clone(), task.clone());
    }

    fn get(&self, id: &str) -> Option<Task> {
        self.tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    fn task_for(&self, request: &SendMessageRequest) -> Result<Task, A2aError> {
        let text = request.message.joined_text();
        if let Some(name) = text.strip_prefix("fail:") {
            let kind = ErrorKind::from_name(name)
                .ok_or_else(|| A2aError::standard(ErrorKind::InvalidParams))?;
            return Err(A2aError::standard(kind));
        }
        assert!(text != "panic", "the agent was told to panic");
        let held = text == "hold";
        let mut task = Task {
            id: self.next_id(),
            context_id: Some(
                request
                    .message
                    .context_id
                    .clone()
                    .unwrap_or_else(|| "context-1".to_owned()),
            ),
            status: TaskStatus::now(if held {
                TaskState::Working
            } else {
                TaskState::Completed
            }),
            history: Some(vec![request.message.clone()]),
            ..Task::default()
        };
        if !held {
            task.artifacts = Some(vec![Artifact {
                artifact_id: "echo".to_owned(),
                parts: vec![Part::text(format!("echo: {text}"))],
                ..Artifact::default()
            }]);
        }
        self.put(&task);
        Ok(task)
    }

    /// The task as it is, then its end, polling the store while it is quiet.
    fn follow(&self, task: Task, watch: Watch) -> Events {
        let (store, id) = (self.clone(), task.id.clone());
        let mut first = Some(task);
        let mut finished = false;
        let inner = std::iter::from_fn(move || {
            if let Some(task) = first.take() {
                return Some(Ok(StreamResponse::Task(task)));
            }
            while !finished {
                if watch.is_closed() {
                    return None;
                }
                let current = store.get(&id)?;
                if current.status.state.is_terminal() {
                    finished = true;
                    return Some(Ok(StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                        task_id: id.clone(),
                        context_id: current.context_id.unwrap_or_default(),
                        status: current.status,
                        ..TaskStatusUpdateEvent::default()
                    })));
                }
                thread::sleep(Duration::from_millis(20));
            }
            None
        });
        Box::new(Counted {
            inner,
            ended: Arc::clone(&self.ended),
        })
    }
}

/// Counts a subscription's end when its iterator is dropped.
struct Counted<I> {
    inner: I,
    ended: Arc<AtomicUsize>,
}

impl<I> Drop for Counted<I> {
    fn drop(&mut self) {
        self.ended.fetch_add(1, Ordering::SeqCst);
    }
}

impl<I: Iterator> Iterator for Counted<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<I::Item> {
        self.inner.next()
    }
}

impl Agent for Echo {
    fn agent_card(&self, _caller: &Caller<'_>) -> AgentCard {
        AgentCard {
            name: "Echo".to_owned(),
            description: "Repeats what it is told".to_owned(),
            version: "1.0.0".to_owned(),
            capabilities: AgentCapabilities {
                streaming: Some(true),
                extended_agent_card: Some(true),
                ..AgentCapabilities::default()
            },
            default_input_modes: vec!["text/plain".to_owned()],
            default_output_modes: vec!["text/plain".to_owned()],
            skills: vec![AgentSkill {
                id: "echo".to_owned(),
                name: "Echo".to_owned(),
                description: "Says it back".to_owned(),
                tags: vec!["echo".to_owned()],
                ..AgentSkill::default()
            }],
            ..AgentCard::default()
        }
    }

    fn send_message(
        &self,
        _caller: &Caller<'_>,
        request: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2aError> {
        Ok(SendMessageResponse::Task(self.task_for(&request)?))
    }

    fn send_streaming_message(
        &self,
        caller: &Caller<'_>,
        request: SendMessageRequest,
    ) -> Result<Events, A2aError> {
        let task = self.task_for(&request)?;
        if task.status.state == TaskState::Working {
            return Ok(self.follow(task, caller.watch()));
        }
        let (id, context) = (task.id.clone(), task.context_id.clone().unwrap_or_default());
        let chunk = |text: &str, first: bool, last: bool| {
            StreamResponse::ArtifactUpdate(TaskArtifactUpdateEvent {
                task_id: id.clone(),
                context_id: context.clone(),
                artifact: Artifact {
                    artifact_id: "echo".to_owned(),
                    parts: vec![Part::text(text)],
                    ..Artifact::default()
                },
                append: Some(!first),
                last_chunk: Some(last),
                ..TaskArtifactUpdateEvent::default()
            })
        };
        let mut working = task.clone();
        working.status = TaskStatus::now(TaskState::Working);
        working.artifacts = None;
        let done = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
            task_id: id.clone(),
            context_id: context.clone(),
            status: task.status.clone(),
            ..TaskStatusUpdateEvent::default()
        });
        let text = request.message.joined_text();
        let items = vec![
            StreamResponse::Task(working),
            chunk(&format!("echo: {text}"), true, false),
            chunk(" (and again)", false, true),
            done,
        ];
        Ok(Box::new(items.into_iter().map(Ok)))
    }

    fn get_task(&self, _caller: &Caller<'_>, request: GetTaskRequest) -> Result<Task, A2aError> {
        self.get(&request.id)
            .ok_or_else(|| A2aError::task_not_found(&request.id))
    }

    fn list_tasks(
        &self,
        _caller: &Caller<'_>,
        request: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2aError> {
        let all: Vec<Task> = self
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter(|task| {
                request
                    .status
                    .as_ref()
                    .is_none_or(|state| *state == task.status.state)
            })
            .cloned()
            .collect();
        let total = u32::try_from(all.len()).unwrap_or(u32::MAX);
        let size = request.page_size.unwrap_or(50).clamp(1, 100);
        Ok(ListTasksResponse {
            tasks: all.into_iter().take(size as usize).collect(),
            next_page_token: String::new(),
            page_size: size,
            total_size: total,
            ..ListTasksResponse::default()
        })
    }

    fn cancel_task(
        &self,
        _caller: &Caller<'_>,
        request: CancelTaskRequest,
    ) -> Result<Task, A2aError> {
        let mut task = self
            .get(&request.id)
            .ok_or_else(|| A2aError::task_not_found(&request.id))?;
        if task.status.state.is_terminal() {
            return Err(A2aError::standard(ErrorKind::TaskNotCancelable));
        }
        task.status = TaskStatus::now(TaskState::Canceled);
        self.put(&task);
        Ok(task)
    }

    fn subscribe_to_task(
        &self,
        caller: &Caller<'_>,
        request: SubscribeToTaskRequest,
    ) -> Result<Events, A2aError> {
        let task = self
            .get(&request.id)
            .ok_or_else(|| A2aError::task_not_found(&request.id))?;
        if task.status.state.is_terminal() {
            return Err(A2aError::new(
                ErrorKind::UnsupportedOperation,
                "the task has ended",
            ));
        }
        Ok(self.follow(task, caller.watch()))
    }

    fn get_extended_agent_card(
        &self,
        caller: &Caller<'_>,
        _request: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2aError> {
        let mut card = self.agent_card(caller);
        card.description = "Repeats what it is told, and shows its extended card".to_owned();
        Ok(card)
    }
}

/// A text message from a user.
pub fn user(text: &str) -> SendMessageRequest {
    SendMessageRequest::new(Message::text(Role::User, format!("message-{text}"), text))
}
