//! The tasks of an agent, kept in memory: what a host that runs work for callers needs to answer
//! `GetTask`, `ListTasks`, `CancelTask` and `SubscribeToTask` the way A2A says.
//!
//! A store holds, for each task, its owner (the [`Caller::peer`] that created it), its status, a
//! bounded history of messages, its artifacts and the subscribers following it. The caller-facing
//! methods take the [`Caller`] and answer as the binding requires: another caller's task is
//! `TaskNotFoundError`, a task that has ended takes no message, no subscription and no
//! cancellation. The methods of the agent that does the work take a task id and no caller, since
//! the work is the host's: [`TaskStore::update_status`] and [`TaskStore::add_artifact`].
//!
//! Every limit is a setting of [`TaskStoreBuilder`]. The store does I/O of no kind and starts no
//! thread; a subscriber waits on its own thread, the one that reads its [`Events`].

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use crate::agent::{Caller, Events, Watch};
use crate::error::{A2aError, ErrorKind};
use crate::model::{
    Artifact, CancelTaskRequest, GetTaskRequest, ListTasksRequest, ListTasksResponse, Message,
    Object, SendMessageRequest, StreamResponse, SubscribeToTaskRequest, Task,
    TaskArtifactUpdateEvent, TaskState, TaskStatus, TaskStatusUpdateEvent,
};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// A task and who it belongs to.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TaskRecord {
    /// The [`Caller::peer`] that made it.
    pub owner: String,
    /// The tenant it was made under.
    pub tenant: Option<String>,
    /// The task.
    pub task: Task,
}

/// What a store holds a host to. Each is a setting of [`TaskStoreBuilder`]; there is no floor, so
/// a limit of zero refuses everything it counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct TaskLimits {
    /// Tasks in the store. At the limit a new task makes room by dropping the task that ended
    /// longest ago, or is refused when none has ended. Default 1024.
    pub max_tasks: usize,
    /// Tasks one owner has in the store, treated like [`TaskLimits::max_tasks`] for that owner, so
    /// one caller cannot fill the store. Default 256.
    pub max_tasks_per_owner: usize,
    /// Messages kept in a task's history; the oldest go first. Default 64.
    pub max_history: usize,
    /// Artifacts of one task. Replacing one is not adding one. Default 16.
    pub max_artifacts: usize,
    /// Bytes of one task's artifacts, as JSON. An addition that would pass it is refused and
    /// changes nothing. Default 1 MiB.
    pub max_artifact_bytes: usize,
    /// Subscribers following one task at once. Default 8.
    pub max_subscribers: usize,
    /// Bytes of a task's history, as JSON; the oldest messages go first. A single message past it
    /// is refused. Default 1 MiB.
    pub max_history_bytes: usize,
    /// Bytes of a task's `metadata`, as JSON; a task made with more is refused. Default 64 KiB.
    pub max_metadata_bytes: usize,
    /// Updates waiting for one subscriber. One that falls this far behind is cut off, and told so,
    /// so that a slow reader never holds up the work. Default 256.
    pub subscriber_queue: usize,
    /// Tasks in a page of `ListTasks` when the request names no size. Default 50.
    pub default_page_size: usize,
    /// The most tasks in a page. Default 100.
    pub max_page_size: usize,
}

impl Default for TaskLimits {
    fn default() -> Self {
        Self {
            max_tasks: 1024,
            max_tasks_per_owner: 256,
            max_history: 64,
            max_history_bytes: 1 << 20,
            max_metadata_bytes: 64 << 10,
            max_artifacts: 16,
            max_artifact_bytes: 1 << 20,
            max_subscribers: 8,
            subscriber_queue: 256,
            default_page_size: 50,
            max_page_size: 100,
        }
    }
}

/// What runs when a caller cancels a task: it stops the work. Called with no lock held.
type CancelHook = Arc<dyn Fn(&Task) -> Result<(), A2aError> + Send + Sync>;

/// Settings for a [`TaskStore`].
#[must_use]
pub struct TaskStoreBuilder {
    limits: TaskLimits,
    cancel: Option<CancelHook>,
}

impl core::fmt::Debug for TaskStoreBuilder {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TaskStoreBuilder")
            .field("limits", &self.limits)
            .field("cancel_hook", &self.cancel.is_some())
            .finish()
    }
}

macro_rules! limit_setters {
    ($($(#[$meta:meta])* $name:ident,)*) => {
        $($(#[$meta])*
        pub fn $name(mut self, limit: usize) -> Self {
            self.limits.$name = limit;
            self
        })*
    };
}

impl TaskStoreBuilder {
    /// Replaces every limit at once; the setters below change one.
    pub fn limits(mut self, limits: TaskLimits) -> Self {
        self.limits = limits;
        self
    }

    limit_setters! {
        /// See [`TaskLimits::max_tasks`].
        max_tasks,
        /// See [`TaskLimits::max_tasks_per_owner`].
        max_tasks_per_owner,
        /// See [`TaskLimits::max_history`].
        max_history,
        /// See [`TaskLimits::max_history_bytes`].
        max_history_bytes,
        /// See [`TaskLimits::max_metadata_bytes`].
        max_metadata_bytes,
        /// See [`TaskLimits::max_artifacts`].
        max_artifacts,
        /// See [`TaskLimits::max_artifact_bytes`].
        max_artifact_bytes,
        /// See [`TaskLimits::max_subscribers`].
        max_subscribers,
        /// See [`TaskLimits::subscriber_queue`].
        subscriber_queue,
        /// See [`TaskLimits::default_page_size`].
        default_page_size,
        /// See [`TaskLimits::max_page_size`].
        max_page_size,
    }

    /// What stops the work when a caller cancels a task. It runs with no lock held; when it
    /// returns `Ok` the store puts the task in `TASK_STATE_CANCELED`, and when it returns an
    /// error that error is the caller's answer and the task is as it was. Without one, cancelling
    /// only ends the task.
    pub fn cancel_hook(
        mut self,
        hook: impl Fn(&Task) -> Result<(), A2aError> + Send + Sync + 'static,
    ) -> Self {
        self.cancel = Some(Arc::new(hook));
        self
    }

    /// The store.
    #[must_use]
    pub fn build(self) -> TaskStore {
        TaskStore {
            shared: Arc::new(Shared {
                inner: Mutex::new(Inner {
                    salt: RandomState::new().build_hasher().finish(),
                    ..Inner::default()
                }),
                limits: self.limits,
                cancel: self.cancel,
            }),
        }
    }
}

/// The tasks of an agent. Clones share one store.
#[derive(Clone)]
pub struct TaskStore {
    shared: Arc<Shared>,
}

struct Shared {
    inner: Mutex<Inner>,
    limits: TaskLimits,
    cancel: Option<CancelHook>,
}

#[derive(Default)]
struct Inner {
    tasks: HashMap<String, Entry>,
    /// Bumped by every change, so that it orders tasks by when they last changed.
    clock: u64,
    /// Bumped by every id made, which with a random part keeps ids apart.
    made: u64,
    /// Hides the clock in a page token.
    salt: u64,
}

struct Entry {
    owner: String,
    tenant: Option<String>,
    task: Task,
    /// The `clock` when the task last changed.
    seq: u64,
    artifact_bytes: usize,
    /// A cancellation is under way: the hook is running.
    cancelling: bool,
    subscribers: Vec<Subscriber>,
}

struct Subscriber {
    queue: SyncSender<StreamResponse>,
    lagged: Arc<AtomicBool>,
    /// Gone once the subscription is dropped, which frees its place.
    alive: Weak<()>,
}

impl core::fmt::Debug for TaskStore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TaskStore")
            .field("limits", &self.shared.limits)
            .finish_non_exhaustive()
    }
}

impl TaskStore {
    /// A store with the default limits and no cancel hook, or settings of your own.
    pub fn builder() -> TaskStoreBuilder {
        TaskStoreBuilder {
            limits: TaskLimits::default(),
            cancel: None,
        }
    }

    /// The limits this store was built with.
    #[must_use]
    pub fn limits(&self) -> TaskLimits {
        self.shared.limits
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.shared
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes a task for the caller from a message that names none: `TASK_STATE_SUBMITTED`, the
    /// message as its history, the request's `tenant` and `metadata`, and the message's
    /// `contextId` or a new one.
    ///
    /// # Errors
    ///
    /// `InvalidParamsError` when the message names a task, `InvalidRequestError` when a limit
    /// leaves no room.
    pub fn create(
        &self,
        caller: &Caller<'_>,
        request: &SendMessageRequest,
    ) -> Result<Task, A2aError> {
        if request.message.task_id.is_some() {
            return Err(A2aError::new(
                ErrorKind::InvalidParams,
                "a new task takes a message that names no task",
            ));
        }
        let limits = &self.shared.limits;
        check_message(&request.message, limits)?;
        check_metadata(request.metadata.as_ref(), limits)?;
        let mut inner = self.lock();
        inner.make_room(caller.peer, limits)?;
        let id = inner.new_id("task");
        let context = match set(request.message.context_id.as_deref()) {
            Some(context) => context.to_owned(),
            None => inner.new_id("context"),
        };
        let mut message = request.message.clone();
        message.task_id = Some(id.clone());
        message.context_id = Some(context.clone());
        let task = Task {
            id: id.clone(),
            context_id: Some(context),
            status: TaskStatus::now(TaskState::Submitted),
            artifacts: None,
            history: Some(vec![message]),
            metadata: request.metadata.clone(),
            ..Task::default()
        };
        let seq = inner.touch();
        inner.tasks.insert(
            id,
            Entry {
                owner: caller.peer.to_owned(),
                tenant: set(request.tenant.as_deref()).map(str::to_owned),
                task: task.clone(),
                seq,
                artifact_bytes: 0,
                cancelling: false,
                subscribers: Vec::new(),
            },
        );
        Ok(task)
    }

    /// Adds a message that names a task to that task's history: the caller's answer to a task
    /// that asked for one, or a further instruction. It changes no state; the host decides
    /// whether the task is working again.
    ///
    /// # Errors
    ///
    /// `InvalidParamsError` when the message names no task or another context,
    /// `TaskNotFoundError` for a task that is not the caller's, `UnsupportedOperationError` for
    /// one that has ended.
    pub fn append(
        &self,
        caller: &Caller<'_>,
        request: &SendMessageRequest,
    ) -> Result<Task, A2aError> {
        let Some(id) = &request.message.task_id else {
            return Err(A2aError::new(
                ErrorKind::InvalidParams,
                "the message names no task",
            ));
        };
        let limits = &self.shared.limits;
        check_message(&request.message, limits)?;
        let mut inner = self.lock();
        let seq = inner.touch();
        let entry = inner.visible(caller.peer, request.tenant.as_deref(), id)?;
        ended_means(entry, ErrorKind::UnsupportedOperation)?;
        if set(request.message.context_id.as_deref())
            .is_some_and(|context| Some(context) != entry.task.context_id.as_deref())
        {
            return Err(A2aError::new(
                ErrorKind::InvalidParams,
                "the message is of another context than its task",
            ));
        }
        let mut message = request.message.clone();
        message.context_id.clone_from(&entry.task.context_id);
        entry.remember(message, limits);
        entry.seq = seq;
        Ok(entry.task.clone())
    }

    /// `GetTask`: the caller's task, with the last `historyLength` messages when the request asks
    /// for that many (none for 0), else all that are kept.
    ///
    /// # Errors
    ///
    /// `TaskNotFoundError` for a task that is not the caller's or not of the request's tenant.
    pub fn get(&self, caller: &Caller<'_>, request: &GetTaskRequest) -> Result<Task, A2aError> {
        let mut inner = self.lock();
        let entry = inner.visible(caller.peer, request.tenant.as_deref(), &request.id)?;
        Ok(trimmed(&entry.task, request.history_length, true))
    }

    /// `ListTasks`: only the caller's tasks, most recently changed first, filtered by the
    /// request's context, state, tenant and `statusTimestampAfter`, one page at a time.
    /// Artifacts are left out unless `includeArtifacts` is true.
    ///
    /// # Errors
    ///
    /// `InvalidParamsError` for a page token that is not one this store made.
    pub fn list(
        &self,
        caller: &Caller<'_>,
        request: &ListTasksRequest,
    ) -> Result<ListTasksResponse, A2aError> {
        let limits = &self.shared.limits;
        let inner = self.lock();
        let before = match set(request.page_token.as_deref()) {
            None => u64::MAX,
            Some(token) => token
                .strip_prefix('p')
                .and_then(|hex| u64::from_str_radix(hex, 16).ok())
                .map(|hidden| hidden ^ inner.salt)
                .ok_or_else(|| {
                    A2aError::new(
                        ErrorKind::InvalidParams,
                        "the page token is not one of ours",
                    )
                })?,
        };
        // Zero is the same as none, as for any member of a request.
        let size = match request.page_size {
            None | Some(0) => limits.default_page_size,
            Some(size) => size as usize,
        }
        .clamp(1, limits.max_page_size.max(1));
        let after = match set(request.status_timestamp_after.as_deref()) {
            None => None,
            Some(text) => Some(parse_time(text).ok_or_else(|| {
                A2aError::new(
                    ErrorKind::InvalidParams,
                    "statusTimestampAfter is not an RFC 3339 time",
                )
            })?),
        };
        let (tenant, context) = (
            set(request.tenant.as_deref()),
            set(request.context_id.as_deref()),
        );
        let mut matching: Vec<&Entry> = inner
            .tasks
            .values()
            .filter(|entry| entry.owner == caller.peer)
            .filter(|entry| tenant.is_none_or(|tenant| entry.tenant.as_deref() == Some(tenant)))
            .filter(|entry| {
                context.is_none_or(|context| entry.task.context_id.as_deref() == Some(context))
            })
            .filter(|entry| match &request.status {
                Some(state) => &entry.task.status.state == state,
                None => true,
            })
            .filter(|entry| {
                after.is_none_or(|after| {
                    entry
                        .task
                        .status
                        .timestamp
                        .as_deref()
                        .and_then(parse_time)
                        .is_some_and(|stamp| stamp > after)
                })
            })
            .collect();
        matching.sort_by_key(|entry| core::cmp::Reverse(entry.seq));
        let total = matching.len();
        let page: Vec<&Entry> = matching
            .into_iter()
            .filter(|entry| entry.seq < before)
            .take(size + 1)
            .collect();
        let more = page.len() > size;
        let page = &page[..page.len().min(size)];
        let include = request.include_artifacts == Some(true);
        let tasks: Vec<Task> = page
            .iter()
            .map(|entry| trimmed(&entry.task, request.history_length, include))
            .collect();
        Ok(ListTasksResponse {
            next_page_token: match (more, page.last()) {
                (true, Some(last)) => format!("p{:x}", last.seq ^ inner.salt),
                _ => String::new(),
            },
            page_size: u32::try_from(tasks.len()).unwrap_or(u32::MAX),
            total_size: u32::try_from(total).unwrap_or(u32::MAX),
            tasks,
            ..ListTasksResponse::default()
        })
    }

    /// `CancelTask`: runs the cancel hook, if any, then puts the task in `TASK_STATE_CANCELED`.
    ///
    /// # Errors
    ///
    /// `TaskNotFoundError` for a task that is not the caller's, `TaskNotCancelableError` for one
    /// that has ended, or the hook's own error.
    pub fn cancel(
        &self,
        caller: &Caller<'_>,
        request: &CancelTaskRequest,
    ) -> Result<Task, A2aError> {
        let limits = &self.shared.limits;
        let task = {
            let mut inner = self.lock();
            let entry = inner.visible(caller.peer, request.tenant.as_deref(), &request.id)?;
            ended_means(entry, ErrorKind::TaskNotCancelable)?;
            if entry.cancelling {
                // Another cancel is stopping the work; it will answer for both.
                return Ok(entry.task.clone());
            }
            entry.cancelling = true;
            entry.task.clone()
        };
        let stopped = match &self.shared.cancel {
            Some(hook) => catch_unwind(AssertUnwindSafe(|| hook(&task))).unwrap_or_else(|_| {
                Err(A2aError::new(ErrorKind::Internal, "the cancel hook failed"))
            }),
            None => Ok(()),
        };
        let mut inner = self.lock();
        let Some(entry) = inner.tasks.get_mut(&request.id) else {
            return Err(A2aError::task_not_found(&request.id));
        };
        entry.cancelling = false;
        if entry.task.status.state.is_terminal() {
            // The work ended while the hook ran, and what it reported became the cancellation.
            return Ok(entry.task.clone());
        }
        stopped?;
        inner.set_status(&request.id, TaskState::Canceled, None, limits)
    }

    /// `SubscribeToTask`: the task as it is now, then each update in the order it was made, with
    /// nothing between the two missed. An agent that streams a task it has just made subscribes
    /// before it starts the work, or a quick task has ended first and is refused. The iterator
    /// ends after the update that ends the task or when the caller's [`Watch`] closes. One that cannot be served because the subscriber fell
    /// behind, or the store went away, ends with an error, never as a complete stream.
    ///
    /// # Errors
    ///
    /// `TaskNotFoundError` for a task that is not the caller's, `UnsupportedOperationError` for
    /// one that has ended, `InvalidRequestError` when the task has as many subscribers as
    /// [`TaskLimits::max_subscribers`].
    pub fn subscribe(
        &self,
        caller: &Caller<'_>,
        request: &SubscribeToTaskRequest,
    ) -> Result<Events, A2aError> {
        let limits = &self.shared.limits;
        let mut inner = self.lock();
        let entry = inner.visible(caller.peer, request.tenant.as_deref(), &request.id)?;
        ended_means(entry, ErrorKind::UnsupportedOperation)?;
        entry
            .subscribers
            .retain(|subscriber| subscriber.alive.strong_count() > 0);
        if entry.subscribers.len() >= limits.max_subscribers {
            return Err(limit_reached(
                ErrorKind::InvalidRequest,
                "max_subscribers",
                "subscribers of this task",
            ));
        }
        let (queue, updates) = std::sync::mpsc::sync_channel(limits.subscriber_queue.max(1));
        let lagged = Arc::new(AtomicBool::new(false));
        let alive = Arc::new(());
        entry.subscribers.push(Subscriber {
            queue,
            lagged: Arc::clone(&lagged),
            alive: Arc::downgrade(&alive),
        });
        // Taken under the lock that registered the subscriber, so no update is missed or repeated.
        let first = StreamResponse::Task(entry.task.clone());
        drop(inner);
        Ok(Box::new(Subscription {
            first: Some(first),
            updates,
            watch: caller.watch(),
            lagged,
            done: false,
            _alive: alive,
        }))
    }

    /// Moves a task to `state`, with a message for the person or the caller that is kept in the
    /// history, and tells the subscribers. This is for the code that does the task's work, which
    /// is not a caller and owns nothing.
    ///
    /// A task may wait (`INPUT_REQUIRED`, `AUTH_REQUIRED`) and work again as often as it needs.
    /// It does not return to `SUBMITTED`, and once it is in a terminal state nothing changes it.
    ///
    /// # Errors
    ///
    /// `TaskNotFoundError`, `InvalidParamsError` for `SUBMITTED` after the first state, an
    /// unspecified state or one this version does not know, `UnsupportedOperationError` for a task
    /// that has ended.
    pub fn update_status(
        &self,
        id: &str,
        state: TaskState,
        message: Option<Message>,
    ) -> Result<Task, A2aError> {
        let limits = &self.shared.limits;
        self.lock().set_status(id, state, message, limits)
    }

    /// Adds an artifact to a task and tells the subscribers. With `append` the parts join the
    /// artifact of that id if there is one; otherwise an artifact of that id is replaced.
    /// `last_chunk` says no more chunks of it follow. This is for the code that does the work.
    ///
    /// # Errors
    ///
    /// `TaskNotFoundError`, `UnsupportedOperationError` for a task that has ended,
    /// `InvalidAgentResponseError` when [`TaskLimits::max_artifacts`] or
    /// [`TaskLimits::max_artifact_bytes`] would be passed; the task is then as it was.
    pub fn add_artifact(
        &self,
        id: &str,
        artifact: Artifact,
        append: bool,
        last_chunk: bool,
    ) -> Result<(), A2aError> {
        let limits = &self.shared.limits;
        let mut inner = self.lock();
        let seq = inner.touch();
        let entry = inner
            .tasks
            .get_mut(id)
            .ok_or_else(|| A2aError::task_not_found(id))?;
        ended_means(entry, ErrorKind::UnsupportedOperation)?;
        let held: &[Artifact] = entry.task.artifacts.as_deref().unwrap_or_default();
        let at = held
            .iter()
            .position(|held| held.artifact_id == artifact.artifact_id);
        let (merged, replaced_bytes) = match (at, append) {
            (Some(at), true) => {
                let mut merged = held[at].clone();
                merged.parts.extend(artifact.parts.iter().cloned());
                (merged, json_size(&held[at]))
            }
            (Some(at), false) => (artifact.clone(), json_size(&held[at])),
            (None, _) => {
                if held.len() >= limits.max_artifacts {
                    return Err(limit_reached(
                        ErrorKind::InvalidAgentResponse,
                        "max_artifacts",
                        "artifacts",
                    ));
                }
                (artifact.clone(), 0)
            }
        };
        let bytes = entry.artifact_bytes - replaced_bytes + json_size(&merged);
        if bytes > limits.max_artifact_bytes {
            return Err(limit_reached(
                ErrorKind::InvalidAgentResponse,
                "max_artifact_bytes",
                "bytes of artifacts",
            ));
        }
        let artifacts = entry.task.artifacts.get_or_insert_with(Vec::new);
        match at {
            Some(at) => artifacts[at] = merged,
            None => artifacts.push(merged),
        }
        entry.artifact_bytes = bytes;
        entry.seq = seq;
        let event = StreamResponse::ArtifactUpdate(TaskArtifactUpdateEvent {
            task_id: entry.task.id.clone(),
            context_id: entry.task.context_id.clone().unwrap_or_default(),
            artifact,
            append: Some(append),
            last_chunk: Some(last_chunk),
            ..TaskArtifactUpdateEvent::default()
        });
        entry.publish(&event);
        Ok(())
    }

    /// A task with its owner and tenant, whoever owns it: for the host's own code, never for a
    /// caller.
    #[must_use]
    pub fn inspect(&self, id: &str) -> Option<TaskRecord> {
        self.lock().tasks.get(id).map(|entry| TaskRecord {
            owner: entry.owner.clone(),
            tenant: entry.tenant.clone(),
            task: entry.task.clone(),
        })
    }

    /// A task as it is, whoever owns it: for the host's own code, never for a caller.
    #[must_use]
    pub fn snapshot(&self, id: &str) -> Option<Task> {
        self.lock().tasks.get(id).map(|entry| entry.task.clone())
    }
}

impl Inner {
    fn touch(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// An id nobody can predict from another and no two of which are equal.
    fn new_id(&mut self, kind: &str) -> String {
        loop {
            self.made += 1;
            let mut hasher = RandomState::new().build_hasher();
            hasher.write_u64(self.made);
            let id = format!("{kind}-{:016x}", hasher.finish());
            if !self.tasks.contains_key(&id) {
                return id;
            }
        }
    }

    /// The caller's task, which for anyone else is no task at all. An empty tenant is none.
    fn visible(
        &mut self,
        owner: &str,
        tenant: Option<&str>,
        id: &str,
    ) -> Result<&mut Entry, A2aError> {
        let tenant = set(tenant);
        self.tasks
            .get_mut(id)
            .filter(|entry| entry.owner == owner)
            .filter(|entry| tenant.is_none_or(|tenant| entry.tenant.as_deref() == Some(tenant)))
            .ok_or_else(|| A2aError::task_not_found(id))
    }

    /// Makes room for a task of `owner`, by dropping that owner's own ended task that changed
    /// longest ago. Another owner's task is never dropped, so no owner takes another's room.
    fn make_room(&mut self, owner: &str, limits: &TaskLimits) -> Result<(), A2aError> {
        let owned = self
            .tasks
            .values()
            .filter(|entry| entry.owner == owner)
            .count();
        if owned >= limits.max_tasks_per_owner {
            self.drop_ended(owner).ok_or_else(|| {
                limit_reached(
                    ErrorKind::InvalidRequest,
                    "max_tasks_per_owner",
                    "tasks for this caller",
                )
            })?;
        }
        if self.tasks.len() >= limits.max_tasks {
            self.drop_ended(owner)
                .ok_or_else(|| limit_reached(ErrorKind::InvalidRequest, "max_tasks", "tasks"))?;
        }
        Ok(())
    }

    /// Drops the ended task of `owner` that changed longest ago.
    fn drop_ended(&mut self, owner: &str) -> Option<()> {
        let oldest = self
            .tasks
            .iter()
            .filter(|(_, entry)| entry.task.status.state.is_terminal())
            .filter(|(_, entry)| entry.owner == owner)
            .min_by_key(|(_, entry)| entry.seq)
            .map(|(id, _)| id.clone())?;
        self.tasks.remove(&oldest);
        Some(())
    }

    /// Moves a task to `state`. While a cancellation is under way a terminal state lands as
    /// `CANCELED`, since what the work reports then is its being stopped.
    fn set_status(
        &mut self,
        id: &str,
        state: TaskState,
        message: Option<Message>,
        limits: &TaskLimits,
    ) -> Result<Task, A2aError> {
        let seq = self.touch();
        let entry = self
            .tasks
            .get_mut(id)
            .ok_or_else(|| A2aError::task_not_found(id))?;
        ended_means(entry, ErrorKind::UnsupportedOperation)?;
        let known = !matches!(state, TaskState::Unspecified | TaskState::Other(_));
        let back = state == TaskState::Submitted && entry.task.status.state != TaskState::Submitted;
        if !known || back {
            return Err(A2aError::new(
                ErrorKind::InvalidParams,
                format!("a task does not move to {state}"),
            ));
        }
        let state = if entry.cancelling && state.is_terminal() {
            TaskState::Canceled
        } else {
            state
        };
        let message = message.map(|mut message| {
            message.task_id = Some(entry.task.id.clone());
            message.context_id.clone_from(&entry.task.context_id);
            message
        });
        if let Some(message) = &message {
            check_message(message, limits)?;
        }
        let mut status = TaskStatus::now(state);
        if let Some(message) = message {
            entry.remember(message.clone(), limits);
            status.message = Some(message);
        }
        entry.task.status = status.clone();
        entry.seq = seq;
        let ends = entry.task.status.state.is_terminal();
        let event = StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
            task_id: entry.task.id.clone(),
            context_id: entry.task.context_id.clone().unwrap_or_default(),
            status,
            ..TaskStatusUpdateEvent::default()
        });
        entry.publish(&event);
        if ends {
            // Each has been told, and a subscriber ends after the update that ends the task.
            entry.cancelling = false;
            entry.subscribers.clear();
        }
        Ok(entry.task.clone())
    }
}

impl Entry {
    /// Keeps `message`, dropping the oldest while there are too many or they are too large. The
    /// message has passed [`check_message`].
    fn remember(&mut self, message: Message, limits: &TaskLimits) {
        let history = self.task.history.get_or_insert_with(Vec::new);
        history.push(message);
        let mut bytes: usize = history.iter().map(json_len).sum();
        while history.len() > 1
            && (history.len() > limits.max_history || bytes > limits.max_history_bytes)
        {
            bytes -= json_len(&history.remove(0));
        }
        if limits.max_history == 0 {
            history.clear();
        }
    }

    /// Tells the subscribers, in order, without waiting for any: one whose queue is full is cut
    /// off and marked, so that its reader learns it missed something.
    fn publish(&mut self, event: &StreamResponse) {
        self.subscribers.retain(
            |subscriber| match subscriber.queue.try_send(event.clone()) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => {
                    subscriber.lagged.store(true, Ordering::SeqCst);
                    false
                }
                Err(TrySendError::Disconnected(_)) => false,
            },
        );
    }
}

/// A limit that was reached: the caller sees which setting it is, in the error's metadata.
fn limit_reached(kind: ErrorKind, limit: &str, what: &str) -> A2aError {
    A2aError::new(kind, format!("the limit on {what} is reached")).with_metadata("limit", limit)
}

/// A value of a request member, where an empty one is the same as none (proto3 JSON).
fn set(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

/// Refuses a message that alone is past the history's bytes.
fn check_message(message: &Message, limits: &TaskLimits) -> Result<(), A2aError> {
    if json_len(message) > limits.max_history_bytes {
        Err(limit_reached(
            ErrorKind::InvalidRequest,
            "max_history_bytes",
            "bytes of a message",
        ))
    } else {
        Ok(())
    }
}

/// Refuses a task whose metadata is past its bytes.
fn check_metadata(metadata: Option<&Object>, limits: &TaskLimits) -> Result<(), A2aError> {
    if metadata.is_some_and(|metadata| json_len(metadata) > limits.max_metadata_bytes) {
        Err(limit_reached(
            ErrorKind::InvalidRequest,
            "max_metadata_bytes",
            "bytes of metadata",
        ))
    } else {
        Ok(())
    }
}

/// Refuses a task that has ended, as `kind`.
fn ended_means(entry: &Entry, kind: ErrorKind) -> Result<(), A2aError> {
    if entry.task.status.state.is_terminal() {
        Err(A2aError::new(kind, "the task has ended"))
    } else {
        Ok(())
    }
}

/// A task for a reader: the newest `history_length` messages, and its artifacts if wanted.
fn trimmed(task: &Task, history_length: Option<u32>, artifacts: bool) -> Task {
    let mut task = task.clone();
    if let (Some(length), Some(history)) = (history_length, task.history.as_mut()) {
        let keep = length as usize;
        if history.len() > keep {
            history.drain(..history.len() - keep);
        }
    }
    if task.history.as_ref().is_some_and(Vec::is_empty) {
        task.history = None;
    }
    if !artifacts || task.artifacts.as_ref().is_some_and(Vec::is_empty) {
        task.artifacts = None;
    }
    task
}

fn json_len<T: serde::Serialize>(value: &T) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

fn json_size(artifact: &Artifact) -> usize {
    json_len(artifact)
}

fn parse_time(text: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(text, &Rfc3339).ok()
}

/// How often a quiet subscription looks at its watch.
const WATCH_POLL: Duration = Duration::from_millis(50);

/// What a subscriber reads.
struct Subscription {
    first: Option<StreamResponse>,
    updates: Receiver<StreamResponse>,
    watch: Watch,
    lagged: Arc<AtomicBool>,
    done: bool,
    /// Dropped with the subscription, which frees the task's place for another.
    _alive: Arc<()>,
}

impl Iterator for Subscription {
    type Item = Result<StreamResponse, A2aError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(first) = self.first.take() {
            return Some(Ok(first));
        }
        loop {
            if self.watch.is_closed() {
                self.done = true;
                return None;
            }
            match self.updates.recv_timeout(WATCH_POLL) {
                Ok(update) => {
                    self.done = update.ends_task();
                    return Some(Ok(update));
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    self.done = true;
                    let why = if self.lagged.load(Ordering::SeqCst) {
                        "the subscriber fell behind and missed updates"
                    } else {
                        "the task store went away before the task ended"
                    };
                    return Some(Err(A2aError::new(ErrorKind::Internal, why)));
                }
            }
        }
    }
}
