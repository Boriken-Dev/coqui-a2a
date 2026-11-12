//! The A2A data model, as JSON: camelCase fields and ProtoJSON enum names.
//!
//! The types mirror the normative schema of A2A 1.0. A field the schema leaves open (`metadata`, a
//! part's `data`, security schemes, signatures) is a [`serde_json::Value`] and travels unread. The
//! objects with an `extra` field keep the members they do not know in it. [`AgentProvider`],
//! [`AgentExtension`] and [`AgentSkill`] have none and drop them, as do the one-member wrappers
//! [`StreamResponse`] and [`SendMessageResponse`], so a relay that must keep everything carries raw JSON.

use core::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::error::{A2aError, ErrorKind};

/// A JSON object whose keys are strings: the schema's "flexible key-value map".
pub type Object = Map<String, Value>;

/// Defines an enum of ProtoJSON names that keeps a name it does not know, as `Other`.
macro_rules! name_enum {
    ($(#[$meta:meta])* $name:ident {
        $(#[$fmeta:meta])* $first:ident => $ftext:literal,
        $($(#[$vmeta:meta])* $variant:ident => $text:literal,)*
    }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
        #[non_exhaustive]
        pub enum $name {
            $(#[$fmeta])*
            #[default]
            $first,
            $($(#[$vmeta])* $variant,)*
            /// A name this version does not know, kept as received.
            Other(String),
        }

        impl $name {
            /// The name on the wire.
            #[must_use]
            pub fn as_str(&self) -> &str {
                match self {
                    Self::$first => $ftext,
                    $(Self::$variant => $text,)*
                    Self::Other(text) => text,
                }
            }
        }

        impl From<&str> for $name {
            fn from(text: &str) -> Self {
                match text {
                    $ftext => Self::$first,
                    $($text => Self::$variant,)*
                    other => Self::Other(other.to_owned()),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = String::deserialize(deserializer)?;
                Ok(Self::from(text.as_str()))
            }
        }
    };
}

name_enum! {
    /// Where a task is in its life.
    TaskState {
        /// Unknown or indeterminate.
        Unspecified => "TASK_STATE_UNSPECIFIED",
        /// Acknowledged.
        Submitted => "TASK_STATE_SUBMITTED",
        /// Being processed.
        Working => "TASK_STATE_WORKING",
        /// Finished successfully. Terminal.
        Completed => "TASK_STATE_COMPLETED",
        /// Finished with an error. Terminal.
        Failed => "TASK_STATE_FAILED",
        /// Canceled before completion. Terminal.
        Canceled => "TASK_STATE_CANCELED",
        /// Waiting for the user. Interrupted.
        InputRequired => "TASK_STATE_INPUT_REQUIRED",
        /// The agent will not do it. Terminal.
        Rejected => "TASK_STATE_REJECTED",
        /// Waiting for authorization. Interrupted.
        AuthRequired => "TASK_STATE_AUTH_REQUIRED",
    }
}

impl TaskState {
    /// Whether the task has ended: no message and no subscription is accepted after it.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Rejected
        )
    }

    /// Whether the task waits for the client.
    #[must_use]
    pub fn is_interrupted(&self) -> bool {
        matches!(self, Self::InputRequired | Self::AuthRequired)
    }
}

name_enum! {
    /// Who sent a message.
    Role {
        /// Unspecified.
        Unspecified => "ROLE_UNSPECIFIED",
        /// The client.
        User => "ROLE_USER",
        /// The server.
        Agent => "ROLE_AGENT",
    }
}

/// One section of a message or artifact: exactly one of `text`, `raw`, `url` and `data`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Part {
    /// Text content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// File content, as a base64 string (RFC 4648, standard alphabet, padded), unread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>,
    /// Where the file content is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Structured content, any JSON value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    /// Metadata of this part.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Object>,
    /// A file name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// The media type of the content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

impl Part {
    /// A text part.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: Some(text.into()),
            ..Self::default()
        }
    }

    /// A structured-data part.
    #[must_use]
    pub fn data(data: Value) -> Self {
        Self {
            data: Some(data),
            ..Self::default()
        }
    }

    fn validate(&self) -> Result<(), A2aError> {
        let set = usize::from(self.text.is_some())
            + usize::from(self.raw.is_some())
            + usize::from(self.url.is_some())
            + usize::from(self.data.is_some());
        if set == 1 {
            Ok(())
        } else {
            Err(A2aError::new(
                ErrorKind::InvalidParams,
                "a part holds exactly one of text, raw, url and data",
            ))
        }
    }
}

/// One unit of communication between client and server.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    /// Unique, made by the sender.
    #[serde(default)]
    pub message_id: String,
    /// The conversation it belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    /// The task it belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Who sent it.
    pub role: Role,
    /// Its content.
    #[serde(default)]
    pub parts: Vec<Part>,
    /// Metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Object>,
    /// Extension URIs present in it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Vec<String>>,
    /// Tasks it refers to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_task_ids: Option<Vec<String>>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

impl Message {
    /// A message of one text part.
    #[must_use]
    pub fn text(role: Role, message_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            role,
            parts: vec![Part::text(text)],
            ..Self::default()
        }
    }

    /// The text of its text parts, joined.
    #[must_use]
    pub fn joined_text(&self) -> String {
        self.parts
            .iter()
            .filter_map(|part| part.text.as_deref())
            .collect::<Vec<_>>()
            .join("")
    }

    fn validate(&self) -> Result<(), A2aError> {
        if self.message_id.is_empty() {
            return Err(A2aError::new(
                ErrorKind::InvalidParams,
                "a message needs a messageId",
            ));
        }
        if self.parts.is_empty() {
            return Err(A2aError::new(
                ErrorKind::InvalidParams,
                "a message needs at least one part",
            ));
        }
        self.parts.iter().try_for_each(Part::validate)
    }
}

/// A task's state at a moment.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatus {
    /// The state.
    pub state: TaskState,
    /// A message with it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
    /// When it was recorded: ISO 8601 in UTC.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

impl TaskStatus {
    /// A status of `state`, stamped now.
    #[must_use]
    pub fn now(state: TaskState) -> Self {
        Self {
            state,
            timestamp: Some(timestamp_now()),
            ..Self::default()
        }
    }
}

/// An output of a task.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    /// Unique within the task.
    #[serde(default)]
    pub artifact_id: String,
    /// A name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// A description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Its content: at least one part.
    #[serde(default)]
    pub parts: Vec<Part>,
    /// Metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Object>,
    /// Extension URIs present in it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Vec<String>>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// The core unit of action.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    /// Made by the server.
    #[serde(default)]
    pub id: String,
    /// The conversation it belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    /// Where it is.
    pub status: TaskStatus,
    /// Its outputs. Omitted, never `null`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifacts: Option<Vec<Artifact>>,
    /// Its messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<Vec<Message>>,
    /// Metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Object>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// A change of a task's status.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatusUpdateEvent {
    /// The task.
    #[serde(default)]
    pub task_id: String,
    /// Its context.
    #[serde(default)]
    pub context_id: String,
    /// The new status.
    pub status: TaskStatus,
    /// Metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Object>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// An artifact, or a part of one, produced.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskArtifactUpdateEvent {
    /// The task.
    #[serde(default)]
    pub task_id: String,
    /// Its context.
    #[serde(default)]
    pub context_id: String,
    /// The artifact.
    pub artifact: Artifact,
    /// Whether it continues an artifact of the same id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub append: Option<bool>,
    /// Whether it is the last chunk.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_chunk: Option<bool>,
    /// Metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Object>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// One item of a stream: exactly one member is set on the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub enum StreamResponse {
    /// The task's state.
    Task(Task),
    /// A message from the agent.
    Message(Message),
    /// A status change.
    StatusUpdate(TaskStatusUpdateEvent),
    /// An artifact produced.
    ArtifactUpdate(TaskArtifactUpdateEvent),
}

impl StreamResponse {
    /// Whether the task is in a terminal state after this item, which ends its stream.
    #[must_use]
    pub fn ends_task(&self) -> bool {
        match self {
            Self::Task(task) => task.status.state.is_terminal(),
            Self::StatusUpdate(event) => event.status.state.is_terminal(),
            Self::Message(_) | Self::ArtifactUpdate(_) => false,
        }
    }
}

/// The answer to `SendMessage`: a task or a message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
#[expect(
    clippy::large_enum_variant,
    reason = "a response is built once and moved; boxing would change the public type"
)]
pub enum SendMessageResponse {
    /// A task tracking the work.
    Task(Task),
    /// A direct answer.
    Message(Message),
}

/// Push-notification settings. This binding carries none, so the value is unread.
pub type PushNotificationConfig = Value;

/// How a send is done.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageConfiguration {
    /// Media types the client accepts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted_output_modes: Option<Vec<String>>,
    /// A webhook configuration, which this binding does not support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_push_notification_config: Option<PushNotificationConfig>,
    /// The most recent messages of history to return.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_length: Option<u32>,
    /// Return once the task exists instead of when it settles.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_immediately: Option<bool>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// `SendMessage` and `SendStreamingMessage`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequest {
    /// Routing, unread by the binding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The message.
    pub message: Message,
    /// How to send.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<SendMessageConfiguration>,
    /// Metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Object>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

impl SendMessageRequest {
    /// A request carrying `message`.
    #[must_use]
    pub fn new(message: Message) -> Self {
        Self {
            message,
            ..Self::default()
        }
    }

    /// Checks what the schema marks required.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidParams`] when the message lacks an id or parts, or a part does not hold
    /// exactly one kind of content.
    pub fn validate(&self) -> Result<(), A2aError> {
        self.message.validate()
    }
}

/// `GetTask`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTaskRequest {
    /// Routing, unread by the binding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The task.
    #[serde(default)]
    pub id: String,
    /// The most recent messages of history to return.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_length: Option<u32>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// `ListTasks`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksRequest {
    /// Routing, unread by the binding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// Only this context.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    /// Only this state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<TaskState>,
    /// At most this many, 1 to 100, 50 when unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    /// From a previous answer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    /// The most recent messages of history in each task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_length: Option<u32>,
    /// Only tasks whose status changed at or after this time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_timestamp_after: Option<String>,
    /// Whether to include artifacts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_artifacts: Option<bool>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// The answer to `ListTasks`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksResponse {
    /// The page, most recently updated first.
    #[serde(default)]
    pub tasks: Vec<Task>,
    /// The next page, or the empty string. Always present.
    #[serde(default)]
    pub next_page_token: String,
    /// The page size used.
    #[serde(default)]
    pub page_size: u32,
    /// How many tasks there are before paging.
    #[serde(default)]
    pub total_size: u32,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// `CancelTask`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelTaskRequest {
    /// Routing, unread by the binding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The task.
    #[serde(default)]
    pub id: String,
    /// Metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Object>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// `SubscribeToTask`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeToTaskRequest {
    /// Routing, unread by the binding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The task.
    #[serde(default)]
    pub id: String,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// `GetExtendedAgentCard`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetExtendedAgentCardRequest {
    /// Routing, unread by the binding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// One way to reach the agent.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentInterface {
    /// Where it is available.
    #[serde(default)]
    pub url: String,
    /// The binding: `JSONRPC`, `GRPC`, `HTTP+JSON`, or a URI.
    #[serde(default)]
    pub protocol_binding: String,
    /// Routing the client repeats in each request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The A2A version, such as `1.0`.
    #[serde(default)]
    pub protocol_version: String,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// The agent's provider.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProvider {
    /// Its website.
    #[serde(default)]
    pub url: String,
    /// Its name.
    #[serde(default)]
    pub organization: String,
}

/// An extension an agent supports.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentExtension {
    /// Its URI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// How the agent uses it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether a client must understand it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    /// Its settings, unread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Object>,
}

/// Optional abilities.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCapabilities {
    /// Whether it streams.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streaming: Option<bool>,
    /// Whether it sends webhooks. This binding never does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_notifications: Option<bool>,
    /// The extensions it supports.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Vec<AgentExtension>>,
    /// Whether it serves an extended card.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_agent_card: Option<bool>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

/// A thing the agent does.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSkill {
    /// Its identifier.
    #[serde(default)]
    pub id: String,
    /// Its name.
    #[serde(default)]
    pub name: String,
    /// What it is.
    #[serde(default)]
    pub description: String,
    /// Keywords.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Example prompts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub examples: Option<Vec<String>>,
    /// Input media types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_modes: Option<Vec<String>>,
    /// Output media types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_modes: Option<Vec<String>>,
    /// Security requirements, unread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_requirements: Option<Vec<Value>>,
}

/// An agent's manifest.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentCard {
    /// A name.
    #[serde(default)]
    pub name: String,
    /// What it is.
    #[serde(default)]
    pub description: String,
    /// Ways to reach it, preferred first.
    #[serde(default)]
    pub supported_interfaces: Vec<AgentInterface>,
    /// Who provides it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<AgentProvider>,
    /// Its version.
    #[serde(default)]
    pub version: String,
    /// Documentation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documentation_url: Option<String>,
    /// What it can do.
    #[serde(default)]
    pub capabilities: AgentCapabilities,
    /// How to authenticate to it, unread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_schemes: Option<Object>,
    /// What is required, unread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_requirements: Option<Vec<Value>>,
    /// Input media types.
    #[serde(default)]
    pub default_input_modes: Vec<String>,
    /// Output media types.
    #[serde(default)]
    pub default_output_modes: Vec<String>,
    /// Its skills.
    #[serde(default)]
    pub skills: Vec<AgentSkill>,
    /// JWS signatures, unread.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signatures: Option<Vec<Value>>,
    /// An icon.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
    /// Members this version does not know.
    #[serde(flatten)]
    pub extra: Object,
}

impl AgentCard {
    /// The agent a request is for, on a card that lists several: the skill whose `id` is the
    /// request's `tenant`. With no tenant the card has to list exactly one skill.
    ///
    /// A tenant the card does not list is refused the same way whether no such agent exists or
    /// the caller may not use it, so a card shows a caller only the agents it may name: the caller
    /// is the one the agent's `agent_card` was asked for.
    ///
    /// # Errors
    ///
    /// `InvalidParamsError` naming the ids the card lists, for an unlisted tenant, for no tenant
    /// on a card of several skills, and for a card of none.
    pub fn select_skill(&self, tenant: Option<&str>) -> Result<&AgentSkill, A2aError> {
        let listed = || {
            self.skills
                .iter()
                .map(|skill| skill.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        // An empty member is an unset one in proto3 JSON.
        let tenant = tenant.filter(|tenant| !tenant.is_empty());
        match (tenant, self.skills.as_slice()) {
            (_, []) => Err(A2aError::new(
                ErrorKind::InvalidParams,
                "this card lists no agent",
            )),
            (None, [only]) => Ok(only),
            (None, _) => Err(A2aError::new(
                ErrorKind::InvalidParams,
                format!("name the agent in tenant: {}", listed()),
            )),
            (Some(tenant), skills) => {
                skills
                    .iter()
                    .find(|skill| skill.id == tenant)
                    .ok_or_else(|| {
                        A2aError::new(
                            ErrorKind::InvalidParams,
                            format!("no agent is named {tenant:?}: {}", listed()),
                        )
                    })
            }
        }
    }

    /// The interface entry that names this binding, if the card lists one.
    #[must_use]
    pub fn link_interface(&self) -> Option<&AgentInterface> {
        self.supported_interfaces
            .iter()
            .find(|interface| interface.protocol_binding == crate::BINDING)
    }
}

/// The time now in the form the specification fixes: ISO 8601, UTC, milliseconds.
#[must_use]
pub fn timestamp_now() -> String {
    use time::format_description::well_known::Rfc3339;
    let now = time::OffsetDateTime::now_utc();
    let whole_milliseconds = now.nanosecond() / 1_000_000 * 1_000_000;
    now.replace_nanosecond(whole_milliseconds)
        .ok()
        .and_then(|stamp| stamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned())
}

/// Decodes a request object into `T`, naming a failure as invalid parameters.
///
/// # Errors
///
/// [`ErrorKind::InvalidParams`] when `params` is not a `T`.
pub fn params_as<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, A2aError> {
    let params = if params.is_null() {
        Value::Object(Object::new())
    } else {
        params
    };
    serde_json::from_value(params).map_err(|error| {
        A2aError::new(
            ErrorKind::InvalidParams,
            format!("the parameters are not valid: {}", error.classify_text()),
        )
    })
}

/// A serde error's category, without its text: that text may quote the input.
trait ClassifyText {
    fn classify_text(&self) -> &'static str;
}

impl ClassifyText for serde_json::Error {
    fn classify_text(&self) -> &'static str {
        match self.classify() {
            serde_json::error::Category::Data => "a member has the wrong type or is missing",
            serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
                "the JSON is malformed"
            }
            serde_json::error::Category::Io => "the input could not be read",
        }
    }
}

/// Reads a JSON value into `T`, for a response whose shape the caller expects.
///
/// # Errors
///
/// The serde error.
pub fn value_as<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, serde_json::Error> {
    T::deserialize(value).map_err(serde_json::Error::custom)
}
