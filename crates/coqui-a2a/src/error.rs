//! Every A2A error type and how it looks on the wire.
//!
//! An error travels as a JSON-RPC 2.0 error object: `code`, `message` and `data`, an array holding
//! one `google.rpc.ErrorInfo` whose `reason` names the type. The codes of the nine A2A types are the
//! ones the specification fixes for its JSON-RPC binding; [`ErrorKind::ALL`] is the whole table.

use core::fmt;

use serde_json::{Map, Value, json};

/// The `domain` of every `ErrorInfo` an A2A type carries.
pub const DOMAIN: &str = "a2a-protocol.org";
/// The `domain` of the `ErrorInfo` of an error this binding defines.
pub const BINDING_DOMAIN: &str = "boriken.dev";
/// The `@type` of an `ErrorInfo` detail.
pub const ERROR_INFO: &str = "type.googleapis.com/google.rpc.ErrorInfo";

macro_rules! error_kinds {
    ($($(#[$meta:meta])* $variant:ident => ($name:literal, $code:literal, $reason:literal, $message:literal, $domain:ident),)+) => {
        /// A kind of error: the A2A types, the JSON-RPC ones, and those the binding adds.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum ErrorKind {
            $($(#[$meta])* $variant,)+
        }

        impl ErrorKind {
            /// Every kind, in table order.
            pub const ALL: &'static [ErrorKind] = &[$(ErrorKind::$variant,)+];

            /// The JSON-RPC code on the wire.
            #[must_use]
            pub const fn code(self) -> i64 {
                match self { $(Self::$variant => $code,)+ }
            }

            /// The `ErrorInfo` reason: the type's name in upper snake case.
            #[must_use]
            pub const fn reason(self) -> &'static str {
                match self { $(Self::$variant => $reason,)+ }
            }

            /// The standard message.
            #[must_use]
            pub const fn message(self) -> &'static str {
                match self { $(Self::$variant => $message,)+ }
            }

            /// The `ErrorInfo` domain.
            #[must_use]
            pub const fn domain(self) -> &'static str {
                match self { $(Self::$variant => $domain,)+ }
            }

            /// The type's name, as the specification writes it.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self { $(Self::$variant => $name,)+ }
            }
        }
    };
}

error_kinds! {
    /// The task does not exist or is not accessible.
    TaskNotFound => ("TaskNotFoundError", -32001, "TASK_NOT_FOUND", "Task not found", DOMAIN),
    /// The task is in a state that cannot be canceled.
    TaskNotCancelable => ("TaskNotCancelableError", -32002, "TASK_NOT_CANCELABLE", "Task cannot be canceled", DOMAIN),
    /// Push notifications are not supported.
    PushNotificationNotSupported => ("PushNotificationNotSupportedError", -32003, "PUSH_NOTIFICATION_NOT_SUPPORTED", "Push Notification is not supported", DOMAIN),
    /// The operation or an aspect of it is not supported.
    UnsupportedOperation => ("UnsupportedOperationError", -32004, "UNSUPPORTED_OPERATION", "This operation is not supported", DOMAIN),
    /// A media type in the request is not supported.
    ContentTypeNotSupported => ("ContentTypeNotSupportedError", -32005, "CONTENT_TYPE_NOT_SUPPORTED", "Incompatible content types", DOMAIN),
    /// The agent's response does not follow the specification.
    InvalidAgentResponse => ("InvalidAgentResponseError", -32006, "INVALID_AGENT_RESPONSE", "Invalid agent response", DOMAIN),
    /// An extended card was announced and none is configured.
    ExtendedAgentCardNotConfigured => ("ExtendedAgentCardNotConfiguredError", -32007, "EXTENDED_AGENT_CARD_NOT_CONFIGURED", "Extended agent card not configured", DOMAIN),
    /// A required extension was not declared by the client.
    ExtensionSupportRequired => ("ExtensionSupportRequiredError", -32008, "EXTENSION_SUPPORT_REQUIRED", "Extension support required", DOMAIN),
    /// The requested A2A version is not supported.
    VersionNotSupported => ("VersionNotSupportedError", -32009, "VERSION_NOT_SUPPORTED", "Version not supported", DOMAIN),
    /// The agent wants credentials of its own that the request did not carry or that were wrong.
    Unauthenticated => ("UnauthenticatedError", -32010, "UNAUTHENTICATED", "Authentication required", BINDING_DOMAIN),
    /// The caller is known and may not do this.
    PermissionDenied => ("PermissionDeniedError", -32011, "PERMISSION_DENIED", "Permission denied", BINDING_DOMAIN),
    /// The bytes are not JSON.
    ParseError => ("JSONParseError", -32700, "JSON_PARSE_ERROR", "Invalid JSON payload", BINDING_DOMAIN),
    /// The JSON is not a request this binding accepts.
    InvalidRequest => ("InvalidRequestError", -32600, "INVALID_REQUEST", "Request payload validation error", BINDING_DOMAIN),
    /// No such method.
    MethodNotFound => ("MethodNotFoundError", -32601, "METHOD_NOT_FOUND", "Method not found", BINDING_DOMAIN),
    /// The parameters are invalid.
    InvalidParams => ("InvalidParamsError", -32602, "INVALID_PARAMS", "Invalid parameters", BINDING_DOMAIN),
    /// The server failed.
    Internal => ("InternalError", -32603, "INTERNAL_ERROR", "Internal error", BINDING_DOMAIN),
}

impl ErrorKind {
    /// The kind a code stands for.
    #[must_use]
    pub fn from_code(code: i64) -> Option<Self> {
        Self::ALL.iter().copied().find(|kind| kind.code() == code)
    }

    /// The kind a name stands for.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|kind| kind.name() == name)
    }

    /// Whether the specification defines this type, as against JSON-RPC's or this binding's.
    #[must_use]
    pub const fn is_a2a(self) -> bool {
        matches!(
            self,
            Self::TaskNotFound
                | Self::TaskNotCancelable
                | Self::PushNotificationNotSupported
                | Self::UnsupportedOperation
                | Self::ContentTypeNotSupported
                | Self::InvalidAgentResponse
                | Self::ExtendedAgentCardNotConfigured
                | Self::ExtensionSupportRequired
                | Self::VersionNotSupported
        )
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// An error as it is on the wire: a code, a message and details.
#[derive(Debug, Clone, PartialEq)]
pub struct A2aError {
    /// The JSON-RPC code. [`A2aError::kind`] says which type it is, when this version knows.
    pub code: i64,
    /// A message for a person.
    pub message: String,
    /// Detail objects, each with an `@type`.
    pub data: Vec<Value>,
}

impl A2aError {
    /// An error of `kind` with `message`, and the `ErrorInfo` detail the type carries.
    #[must_use]
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            code: kind.code(),
            message: message.into(),
            data: vec![Self::info(kind, Map::new())],
        }
    }

    /// An error of `kind` with its standard message.
    #[must_use]
    pub fn standard(kind: ErrorKind) -> Self {
        Self::new(kind, kind.message())
    }

    fn info(kind: ErrorKind, metadata: Map<String, Value>) -> Value {
        json!({
            "@type": ERROR_INFO,
            "reason": kind.reason(),
            "domain": kind.domain(),
            "metadata": Value::Object(metadata),
        })
    }

    /// Adds a metadata entry to the `ErrorInfo`, such as `taskId`.
    #[must_use]
    pub fn with_metadata(mut self, name: &str, value: &str) -> Self {
        if let Some(Value::Object(info)) = self.data.first_mut()
            && let Some(Value::Object(metadata)) = info.get_mut("metadata")
        {
            metadata.insert(name.to_owned(), Value::String(value.to_owned()));
        }
        self
    }

    /// `TaskNotFoundError` for `id`.
    #[must_use]
    pub fn task_not_found(id: &str) -> Self {
        Self::standard(ErrorKind::TaskNotFound).with_metadata("taskId", id)
    }

    /// The type this version knows the code as.
    #[must_use]
    pub fn kind(&self) -> Option<ErrorKind> {
        ErrorKind::from_code(self.code)
    }

    /// The JSON-RPC error object.
    #[must_use]
    pub fn to_value(&self) -> Value {
        let mut object = Map::new();
        object.insert("code".to_owned(), Value::from(self.code));
        object.insert("message".to_owned(), Value::String(self.message.clone()));
        if !self.data.is_empty() {
            object.insert("data".to_owned(), Value::Array(self.data.clone()));
        }
        Value::Object(object)
    }

    /// Reads a JSON-RPC error object, liberally: a missing message or data is empty.
    ///
    /// Returns `None` when `value` has no integer `code`.
    #[must_use]
    pub fn from_value(value: &Value) -> Option<Self> {
        let object = value.as_object()?;
        let code = object.get("code")?.as_i64()?;
        let message = object
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let data = match object.get("data") {
            Some(Value::Array(items)) => items.clone(),
            _ => Vec::new(),
        };
        Some(Self {
            code,
            message,
            data,
        })
    }
}

impl fmt::Display for A2aError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            Some(kind) => write!(f, "{kind}: {}", self.message),
            None => write!(f, "error {}: {}", self.code, self.message),
        }
    }
}

impl core::error::Error for A2aError {}
