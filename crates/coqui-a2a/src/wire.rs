//! The messages of the binding: one request, and the responses to it.
//!
//! Every message on a stream is one UTF-8 JSON object in JSON-RPC 2.0 form. A request holds `id`,
//! `method`, optional `params` and optional `a2a-service-parameters`; a response holds the same `id`
//! and one of `result` and `error`.

use core::fmt;
use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use crate::error::{A2aError, ErrorKind};

/// The most service parameters a request carries.
pub const MAX_PARAMETERS: usize = 32;
/// The longest service parameter name, in bytes.
pub const MAX_PARAMETER_NAME: usize = 64;
/// The longest service parameter value, in bytes.
pub const MAX_PARAMETER_VALUE: usize = 8192;
/// The member of a request that holds the service parameters.
pub const PARAMETERS_MEMBER: &str = "a2a-service-parameters";

macro_rules! methods {
    ($($variant:ident => ($text:literal, $streams:literal),)+) => {
        /// An operation a stream can carry.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum Method {
            $(#[doc = $text] $variant,)+
        }

        impl Method {
            /// Every method.
            pub const ALL: &'static [Method] = &[$(Method::$variant,)+];

            /// The name on the wire.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self { $(Self::$variant => $text,)+ }
            }

            /// Whether the answer is a stream of items.
            #[must_use]
            pub const fn streams(self) -> bool {
                match self { $(Self::$variant => $streams,)+ }
            }

            /// The method a name stands for.
            #[must_use]
            pub fn from_name(name: &str) -> Option<Self> {
                match name { $($text => Some(Self::$variant),)+ _ => None }
            }
        }
    };
}

methods! {
    SendMessage => ("SendMessage", false),
    SendStreamingMessage => ("SendStreamingMessage", true),
    GetTask => ("GetTask", false),
    ListTasks => ("ListTasks", false),
    CancelTask => ("CancelTask", false),
    SubscribeToTask => ("SubscribeToTask", true),
    CreateTaskPushNotificationConfig => ("CreateTaskPushNotificationConfig", false),
    GetTaskPushNotificationConfig => ("GetTaskPushNotificationConfig", false),
    ListTaskPushNotificationConfigs => ("ListTaskPushNotificationConfigs", false),
    DeleteTaskPushNotificationConfig => ("DeleteTaskPushNotificationConfig", false),
    GetExtendedAgentCard => ("GetExtendedAgentCard", false),
    GetAgentCard => ("GetAgentCard", false),
}

impl Method {
    /// Whether this is one of the four push-notification operations.
    #[must_use]
    pub const fn is_push(self) -> bool {
        matches!(
            self,
            Self::CreateTaskPushNotificationConfig
                | Self::GetTaskPushNotificationConfig
                | Self::ListTaskPushNotificationConfigs
                | Self::DeleteTaskPushNotificationConfig
        )
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Key-value context that applies to a whole request, such as `a2a-version` or an agent's own
/// `authorization`. Names are case-insensitive and kept lower case; values are case-sensitive.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ServiceParameters(BTreeMap<String, String>);

impl fmt::Debug for ServiceParameters {
    /// Names only: a value may be a credential.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.keys()).finish()
    }
}

impl ServiceParameters {
    /// No parameters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `name`, replacing an earlier value.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidParams`] for a name or value outside the bounds, or past
    /// [`MAX_PARAMETERS`] entries.
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), A2aError> {
        let name = name.to_ascii_lowercase();
        let name_ok = !name.is_empty()
            && name.len() <= MAX_PARAMETER_NAME
            && name.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
            });
        let value_ok = value.len() <= MAX_PARAMETER_VALUE && !value.chars().any(char::is_control);
        if !name_ok || !value_ok {
            return Err(A2aError::new(
                ErrorKind::InvalidParams,
                "a service parameter has a name or value outside the bounds",
            ));
        }
        if !self.0.contains_key(&name) && self.0.len() >= MAX_PARAMETERS {
            return Err(A2aError::new(
                ErrorKind::InvalidParams,
                "too many service parameters",
            ));
        }
        self.0.insert(name, value.to_owned());
        Ok(())
    }

    /// The value of `name`, whatever its case.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(&name.to_ascii_lowercase()).map(String::as_str)
    }

    /// The names and values.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn to_value(&self) -> Value {
        Value::Object(
            self.0
                .iter()
                .map(|(name, value)| (name.clone(), Value::String(value.clone())))
                .collect(),
        )
    }

    fn from_value(value: &Value) -> Result<Self, A2aError> {
        let object = value.as_object().ok_or_else(|| {
            A2aError::new(
                ErrorKind::InvalidRequest,
                "the service parameters are not an object",
            )
        })?;
        let mut parameters = Self::new();
        for (name, value) in object {
            let text = value.as_str().ok_or_else(|| {
                A2aError::new(
                    ErrorKind::InvalidRequest,
                    "a service parameter is not a string",
                )
            })?;
            parameters
                .set(name, text)
                .map_err(|error| A2aError::new(ErrorKind::InvalidRequest, error.message))?;
        }
        Ok(parameters)
    }
}

/// A request: one operation.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    /// Echoed in each response: a string, a number or `null`.
    pub id: Value,
    /// The method name, as received.
    pub method: String,
    /// The parameters, `null` when absent.
    pub params: Value,
    /// The service parameters.
    pub service_parameters: ServiceParameters,
}

impl Request {
    /// A request for `method`.
    #[must_use]
    pub fn new(id: Value, method: Method, params: Value) -> Self {
        Self {
            id,
            method: method.name().to_owned(),
            params,
            service_parameters: ServiceParameters::new(),
        }
    }

    /// The bytes of the message.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut object = Map::new();
        object.insert("jsonrpc".to_owned(), json!("2.0"));
        object.insert("id".to_owned(), self.id.clone());
        object.insert("method".to_owned(), Value::String(self.method.clone()));
        if !self.params.is_null() {
            object.insert("params".to_owned(), self.params.clone());
        }
        if !self.service_parameters.is_empty() {
            object.insert(
                PARAMETERS_MEMBER.to_owned(),
                self.service_parameters.to_value(),
            );
        }
        Value::Object(object).to_string().into_bytes()
    }

    /// Reads a request, liberally about members it does not know.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::ParseError`] for bytes that are not JSON, and [`ErrorKind::InvalidRequest`] for
    /// JSON that is not a request. The error carries no part of the input.
    // Over the lint's limit only when another crate turns on serde_json's `preserve_order`, and this is the rare path.
    #[allow(clippy::result_large_err)]
    pub fn decode(bytes: &[u8]) -> Result<Self, (Value, A2aError)> {
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|_| (Value::Null, A2aError::standard(ErrorKind::ParseError)))?;
        let Value::Object(mut object) = value else {
            return Err((
                Value::Null,
                A2aError::new(ErrorKind::InvalidRequest, "a request is a JSON object"),
            ));
        };
        let id = object.remove("id").unwrap_or(Value::Null);
        if !(id.is_string() || id.is_number() || id.is_null()) {
            return Err((
                Value::Null,
                A2aError::new(
                    ErrorKind::InvalidRequest,
                    "the id is a string, a number or null",
                ),
            ));
        }
        let invalid = |message: &'static str| {
            (
                id.clone(),
                A2aError::new(ErrorKind::InvalidRequest, message),
            )
        };
        if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(invalid("a request says jsonrpc 2.0"));
        }
        let method = match object.remove("method") {
            Some(Value::String(method)) => method,
            _ => return Err(invalid("a request names a method")),
        };
        let params = object.remove("params").unwrap_or(Value::Null);
        if !(params.is_null() || params.is_object()) {
            return Err(invalid("params is an object"));
        }
        let service_parameters = match object.remove(PARAMETERS_MEMBER) {
            None | Some(Value::Null) => ServiceParameters::new(),
            Some(value) => {
                ServiceParameters::from_value(&value).map_err(|error| (id.clone(), error))?
            }
        };
        Ok(Self {
            id,
            method,
            params,
            service_parameters,
        })
    }
}

/// A response: a result, or an error.
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    /// The request's `id`.
    pub id: Value,
    /// What came back.
    pub outcome: Result<Value, A2aError>,
}

impl Response {
    /// A result.
    #[must_use]
    pub fn result(id: Value, result: Value) -> Self {
        Self {
            id,
            outcome: Ok(result),
        }
    }

    /// An error.
    #[must_use]
    pub fn error(id: Value, error: A2aError) -> Self {
        Self {
            id,
            outcome: Err(error),
        }
    }

    /// The bytes of the message.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut object = Map::new();
        object.insert("jsonrpc".to_owned(), json!("2.0"));
        object.insert("id".to_owned(), self.id.clone());
        match &self.outcome {
            Ok(result) => object.insert("result".to_owned(), result.clone()),
            Err(error) => object.insert("error".to_owned(), error.to_value()),
        };
        Value::Object(object).to_string().into_bytes()
    }

    /// Reads a response.
    ///
    /// # Errors
    ///
    /// `None` when the bytes are not a JSON-RPC response holding exactly one of `result` and
    /// `error`.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let Value::Object(mut object) = serde_json::from_slice(bytes).ok()? else {
            return None;
        };
        let id = object.remove("id").unwrap_or(Value::Null);
        match (object.remove("result"), object.remove("error")) {
            (Some(result), None) => Some(Self::result(id, result)),
            (None, Some(error)) => Some(Self::error(id, A2aError::from_value(&error)?)),
            _ => None,
        }
    }
}

/// A response read only as far as a relay needs. Nothing is built from `result` or `error`, so what a
/// hostile peer sends costs its own size here, where a [`Value`] of small members costs many times that.
#[cfg(feature = "loopback")]
#[derive(serde::Deserialize)]
pub(crate) struct RawResponse<'a> {
    #[serde(borrow, default)]
    pub(crate) id: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, default)]
    pub(crate) result: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, default)]
    pub(crate) error: Option<&'a serde_json::value::RawValue>,
}

#[cfg(feature = "loopback")]
impl<'a> RawResponse<'a> {
    /// Reads a response. `None` when the bytes are not a JSON object with no member twice.
    pub(crate) fn decode(bytes: &'a [u8]) -> Option<Self> {
        // A derived struct also reads from an array.
        if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
            return None;
        }
        serde_json::from_slice(bytes).ok()
    }
}
