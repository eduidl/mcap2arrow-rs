//! Message types produced by [`McapReader`](crate::McapReader).

use crate::message_encoding::MessageEncoding;
use crate::schema_encoding::SchemaEncoding;
use crate::value::Value;

/// A decoded message together with its topic and timing metadata.
pub struct TypedMessage {
    pub topic: String,
    pub schema_name: String,
    pub schema_encoding: SchemaEncoding,
    pub message_encoding: MessageEncoding,
    pub log_time: u64,
    pub publish_time: u64,
    pub value: Value,
}

/// Summary information about a single topic in an MCAP file.
#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub topic: String,
    pub schema_name: String,
    pub schema_encoding: SchemaEncoding,
    pub message_encoding: MessageEncoding,
    pub message_count: u64,
}
