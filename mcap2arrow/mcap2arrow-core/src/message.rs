//! Message types used for Arrow conversion.

use crate::value::Value;

/// A decoded message payload used for Arrow conversion.
#[derive(Debug)]
pub struct DecodedMessage {
    pub log_time: u64,
    pub publish_time: u64,
    pub value: Value,
}
