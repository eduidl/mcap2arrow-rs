//! Decoder trait and encoding key used to register pluggable message decoders.

use crate::{
    message_encoding::MessageEncoding, schema::FieldDef, schema_encoding::SchemaEncoding,
    value::Value,
};

/// Key identifying a (schema_encoding, message_encoding) pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EncodingKey {
    pub schema_encoding: SchemaEncoding,
    pub message_encoding: MessageEncoding,
}

impl EncodingKey {
    pub fn new(schema_encoding: SchemaEncoding, message_encoding: MessageEncoding) -> Self {
        Self {
            schema_encoding,
            message_encoding,
        }
    }
}

/// Trait for decoding raw MCAP message bytes into [`Value`].
///
/// Implementations are registered with `mcap2arrow::McapReader` and
/// dispatched based on [`EncodingKey`].
///
/// Decoding failures (corrupt data, schema mismatch, etc.) should **panic**
/// rather than return errors — they are not recoverable at the reader level.
pub trait MessageDecoder: Send + Sync {
    /// Returns the encoding pair this decoder handles.
    fn encoding_key(&self) -> EncodingKey;

    /// Decode a single message into a [`Value`].
    ///
    /// # Panics
    ///
    /// Panics if `schema_data` or `message_data` cannot be decoded.
    fn decode(&self, schema_name: &str, schema_data: &[u8], message_data: &[u8]) -> Value;

    /// Derive a schema from MCAP schema metadata.
    ///
    /// # Panics
    ///
    /// Panics if `schema_data` cannot be parsed.
    fn derive_schema(&self, schema_name: &str, schema_data: &[u8]) -> Vec<FieldDef>;
}
