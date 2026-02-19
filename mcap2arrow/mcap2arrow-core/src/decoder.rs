//! Decoder trait and encoding key used to register pluggable message decoders.

use anyhow::Result;

use crate::message_encoding::MessageEncoding;
use crate::schema::FieldDef;
use crate::schema_encoding::SchemaEncoding;
use crate::value::Value;

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
/// Implementations are registered with [`McapReader`](crate::McapReader) and
/// dispatched based on [`EncodingKey`].
pub trait MessageDecoder: Send + Sync {
    /// Returns the encoding pair this decoder handles.
    fn encoding_key(&self) -> EncodingKey;

    /// Decode a single message into a [`Value`].
    fn decode(
        &self,
        schema_name: &str,
        schema_data: &[u8],
        message_data: &[u8],
    ) -> Result<Value>;

    /// Optionally derive a schema from MCAP schema metadata.
    ///
    /// Default implementation returns `Ok(None)` so decoders can opt out.
    fn derive_schema(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
    ) -> Result<Option<Vec<FieldDef>>> {
        Ok(None)
    }
}
