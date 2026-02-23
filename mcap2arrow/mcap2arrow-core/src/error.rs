//! Error types for the decoder layer.

/// Error returned by [`MessageDecoder`](crate::MessageDecoder) implementations.
#[derive(Debug, thiserror::Error)]
pub enum DecoderError {
    /// Schema data (e.g., a serialized `FileDescriptorSet`) could not be parsed.
    #[error("failed to parse schema '{schema_name}': {source}")]
    SchemaParse {
        schema_name: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Schema data is structurally invalid (e.g., missing descriptor, broken map fields).
    #[error("invalid schema '{schema_name}': {detail}")]
    SchemaInvalid { schema_name: String, detail: String },

    /// Message payload bytes could not be decoded.
    #[error("failed to decode message for schema '{schema_name}': {source}")]
    MessageDecode {
        schema_name: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
