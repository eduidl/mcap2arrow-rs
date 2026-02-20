//! Error types for the MCAP reader.

/// Errors produced by [`McapReader`](crate::McapReader).
#[derive(Debug, thiserror::Error)]
pub enum McapReaderError {
    /// I/O error while opening or memory-mapping a file.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Error from the underlying `mcap` crate (bad magic, CRC mismatch, …).
    #[error(transparent)]
    Mcap(#[from] mcap::McapError),

    /// The MCAP file has no summary section.
    #[error("MCAP summary not available in {path}")]
    SummaryNotAvailable { path: String },

    /// The MCAP summary section has no statistics record.
    #[error("MCAP summary stats required in {path}")]
    StatsRequired { path: String },

    /// A channel that was about to be decoded has no schema attached.
    #[error("schema required for topic '{topic}' (channel id {channel_id})")]
    SchemaRequired { topic: String, channel_id: u16 },

    /// No [`MessageDecoder`](crate::MessageDecoder) was registered for the
    /// encoding pair found on a channel.
    #[error("no decoder registered for schema_encoding='{schema_encoding}', message_encoding='{message_encoding}' on topic '{topic}'")]
    NoDecoder {
        schema_encoding: String,
        message_encoding: String,
        topic: String,
    },

    /// An error returned by the user-supplied callback in
    /// [`McapReader::for_each_message`](crate::McapReader::for_each_message).
    #[error(transparent)]
    Callback(Box<dyn std::error::Error + Send + Sync>),
}
