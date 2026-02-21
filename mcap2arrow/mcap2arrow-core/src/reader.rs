//! MCAP file reader with pluggable decoder support.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use memmap2::Mmap;

use crate::decoder::{EncodingKey, MessageDecoder};
use crate::error::McapReaderError;
use crate::message::{TopicInfo, TypedMessage};
use crate::message_encoding::MessageEncoding;
use crate::schema_encoding::SchemaEncoding;

/// Reads an MCAP file and decodes messages using registered [`MessageDecoder`]s.
pub struct McapReader {
    decoders: HashMap<EncodingKey, Box<dyn MessageDecoder>>,
}

impl McapReader {
    pub fn new() -> Self {
        Self {
            decoders: HashMap::new(),
        }
    }

    /// Register a decoder for a specific encoding pair.
    pub fn register_decoder(&mut self, decoder: Box<dyn MessageDecoder>) {
        self.decoders.insert(decoder.encoding_key(), decoder);
    }

    fn mmap_file(&self, path: &Path) -> Result<Mmap, McapReaderError> {
        let file = fs::File::open(path)?;
        Ok(unsafe { Mmap::map(&file) }?)
    }

    fn read_summary(&self, path: &Path) -> Result<mcap::read::Summary, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        mcap::read::Summary::read(&mmap)?.ok_or_else(|| McapReaderError::SummaryNotAvailable {
            path: path.display().to_string(),
        })
    }

    /// Iterate over messages in the MCAP file, optionally filtered by topic.
    ///
    /// All processed channels must have schema metadata, and a matching decoder
    /// must be registered for each encountered encoding pair.
    pub fn for_each_message(
        &self,
        path: &Path,
        topic_filter: Option<&str>,
        mut callback: impl FnMut(TypedMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        let mmap = self.mmap_file(path)?;

        for message in mcap::MessageStream::new(&mmap)? {
            let message = message?;
            let channel = &message.channel;

            if let Some(filter) = topic_filter {
                if channel.topic != filter {
                    continue;
                }
            }

            let schema =
                channel
                    .schema
                    .as_ref()
                    .ok_or_else(|| McapReaderError::SchemaRequired {
                        topic: channel.topic.clone(),
                        channel_id: channel.id,
                    })?;
            let schema_name = schema.name.as_str();
            let schema_enc = SchemaEncoding::from(schema.encoding.as_str());
            let message_enc = MessageEncoding::from(channel.message_encoding.as_str());

            let key = EncodingKey::new(schema_enc.clone(), message_enc.clone());

            let decoder = self
                .decoders
                .get(&key)
                .ok_or_else(|| McapReaderError::NoDecoder {
                    schema_encoding: schema_enc.to_string(),
                    message_encoding: message_enc.to_string(),
                    topic: channel.topic.clone(),
                })?;
            let value = decoder.decode(&schema.name, &schema.data, &message.data);

            callback(TypedMessage {
                topic: channel.topic.clone(),
                schema_name: schema_name.to_string(),
                schema_encoding: schema_enc,
                message_encoding: message_enc,
                log_time: message.log_time,
                publish_time: message.publish_time,
                value,
            })
            .map_err(McapReaderError::Callback)?;
        }

        Ok(())
    }

    /// Return the total message count from the MCAP summary section.
    ///
    /// MCAP summary and summary stats are required.
    pub fn message_count(
        &self,
        path: &Path,
        topic_filter: Option<&str>,
    ) -> Result<Option<u64>, McapReaderError> {
        let summary = self.read_summary(path)?;

        let stats = summary
            .stats
            .as_ref()
            .ok_or_else(|| McapReaderError::StatsRequired {
                path: path.display().to_string(),
            })?;

        match topic_filter {
            None => Ok(Some(stats.message_count)),
            Some(topic) => {
                let count = summary
                    .channels
                    .values()
                    .filter(|ch| ch.topic == topic)
                    .map(|ch| {
                        stats
                            .channel_message_counts
                            .get(&ch.id)
                            .copied()
                            .unwrap_or(0)
                    })
                    .sum();
                Ok(Some(count))
            }
        }
    }

    /// List all topics in the MCAP file with their metadata.
    ///
    /// MCAP summary is required. Schema-less channels are represented with an
    /// empty schema name and [`SchemaEncoding::None`].
    pub fn list_topics(&self, path: &Path) -> Result<Vec<TopicInfo>, McapReaderError> {
        let summary = self.read_summary(path)?;

        let channel_message_counts = summary.stats.as_ref().map(|s| &s.channel_message_counts);

        let mut topics: Vec<TopicInfo> = summary
            .channels
            .values()
            .map(|channel| {
                let message_count = channel_message_counts
                    .and_then(|counts| counts.get(&channel.id))
                    .copied()
                    .unwrap_or(0);

                TopicInfo {
                    topic: channel.topic.clone(),
                    schema_name: channel
                        .schema
                        .as_ref()
                        .map(|s| s.name.clone())
                        .unwrap_or_default(),
                    schema_encoding: channel
                        .schema
                        .as_ref()
                        .map(|s| SchemaEncoding::from(s.encoding.as_str()))
                        .unwrap_or(SchemaEncoding::None),
                    message_encoding: MessageEncoding::from(channel.message_encoding.as_str()),
                    message_count,
                }
            })
            .collect();

        topics.sort_by(|a, b| a.topic.cmp(&b.topic));
        Ok(topics)
    }
}

impl Default for McapReader {
    fn default() -> Self {
        Self::new()
    }
}
