//! MCAP file reader with pluggable decoder support.

use std::{collections::HashMap, fs, path::Path, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use mcap2arrow_arrow::{arrow_value_rows_to_record_batch, field_defs_to_arrow_schema};
use mcap2arrow_core::{
    DecodedMessage, EncodingKey, MessageDecoder, MessageEncoding, SchemaEncoding,
};
#[cfg(feature = "protobuf")]
use mcap2arrow_protobuf::ProtobufDecoder;
use memmap2::Mmap;

use crate::error::McapReaderError;

/// Reads an MCAP file and decodes messages using registered [`MessageDecoder`]s.
pub struct McapReader {
    decoders: HashMap<EncodingKey, Arc<dyn MessageDecoder>>,
    batch_size: usize,
}

/// Builder for configuring [`McapReader`].
pub struct McapReaderBuilder {
    decoders: Vec<Arc<dyn MessageDecoder>>,
    batch_size: usize,
}

struct TopicBatchContext<'a> {
    channel_id: u16,
    schema: Arc<mcap::Schema<'a>>,
    decoder: Arc<dyn MessageDecoder>,
    arrow_schema: SchemaRef,
}

impl McapReader {
    /// Create a builder for [`McapReader`].
    pub fn builder() -> McapReaderBuilder {
        McapReaderBuilder {
            decoders: Vec::new(),
            batch_size: 1024,
        }
    }

    pub fn new() -> Self {
        Self {
            decoders: HashMap::new(),
            batch_size: 1024,
        }
    }

    /// Register a decoder for a specific encoding pair.
    pub fn register_decoder(&mut self, decoder: Box<dyn MessageDecoder>) {
        self.register_shared_decoder(Arc::from(decoder));
    }

    /// Register a shared decoder for a specific encoding pair.
    pub fn register_shared_decoder(&mut self, decoder: Arc<dyn MessageDecoder>) {
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

    fn find_decoder(
        &self,
        topic: &str,
        schema_enc: &SchemaEncoding,
        message_enc: &MessageEncoding,
    ) -> Result<&Arc<dyn MessageDecoder>, McapReaderError> {
        let key = EncodingKey::new(schema_enc.clone(), message_enc.clone());
        self.decoders
            .get(&key)
            .ok_or_else(|| McapReaderError::NoDecoder {
                schema_encoding: schema_enc.to_string(),
                message_encoding: message_enc.to_string(),
                topic: topic.to_string(),
            })
    }

    fn resolve_topic_batch_context<'a>(
        &'a self,
        summary: &'a mcap::read::Summary,
        topic: &str,
    ) -> Result<TopicBatchContext<'a>, McapReaderError> {
        let channel = get_channel_from_summary(summary, topic)?;
        let schema = Arc::clone(get_schema_from_channel(channel)?);
        let schema_enc = SchemaEncoding::from(schema.encoding.as_str());
        let message_enc = MessageEncoding::from(channel.message_encoding.as_str());
        let decoder = Arc::clone(self.find_decoder(&channel.topic, &schema_enc, &message_enc)?);
        let field_defs = decoder.derive_schema(&schema.name, &schema.data);

        if field_defs.is_empty() {
            return Err(McapReaderError::EmptyDerivedSchema {
                topic: topic.to_string(),
                schema_name: schema.name.clone(),
            });
        }

        Ok(TopicBatchContext {
            channel_id: channel.id,
            schema,
            decoder,
            arrow_schema: Arc::new(field_defs_to_arrow_schema(&field_defs)),
        })
    }

    /// Read all messages for a topic and emit Arrow RecordBatches to callback.
    pub fn for_each_record_batch(
        &self,
        path: &Path,
        topic: &str,
        mut callback: impl FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        fn flush_batch<F>(
            schema: &SchemaRef,
            rows: &mut Vec<DecodedMessage>,
            callback: &mut F,
        ) -> Result<(), McapReaderError>
        where
            F: FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
        {
            if rows.is_empty() {
                return Ok(());
            }

            let batch = arrow_value_rows_to_record_batch(schema, rows.as_slice());
            rows.clear();
            callback(batch).map_err(McapReaderError::Callback)
        }

        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path)?;
        let context = self.resolve_topic_batch_context(&summary, topic)?;
        let mut rows = Vec::with_capacity(self.batch_size);

        for message in mcap::MessageStream::new(&mmap)? {
            let message = message?;
            let channel = &message.channel;
            if channel.topic != topic {
                continue;
            }
            if channel.id != context.channel_id {
                panic!(
                    "multiple channels found for topic '{}' (expected channel id {}, got {})",
                    topic, context.channel_id, channel.id
                );
            }

            rows.push(DecodedMessage {
                log_time: message.log_time,
                publish_time: message.publish_time,
                value: context.decoder.decode(
                    &context.schema.name,
                    &context.schema.data,
                    &message.data,
                ),
            });

            if rows.len() >= self.batch_size {
                flush_batch(&context.arrow_schema, &mut rows, &mut callback)?;
            }
        }

        flush_batch(&context.arrow_schema, &mut rows, &mut callback)
    }

    /// Return the total message count from the MCAP summary section.
    ///
    /// MCAP summary and summary stats are required.
    pub fn message_count(&self, path: &Path, topic: &str) -> Result<u64, McapReaderError> {
        let summary = self.read_summary(path)?;
        let channel = get_channel_from_summary(&summary, topic)?;

        let stats = summary
            .stats
            .as_ref()
            .ok_or_else(|| McapReaderError::StatsRequired {
                path: path.display().to_string(),
            })?;

        Ok(stats
            .channel_message_counts
            .get(&channel.id)
            .copied()
            .unwrap_or_default())
    }
}

impl Default for McapReader {
    fn default() -> Self {
        Self::new()
    }
}

impl McapReaderBuilder {
    /// Register a message decoder.
    pub fn with_decoder(mut self, decoder: Box<dyn MessageDecoder>) -> Self {
        self.decoders.push(Arc::from(decoder));
        self
    }

    /// Set the number of messages per RecordBatch (default: 1024).
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Register all built-in decoders (Protobuf).
    pub fn with_default_decoders(self) -> Self {
        let s = self;
        #[cfg(feature = "protobuf")]
        let s = s.with_decoder(Box::new(ProtobufDecoder::new()));
        s
    }

    /// Build the reader.
    pub fn build(self) -> McapReader {
        let mut reader = McapReader::new();
        reader.batch_size = self.batch_size;
        for decoder in self.decoders {
            reader.register_shared_decoder(decoder);
        }
        reader
    }
}

fn get_channel_from_summary<'a>(
    summary: &'a mcap::read::Summary,
    topic: &str,
) -> Result<&'a Arc<mcap::Channel<'a>>, McapReaderError> {
    let mut channels = summary.channels.values().filter(|ch| ch.topic == topic);
    let first = channels
        .next()
        .ok_or_else(|| McapReaderError::TopicNotFound {
            topic: topic.to_string(),
        })?;
    if let Some(other) = channels.next() {
        panic!(
            "multiple channels found for topic '{}' (channel ids: {}, {})",
            topic, first.id, other.id
        );
    }
    Ok(first)
}

fn get_schema_from_channel<'a>(
    channel: &'a Arc<mcap::Channel>,
) -> Result<&'a Arc<mcap::Schema<'a>>, McapReaderError> {
    channel
        .schema
        .as_ref()
        .ok_or_else(|| McapReaderError::SchemaRequired {
            topic: channel.topic.clone(),
            channel_id: channel.id,
        })
}
