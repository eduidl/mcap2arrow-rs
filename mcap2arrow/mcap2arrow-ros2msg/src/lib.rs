//! ROS 2 .msg → CDR decoder for `mcap2arrow`.
//!
//! Implements [`MessageDecoder`] for the
//! `(schema_encoding = ros2msg, message_encoding = cdr)` key.
//!
//! Unlike the IDL decoder, a .msg schema is a single file, so no
//! multi-section bundle splitting is needed.  `builtin_interfaces` types
//! (`Time`, `Duration`) are injected automatically via
//! [`resolve_single_struct`].
//!
//! # Pipeline
//!
//! ```text
//! schema bytes (UTF-8 .msg)
//!   └─ parse_msg             – re_ros_msg parser → StructDef
//!       └─ resolve_single_struct  – type resolution → ResolvedSchema
//!           └─ decode_cdr_to_value – CDR bytes → Value
//! ```

mod parser;

use mcap2arrow_core::{
    DecoderError, EncodingKey, MessageDecoder, MessageEncoding, SchemaEncoding, TopicDecoder,
};
use mcap2arrow_ros2_common::{ResolvedSchema, Ros2CdrTopicDecoder, resolve_single_struct};
pub use parser::parse_msg;

/// [`MessageDecoder`] for ROS 2 .msg schemas with CDR-encoded messages.
pub struct Ros2MsgDecoder;

impl Ros2MsgDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Ros2MsgDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageDecoder for Ros2MsgDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::Ros2Msg, MessageEncoding::Cdr)
    }

    fn build_topic_decoder(
        &self,
        schema_name: &str,
        schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError> {
        let resolved = resolve_for_cdr(schema_name, schema_data)?;
        Ok(Box::new(Ros2CdrTopicDecoder::new(resolved)))
    }
}

/// Parse and resolve a .msg schema into a [`ResolvedSchema`] ready for CDR decoding.
pub fn resolve_for_cdr(
    schema_name: &str,
    schema_data: &[u8],
) -> Result<ResolvedSchema, DecoderError> {
    let schema_str = std::str::from_utf8(schema_data).map_err(|e| DecoderError::SchemaParse {
        schema_name: schema_name.to_string(),
        source: Box::new(e),
    })?;
    let struct_def = parse_msg(schema_name, schema_str).map_err(|e| DecoderError::SchemaParse {
        schema_name: schema_name.to_string(),
        source: e.into(),
    })?;
    resolve_single_struct(schema_name, struct_def).map_err(|e| DecoderError::SchemaParse {
        schema_name: schema_name.to_string(),
        source: e.into(),
    })
}
