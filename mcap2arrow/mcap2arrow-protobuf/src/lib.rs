//! Protobuf [`MessageDecoder`] implementation for the mcap2arrow pipeline.
//!
//! This crate provides [`ProtobufDecoder`], which decodes protobuf-encoded
//! MCAP messages into the intermediate [`Value`] representation used by
//! mcap2arrow-core.  It also re-exports the lower-level helpers
//! [`decode_protobuf_to_value`] and [`protobuf_descriptor_to_schema`] for
//! direct use.

mod policy;
mod proto_to_arrow;
mod schema;

use mcap2arrow_core::{
    EncodingKey, FieldDef, MessageDecoder, MessageEncoding, SchemaEncoding, Value,
};
pub use policy::PresencePolicy;
pub use proto_to_arrow::{decode_protobuf_to_value, decode_protobuf_to_value_with_policy};
pub use schema::{protobuf_descriptor_to_schema, protobuf_descriptor_to_schema_with_policy};

/// Stateless decoder that converts protobuf-encoded MCAP messages into
/// [`Value`] / [`FieldDef`] via the [`MessageDecoder`] trait.
pub struct ProtobufDecoder {
    presence_policy: PresencePolicy,
}

impl ProtobufDecoder {
    pub fn new() -> Self {
        Self::new_with_presence_policy(PresencePolicy::PresenceAware)
    }

    pub fn new_with_presence_policy(presence_policy: PresencePolicy) -> Self {
        Self { presence_policy }
    }
}

impl Default for ProtobufDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageDecoder for ProtobufDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::Protobuf, MessageEncoding::Protobuf)
    }

    fn decode(&self, schema_name: &str, schema_data: &[u8], message_data: &[u8]) -> Value {
        decode_protobuf_to_value_with_policy(
            schema_name,
            schema_data,
            message_data,
            self.presence_policy,
        )
    }

    fn derive_schema(&self, schema_name: &str, schema_data: &[u8]) -> Vec<FieldDef> {
        protobuf_descriptor_to_schema_with_policy(schema_name, schema_data, self.presence_policy)
    }
}
