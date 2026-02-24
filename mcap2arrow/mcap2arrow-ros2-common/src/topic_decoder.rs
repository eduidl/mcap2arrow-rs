use mcap2arrow_core::{DecoderError, FieldDefs, TopicDecoder, Value};

use crate::{ResolvedSchema, decode_cdr_to_value, resolved_schema_to_field_defs};

/// Shared ROS 2 CDR topic decoder used by both `ros2msg` and `ros2idl`.
pub struct Ros2CdrTopicDecoder {
    resolved: ResolvedSchema,
    field_defs: FieldDefs,
}

impl Ros2CdrTopicDecoder {
    pub fn new(resolved: ResolvedSchema) -> Self {
        let field_defs = resolved_schema_to_field_defs(&resolved);
        Self {
            resolved,
            field_defs,
        }
    }
}

impl TopicDecoder for Ros2CdrTopicDecoder {
    fn decode(&self, message_data: &[u8]) -> Result<Value, DecoderError> {
        decode_cdr_to_value(&self.resolved, message_data)
    }

    fn field_defs(&self) -> &FieldDefs {
        &self.field_defs
    }
}
