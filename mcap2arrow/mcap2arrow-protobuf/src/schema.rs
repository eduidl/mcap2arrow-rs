//! Convert a protobuf `FileDescriptorSet` into [`FieldDef`] schema.

use mcap2arrow_core::{DataTypeDef, ElementDef, FieldDef};
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};

use crate::PresencePolicy;

/// Derive an Arrow-independent schema ([`Vec<FieldDef>`]) from the given
/// protobuf `FileDescriptorSet` bytes.
///
/// `schema_name` is the fully-qualified protobuf message name
/// (e.g. `"my.package.MyMessage"`).  `schema_data` must be a valid
/// serialized `google.protobuf.FileDescriptorSet`.
///
/// # Panics
///
/// Panics if the descriptor set cannot be decoded or the target message
/// descriptor is missing.
pub fn protobuf_descriptor_to_schema(
    schema_name: &str,
    schema_data: &[u8],
) -> Vec<FieldDef> {
    protobuf_descriptor_to_schema_with_policy(
        schema_name,
        schema_data,
        PresencePolicy::PresenceAware,
    )
}

/// Derive an Arrow-independent schema with a presence policy.
pub fn protobuf_descriptor_to_schema_with_policy(
    schema_name: &str,
    schema_data: &[u8],
    policy: PresencePolicy,
) -> Vec<FieldDef> {
    let pool = DescriptorPool::decode(schema_data).unwrap_or_else(|e| {
        panic!("failed to decode protobuf descriptor set for '{schema_name}': {e}")
    });
    let message_desc: MessageDescriptor = pool
        .get_message_by_name(schema_name)
        .unwrap_or_else(|| panic!("protobuf message descriptor not found: '{schema_name}'"));
    message_fields_to_field_defs(&message_desc, policy)
}

fn message_fields_to_field_defs(
    desc: &MessageDescriptor,
    policy: PresencePolicy,
) -> Vec<FieldDef> {
    desc.fields()
        .map(|f| field_descriptor_to_field_def(&f, policy))
        .collect()
}

fn field_descriptor_to_field_def(
    fd: &FieldDescriptor,
    policy: PresencePolicy,
) -> FieldDef {
    let inner_dt = kind_to_data_type_def(fd, policy);

    let dt = if fd.is_list() {
        DataTypeDef::List(Box::new(ElementDef::new(inner_dt, false)))
    } else if fd.is_map() {
        let Kind::Message(entry_desc) = fd.kind() else {
            panic!("map field `{}` has non-message kind: {:?}", fd.name(), fd.kind());
        };
        let key_field = entry_desc
            .get_field_by_name("key")
            .unwrap_or_else(|| panic!("map entry `{}` missing key field", fd.name()));
        let value_field = entry_desc
            .get_field_by_name("value")
            .unwrap_or_else(|| panic!("map entry `{}` missing value field", fd.name()));
        let key_dt = kind_to_data_type_def(&key_field, policy);
        let val_dt = kind_to_data_type_def(&value_field, policy);
        DataTypeDef::Map {
            key: Box::new(ElementDef::new(key_dt, false)),
            value: Box::new(ElementDef::new(val_dt, false)),
        }
    } else {
        inner_dt
    };

    let nullable = match policy {
        PresencePolicy::AlwaysDefault => false,
        PresencePolicy::PresenceAware => fd.supports_presence(),
    };
    FieldDef::new(fd.name(), dt, nullable)
}

fn kind_to_data_type_def(fd: &FieldDescriptor, policy: PresencePolicy) -> DataTypeDef {
    match fd.kind() {
        Kind::Double => DataTypeDef::F64,
        Kind::Float => DataTypeDef::F32,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => DataTypeDef::I32,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => DataTypeDef::I64,
        Kind::Uint32 | Kind::Fixed32 => DataTypeDef::U32,
        Kind::Uint64 | Kind::Fixed64 => DataTypeDef::U64,
        Kind::Bool => DataTypeDef::Bool,
        Kind::String => DataTypeDef::String,
        Kind::Bytes => DataTypeDef::Bytes,
        Kind::Enum(_) => DataTypeDef::String,
        Kind::Message(msg_desc) => {
            let fields = message_fields_to_field_defs(&msg_desc, policy);
            DataTypeDef::Struct(fields)
        }
    }
}
