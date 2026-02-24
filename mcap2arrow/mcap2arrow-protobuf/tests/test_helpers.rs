//! Shared helpers for building protobuf `FileDescriptorSet` bytes in tests.

use prost::Message;
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, OneofDescriptorProto,
    field_descriptor_proto::{Label, Type},
};

/// Build a `FileDescriptorSet` containing a single file with the given message
/// types and serialize it to bytes.
pub fn build_fds(file_name: &str, messages: Vec<DescriptorProto>) -> Vec<u8> {
    build_fds_with_enums(file_name, messages, vec![])
}

/// Build a `FileDescriptorSet` with messages and top-level enums.
pub fn build_fds_with_enums(
    file_name: &str,
    messages: Vec<DescriptorProto>,
    enums: Vec<EnumDescriptorProto>,
) -> Vec<u8> {
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some(file_name.to_string()),
            message_type: messages,
            enum_type: enums,
            syntax: Some("proto3".to_string()),
            ..Default::default()
        }],
    };
    fds.encode_to_vec()
}

/// Create a scalar field descriptor.
pub fn scalar_field(name: &str, number: i32, typ: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        r#type: Some(typ.into()),
        label: Some(Label::Optional.into()),
        ..Default::default()
    }
}

/// Create a repeated (list) field descriptor.
pub fn repeated_field(name: &str, number: i32, typ: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        r#type: Some(typ.into()),
        label: Some(Label::Repeated.into()),
        ..Default::default()
    }
}

/// Create a message-typed field descriptor.
pub fn message_field(
    name: &str,
    number: i32,
    type_name: &str,
    label: Label,
) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        r#type: Some(Type::Message.into()),
        type_name: Some(type_name.to_string()),
        label: Some(label.into()),
        ..Default::default()
    }
}

/// Create an enum-typed field descriptor.
pub fn enum_field(name: &str, number: i32, type_name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        r#type: Some(Type::Enum.into()),
        type_name: Some(type_name.to_string()),
        label: Some(Label::Optional.into()),
        ..Default::default()
    }
}

/// Create a simple enum descriptor.
pub fn simple_enum(name: &str, values: &[(&str, i32)]) -> EnumDescriptorProto {
    EnumDescriptorProto {
        name: Some(name.to_string()),
        value: values
            .iter()
            .map(|(n, num)| EnumValueDescriptorProto {
                name: Some(n.to_string()),
                number: Some(*num),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Create a map entry message (protobuf encodes maps as repeated message
/// fields with a special map_entry option).
pub fn map_entry_message(name: &str, key_type: Type, value_type: Type) -> DescriptorProto {
    use prost_types::MessageOptions;
    DescriptorProto {
        name: Some(name.to_string()),
        field: vec![
            scalar_field("key", 1, key_type),
            scalar_field("value", 2, value_type),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Create a proto3 optional scalar field descriptor.
///
/// Caller is responsible for ensuring the containing message has a matching
/// oneof declaration at `oneof_index`.
pub fn proto3_optional_scalar_field(
    name: &str,
    number: i32,
    typ: Type,
    oneof_index: i32,
) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        r#type: Some(typ.into()),
        label: Some(Label::Optional.into()),
        oneof_index: Some(oneof_index),
        proto3_optional: Some(true),
        ..Default::default()
    }
}

/// Create a synthetic oneof declaration used by proto3 optional fields.
pub fn synthetic_oneof(name: &str) -> OneofDescriptorProto {
    OneofDescriptorProto {
        name: Some(name.to_string()),
        ..Default::default()
    }
}
