use std::collections::HashSet;

use mcap2arrow_core::{EncodingKey, MessageEncoding, SchemaEncoding};

#[test]
fn schema_encoding_known_value_mapping() {
    let enc = SchemaEncoding::from("protobuf");
    assert_eq!(enc, SchemaEncoding::Protobuf);
    assert_eq!(enc.as_str(), "protobuf");
    assert_eq!(enc.to_string(), "protobuf");
}

#[test]
fn schema_encoding_empty_string_maps_to_none() {
    let enc = SchemaEncoding::from("");
    assert_eq!(enc, SchemaEncoding::None);
    assert_eq!(enc.to_string(), "");
}

#[test]
fn schema_encoding_unknown_passthrough() {
    let enc = SchemaEncoding::from("custom/foo");
    assert_eq!(enc, SchemaEncoding::Unknown("custom/foo".to_string()));
    assert_eq!(enc.as_str(), "custom/foo");
}

#[test]
fn message_encoding_known_mappings() {
    assert_eq!(MessageEncoding::from("ros1"), MessageEncoding::Ros1);
    assert_eq!(MessageEncoding::from("cdr"), MessageEncoding::Cdr);
    assert_eq!(MessageEncoding::from("protobuf"), MessageEncoding::Protobuf);
    assert_eq!(MessageEncoding::from("flatbuffer"), MessageEncoding::FlatBuffer);
    assert_eq!(MessageEncoding::from("cbor"), MessageEncoding::Cbor);
    assert_eq!(MessageEncoding::from("msgpack"), MessageEncoding::MsgPack);
    assert_eq!(MessageEncoding::from("json"), MessageEncoding::Json);
}

#[test]
fn message_encoding_unknown_passthrough() {
    let enc = MessageEncoding::from("custom/bar");
    assert_eq!(enc, MessageEncoding::Unknown("custom/bar".to_string()));
    assert_eq!(enc.as_str(), "custom/bar");
}

#[test]
fn message_encoding_display_matches_as_str() {
    let values = [
        MessageEncoding::Ros1,
        MessageEncoding::Cdr,
        MessageEncoding::Protobuf,
        MessageEncoding::FlatBuffer,
        MessageEncoding::Cbor,
        MessageEncoding::MsgPack,
        MessageEncoding::Json,
        MessageEncoding::Unknown("x/y".to_string()),
    ];
    for value in values {
        assert_eq!(value.to_string(), value.as_str());
    }
}

#[test]
fn encoding_key_new_sets_fields() {
    let key = EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json);
    assert_eq!(key.schema_encoding, SchemaEncoding::JsonSchema);
    assert_eq!(key.message_encoding, MessageEncoding::Json);
}

#[test]
fn encoding_key_hash_and_eq_behavior() {
    let a = EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json);
    let a2 = EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json);
    let b = EncodingKey::new(SchemaEncoding::Protobuf, MessageEncoding::Protobuf);

    let mut set = HashSet::new();
    set.insert(a);
    set.insert(a2);
    set.insert(b);

    assert_eq!(set.len(), 2);
}
