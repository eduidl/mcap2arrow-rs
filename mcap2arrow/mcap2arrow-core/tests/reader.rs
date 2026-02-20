use std::path::PathBuf;

use mcap2arrow_core::{
    DataTypeDef, EncodingKey, FieldDef, McapReader, McapReaderError, MessageDecoder,
    MessageEncoding, SchemaEncoding, Value,
};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

struct TestJsonDecoder;
struct OverriddenJsonDecoder;

impl MessageDecoder for TestJsonDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json)
    }

    fn decode(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
        _message_data: &[u8],
    ) -> Value {
        Value::Struct(vec![Value::I64(1)])
    }

    fn derive_schema(&self, _schema_name: &str, _schema_data: &[u8]) -> Vec<FieldDef> {
        vec![FieldDef::new("value", DataTypeDef::I64, true)]
    }
}

impl MessageDecoder for OverriddenJsonDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json)
    }

    fn decode(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
        _message_data: &[u8],
    ) -> Value {
        Value::Struct(vec![Value::I64(2)])
    }

    fn derive_schema(&self, _schema_name: &str, _schema_data: &[u8]) -> Vec<FieldDef> {
        vec![FieldDef::new("value", DataTypeDef::I64, true)]
    }
}

#[test]
fn message_count_with_summary() {
    let reader = McapReader::new();
    let path = fixture_path("with_summary.mcap");

    assert_eq!(reader.message_count(&path, None).unwrap(), Some(3));
    assert_eq!(reader.message_count(&path, Some("/decoded")).unwrap(), Some(2));
}

#[test]
fn message_count_no_summary_returns_error() {
    let reader = McapReader::new();
    let path = fixture_path("no_summary.mcap");
    assert!(matches!(
        reader.message_count(&path, None),
        Err(McapReaderError::SummaryNotAvailable { .. })
    ));
}

#[test]
fn message_count_unknown_topic_is_zero() {
    let reader = McapReader::new();
    let path = fixture_path("with_summary.mcap");
    assert_eq!(reader.message_count(&path, Some("/unknown")).unwrap(), Some(0));
}

#[test]
fn list_topics_from_summary_allows_schema_less_channel() {
    let reader = McapReader::new();
    let topics = reader.list_topics(&fixture_path("with_summary.mcap")).unwrap();
    assert_eq!(topics.len(), 2);
    assert_eq!(topics[0].topic, "/decoded");
    assert_eq!(topics[1].topic, "/raw");

    assert_eq!(topics[0].schema_name, "test.Msg");
    assert_eq!(topics[0].schema_encoding, SchemaEncoding::JsonSchema);
    assert_eq!(topics[0].message_encoding, MessageEncoding::Json);
    assert_eq!(topics[0].message_count, 2);

    assert_eq!(topics[1].schema_name, "");
    assert_eq!(topics[1].schema_encoding, SchemaEncoding::None);
    assert_eq!(
        topics[1].message_encoding,
        MessageEncoding::Unknown("application/octet-stream".to_string())
    );
    assert_eq!(topics[1].message_count, 1);
}

#[test]
fn list_topics_without_summary_returns_error() {
    let reader = McapReader::new();
    assert!(matches!(
        reader.list_topics(&fixture_path("no_summary.mcap")),
        Err(McapReaderError::SummaryNotAvailable { .. })
    ));
}

#[test]
fn for_each_message_without_decoder_returns_error() {
    let reader = McapReader::new();
    let err = reader
        .for_each_message(&fixture_path("with_summary.mcap"), Some("/decoded"), |_msg| {
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, McapReaderError::NoDecoder { .. }));
}

#[test]
fn for_each_message_non_matching_topic_filter_is_ok_and_no_callback() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let mut called = false;
    reader
        .for_each_message(&fixture_path("with_summary.mcap"), Some("/unknown"), |_msg| {
            called = true;
            Ok(())
        })
        .unwrap();
    assert!(!called);
}

#[test]
fn for_each_message_with_decoder_decodes_only_supported_channel() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let mut decoded_topics = Vec::new();
    let mut first_schema_name = None;
    let mut first_schema_encoding = None;
    let mut first_message_encoding = None;
    let mut first_log_time = None;
    let mut first_publish_time = None;

    reader
        .for_each_message(&fixture_path("with_summary.mcap"), Some("/decoded"), |msg| {
            decoded_topics.push(msg.topic.clone());
            if first_schema_name.is_none() {
                first_schema_name = Some(msg.schema_name.clone());
                first_schema_encoding = Some(msg.schema_encoding.clone());
                first_message_encoding = Some(msg.message_encoding.clone());
                first_log_time = Some(msg.log_time);
                first_publish_time = Some(msg.publish_time);
            }
            assert!(matches!(msg.value, Value::Struct(_)));
            Ok(())
        })
        .unwrap();

    assert_eq!(
        decoded_topics,
        vec!["/decoded".to_string(), "/decoded".to_string()]
    );
    assert_eq!(first_schema_name.as_deref(), Some("test.Msg"));
    assert_eq!(first_schema_encoding, Some(SchemaEncoding::JsonSchema));
    assert_eq!(first_message_encoding, Some(MessageEncoding::Json));
    let log_time = first_log_time.expect("expected decoded message log_time");
    let publish_time = first_publish_time.expect("expected decoded message publish_time");
    assert!(log_time > 0);
    assert!(publish_time >= log_time);
}

#[test]
fn for_each_message_errors_when_schema_is_missing() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let err = reader
        .for_each_message(&fixture_path("with_summary.mcap"), None, |_msg| Ok(()))
        .unwrap_err();
    assert!(matches!(
        err,
        McapReaderError::SchemaRequired { ref topic, .. } if topic == "/raw"
    ));
}

#[test]
fn for_each_message_topic_filter_limits_output() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let mut topics = Vec::new();

    reader
        .for_each_message(&fixture_path("with_summary.mcap"), Some("/decoded"), |msg| {
            topics.push(msg.topic);
            Ok(())
        })
        .unwrap();

    assert_eq!(
        topics,
        vec!["/decoded".to_string(), "/decoded".to_string()]
    );
}

#[test]
fn for_each_message_propagates_callback_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let err = reader
        .for_each_message(&fixture_path("with_summary.mcap"), Some("/decoded"), |_msg| {
            Err("callback failed".into())
        })
        .unwrap_err();
    assert!(matches!(err, McapReaderError::Callback(_)));
    assert!(err.to_string().contains("callback failed"));
}

#[test]
fn register_decoder_overwrites_same_encoding_key() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    reader.register_decoder(Box::new(OverriddenJsonDecoder));

    let mut first = None;
    reader
        .for_each_message(&fixture_path("with_summary.mcap"), Some("/decoded"), |msg| {
            if first.is_none() {
                first = Some(msg.value);
            }
            Ok(())
        })
        .unwrap();

    match first {
        Some(Value::Struct(fields)) => match fields.first() {
            Some(Value::I64(v)) => assert_eq!(*v, 2),
            other => panic!("unexpected first struct field: {:?}", other),
        },
        other => panic!("unexpected decoded value: {:?}", other),
    }
}
