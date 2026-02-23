use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::array::Int64Array;
use mcap2arrow::{McapReader, McapReaderError};
use mcap2arrow_core::{
    DataTypeDef, DecoderError, EncodingKey, FieldDef, FieldDefs, MessageDecoder, MessageEncoding,
    SchemaEncoding, Value,
};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn collect_i64_values(reader: &McapReader, path: &Path, topic: &str) -> Vec<i64> {
    let mut values = Vec::new();
    reader
        .for_each_record_batch(path, topic, |batch| {
            let value_idx = batch
                .schema()
                .index_of("value")
                .expect("missing 'value' column");
            let values_col = batch
                .column(value_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("expected Int64Array for 'value' column");

            for i in 0..values_col.len() {
                values.push(values_col.value(i));
            }
            Ok(())
        })
        .unwrap();
    values
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
    ) -> Result<Value, DecoderError> {
        Ok(Value::Struct(vec![Value::I64(1)]))
    }

    fn derive_schema(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
    ) -> Result<FieldDefs, DecoderError> {
        Ok(vec![FieldDef::new("value", DataTypeDef::I64, true)].into())
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
    ) -> Result<Value, DecoderError> {
        Ok(Value::Struct(vec![Value::I64(2)]))
    }

    fn derive_schema(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
    ) -> Result<FieldDefs, DecoderError> {
        Ok(vec![FieldDef::new("value", DataTypeDef::I64, true)].into())
    }
}

#[test]
fn message_count_with_summary() {
    let reader = McapReader::new();
    let path = fixture_path("with_summary.mcap");

    assert_eq!(reader.message_count(&path, "/decoded").unwrap(), 2);
}

#[test]
fn message_count_no_summary_returns_error() {
    let reader = McapReader::new();
    let path = fixture_path("no_summary.mcap");
    assert!(matches!(
        reader.message_count(&path, "/decoded"),
        Err(McapReaderError::SummaryNotAvailable { .. })
    ));
}

#[test]
fn message_count_unknown_topic_returns_error() {
    let reader = McapReader::new();
    let path = fixture_path("with_summary.mcap");
    assert!(matches!(
        reader.message_count(&path, "/unknown"),
        Err(McapReaderError::TopicNotFound { .. })
    ));
}

#[test]
fn builder_default_matches_new_without_decoders() {
    let new_reader = McapReader::new();
    let built_reader = McapReader::builder().build();
    let path = fixture_path("with_summary.mcap");

    assert_eq!(
        new_reader.message_count(&path, "/decoded").unwrap(),
        built_reader.message_count(&path, "/decoded").unwrap()
    );
}

#[test]
fn for_each_record_batch_without_decoder_returns_error() {
    let reader = McapReader::new();
    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |_batch| {
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, McapReaderError::NoDecoder { .. }));
}

#[test]
fn for_each_record_batch_unknown_topic_returns_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/unknown", |_batch| {
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(
        err,
        McapReaderError::TopicNotFound { ref topic } if topic == "/unknown"
    ));
}

#[test]
fn for_each_record_batch_errors_when_schema_is_missing() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/raw", |_batch| Ok(()))
        .unwrap_err();
    assert!(matches!(
        err,
        McapReaderError::SchemaRequired { ref topic, .. } if topic == "/raw"
    ));
}

#[test]
fn for_each_record_batch_propagates_callback_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |_batch| {
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

    let values = collect_i64_values(&reader, &fixture_path("with_summary.mcap"), "/decoded");
    assert_eq!(values, vec![2, 2]);
}

#[test]
fn register_shared_decoder_decodes_messages() {
    let mut reader = McapReader::new();
    reader.register_shared_decoder(Arc::new(TestJsonDecoder));

    let values = collect_i64_values(&reader, &fixture_path("with_summary.mcap"), "/decoded");
    assert_eq!(values.len(), 2);
}
