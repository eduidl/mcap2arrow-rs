use std::path::PathBuf;

use mcap2arrow::McapReader;
use mcap2arrow_core::{
    DataTypeDef, EncodingKey, FieldDef, FieldDefs, MessageDecoder, MessageEncoding, SchemaEncoding,
    Value,
};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

struct TestJsonDecoder;

impl MessageDecoder for TestJsonDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json)
    }

    fn decode(&self, _schema_name: &str, _schema_data: &[u8], _message_data: &[u8]) -> Value {
        Value::Struct(vec![Value::I64(1)])
    }

    fn derive_schema(&self, _schema_name: &str, _schema_data: &[u8]) -> FieldDefs {
        vec![FieldDef::new("value", DataTypeDef::I64, true)].into()
    }
}

#[test]
fn for_each_record_batch_emits_batches_by_batch_size() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_batch_size(1)
        .build();

    let mut batch_rows = Vec::new();
    reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |batch| {
            batch_rows.push(batch.num_rows());
            Ok(())
        })
        .unwrap();

    assert_eq!(batch_rows, vec![1, 1]);
}

#[test]
fn for_each_record_batch_flushes_final_partial_batch() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_batch_size(3)
        .build();

    let mut batch_rows = Vec::new();
    reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |batch| {
            batch_rows.push(batch.num_rows());
            Ok(())
        })
        .unwrap();

    assert_eq!(batch_rows, vec![2]);
}

#[test]
fn for_each_record_batch_propagates_callback_error() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_batch_size(1)
        .build();

    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |_batch| {
            Err("callback failed".into())
        })
        .unwrap_err();

    assert!(err.to_string().contains("callback failed"));
}

#[test]
fn for_each_record_batch_errors_when_decoder_is_missing() {
    let reader = McapReader::builder().with_batch_size(1).build();

    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |_batch| {
            Ok(())
        })
        .unwrap_err();

    assert!(err.to_string().contains("no decoder registered"));
}
