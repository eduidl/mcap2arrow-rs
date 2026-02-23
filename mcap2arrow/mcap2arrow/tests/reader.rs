use std::path::PathBuf;

use mcap2arrow::McapReader;
use mcap2arrow_core::{
    DataTypeDef, DecoderError, EncodingKey, FieldDef, FieldDefs, MessageDecoder, MessageEncoding,
    SchemaEncoding, TopicDecoder, Value,
};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

struct TestJsonDecoder;
struct TestJsonTopicDecoder {
    field_defs: FieldDefs,
}

impl MessageDecoder for TestJsonDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json)
    }

    fn build_topic_decoder(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError> {
        Ok(Box::new(TestJsonTopicDecoder {
            field_defs: vec![FieldDef::new("value", DataTypeDef::I64, true)].into(),
        }))
    }
}

impl TopicDecoder for TestJsonTopicDecoder {
    fn decode(&self, _message_data: &[u8]) -> Result<Value, DecoderError> {
        Ok(Value::Struct(vec![Value::I64(1)]))
    }

    fn field_defs(&self) -> &FieldDefs {
        &self.field_defs
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
