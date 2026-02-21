//! Conversion from decoded `DecodedMessage` rows to Arrow `RecordBatch`.
//!
//! The output schema is the given body schema with `@log_time` and
//! `@publish_time` timestamp columns prepended.

mod append;
mod builder;
mod scalar;

use std::sync::Arc;

use arrow::array::{ArrayRef, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use mcap2arrow_core::{DecodedMessage, Value};

/// Convert decoded rows to a RecordBatch.
///
/// `body_schema` must describe only the message body fields (no timestamp columns).
/// The returned `RecordBatch` prepends `@log_time` and `@publish_time`.
///
/// # Panics
/// Panics if:
/// - `rows` is empty.
/// - a row root value is neither `Struct` nor `Null`.
/// - a value shape does not match the provided Arrow data type.
/// - an unsupported Arrow data type is present in `body_schema`.
pub fn arrow_value_rows_to_record_batch(
    body_schema: &Schema,
    rows: &[DecodedMessage],
) -> RecordBatch {
    if rows.is_empty() {
        panic!("Cannot create RecordBatch from empty rows");
    }

    let full_schema = Arc::new(crate::schema_convert::with_timestamp_fields(
        body_schema.clone(),
    ));
    let body_fields = body_schema.fields();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(body_fields.len() + 2);

    arrays.push(Arc::new(
        rows.iter()
            .map(|r| Some(i64::try_from(r.log_time).expect("log_time exceeds i64::MAX")))
            .collect::<TimestampNanosecondArray>()
            .with_timezone(crate::TIMESTAMP_TZ),
    ) as ArrayRef);

    arrays.push(Arc::new(
        rows.iter()
            .map(|r| Some(i64::try_from(r.publish_time).expect("publish_time exceeds i64::MAX")))
            .collect::<TimestampNanosecondArray>()
            .with_timezone(crate::TIMESTAMP_TZ),
    ) as ArrayRef);

    for (i, field) in body_fields.iter().enumerate() {
        let values: Vec<&Value> = rows.iter().map(|r| extract_field(&r.value, i)).collect();
        arrays.push(build_array_from_values(field.data_type(), &values));
    }

    RecordBatch::try_new(full_schema, arrays)
        .expect("RecordBatch::try_new failed: schema/array type mismatch")
}

fn extract_field(root: &Value, field_index: usize) -> &Value {
    match root {
        Value::Struct(children) => children.get(field_index).unwrap_or(&Value::Null),
        Value::Null => &Value::Null,
        other => panic!("expected Struct or Null as message root, got {other:?}"),
    }
}

fn build_array_from_values(dt: &DataType, values: &[&Value]) -> ArrayRef {
    let capacity = match dt {
        DataType::List(_) | DataType::Map(_, _) => values.len().saturating_mul(4),
        _ => values.len(),
    };
    let mut builder = builder::make_builder(dt, capacity);
    for value in values {
        append::append_value_to_builder(&mut builder, dt, value);
    }
    builder.finish()
}
