use arrow::datatypes::{DataType, TimeUnit};
use mcap2arrow_core::Value;

pub(super) enum ScalarValue<'a> {
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<&'a str>),
    Binary(Option<&'a [u8]>),
    TimestampNanosecond(Option<i64>),
}

pub(super) fn scalar_value_for_datatype<'a>(
    dt: &DataType,
    value: &'a Value,
) -> Option<ScalarValue<'a>> {
    Some(match dt {
        DataType::Null => ScalarValue::Null,
        DataType::Boolean => ScalarValue::Boolean(value.as_bool()),
        DataType::Int8 => ScalarValue::Int8(value.as_i8()),
        DataType::Int16 => ScalarValue::Int16(value.as_i16()),
        DataType::Int32 => ScalarValue::Int32(value.as_i32()),
        DataType::Int64 => ScalarValue::Int64(value.as_i64()),
        DataType::UInt8 => ScalarValue::UInt8(value.as_u8()),
        DataType::UInt16 => ScalarValue::UInt16(value.as_u16()),
        DataType::UInt32 => ScalarValue::UInt32(value.as_u32()),
        DataType::UInt64 => ScalarValue::UInt64(value.as_u64()),
        DataType::Float32 => ScalarValue::Float32(value.as_f32()),
        DataType::Float64 => ScalarValue::Float64(value.as_f64()),
        DataType::Utf8 => ScalarValue::Utf8(value.as_str()),
        DataType::Binary => ScalarValue::Binary(value.as_bytes()),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            ScalarValue::TimestampNanosecond(value.as_i64())
        }
        _ => return None,
    })
}
