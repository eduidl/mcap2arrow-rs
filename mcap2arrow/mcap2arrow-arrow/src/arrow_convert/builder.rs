use arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, MapBuilder,
    MapFieldNames, NullBuilder, StringBuilder, StructBuilder, TimestampNanosecondBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, TimeUnit};

pub(super) fn make_builder(dt: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 64)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 64)),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Box::new(TimestampNanosecondBuilder::with_capacity(capacity))
        }
        DataType::List(field) => {
            let child = make_builder(field.data_type(), capacity);
            Box::new(ListBuilder::new(child).with_field(field.clone()))
        }
        DataType::FixedSizeList(field, size) => {
            let child = make_builder(field.data_type(), capacity * (*size as usize));
            Box::new(FixedSizeListBuilder::new(child, *size).with_field(field.clone()))
        }
        DataType::Struct(fields) => {
            let child_builders: Vec<Box<dyn ArrayBuilder>> = fields
                .iter()
                .map(|f| make_builder(f.data_type(), capacity))
                .collect();
            let fields_vec: Vec<Field> = fields.iter().map(|f| f.as_ref().clone()).collect();
            Box::new(StructBuilder::new(fields_vec, child_builders))
        }
        DataType::Map(entry_field, _) => {
            let (key_field, value_field) = match entry_field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    (fields[0].clone(), fields[1].clone())
                }
                other => panic!("Map entry field must be Struct with 2 fields, got: {other:?}"),
            };
            let key_builder = make_builder(key_field.data_type(), capacity);
            let value_builder = make_builder(value_field.data_type(), capacity);
            Box::new(
                MapBuilder::new(
                    Some(MapFieldNames {
                        entry: entry_field.name().to_string(),
                        key: key_field.name().to_string(),
                        value: value_field.name().to_string(),
                    }),
                    key_builder,
                    value_builder,
                )
                .with_keys_field(key_field)
                .with_values_field(value_field),
            )
        }
        other => panic!("unsupported DataType for builder: {other:?}"),
    }
}
