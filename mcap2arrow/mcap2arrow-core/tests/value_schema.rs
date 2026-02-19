use mcap2arrow_core::{DataTypeDef, FieldDef, Value};

#[test]
fn value_string_creates_arc_str_value() {
    let value = Value::string("hello");
    match value {
        Value::String(s) => assert_eq!(&*s, "hello"),
        other => panic!("unexpected value variant: {:?}", other),
    }
}

#[test]
fn field_def_new_sets_all_fields() {
    let field = FieldDef::new("count", DataTypeDef::I64, false);
    assert_eq!(field.name, "count");
    assert!(matches!(field.data_type, DataTypeDef::I64));
    assert!(!field.nullable);
}
