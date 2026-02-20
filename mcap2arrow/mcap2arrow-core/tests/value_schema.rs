use mcap2arrow_core::{DataTypeDef, ElementDef, FieldDef, Value};

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
    assert!(matches!(field.element.data_type, DataTypeDef::I64));
    assert!(!field.element.nullable);
}

#[test]
fn element_def_new_sets_all_fields() {
    let element = ElementDef::new(DataTypeDef::I32, true);
    assert!(matches!(element.data_type, DataTypeDef::I32));
    assert!(element.nullable);
}
