use mcap2arrow_core::{format_field_defs, DataTypeDef, ElementDef, FieldDef, FieldDefs};

#[test]
fn nested_struct_keeps_type_line_and_indentation() -> Result<(), std::fmt::Error> {
    let fields = vec![FieldDef::new(
        "field_root",
        DataTypeDef::Struct(vec![
            FieldDef::new("field_a", DataTypeDef::F64, true),
            FieldDef::new(
                "field_b",
                DataTypeDef::Struct(
                    vec![FieldDef::new("field_c", DataTypeDef::String, true)].into(),
                ),
                true,
            ),
        ]
        .into()),
        true,
    )];

    let text = format_field_defs(&fields)?;
    let expected = "\
field_root:
    type: struct
    nullable: true
    fields:
        field_a: { type: f64, nullable: true }
        field_b:
            type: struct
            nullable: true
            fields:
                field_c: { type: string, nullable: true }
";
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn list_of_complex_item_is_rendered_as_block() -> Result<(), std::fmt::Error> {
    let fields = vec![FieldDef::new(
        "field_root",
        DataTypeDef::Struct(vec![FieldDef::new(
            "field_list",
            DataTypeDef::List(Box::new(ElementDef::new(
                DataTypeDef::Struct(vec![
                    FieldDef::new("item_a", DataTypeDef::I32, true),
                    FieldDef::new("item_b", DataTypeDef::String, true),
                ]
                .into()),
                true,
            ))),
            true,
        )]
        .into()),
        true,
    )];

    let text = format_field_defs(&fields)?;
    let expected = "\
field_root:
    type: struct
    nullable: true
    fields:
        field_list:
            type: list
            nullable: true
            item:
                type: struct
                nullable: true
                fields:
                    item_a: { type: i32, nullable: true }
                    item_b: { type: string, nullable: true }
";
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn field_defs_display_matches_formatter() -> Result<(), std::fmt::Error> {
    let fields: FieldDefs = vec![FieldDef::new("field_a", DataTypeDef::I32, false)].into();
    assert_eq!(fields.to_string(), format_field_defs(fields.as_slice())?);
    Ok(())
}
