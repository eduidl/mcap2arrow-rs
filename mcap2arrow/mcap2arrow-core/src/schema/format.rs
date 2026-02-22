use std::fmt::{Error, Result, Write as _};

use super::{DataTypeDef, FieldDef};

/// Format field definitions in a readable style:
/// primitive fields are rendered in one line, compound fields are pretty-printed.
/// Nested fields follow the same rule.
pub fn format_field_defs(
    fields: impl AsRef<[FieldDef]>,
) -> std::result::Result<String, Error> {
    let fields = fields.as_ref();
    let mut out = String::new();

    for field in fields.iter() {
        format_field(field, 0, &mut out)?;
    }

    Ok(out)
}

fn format_field(field: &FieldDef, indent: usize, out: &mut String) -> Result {
    format_labeled_type(
        &field.name,
        &field.element.data_type,
        field.element.nullable,
        indent,
        out,
    )
}

fn format_data_type(
    data_type: &DataTypeDef,
    nullable: bool,
    indent: usize,
    out: &mut String,
) -> Result {
    let pad = " ".repeat(indent);
    writeln!(out, "{pad}type: {}", data_type.type_name())?;
    writeln!(out, "{pad}nullable: {}", nullable)?;

    match data_type {
        DataTypeDef::Struct(fields) => {
            writeln!(out, "{pad}fields:")?;
            for child in fields.iter() {
                format_field(child, indent + 4, out)?;
            }
        }
        DataTypeDef::List(elem) => {
            format_labeled_type("item", &elem.data_type, elem.nullable, indent, out)?;
        }
        DataTypeDef::Array(elem, size) => {
            format_labeled_type("item", &elem.data_type, elem.nullable, indent, out)?;
            writeln!(out, "{pad}size: {}", size)?;
        }
        DataTypeDef::Map { key, value } => {
            format_labeled_type("key", &key.data_type, key.nullable, indent, out)?;
            format_labeled_type("value", &value.data_type, value.nullable, indent, out)?;
        }
        _ => unreachable!("{data_type:?} is not a compound type"),
    }

    Ok(())
}

fn format_labeled_type(
    label: &str,
    data_type: &DataTypeDef,
    nullable: bool,
    indent: usize,
    out: &mut String,
) -> Result {
    let pad = " ".repeat(indent);
    if data_type.is_primitive() {
        writeln!(
            out,
            "{pad}{label}: {{ type: {}, nullable: {nullable} }}",
            data_type.type_name()
        )?;
    } else {
        writeln!(out, "{pad}{label}:")?;
        format_data_type(data_type, nullable, indent + 4, out)?;
    }
    Ok(())
}
