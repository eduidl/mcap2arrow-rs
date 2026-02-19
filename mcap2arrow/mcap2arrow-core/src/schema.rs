//! Arrow-independent schema intermediate representation.

/// Arrow-independent data type definition for schema intermediate representation.
///
/// Variant names mirror [`Value`] for consistency (values ↔ types).
#[derive(Debug, Clone, PartialEq)]
pub enum DataTypeDef {
    Null,
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    String,
    Bytes,
    Struct(Vec<FieldDef>),
    List(Box<FieldDef>),
    Array(Box<FieldDef>, usize),
    Map {
        key: Box<FieldDef>,
        value: Box<FieldDef>,
    },
}

/// Arrow-independent field definition for schema intermediate representation.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub data_type: DataTypeDef,
    pub nullable: bool,
}

impl FieldDef {
    pub fn new(name: impl Into<String>, data_type: DataTypeDef, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}
