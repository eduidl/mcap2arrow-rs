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
    List(Box<ElementDef>),
    Array(Box<ElementDef>, usize),
    Map {
        key: Box<ElementDef>,
        value: Box<ElementDef>,
    },
}

/// Arrow-independent nested element definition used in composite types.
#[derive(Debug, Clone, PartialEq)]
pub struct ElementDef {
    pub data_type: DataTypeDef,
    pub nullable: bool,
}

impl ElementDef {
    pub fn new(data_type: DataTypeDef, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }
}

/// Arrow-independent field definition for schema intermediate representation.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub element: ElementDef,
}

impl FieldDef {
    pub fn new(name: impl Into<String>, data_type: DataTypeDef, nullable: bool) -> Self {
        Self {
            name: name.into(),
            element: ElementDef::new(data_type, nullable),
        }
    }
}
