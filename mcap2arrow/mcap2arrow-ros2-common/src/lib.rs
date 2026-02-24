//! Shared types, CDR decoding, and schema conversion used by both
//! `mcap2arrow-ros2idl` and `mcap2arrow-ros2msg`.
//!
//! Key components:
//! - [`ast`] — AST types produced by IDL/msg parsers
//! - [`cdr`] — CDR byte stream → [`mcap2arrow_core::Value`] decoder
//! - [`schema`] — [`type_resolver::ResolvedSchema`] → [`mcap2arrow_core::FieldDefs`] conversion
//! - [`type_resolver`] — type-name resolution and injection of ROS 2 builtin types

pub mod ast;
mod cdr;
mod error;
mod schema;
mod topic_decoder;
mod type_resolver;

pub use ast::{ConstDef, EnumDef, FieldDef, ParsedSection, PrimitiveType, StructDef, TypeExpr};
pub use cdr::decode_cdr_to_value;
pub use error::Ros2Error;
pub use schema::resolved_schema_to_field_defs;
pub use topic_decoder::Ros2CdrTopicDecoder;
pub use type_resolver::{
    ResolvedField, ResolvedSchema, ResolvedStruct, ResolvedType, ensure_builtin_structs,
    resolve_parsed_section, resolve_single_struct, resolve_struct,
};
