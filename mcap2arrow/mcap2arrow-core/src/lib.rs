//! Encoding-agnostic core types and decoder contracts for `mcap2arrow`.
//!
//! This crate provides Arrow-independent intermediate representations
//! ([`Value`] / [`DataTypeDef`]) and the [`MessageDecoder`] trait.

mod decoder;
mod message;
mod message_encoding;
mod schema;
mod schema_encoding;
mod value;

pub use decoder::{EncodingKey, MessageDecoder};
pub use message::DecodedMessage;
pub use message_encoding::MessageEncoding;
pub use schema::{DataTypeDef, ElementDef, FieldDef};
pub use schema_encoding::SchemaEncoding;
pub use value::Value;
