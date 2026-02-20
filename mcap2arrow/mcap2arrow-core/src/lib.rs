//! Encoding-agnostic core for reading MCAP files and converting messages into
//! Arrow-independent intermediate representations ([`Value`] / [`DataTypeDef`]).

mod decoder;
mod error;
mod message;
mod message_encoding;
mod reader;
mod schema;
mod schema_encoding;
mod value;

pub use decoder::{EncodingKey, MessageDecoder};
pub use error::McapReaderError;
pub use message::{TopicInfo, TypedMessage};
pub use message_encoding::MessageEncoding;
pub use reader::McapReader;
pub use schema::{DataTypeDef, ElementDef, FieldDef};
pub use schema_encoding::SchemaEncoding;
pub use value::Value;
