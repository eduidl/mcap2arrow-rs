use arrow::error::ArrowError;
use mcap2arrow_core::ValueTypeError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ArrowConvertError {
    #[error("Cannot create RecordBatch from empty rows")]
    EmptyRows,
    #[error("value type mismatch: {0}")]
    ValueType(#[from] ValueTypeError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}
