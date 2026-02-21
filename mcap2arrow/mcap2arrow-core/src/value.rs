//! Type-safe intermediate representation produced by message decoders.

use std::sync::Arc;

/// Value produced by message decoders.
/// All types are explicit; no lossy conversions.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(Arc<str>),
    Bytes(Arc<[u8]>),
    Struct(Vec<Value>),
    List(Vec<Value>),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
}

impl Value {
    pub fn string(s: impl AsRef<str>) -> Self {
        Self::String(Arc::from(s.as_ref()))
    }

    pub fn as_i8(&self) -> Option<i8> {
        match self {
            Value::I8(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected I8, got {:?}", self),
        }
    }

    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Value::I16(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected I16, got {:?}", self),
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::I32(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected I32, got {:?}", self),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::I64(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected I64, got {:?}", self),
        }
    }

    pub fn as_u8(&self) -> Option<u8> {
        match self {
            Value::U8(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected U8, got {:?}", self),
        }
    }

    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Value::U16(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected U16, got {:?}", self),
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Value::U32(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected U32, got {:?}", self),
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::U64(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected U64, got {:?}", self),
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected Bool, got {:?}", self),
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Value::F32(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected F32, got {:?}", self),
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::F64(v) => Some(*v),
            Value::Null => None,
            _ => panic!("expected F64, got {:?}", self),
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v.as_ref()),
            Value::Null => None,
            _ => panic!("expected String, got {:?}", self),
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(v) => Some(v.as_ref()),
            Value::Null => None,
            _ => panic!("expected Bytes, got {:?}", self),
        }
    }
}
