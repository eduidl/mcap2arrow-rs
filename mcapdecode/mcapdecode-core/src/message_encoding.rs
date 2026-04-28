use std::fmt;

/// Message encodings defined in the mcap spec registry.
/// <https://mcap.dev/spec/registry>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MessageEncoding {
    /// ROS 1 (`ros1`)
    Ros1,
    /// CDR - used by ROS 2 (`cdr`)
    Cdr,
    /// Protocol Buffers (`protobuf`)
    Protobuf,
    /// FlatBuffers (`flatbuffer`)
    FlatBuffer,
    /// CBOR (`cbor`)
    Cbor,
    /// MessagePack (`msgpack`)
    MsgPack,
    /// JSON (`json`)
    Json,
    /// Unknown/custom encoding
    Unknown(String),
}

impl MessageEncoding {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Ros1 => "ros1",
            Self::Cdr => "cdr",
            Self::Protobuf => "protobuf",
            Self::FlatBuffer => "flatbuffer",
            Self::Cbor => "cbor",
            Self::MsgPack => "msgpack",
            Self::Json => "json",
            Self::Unknown(s) => s,
        }
    }
}

impl From<&str> for MessageEncoding {
    fn from(s: &str) -> Self {
        match s {
            "ros1" => Self::Ros1,
            "cdr" => Self::Cdr,
            "protobuf" => Self::Protobuf,
            "flatbuffer" => Self::FlatBuffer,
            "cbor" => Self::Cbor,
            "msgpack" => Self::MsgPack,
            "json" => Self::Json,
            other => Self::Unknown(other.to_string()),
        }
    }
}

impl fmt::Display for MessageEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
