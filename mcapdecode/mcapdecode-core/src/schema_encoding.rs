use std::fmt;

/// Schema encodings defined in the mcap spec registry.
/// <https://mcap.dev/spec/registry>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchemaEncoding {
    /// No schema (self-describing formats like JSON)
    None,
    /// Protocol Buffers (`protobuf`)
    Protobuf,
    /// FlatBuffers (`flatbuffer`)
    FlatBuffer,
    /// ROS 1 Message (`ros1msg`)
    Ros1Msg,
    /// ROS 2 Message (`ros2msg`)
    Ros2Msg,
    /// ROS 2 IDL (`ros2idl`)
    Ros2Idl,
    /// OMG IDL (`omgidl`)
    OmgIdl,
    /// JSON Schema (`jsonschema`)
    JsonSchema,
    /// Unknown/custom encoding
    Unknown(String),
}

impl SchemaEncoding {
    pub fn as_str(&self) -> &str {
        match self {
            Self::None => "",
            Self::Protobuf => "protobuf",
            Self::FlatBuffer => "flatbuffer",
            Self::Ros1Msg => "ros1msg",
            Self::Ros2Msg => "ros2msg",
            Self::Ros2Idl => "ros2idl",
            Self::OmgIdl => "omgidl",
            Self::JsonSchema => "jsonschema",
            Self::Unknown(s) => s,
        }
    }
}

impl From<&str> for SchemaEncoding {
    fn from(s: &str) -> Self {
        match s {
            "" => Self::None,
            "protobuf" => Self::Protobuf,
            "flatbuffer" => Self::FlatBuffer,
            "ros1msg" => Self::Ros1Msg,
            "ros2msg" => Self::Ros2Msg,
            "ros2idl" => Self::Ros2Idl,
            "omgidl" => Self::OmgIdl,
            "jsonschema" => Self::JsonSchema,
            other => Self::Unknown(other.to_string()),
        }
    }
}

impl fmt::Display for SchemaEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
