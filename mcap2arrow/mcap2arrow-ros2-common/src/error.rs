//! Shared error type for ROS 2 parsing/resolution/decoding helpers.

/// Lightweight error wrapper used in internal ROS 2 modules instead of raw strings.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct Ros2Error(pub String);

impl From<String> for Ros2Error {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Ros2Error {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}
