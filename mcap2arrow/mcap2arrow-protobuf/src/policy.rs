/// Policy for handling protobuf field presence when decoding values and
/// deriving nullability in schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PresencePolicy {
    /// Legacy behavior: always read fields via protobuf default semantics.
    ///
    /// Missing fields are materialized as default values and all fields are
    /// treated as nullable in derived schema.
    AlwaysDefault,
    /// Presence-aware behavior (default):
    ///
    /// If a field supports presence and is not set, decode it as `Value::Null`.
    /// For fields that do not support presence, use protobuf default values.
    #[default]
    PresenceAware,
}
