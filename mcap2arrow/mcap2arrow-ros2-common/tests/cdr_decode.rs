use std::collections::HashMap;

use mcap2arrow_core::Value;
use mcap2arrow_ros2_common::{
    decode_cdr_to_value, PrimitiveType, ResolvedField, ResolvedSchema, ResolvedStruct, ResolvedType,
};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Pad `buf` to the next `n`-byte boundary.
fn align(buf: &mut Vec<u8>, n: usize) {
    let pad = (n - (buf.len() % n)) % n;
    for _ in 0..pad {
        buf.push(0);
    }
}

/// Build a one-struct schema with the given fields and enum map.
fn make_schema(
    fields: Vec<ResolvedField>,
    enums: HashMap<Vec<String>, Vec<String>>,
) -> ResolvedSchema {
    let root = vec!["ex".to_string(), "msg".to_string(), "A".to_string()];
    let mut structs = HashMap::new();
    structs.insert(root.clone(), ResolvedStruct { fields });
    ResolvedSchema {
        root,
        structs,
        enums,
    }
}

/// Build a minimal CDR buffer: 4-byte little-endian encapsulation header + payload.
fn cdr_with_payload(payload: Vec<u8>) -> Vec<u8> {
    let mut buf = vec![0x00, 0x01, 0x00, 0x00]; // CDR_LE encapsulation
    buf.extend(payload);
    buf
}

// ── existing tests ────────────────────────────────────────────────────────────

#[test]
fn decodes_f64_with_encapsulation_relative_alignment() {
    let schema = make_schema(
        vec![
            ResolvedField {
                name: "flag".to_string(),
                ty: ResolvedType::Primitive(PrimitiveType::U8),
                fixed_len: None,
            },
            ResolvedField {
                name: "value".to_string(),
                ty: ResolvedType::Primitive(PrimitiveType::F64),
                fixed_len: None,
            },
        ],
        HashMap::new(),
    );

    let mut cdr = vec![0x00, 0x01, 0x00, 0x00];
    cdr.push(7);
    cdr.extend_from_slice(&[0; 7]);
    cdr.extend_from_slice(&(1.25f64).to_bits().to_le_bytes());

    let value = decode_cdr_to_value(&schema, &cdr).expect("decode should succeed");
    let Value::Struct(fields) = value else {
        panic!("expected root struct");
    };
    assert_eq!(fields.len(), 2);
    assert!(matches!(fields[0], Value::U8(7)));
    assert!(matches!(fields[1], Value::F64(v) if (v - 1.25).abs() < f64::EPSILON));
}

#[test]
fn fails_on_sequence_bound_overflow() {
    let schema = make_schema(
        vec![ResolvedField {
            name: "data".to_string(),
            ty: ResolvedType::Sequence {
                elem: Box::new(ResolvedType::Primitive(PrimitiveType::U8)),
                max_len: Some(1),
            },
            fixed_len: None,
        }],
        HashMap::new(),
    );

    let mut cdr = vec![0x00, 0x01, 0x00, 0x00];
    align(&mut cdr, 4);
    cdr.extend_from_slice(&(2u32).to_le_bytes());
    cdr.extend_from_slice(&[1, 2]);

    let err = decode_cdr_to_value(&schema, &cdr).expect_err("decode should fail");
    assert!(format!("{err:#}").contains("sequence bound overflow"));
}

#[test]
fn decodes_enum_index_to_variant_name() {
    let enum_name = vec!["ex".to_string(), "msg".to_string(), "State".to_string()];
    let schema = make_schema(
        vec![ResolvedField {
            name: "state".to_string(),
            ty: ResolvedType::Enum(enum_name.clone()),
            fixed_len: None,
        }],
        HashMap::from([(enum_name, vec!["OK".to_string(), "WARN".to_string()])]),
    );

    let mut cdr = vec![0x00, 0x01, 0x00, 0x00];
    align(&mut cdr, 4);
    cdr.extend_from_slice(&(1u32).to_le_bytes());

    let value = decode_cdr_to_value(&schema, &cdr).expect("decode should succeed");
    let Value::Struct(fields) = value else {
        panic!("expected root struct");
    };
    assert_eq!(fields.len(), 1);
    assert!(matches!(&fields[0], Value::String(s) if s.as_ref() == "WARN"));
}

#[test]
fn fails_on_string_without_null_terminator() {
    let schema = make_schema(
        vec![ResolvedField {
            name: "name".to_string(),
            ty: ResolvedType::Primitive(PrimitiveType::String),
            fixed_len: None,
        }],
        HashMap::new(),
    );

    let mut cdr = vec![0x00, 0x01, 0x00, 0x00];
    align(&mut cdr, 4);
    cdr.extend_from_slice(&(2u32).to_le_bytes());
    cdr.extend_from_slice(b"ab");

    let err = decode_cdr_to_value(&schema, &cdr).expect_err("decode should fail");
    assert!(format!("{err:#}").contains("string missing null terminator"));
}

// ── new tests ─────────────────────────────────────────────────────────────────

/// Decoding a bool field: 0 → false, non-zero → true.
#[test]
fn decodes_bool_fields() {
    let schema = make_schema(
        vec![
            ResolvedField {
                name: "a".to_string(),
                ty: ResolvedType::Primitive(PrimitiveType::Bool),
                fixed_len: None,
            },
            ResolvedField {
                name: "b".to_string(),
                ty: ResolvedType::Primitive(PrimitiveType::Bool),
                fixed_len: None,
            },
        ],
        HashMap::new(),
    );

    let cdr = cdr_with_payload(vec![0x00, 0x01]);
    let value = decode_cdr_to_value(&schema, &cdr).expect("decode should succeed");
    let Value::Struct(fields) = value else {
        panic!("expected struct");
    };
    assert!(matches!(fields[0], Value::Bool(false)));
    assert!(matches!(fields[1], Value::Bool(true)));
}

/// A fixed-length array field (`fixed_len = Some(3)`) produces `Value::Array`.
#[test]
fn decodes_fixed_length_array() {
    let schema = make_schema(
        vec![ResolvedField {
            name: "coords".to_string(),
            ty: ResolvedType::Primitive(PrimitiveType::I32),
            fixed_len: Some(3),
        }],
        HashMap::new(),
    );

    let mut payload = Vec::new();
    for v in [10i32, 20, 30] {
        // align to 4 within the payload; offset starts at 4 (after header)
        align(&mut payload, 4);
        payload.extend_from_slice(&v.to_le_bytes());
    }
    let cdr = cdr_with_payload(payload);

    let value = decode_cdr_to_value(&schema, &cdr).expect("decode should succeed");
    let Value::Struct(fields) = value else {
        panic!("expected struct");
    };
    let Value::Array(elems) = &fields[0] else {
        panic!("expected array");
    };
    assert_eq!(elems.len(), 3);
    assert!(matches!(elems[0], Value::I32(10)));
    assert!(matches!(elems[1], Value::I32(20)));
    assert!(matches!(elems[2], Value::I32(30)));
}

/// A nested struct field decodes recursively into `Value::Struct`.
#[test]
fn decodes_nested_struct() {
    // Schema: root = { inner: Inner }  where Inner = { x: u32, y: u32 }
    let inner_name = vec!["ex".to_string(), "msg".to_string(), "Inner".to_string()];
    let root_name = vec!["ex".to_string(), "msg".to_string(), "Root".to_string()];

    let inner_struct = ResolvedStruct {
        fields: vec![
            ResolvedField {
                name: "x".to_string(),
                ty: ResolvedType::Primitive(PrimitiveType::U32),
                fixed_len: None,
            },
            ResolvedField {
                name: "y".to_string(),
                ty: ResolvedType::Primitive(PrimitiveType::U32),
                fixed_len: None,
            },
        ],
    };
    let root_struct = ResolvedStruct {
        fields: vec![ResolvedField {
            name: "inner".to_string(),
            ty: ResolvedType::Struct(inner_name.clone()),
            fixed_len: None,
        }],
    };

    let mut structs = HashMap::new();
    structs.insert(inner_name, inner_struct);
    structs.insert(root_name.clone(), root_struct);
    let schema = ResolvedSchema {
        root: root_name,
        structs,
        enums: HashMap::new(),
    };

    let mut payload = Vec::new();
    payload.extend_from_slice(&42u32.to_le_bytes()); // x (already aligned at offset 4)
    payload.extend_from_slice(&99u32.to_le_bytes()); // y
    let cdr = cdr_with_payload(payload);

    let value = decode_cdr_to_value(&schema, &cdr).expect("decode should succeed");
    let Value::Struct(root_fields) = value else {
        panic!("expected root struct");
    };
    let Value::Struct(inner_fields) = &root_fields[0] else {
        panic!("expected inner struct");
    };
    assert!(matches!(inner_fields[0], Value::U32(42)));
    assert!(matches!(inner_fields[1], Value::U32(99)));
}

/// An enum index beyond the variant list falls back to the raw integer string.
#[test]
fn decodes_out_of_range_enum_index_as_raw_number() {
    let enum_name = vec!["ex".to_string(), "msg".to_string(), "E".to_string()];
    let schema = make_schema(
        vec![ResolvedField {
            name: "e".to_string(),
            ty: ResolvedType::Enum(enum_name.clone()),
            fixed_len: None,
        }],
        HashMap::from([(enum_name, vec!["A".to_string()])]),
    );

    // Index 5 is out of range (only variant 0 exists).
    let mut payload = Vec::new();
    payload.extend_from_slice(&5u32.to_le_bytes());
    let cdr = cdr_with_payload(payload);

    let value = decode_cdr_to_value(&schema, &cdr).expect("decode should succeed");
    let Value::Struct(fields) = value else {
        panic!("expected struct");
    };
    assert!(matches!(&fields[0], Value::String(s) if s.as_ref() == "5"));
}

/// An incomplete CDR encapsulation header (< 4 bytes) returns an error.
#[test]
fn fails_on_truncated_encapsulation_header() {
    let schema = make_schema(vec![], HashMap::new());
    let cdr = vec![0x00, 0x01]; // only 2 bytes instead of 4
    let err = decode_cdr_to_value(&schema, &cdr).expect_err("should fail");
    assert!(format!("{err:#}").contains("incomplete encapsulation header"));
}

/// An unsupported endianness byte in the encapsulation header returns an error.
#[test]
fn fails_on_big_endian_encapsulation() {
    let schema = make_schema(vec![], HashMap::new());
    // Byte 1 = 0x00 → big-endian (only 0x01 little-endian is supported).
    let cdr = vec![0x00, 0x00, 0x00, 0x00];
    let err = decode_cdr_to_value(&schema, &cdr).expect_err("should fail");
    assert!(format!("{err:#}").contains("unsupported CDR endianness"));
}

/// An unbounded sequence decodes its length-prefixed elements into `Value::List`.
#[test]
fn decodes_unbounded_sequence() {
    let schema = make_schema(
        vec![ResolvedField {
            name: "vals".to_string(),
            ty: ResolvedType::Sequence {
                elem: Box::new(ResolvedType::Primitive(PrimitiveType::U8)),
                max_len: None,
            },
            fixed_len: None,
        }],
        HashMap::new(),
    );

    let mut payload = Vec::new();
    payload.extend_from_slice(&3u32.to_le_bytes()); // length = 3
    payload.extend_from_slice(&[10, 20, 30]);
    let cdr = cdr_with_payload(payload);

    let value = decode_cdr_to_value(&schema, &cdr).expect("decode should succeed");
    let Value::Struct(fields) = value else {
        panic!("expected struct");
    };
    let Value::List(elems) = &fields[0] else {
        panic!("expected list");
    };
    assert_eq!(elems.len(), 3);
    assert!(matches!(elems[0], Value::U8(10)));
    assert!(matches!(elems[1], Value::U8(20)));
    assert!(matches!(elems[2], Value::U8(30)));
}

/// A null-terminated string decodes correctly to the UTF-8 content before `\0`.
#[test]
fn decodes_string_with_null_terminator() {
    let schema = make_schema(
        vec![ResolvedField {
            name: "label".to_string(),
            ty: ResolvedType::Primitive(PrimitiveType::String),
            fixed_len: None,
        }],
        HashMap::new(),
    );

    let s = b"hello\0";
    let mut payload = Vec::new();
    payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
    payload.extend_from_slice(s);
    let cdr = cdr_with_payload(payload);

    let value = decode_cdr_to_value(&schema, &cdr).expect("decode should succeed");
    let Value::Struct(fields) = value else {
        panic!("expected struct");
    };
    assert!(matches!(&fields[0], Value::String(s) if s.as_ref() == "hello"));
}
