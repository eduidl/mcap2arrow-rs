use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use mcap2arrow_arrow::{flatten_record_batch, ArrayPolicy, FlattenPolicy, ListPolicy, MapPolicy};

fn make_batch(fields: Vec<Field>, arrays: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn make_struct(children: Vec<(Field, ArrayRef)>) -> (Field, ArrayRef) {
    let arcs: Vec<(Arc<Field>, ArrayRef)> = children
        .into_iter()
        .map(|(f, a)| (Arc::new(f), a))
        .collect();
    let fields = Fields::from(arcs.iter().map(|(f, _)| f.clone()).collect::<Vec<_>>());
    let arrays: Vec<ArrayRef> = arcs.into_iter().map(|(_, a)| a).collect();
    let struct_array = StructArray::from(fields.iter().cloned().zip(arrays).collect::<Vec<_>>());
    let parent_field = Field::new("", DataType::Struct(fields), false);
    (parent_field, Arc::new(struct_array) as ArrayRef)
}

fn drop_all() -> FlattenPolicy {
    FlattenPolicy {
        list: ListPolicy::Drop,
        list_flatten_fixed_size: 1,
        array: ArrayPolicy::Drop,
        map: MapPolicy::Drop,
    }
}

#[test]
fn parse_list_policy_from_str() {
    assert_eq!("drop".parse::<ListPolicy>().unwrap(), ListPolicy::Drop);
    assert_eq!("KEEP".parse::<ListPolicy>().unwrap(), ListPolicy::Keep);
    assert_eq!(
        "flatten-fixed".parse::<ListPolicy>().unwrap(),
        ListPolicy::FlattenFixed
    );
    assert!("flatten-fixed:3".parse::<ListPolicy>().is_err());
}

#[test]
fn parse_array_and_map_policy_from_str() {
    assert_eq!("drop".parse::<ArrayPolicy>().unwrap(), ArrayPolicy::Drop);
    assert_eq!(
        "Flatten".parse::<ArrayPolicy>().unwrap(),
        ArrayPolicy::Flatten
    );
    assert_eq!("keep".parse::<MapPolicy>().unwrap(), MapPolicy::Keep);
    assert!("invalid".parse::<ArrayPolicy>().is_err());
    assert!("invalid".parse::<MapPolicy>().is_err());
}

// A flat batch with no Struct columns passes through unchanged.
#[test]
fn flat_batch_passthrough() {
    let batch = make_batch(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, true),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    );

    let (flat, dropped) = flatten_record_batch(&batch, None, &drop_all()).unwrap();

    assert_eq!(flat.num_columns(), 2);
    assert_eq!(flat.schema().field(0).name(), "x");
    assert_eq!(flat.schema().field(1).name(), "y");
    assert_eq!(flat.num_rows(), 3);
    assert!(dropped.is_empty());
}

// A Struct column is expanded into dot-separated leaf columns.
#[test]
fn struct_is_flattened() {
    let (mut sf, struct_arr) = make_struct(vec![
        (
            Field::new("a", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
        ),
        (
            Field::new("b", DataType::Utf8, true),
            Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
        ),
    ]);
    sf = Field::new("s", sf.data_type().clone(), false);
    let batch = make_batch(vec![sf], vec![struct_arr]);

    let (flat, dropped) = flatten_record_batch(&batch, None, &drop_all()).unwrap();

    assert_eq!(flat.num_columns(), 2);
    assert_eq!(flat.schema().field(0).name(), "s.a");
    assert_eq!(flat.schema().field(1).name(), "s.b");
    assert!(dropped.is_empty());
    // Values are preserved.
    let col_a = flat
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col_a.values(), &[10, 20]);
}

// Deeply nested Structs are expanded recursively.
#[test]
fn nested_struct_is_flattened() {
    let (mut inner_sf, inner_arr) = make_struct(vec![(
        Field::new("c", DataType::Int32, false),
        Arc::new(Int32Array::from(vec![99])) as ArrayRef,
    )]);
    inner_sf = Field::new("inner", inner_sf.data_type().clone(), false);

    let (mut outer_sf, outer_arr) = make_struct(vec![(inner_sf, inner_arr)]);
    outer_sf = Field::new("outer", outer_sf.data_type().clone(), false);

    let batch = make_batch(vec![outer_sf], vec![outer_arr]);
    let (flat, dropped) = flatten_record_batch(&batch, None, &drop_all()).unwrap();

    assert_eq!(flat.num_columns(), 1);
    assert_eq!(flat.schema().field(0).name(), "outer.inner.c");
    assert!(dropped.is_empty());
}

// A custom separator is used instead of the default '.'.
#[test]
fn custom_separator() {
    let (mut sf, struct_arr) = make_struct(vec![(
        Field::new("a", DataType::Int32, false),
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
    )]);
    sf = Field::new("s", sf.data_type().clone(), false);
    let batch = make_batch(vec![sf], vec![struct_arr]);

    let (flat, _) = flatten_record_batch(&batch, Some('/'), &drop_all()).unwrap();

    assert_eq!(flat.schema().field(0).name(), "s/a");
}

// List column: Drop policy.
#[test]
fn list_policy_drop() {
    let item_field = Arc::new(Field::new("item", DataType::Int32, true));
    let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
    let list_array = ListArray::new(item_field.clone(), offsets, values, None);

    let batch = make_batch(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("lst", DataType::List(item_field), false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    );

    let (flat, dropped) = flatten_record_batch(&batch, None, &drop_all()).unwrap();

    assert_eq!(flat.num_columns(), 1);
    assert_eq!(flat.schema().field(0).name(), "x");
    assert_eq!(dropped, vec!["lst"]);
}

// List column: Keep policy.
#[test]
fn list_policy_keep() {
    let item_field = Arc::new(Field::new("item", DataType::Int32, true));
    let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
    let list_array = ListArray::new(item_field.clone(), offsets, values, None);

    let batch = make_batch(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("lst", DataType::List(item_field), false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    );

    let policy = FlattenPolicy {
        list: ListPolicy::Keep,
        list_flatten_fixed_size: 1,
        array: ArrayPolicy::Drop,
        map: MapPolicy::Drop,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();

    assert_eq!(flat.num_columns(), 2);
    assert_eq!(flat.schema().field(1).name(), "lst");
    assert!(dropped.is_empty());
}

// List column: FlattenFixed policy — pads short rows with nulls.
#[test]
fn list_policy_flatten_fixed() {
    let item_field = Arc::new(Field::new("item", DataType::Int32, true));
    let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
    // Row 0: [1, 2]  Row 1: [3]
    let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
    let list_array = ListArray::new(item_field.clone(), offsets, values, None);

    let batch = make_batch(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("lst", DataType::List(item_field), false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    );

    let policy = FlattenPolicy {
        list: ListPolicy::FlattenFixed,
        list_flatten_fixed_size: 2,
        array: ArrayPolicy::Drop,
        map: MapPolicy::Drop,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();

    // x, lst.0, lst.1
    assert_eq!(flat.num_columns(), 3);
    assert_eq!(flat.schema().field(1).name(), "lst.0");
    assert_eq!(flat.schema().field(2).name(), "lst.1");
    assert!(dropped.is_empty());

    let col0 = flat
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let col1 = flat
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    // Row 0: [1, 2] → col0=1, col1=2
    assert_eq!(col0.value(0), 1);
    assert_eq!(col1.value(0), 2);
    // Row 1: [3]   → col0=3, col1=null
    assert_eq!(col0.value(1), 3);
    assert!(col1.is_null(1));
}

// FixedSizeList column: Drop / Keep policies.
#[test]
fn array_policy_drop_and_keep() {
    let item_field = Arc::new(Field::new("item", DataType::Int32, false));
    let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let fsl = FixedSizeListArray::new(item_field.clone(), 2, values, None);
    let fsl_field = Field::new("fsl", DataType::FixedSizeList(item_field, 2), false);

    let batch = make_batch(
        vec![Field::new("x", DataType::Int32, false), fsl_field],
        vec![
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(fsl) as ArrayRef,
        ],
    );

    // Drop
    let (flat, dropped) = flatten_record_batch(&batch, None, &drop_all()).unwrap();
    assert_eq!(flat.num_columns(), 1);
    assert_eq!(dropped, vec!["fsl"]);

    // Keep
    let policy = FlattenPolicy {
        list: ListPolicy::Drop,
        list_flatten_fixed_size: 1,
        array: ArrayPolicy::Keep,
        map: MapPolicy::Drop,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();
    assert_eq!(flat.num_columns(), 2);
    assert_eq!(flat.schema().field(1).name(), "fsl");
    assert!(dropped.is_empty());
}

// FixedSizeList column: Flatten policy expands using schema size.
#[test]
fn array_policy_flatten() {
    let item_field = Arc::new(Field::new("item", DataType::Int32, false));
    // Row 0: [1, 2]  Row 1: [3, 4]
    let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let fsl = FixedSizeListArray::new(item_field.clone(), 2, values, None);
    let fsl_field = Field::new("fsl", DataType::FixedSizeList(item_field, 2), false);

    let batch = make_batch(
        vec![Field::new("x", DataType::Int32, false), fsl_field],
        vec![
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(fsl) as ArrayRef,
        ],
    );

    let policy = FlattenPolicy {
        list: ListPolicy::Drop,
        list_flatten_fixed_size: 1,
        array: ArrayPolicy::Flatten,
        map: MapPolicy::Drop,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();

    // x, fsl.0, fsl.1
    assert_eq!(flat.num_columns(), 3);
    assert_eq!(flat.schema().field(1).name(), "fsl.0");
    assert_eq!(flat.schema().field(2).name(), "fsl.1");
    assert!(dropped.is_empty());

    let col0 = flat
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let col1 = flat
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col0.values(), &[1, 3]); // first element of each row
    assert_eq!(col1.values(), &[2, 4]); // second element of each row
}

// Map column: Drop / Keep policies.
#[test]
fn map_policy_drop_and_keep() {
    // Build a minimal MapArray: {"a": 1} for one row.
    let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
    let val_field = Arc::new(Field::new("value", DataType::Int32, true));
    let entry_fields = Fields::from(vec![key_field.clone(), val_field.clone()]);
    let entry_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(entry_fields.clone()),
        false,
    ));

    let keys = Arc::new(StringArray::from(vec!["a"])) as ArrayRef;
    let vals = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    let struct_arr = StructArray::from(vec![(key_field, keys), (val_field, vals)]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1]));
    let map_array = MapArray::new(entry_field, offsets, struct_arr, None, false);
    let map_field = Field::new(
        "m",
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
            false,
        ),
        false,
    );

    let batch = make_batch(
        vec![Field::new("x", DataType::Int32, false), map_field],
        vec![
            Arc::new(Int32Array::from(vec![99])) as ArrayRef,
            Arc::new(map_array) as ArrayRef,
        ],
    );

    // Drop
    let (flat, dropped) = flatten_record_batch(&batch, None, &drop_all()).unwrap();
    assert_eq!(flat.num_columns(), 1);
    assert_eq!(dropped, vec!["m"]);

    // Keep
    let policy = FlattenPolicy {
        list: ListPolicy::Drop,
        list_flatten_fixed_size: 1,
        array: ArrayPolicy::Drop,
        map: MapPolicy::Keep,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();
    assert_eq!(flat.num_columns(), 2);
    assert_eq!(flat.schema().field(1).name(), "m");
    assert!(dropped.is_empty());
}

// ─── Nested: Struct child is a List (ListPolicy applied to child path) ───

// The list child "s.lst" is dropped; the scalar sibling "s.a" survives.
#[test]
fn struct_child_is_list_drop() {
    let item_field = Arc::new(Field::new("item", DataType::Int32, true));
    let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
    let list_array = ListArray::new(item_field.clone(), offsets, values, None);

    let (mut sf, struct_arr) = make_struct(vec![
        (
            Field::new("a", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
        ),
        (
            Field::new("lst", DataType::List(item_field), false),
            Arc::new(list_array) as ArrayRef,
        ),
    ]);
    sf = Field::new("s", sf.data_type().clone(), false);

    let batch = make_batch(vec![sf], vec![struct_arr]);
    let (flat, dropped) = flatten_record_batch(&batch, None, &drop_all()).unwrap();

    assert_eq!(flat.num_columns(), 1);
    assert_eq!(flat.schema().field(0).name(), "s.a");
    assert_eq!(dropped, vec!["s.lst"]);
}

// ─── Nested: Struct child is a FixedSizeList (ArrayPolicy::Flatten) ───

// s.fsl expands to s.fsl.0 and s.fsl.1.
#[test]
fn struct_child_is_fsl_flatten() {
    let item_field = Arc::new(Field::new("item", DataType::Int32, false));
    let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let fsl = FixedSizeListArray::new(item_field.clone(), 2, values, None);
    let fsl_field = Field::new("fsl", DataType::FixedSizeList(item_field, 2), false);

    let (mut sf, struct_arr) = make_struct(vec![(fsl_field, Arc::new(fsl) as ArrayRef)]);
    sf = Field::new("s", sf.data_type().clone(), false);

    let batch = make_batch(vec![sf], vec![struct_arr]);
    let policy = FlattenPolicy {
        list: ListPolicy::Drop,
        list_flatten_fixed_size: 1,
        array: ArrayPolicy::Flatten,
        map: MapPolicy::Drop,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();

    assert_eq!(flat.num_columns(), 2);
    assert_eq!(flat.schema().field(0).name(), "s.fsl.0");
    assert_eq!(flat.schema().field(1).name(), "s.fsl.1");
    assert!(dropped.is_empty());

    let col0 = flat
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let col1 = flat
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col0.values(), &[1, 3]); // first element of each row
    assert_eq!(col1.values(), &[2, 4]); // second element of each row
}

// ─── Nested: Struct child is a Map (MapPolicy::Keep) ───

// s.m is kept as-is.
#[test]
fn struct_child_is_map_keep() {
    let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
    let val_field = Arc::new(Field::new("value", DataType::Int32, true));
    let entry_fields = Fields::from(vec![key_field.clone(), val_field.clone()]);
    let entry_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(entry_fields.clone()),
        false,
    ));
    let keys = Arc::new(StringArray::from(vec!["a"])) as ArrayRef;
    let vals = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    let map_entries = StructArray::from(vec![(key_field, keys), (val_field, vals)]);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1]));
    let map_array = MapArray::new(entry_field, offsets, map_entries, None, false);
    let map_field = Field::new(
        "m",
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
            false,
        ),
        false,
    );

    let (mut sf, struct_arr) = make_struct(vec![(map_field, Arc::new(map_array) as ArrayRef)]);
    sf = Field::new("s", sf.data_type().clone(), false);

    let batch = make_batch(vec![sf], vec![struct_arr]);
    let policy = FlattenPolicy {
        list: ListPolicy::Drop,
        list_flatten_fixed_size: 1,
        array: ArrayPolicy::Drop,
        map: MapPolicy::Keep,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();

    assert_eq!(flat.num_columns(), 1);
    assert_eq!(flat.schema().field(0).name(), "s.m");
    assert!(dropped.is_empty());
}

// ─── Nested: List<Struct> with FlattenFixed — struct leaves are emitted ───

// lst: List<{x,y}> with FlattenFixed and size=2 → lst.0.x, lst.0.y, lst.1.x, lst.1.y.
#[test]
fn list_of_struct_flatten_fixed() {
    // Inner values: 4 struct elements laid out flat for 2 outer rows × 2 slots.
    let (inner_sf, inner_arr) = make_struct(vec![
        (
            Field::new("x", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef,
        ),
        (
            Field::new("y", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ),
    ]);
    // Row 0: [inner[0], inner[1]]  Row 1: [inner[2], inner[3]]
    let item_field = Arc::new(Field::new("item", inner_sf.data_type().clone(), true));
    let offsets = OffsetBuffer::new(vec![0i32, 2, 4].into());
    let list_array = ListArray::new(item_field.clone(), offsets, inner_arr, None);

    let batch = make_batch(
        vec![Field::new("lst", DataType::List(item_field), false)],
        vec![Arc::new(list_array) as ArrayRef],
    );
    let policy = FlattenPolicy {
        list: ListPolicy::FlattenFixed,
        list_flatten_fixed_size: 2,
        array: ArrayPolicy::Drop,
        map: MapPolicy::Drop,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();

    assert_eq!(flat.num_columns(), 4);
    assert_eq!(flat.schema().field(0).name(), "lst.0.x");
    assert_eq!(flat.schema().field(1).name(), "lst.0.y");
    assert_eq!(flat.schema().field(2).name(), "lst.1.x");
    assert_eq!(flat.schema().field(3).name(), "lst.1.y");
    assert!(dropped.is_empty());

    // Row 0: slot0={x:10,y:1}  slot1={x:20,y:2}
    // Row 1: slot0={x:30,y:3}  slot1={x:40,y:4}
    let col_0x = flat
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let col_0y = flat
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let col_1x = flat
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let col_1y = flat
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col_0x.values(), &[10, 30]);
    assert_eq!(col_0y.values(), &[1, 3]);
    assert_eq!(col_1x.values(), &[20, 40]);
    assert_eq!(col_1y.values(), &[2, 4]);
}

// ─── Nested: FixedSizeList<Struct> with Flatten — struct leaves are emitted ───

// fsl: FixedSizeList<{x,y}, 2> with Flatten → fsl.0.x, fsl.0.y, fsl.1.x, fsl.1.y.
#[test]
fn fsl_of_struct_flatten() {
    // 2 outer rows × 2 elements = 4 inner struct elements.
    let (inner_sf, inner_arr) = make_struct(vec![
        (
            Field::new("x", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ),
        (
            Field::new("y", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef,
        ),
    ]);
    let item_field = Arc::new(Field::new("item", inner_sf.data_type().clone(), false));
    let fsl = FixedSizeListArray::new(item_field.clone(), 2, inner_arr, None);
    let fsl_field = Field::new("fsl", DataType::FixedSizeList(item_field, 2), false);

    let batch = make_batch(vec![fsl_field], vec![Arc::new(fsl) as ArrayRef]);
    let policy = FlattenPolicy {
        list: ListPolicy::Drop,
        list_flatten_fixed_size: 1,
        array: ArrayPolicy::Flatten,
        map: MapPolicy::Drop,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();

    assert_eq!(flat.num_columns(), 4);
    assert_eq!(flat.schema().field(0).name(), "fsl.0.x");
    assert_eq!(flat.schema().field(1).name(), "fsl.0.y");
    assert_eq!(flat.schema().field(2).name(), "fsl.1.x");
    assert_eq!(flat.schema().field(3).name(), "fsl.1.y");
    assert!(dropped.is_empty());

    // expand_fixed_size_list: element i of row r → index r*size+i
    // i=0: row0→idx0={x:1,y:10}, row1→idx2={x:3,y:30}
    // i=1: row0→idx1={x:2,y:20}, row1→idx3={x:4,y:40}
    let col_0x = flat
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let col_0y = flat
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let col_1x = flat
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let col_1y = flat
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(col_0x.values(), &[1, 3]);
    assert_eq!(col_0y.values(), &[10, 30]);
    assert_eq!(col_1x.values(), &[2, 4]);
    assert_eq!(col_1y.values(), &[20, 40]);
}

// ─── Null: StructArray with a null row ───

// flatten_record_batch must not panic when a struct row is null.
#[test]
fn nullable_struct() {
    let x_field = Arc::new(Field::new("x", DataType::Int32, true));
    let fields = Fields::from(vec![x_field]);
    let x_arr = Arc::new(Int32Array::from(vec![Some(10), Some(20)])) as ArrayRef;
    let nulls = NullBuffer::from(vec![true, false]); // row 1 is null
    let struct_array = StructArray::new(fields.clone(), vec![x_arr], Some(nulls));

    let sf = Field::new("s", DataType::Struct(fields), true);
    let batch = make_batch(vec![sf], vec![Arc::new(struct_array) as ArrayRef]);
    let (flat, dropped) = flatten_record_batch(&batch, None, &drop_all()).unwrap();

    assert_eq!(flat.num_columns(), 1);
    assert_eq!(flat.schema().field(0).name(), "s.x");
    assert_eq!(flat.num_rows(), 2);
    assert!(dropped.is_empty());
}

// ─── Sibling Structs: two struct columns at the same top level ───

// s1.a and s2.a share a child name but differ in prefix, so no collision.
#[test]
fn sibling_structs() {
    let (mut sf1, arr1) = make_struct(vec![(
        Field::new("a", DataType::Int32, false),
        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
    )]);
    sf1 = Field::new("s1", sf1.data_type().clone(), false);

    let (mut sf2, arr2) = make_struct(vec![(
        Field::new("a", DataType::Utf8, false),
        Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
    )]);
    sf2 = Field::new("s2", sf2.data_type().clone(), false);

    let batch = make_batch(vec![sf1, sf2], vec![arr1, arr2]);
    let (flat, dropped) = flatten_record_batch(&batch, None, &drop_all()).unwrap();

    assert_eq!(flat.num_columns(), 2);
    assert_eq!(flat.schema().field(0).name(), "s1.a");
    assert_eq!(flat.schema().field(1).name(), "s2.a");
    assert!(dropped.is_empty());
}

// ─── Edge: FlattenFixed with size=0 silently removes the list without recording a drop ───

// The column vanishes — it is neither emitted nor added to the dropped list.
#[test]
fn flatten_fixed_zero() {
    let item_field = Arc::new(Field::new("item", DataType::Int32, true));
    let values = Arc::new(Int32Array::from(vec![] as Vec<i32>));
    let offsets = OffsetBuffer::new(vec![0i32, 0, 0].into()); // 2 empty rows
    let list_array = ListArray::new(item_field.clone(), offsets, values, None);

    let batch = make_batch(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("lst", DataType::List(item_field), false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(list_array) as ArrayRef,
        ],
    );
    let policy = FlattenPolicy {
        list: ListPolicy::FlattenFixed,
        list_flatten_fixed_size: 0,
        array: ArrayPolicy::Drop,
        map: MapPolicy::Drop,
    };
    let (flat, dropped) = flatten_record_batch(&batch, None, &policy).unwrap();

    assert_eq!(flat.num_columns(), 1);
    assert_eq!(flat.schema().field(0).name(), "x");
    assert!(dropped.is_empty());
}

// A path collision returns InvalidArgumentError.
#[test]
fn collision_returns_error() {
    // A top-level field literally named "s.a" collides with the flattened
    // path of struct "s" child "a".
    let (mut sf, struct_arr) = make_struct(vec![(
        Field::new("a", DataType::Int32, false),
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
    )]);
    sf = Field::new("s", sf.data_type().clone(), false);

    let batch = make_batch(
        vec![Field::new("s.a", DataType::Int32, false), sf],
        vec![Arc::new(Int32Array::from(vec![99])) as ArrayRef, struct_arr],
    );

    let err = flatten_record_batch(&batch, None, &drop_all()).unwrap_err();
    assert!(
        matches!(err, ArrowError::InvalidArgumentError(_)),
        "expected InvalidArgumentError, got {err:?}"
    );
    assert!(err.to_string().contains("s.a"));
}
