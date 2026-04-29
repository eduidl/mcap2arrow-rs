use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, FixedSizeListArray, Float64Array, Int32Array, ListArray, StringArray,
        StructArray,
    },
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, Fields, Schema},
    record_batch::RecordBatch,
};
use mcapdecode_arrow::project_record_batch;

fn paths(s: &[&str]) -> Vec<String> {
    s.iter().map(|s| s.to_string()).collect()
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

/// `{ x: f64, y: f64, name: String }`
fn flat_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![3.0, 4.0])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ],
    )
    .unwrap()
}

/// `{ id: i32, position: Struct<x: f64, y: f64, z: f64>, name: String }`
fn nested_batch() -> RecordBatch {
    let pos_fields = Fields::from(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("z", DataType::Float64, false),
    ]);
    let position = StructArray::new(
        pos_fields.clone(),
        vec![
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![3.0, 4.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![5.0, 6.0])) as ArrayRef,
        ],
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("position", DataType::Struct(pos_fields), false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(position) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ],
    )
    .unwrap()
}

/// `{ outer: Struct<inner: Struct<value: f64, label: String>, count: i32> }`
fn deep_nested_batch() -> RecordBatch {
    let inner_fields = Fields::from(vec![
        Field::new("value", DataType::Float64, false),
        Field::new("label", DataType::Utf8, false),
    ]);
    let inner = StructArray::new(
        inner_fields.clone(),
        vec![
            Arc::new(Float64Array::from(vec![42.0])) as ArrayRef,
            Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
        ],
        None,
    );
    let outer_fields = Fields::from(vec![
        Field::new("inner", DataType::Struct(inner_fields), false),
        Field::new("count", DataType::Int32, false),
    ]);
    let outer = StructArray::new(
        outer_fields.clone(),
        vec![
            Arc::new(inner) as ArrayRef,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        ],
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "outer",
        DataType::Struct(outer_fields),
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(outer) as ArrayRef]).unwrap()
}

/// `{ values: List<f64>, name: String }`
fn list_batch() -> RecordBatch {
    let item_field = Arc::new(Field::new("item", DataType::Float64, true));
    let values_data = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 4]));
    let list_arr = ListArray::new(item_field, offsets, values_data, None);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(list_arr) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ],
    )
    .unwrap()
}

/// `{ points: List<Struct<x: f64, y: f64>>, id: i32 }`
fn list_of_struct_batch() -> RecordBatch {
    let item_struct_fields = Fields::from(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
    ]);
    // Two rows: row0=[{x:1,y:2},{x:3,y:4}], row1=[{x:5,y:6}]
    let struct_values = StructArray::new(
        item_struct_fields.clone(),
        vec![
            Arc::new(Float64Array::from(vec![1.0, 3.0, 5.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0])) as ArrayRef,
        ],
        None,
    );
    let item_field = Arc::new(Field::new(
        "item",
        DataType::Struct(item_struct_fields),
        false,
    ));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 3]));
    let list_arr = ListArray::new(item_field, offsets, Arc::new(struct_values), None);

    let schema = Arc::new(Schema::new(vec![
        Field::new("points", list_arr.data_type().clone(), false),
        Field::new("id", DataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(list_arr) as ArrayRef,
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        ],
    )
    .unwrap()
}

/// `{ pts: FixedSizeList<Struct<x: f64, y: f64>, 2> }`
fn fixed_list_of_struct_batch() -> RecordBatch {
    let item_struct_fields = Fields::from(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
    ]);
    // Two rows, each with 2 points → 4 flat struct entries
    let struct_values = StructArray::new(
        item_struct_fields.clone(),
        vec![
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![5.0, 6.0, 7.0, 8.0])) as ArrayRef,
        ],
        None,
    );
    let item_field = Arc::new(Field::new(
        "item",
        DataType::Struct(item_struct_fields),
        false,
    ));
    let fsl = FixedSizeListArray::new(item_field, 2, Arc::new(struct_values), None);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "pts",
        fsl.data_type().clone(),
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(fsl) as ArrayRef]).unwrap()
}

fn field_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

fn struct_child_names(batch: &RecordBatch, col: usize) -> Vec<String> {
    match batch.schema().field(col).data_type() {
        DataType::Struct(fields) => fields.iter().map(|f| f.name().clone()).collect(),
        _ => panic!("column {col} is not a struct"),
    }
}

// ---------------------------------------------------------------------------
// Tests: empty paths
// ---------------------------------------------------------------------------

#[test]
fn empty_paths_returns_batch_unchanged() {
    let batch = flat_batch();
    let result = project_record_batch(&batch, &[]).unwrap();
    assert_eq!(result.schema(), batch.schema());
    assert_eq!(result.num_columns(), 3);
}

// ---------------------------------------------------------------------------
// Tests: top-level field selection
// ---------------------------------------------------------------------------

#[test]
fn select_top_level_fields() {
    let result = project_record_batch(&flat_batch(), &paths(&["x", "name"])).unwrap();
    assert_eq!(result.num_columns(), 2);
    assert_eq!(field_names(&result), vec!["x", "name"]);
}

#[test]
fn column_order_follows_schema_not_request_order() {
    // Request ["name", "id"] but schema order is [id, position, name].
    let result = project_record_batch(&nested_batch(), &paths(&["name", "id"])).unwrap();
    assert_eq!(field_names(&result), vec!["id", "name"]);
}

// ---------------------------------------------------------------------------
// Tests: struct field selection
// ---------------------------------------------------------------------------

#[test]
fn select_whole_struct() {
    let result = project_record_batch(&nested_batch(), &paths(&["position"])).unwrap();
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.schema().field(0).name(), "position");
    assert_eq!(struct_child_names(&result, 0), ["x", "y", "z"]);
}

#[test]
fn select_one_nested_field() {
    let result = project_record_batch(&nested_batch(), &paths(&["position.x"])).unwrap();
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.schema().field(0).name(), "position");
    assert_eq!(struct_child_names(&result, 0), ["x"]);
}

#[test]
fn select_multiple_nested_fields() {
    let result =
        project_record_batch(&nested_batch(), &paths(&["position.x", "position.y"])).unwrap();
    assert_eq!(result.num_columns(), 1);
    assert_eq!(struct_child_names(&result, 0), ["x", "y"]);
}

#[test]
fn mix_top_level_and_nested() {
    let result = project_record_batch(&nested_batch(), &paths(&["id", "position.x"])).unwrap();
    assert_eq!(result.num_columns(), 2);
    assert_eq!(field_names(&result), vec!["id", "position"]);
    assert_eq!(struct_child_names(&result, 1), ["x"]);
}

// ---------------------------------------------------------------------------
// Tests: broad path supersedes specific
// ---------------------------------------------------------------------------

#[test]
fn broad_then_specific_keeps_all_children() {
    // "position" then "position.x" → All wins
    let result =
        project_record_batch(&nested_batch(), &paths(&["position", "position.x"])).unwrap();
    assert_eq!(struct_child_names(&result, 0), ["x", "y", "z"]);
}

#[test]
fn specific_then_broad_keeps_all_children() {
    // "position.x" then "position" → All wins
    let result =
        project_record_batch(&nested_batch(), &paths(&["position.x", "position"])).unwrap();
    assert_eq!(struct_child_names(&result, 0), ["x", "y", "z"]);
}

// ---------------------------------------------------------------------------
// Tests: deep nesting
// ---------------------------------------------------------------------------

#[test]
fn deep_nested_path() {
    // outer.inner.value → outer{ inner{ value } }
    let result =
        project_record_batch(&deep_nested_batch(), &paths(&["outer.inner.value"])).unwrap();
    assert_eq!(result.num_columns(), 1);
    let schema = result.schema();
    let DataType::Struct(outer_children) = schema.field(0).data_type() else {
        panic!()
    };
    assert_eq!(outer_children.len(), 1);
    assert_eq!(outer_children[0].name(), "inner");
    let DataType::Struct(inner_children) = outer_children[0].data_type() else {
        panic!()
    };
    assert_eq!(inner_children.len(), 1);
    assert_eq!(inner_children[0].name(), "value");
}

#[test]
fn deep_nested_broad_path_keeps_subtree() {
    // outer.inner → outer{ inner{ value, label } }
    let result = project_record_batch(&deep_nested_batch(), &paths(&["outer.inner"])).unwrap();
    let schema = result.schema();
    let DataType::Struct(outer_children) = schema.field(0).data_type() else {
        panic!()
    };
    let DataType::Struct(inner_children) = outer_children[0].data_type() else {
        panic!()
    };
    assert_eq!(inner_children.len(), 2);
}

// ---------------------------------------------------------------------------
// Tests: data values
// ---------------------------------------------------------------------------

#[test]
fn projected_values_are_correct() {
    let result = project_record_batch(&flat_batch(), &paths(&["y"])).unwrap();
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(col.values(), &[3.0, 4.0]);
}

// ---------------------------------------------------------------------------
// Tests: list fields (primitive item)
// ---------------------------------------------------------------------------

#[test]
fn select_list_field_by_name() {
    let result = project_record_batch(&list_batch(), &paths(&["values"])).unwrap();
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.schema().field(0).name(), "values");
    assert!(matches!(
        result.schema().field(0).data_type(),
        DataType::List(_)
    ));
}

#[test]
fn list_field_with_subpath_returns_whole_list() {
    // List<f64> is not a struct, so sub-path is ignored and the column is kept whole.
    let result = project_record_batch(&list_batch(), &paths(&["values.item"])).unwrap();
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.schema().field(0).name(), "values");
}

#[test]
fn unselected_list_field_is_dropped() {
    let result = project_record_batch(&list_batch(), &paths(&["name"])).unwrap();
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.schema().field(0).name(), "name");
}

// ---------------------------------------------------------------------------
// Tests: List<Struct> and FixedSizeList<Struct>
// ---------------------------------------------------------------------------

#[test]
fn list_of_struct_subfield_pruning() {
    // points.x → List<Struct<x>> (y is dropped)
    let result = project_record_batch(&list_of_struct_batch(), &paths(&["points.x"])).unwrap();
    assert_eq!(result.num_columns(), 1);
    let schema = result.schema();
    let DataType::List(item_field) = schema.field(0).data_type() else {
        panic!()
    };
    let DataType::Struct(child_fields) = item_field.data_type() else {
        panic!()
    };
    assert_eq!(child_fields.len(), 1);
    assert_eq!(child_fields[0].name(), "x");
}

#[test]
fn list_of_struct_whole_item_selected() {
    // "points" → entire list preserved
    let result = project_record_batch(&list_of_struct_batch(), &paths(&["points"])).unwrap();
    let schema = result.schema();
    let DataType::List(item_field) = schema.field(0).data_type() else {
        panic!()
    };
    let DataType::Struct(child_fields) = item_field.data_type() else {
        panic!()
    };
    assert_eq!(child_fields.len(), 2);
}

#[test]
fn fixed_list_of_struct_subfield_pruning() {
    // pts.x → FixedSizeList<Struct<x>, 2>
    let result = project_record_batch(&fixed_list_of_struct_batch(), &paths(&["pts.x"])).unwrap();
    let schema = result.schema();
    let DataType::FixedSizeList(item_field, size) = schema.field(0).data_type() else {
        panic!()
    };
    assert_eq!(*size, 2);
    let DataType::Struct(child_fields) = item_field.data_type() else {
        panic!()
    };
    assert_eq!(child_fields.len(), 1);
    assert_eq!(child_fields[0].name(), "x");
}

// ---------------------------------------------------------------------------
// Tests: error cases
// ---------------------------------------------------------------------------

#[test]
fn nonexistent_top_level_field_errors() {
    let err = project_record_batch(&flat_batch(), &paths(&["missing"])).unwrap_err();
    assert!(err.to_string().contains("missing"));
}

#[test]
fn nonexistent_nested_field_errors() {
    let err = project_record_batch(&nested_batch(), &paths(&["position.w"])).unwrap_err();
    assert!(err.to_string().contains("w"));
}

#[test]
fn nested_path_on_scalar_field_errors() {
    let err = project_record_batch(&flat_batch(), &paths(&["x.value"])).unwrap_err();
    assert!(err.to_string().contains("x"));
}
