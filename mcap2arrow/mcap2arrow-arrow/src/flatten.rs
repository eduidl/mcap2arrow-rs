use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeListArray, Int32Array, ListArray, StructArray};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommonPostProcess {
    Drop,
    Keep,
    None,
}

impl CommonPostProcess {
    fn apply(&self, path: &str, field: &Field, col: &ArrayRef, out: &mut Collector) {
        match self {
            CommonPostProcess::Drop => out.drop(path),
            CommonPostProcess::Keep => out.keep(path, field, col),
            CommonPostProcess::None => (),
        }
    }

    fn from_list_policy(policy: &ListPolicy) -> Self {
        match policy {
            ListPolicy::Drop => CommonPostProcess::Drop,
            ListPolicy::Keep => CommonPostProcess::Keep,
            ListPolicy::FlattenFixed(_) => CommonPostProcess::None,
        }
    }

    fn from_array_policy(policy: &ArrayPolicy) -> Self {
        match policy {
            ArrayPolicy::Drop => CommonPostProcess::Drop,
            ArrayPolicy::Keep => CommonPostProcess::Keep,
            ArrayPolicy::Flatten => CommonPostProcess::None,
        }
    }

    fn from_map_policy(policy: &MapPolicy) -> Self {
        match policy {
            MapPolicy::Drop => CommonPostProcess::Drop,
            MapPolicy::Keep => CommonPostProcess::Keep,
        }
    }
}

/// Policy for [`DataType::List`] columns during [`flatten_record_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListPolicy {
    /// Drop the column entirely (e.g. for CSV output).
    Drop,
    /// Pass the column through unchanged (e.g. for Parquet output).
    Keep,
    /// Expand the list to exactly `n` scalar columns named `{col}.0` …
    /// `{col}.{n-1}`, padding short lists with nulls and truncating long ones.
    /// The size must be supplied explicitly by the caller.
    FlattenFixed(usize),
}

/// Policy for [`DataType::FixedSizeList`] columns during [`flatten_record_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayPolicy {
    /// Drop the column entirely.
    Drop,
    /// Pass the column through unchanged.
    Keep,
    /// Expand into one scalar column per element using the list size recorded
    /// in the schema; column names become `{col}.0`, `{col}.1`, …
    Flatten,
}

/// Policy for [`DataType::Map`] columns during [`flatten_record_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapPolicy {
    /// Drop the column entirely (e.g. for CSV output).
    Drop,
    /// Pass the column through unchanged (e.g. for Parquet output).
    Keep,
}

/// Aggregate policy controlling how each compound type is handled during
/// [`flatten_record_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlattenPolicy {
    pub list: ListPolicy,
    pub array: ArrayPolicy,
    pub map: MapPolicy,
}

impl FlattenPolicy {
    /// Typical policy for CSV: drop variable-length types, flatten fixed-size arrays.
    pub fn for_csv() -> Self {
        Self {
            list: ListPolicy::Drop,
            array: ArrayPolicy::Flatten,
            map: MapPolicy::Drop,
        }
    }

    /// Typical policy for Parquet: pass all compound types through unchanged.
    pub fn for_parquet() -> Self {
        Self {
            list: ListPolicy::Keep,
            array: ArrayPolicy::Keep,
            map: MapPolicy::Keep,
        }
    }
}

/// Accumulates the output of [`collect_columns`].
#[derive(Default)]
struct Collector {
    fields: Vec<Field>,
    arrays: Vec<ArrayRef>,
    dropped: BTreeSet<String>,
}

impl Collector {
    fn push(&mut self, field: Field, array: ArrayRef) {
        self.fields.push(field);
        self.arrays.push(array);
    }

    /// Keep `col` under a new field whose name is `path`, type and nullability
    /// are taken from `field`.
    fn keep(&mut self, path: &str, field: &Field, col: &ArrayRef) {
        self.push(
            Field::new(path, field.data_type().clone(), field.is_nullable()),
            col.clone(),
        );
    }

    fn drop(&mut self, path: &str) {
        self.dropped.insert(path.to_owned());
    }
}

/// Flatten a [`RecordBatch`] by recursively walking the schema and applying
/// the per-type [`FlattenPolicy`] to `List`, `FixedSizeList`, and `Map` columns,
/// expanding `Struct` columns inline.
///
/// `separator` controls the character inserted between path segments;
/// defaults to `'.'` when `None`.
///
/// Dropped column paths are returned as the second element of the tuple.
///
/// # Errors
///
/// Returns [`ArrowError::InvalidArgumentError`] if flattening would produce
/// two columns with the same path.
pub fn flatten_record_batch(
    batch: &RecordBatch,
    separator: Option<char>,
    policy: &FlattenPolicy,
) -> Result<(RecordBatch, Vec<String>), ArrowError> {
    let sep = separator.unwrap_or('.').to_string();

    let mut collector = Collector::default();

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i).clone();
        collect_columns(field, field.name(), &col, &sep, policy, &mut collector)?;
    }

    // Collision check on the paths collected above.
    let mut seen_names: HashSet<String> = HashSet::with_capacity(collector.fields.len());
    for f in &collector.fields {
        if !seen_names.insert(f.name().clone()) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "flattening column name collision: '{}'",
                f.name()
            )));
        }
    }

    let schema = Arc::new(Schema::new(collector.fields));
    let result = RecordBatch::try_new(schema, collector.arrays)?;

    Ok((result, collector.dropped.into_iter().collect()))
}

/// Recursively collect output columns by applying `policy` to every node in
/// the schema tree. `Struct` columns are expanded inline; the policy is applied
/// to `List`, `FixedSizeList`, and `Map` columns at every nesting level.
fn collect_columns(
    field: &Field,
    path: &str,
    col: &ArrayRef,
    sep: &str,
    policy: &FlattenPolicy,
    out: &mut Collector,
) -> Result<(), ArrowError> {
    let common_post_process: CommonPostProcess = match field.data_type() {
        DataType::Struct(child_fields) => {
            let struct_arr = col
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("DataType::Struct matches StructArray");
            for (i, child_field) in child_fields.iter().enumerate() {
                let child_col = struct_arr.column(i).clone();
                let child_path = format!("{path}{sep}{}", child_field.name());
                collect_columns(child_field, &child_path, &child_col, sep, policy, out)?;
            }
            CommonPostProcess::None
        }
        DataType::List(_) => {
            if let ListPolicy::FlattenFixed(n) = &policy.list {
                let list_arr = col
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("DataType::List matches ListArray");
                for (f, a) in expand_list_fixed(list_arr, path, *n, sep)? {
                    collect_columns(&f, f.name(), &a, sep, policy, out)?;
                }
            };
            CommonPostProcess::from_list_policy(&policy.list)
        }
        DataType::FixedSizeList(_, _) => {
            if policy.array == ArrayPolicy::Flatten {
                let fsl_arr = col
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .expect("DataType::FixedSizeList matches FixedSizeListArray");
                for (f, a) in expand_fixed_size_list(fsl_arr, path, sep)? {
                    collect_columns(&f, f.name(), &a, sep, policy, out)?;
                }
            };
            CommonPostProcess::from_array_policy(&policy.array)
        }
        DataType::Map(_, _) => CommonPostProcess::from_map_policy(&policy.map),
        _ => CommonPostProcess::Keep,
    };

    common_post_process.apply(path, field, col, out);
    Ok(())
}

/// Shared expansion core: for each element index `i` in `0..n`, builds a column
/// by calling `index_fn(row, i)` for every row to select an entry from `values`.
/// `None` from `index_fn` produces a null in the output column.
/// Column names: `{name}{sep}0`, `{name}{sep}1`, …
fn expand_by_index(
    values: &ArrayRef,
    item_type: DataType,
    n_rows: usize,
    n: usize,
    name: &str,
    sep: &str,
    index_fn: impl Fn(usize, usize) -> Option<i32>,
) -> Result<Vec<(Field, ArrayRef)>, ArrowError> {
    (0..n)
        .map(|i| {
            let indices: Int32Array = (0..n_rows).map(|row| index_fn(row, i)).collect();
            let col = take(values.as_ref(), &indices, None)?;
            let field = Field::new(format!("{name}{sep}{i}"), item_type.clone(), true);
            Ok((field, col))
        })
        .collect()
}

/// Expand a [`FixedSizeListArray`] into one column per element.
/// Column names: `{name}{sep}0`, `{name}{sep}1`, …
/// Null rows in the parent array produce null values in every child column.
fn expand_fixed_size_list(
    array: &FixedSizeListArray,
    name: &str,
    sep: &str,
) -> Result<Vec<(Field, ArrayRef)>, ArrowError> {
    let n = array.value_length() as usize;
    let item_type = match array.data_type() {
        DataType::FixedSizeList(item_field, _) => item_field.data_type().clone(),
        _ => unreachable!(),
    };
    expand_by_index(
        array.values(),
        item_type,
        array.len(),
        n,
        name,
        sep,
        |row, i| {
            if array.is_null(row) {
                None
            } else {
                Some((row * n + i) as i32)
            }
        },
    )
}

/// Expand a [`ListArray`] into exactly `n` columns, padding short rows with nulls.
/// Column names: `{name}{sep}0`, `{name}{sep}1`, …
fn expand_list_fixed(
    array: &ListArray,
    name: &str,
    n: usize,
    sep: &str,
) -> Result<Vec<(Field, ArrayRef)>, ArrowError> {
    let item_type = match array.data_type() {
        DataType::List(item_field) => item_field.data_type().clone(),
        _ => unreachable!(),
    };
    let offsets = array.value_offsets();
    expand_by_index(
        array.values(),
        item_type,
        array.len(),
        n,
        name,
        sep,
        |row, i| {
            if array.is_null(row) {
                return None;
            }
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            if i < end - start {
                Some((start + i) as i32)
            } else {
                None
            }
        },
    )
}
