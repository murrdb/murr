use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use log::{debug, info};
use tokio::task::JoinSet;

use crate::core::{DType, MurrError, TableSchema};
use crate::io::column::float32::reader::Float32ColumnReader;
use crate::io::column::utf8::reader::Utf8ColumnReader;
use crate::io::column::ColumnReader;
use crate::io::directory::DirectoryReader;
use crate::io::table::index::KeyIndex;
use crate::io::table::key_offset::KeyOffset;

struct ColumnArray {
    index: usize,
    field: Field,
    array: Arc<dyn Array>,
}

pub struct TableReader<R: DirectoryReader> {
    schema: TableSchema,
    reader: Arc<R>,
    columns: HashMap<String, Arc<dyn ColumnReader<R>>>,
    index: RwLock<KeyIndex>,
}

impl<R: DirectoryReader> TableReader<R> {
    pub async fn open(
        schema: TableSchema,
        reader: Arc<R>,
    ) -> Result<Self, MurrError> {
        info!(
            "table reader open: key='{}', schema columns: [{}]",
            schema.key,
            schema.columns.keys().cloned().collect::<Vec<_>>().join(", ")
        );
        let info = reader.info();
        info!(
            "directory reader opened: {} columns, {} segments, max_segment_id={}",
            info.columns.len(),
            info.columns.values().flat_map(|c| c.segments.keys()).collect::<HashSet<_>>().len(),
            info.max_segment_id
        );

        let mut columns: HashMap<String, Arc<dyn ColumnReader<R>>> = HashMap::new();
        for (col_name, col_segments) in &info.columns {
            let num_segments = col_segments.segments.len();
            let col_reader = open_column_reader(
                &col_segments.column.dtype,
                reader.clone(),
                col_segments,
            )
            .await?;
            columns.insert(col_name.clone(), col_reader);
            debug!(
                "opened column reader '{}' (dtype={:?}, nullable={}, segments={})",
                col_name, col_segments.column.dtype, col_segments.column.nullable, num_segments
            );
        }

        let mut index = KeyIndex::new();
        let key_col_name = &schema.key;
        if let Some(key_col_segments) = info.columns.get(key_col_name) {
            let mut seg_ids: Vec<u32> = key_col_segments.segments.keys().copied().collect();
            seg_ids.sort();
            let key_col_reader = columns
                .get(key_col_name)
                .ok_or_else(|| MurrError::TableError("key column reader not found".into()))?;
            for &seg_id in &seg_ids {
                let seg_info = &key_col_segments.segments[&seg_id];
                let keys =
                    read_segment_keys(key_col_reader.as_ref(), seg_id, seg_info.num_values)
                        .await?;
                index.add_segment(seg_id, &keys);
                debug!(
                    "indexed segment {}: {} keys (total index size: {})",
                    seg_id, seg_info.num_values, index.len()
                );
            }
        }
        info!("table reader open complete: {} keys indexed across {} columns",
            index.len(), columns.len());

        Ok(TableReader {
            schema,
            reader,
            columns,
            index: RwLock::new(index),
        })
    }

    pub async fn reopen(self) -> Result<Self, MurrError> {
        let old_info = self.reader.info().clone();
        let old_key_seg_ids: HashSet<u32> = old_info
            .columns
            .get(&self.schema.key)
            .map(|cs| cs.segments.keys().copied().collect())
            .unwrap_or_default();

        info!(
            "table reader reopen: previous max_segment_id={}, {} key segments",
            old_info.max_segment_id, old_key_seg_ids.len()
        );

        let new_reader = Arc::new(self.reader.reopen_reader().await?);
        let new_info = new_reader.info();
        let new_all_seg_ids: HashSet<u32> = new_info
            .columns
            .values()
            .flat_map(|c| c.segments.keys())
            .copied()
            .collect();

        info!(
            "directory reader reopened: max_segment_id {} -> {}, segments {} -> {}",
            old_info.max_segment_id,
            new_info.max_segment_id,
            old_key_seg_ids.len(),
            new_all_seg_ids.len()
        );

        let mut index = self
            .index
            .into_inner()
            .map_err(|e| MurrError::TableError(format!("index lock poisoned: {e}")))?;

        let mut columns: HashMap<String, Arc<dyn ColumnReader<R>>> = HashMap::new();
        let mut reused_count = 0usize;
        let mut new_count = 0usize;
        for (col_name, col_segments) in &new_info.columns {
            let col_reader = match self.columns.get(col_name) {
                Some(prev) => {
                    reused_count += 1;
                    prev.reopen(new_reader.clone(), col_segments).await?
                }
                None => {
                    new_count += 1;
                    open_column_reader(
                        &col_segments.column.dtype,
                        new_reader.clone(),
                        col_segments,
                    )
                    .await?
                }
            };
            columns.insert(col_name.clone(), col_reader);
        }
        debug!(
            "column readers: {} reopened, {} newly opened",
            reused_count, new_count
        );

        let key_col_name = &self.schema.key;
        if let Some(key_col_segments) = new_info.columns.get(key_col_name) {
            let mut new_seg_ids: Vec<u32> = key_col_segments
                .segments
                .keys()
                .copied()
                .filter(|id| !old_key_seg_ids.contains(id))
                .collect();
            new_seg_ids.sort();

            if !new_seg_ids.is_empty() {
                let key_col_reader = columns
                    .get(key_col_name)
                    .ok_or_else(|| {
                        MurrError::TableError("key column reader not found".into())
                    })?;
                let prev_index_len = index.len();
                for &seg_id in &new_seg_ids {
                    let seg_info = &key_col_segments.segments[&seg_id];
                    let keys =
                        read_segment_keys(key_col_reader.as_ref(), seg_id, seg_info.num_values)
                            .await?;
                    index.add_segment(seg_id, &keys);
                    debug!(
                        "indexed new segment {}: {} keys",
                        seg_id, seg_info.num_values
                    );
                }
                info!(
                    "incremental index: {} new segments, {} -> {} keys",
                    new_seg_ids.len(),
                    prev_index_len,
                    index.len()
                );
            } else {
                debug!("no new segments to index");
            }
        }

        Ok(TableReader {
            schema: self.schema,
            reader: new_reader,
            columns,
            index: RwLock::new(index),
        })
    }

    pub async fn read(
        &self,
        keys: &[&str],
        columns: &[&str],
    ) -> Result<RecordBatch, MurrError> {
        debug!(
            "read: {} keys, columns=[{}]",
            keys.len(),
            columns.join(", ")
        );
        let key_offsets = {
            let index = self
                .index
                .read()
                .map_err(|e| MurrError::TableError(format!("index lock poisoned: {e}")))?;
            index.get(keys)
        };
        let missing_count = key_offsets.iter().filter(|k| k.is_missing()).count();
        if missing_count > 0 {
            debug!("{} of {} keys missing", missing_count, keys.len());
        }

        let key_offsets: Arc<[KeyOffset]> = key_offsets.into();
        let mut set = JoinSet::new();
        for (index, &col_name) in columns.iter().enumerate() {
            let col_reader = self.columns.get(col_name).ok_or_else(|| {
                MurrError::TableError(format!("column '{}' not found", col_name))
            })?.clone();
            let col_schema = self.schema.columns.get(col_name).ok_or_else(|| {
                MurrError::TableError(format!("column '{}' not in schema", col_name))
            })?;
            let field = Field::new(col_name, DataType::from(&col_schema.dtype), true);
            let key_offsets = key_offsets.clone();
            set.spawn(async move {
                read_column(index, col_reader, field, key_offsets).await
            });
        }

        let mut column_arrays: Vec<ColumnArray> = Vec::with_capacity(set.len());
        while let Some(result) = set.join_next().await {
            column_arrays.push(result.map_err(|e| MurrError::TableError(e.to_string()))??);
        }
        column_arrays.sort_by_key(|ca| ca.index);

        let fields: Vec<Field> = column_arrays.iter().map(|ca| ca.field.clone()).collect();
        let arrays: Vec<Arc<dyn Array>> = column_arrays.into_iter().map(|ca| ca.array).collect();

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| MurrError::ArrowError(e.to_string()))
    }
}

async fn read_column<R: DirectoryReader>(
    index: usize,
    col_reader: Arc<dyn ColumnReader<R>>,
    field: Field,
    key_offsets: Arc<[KeyOffset]>,
) -> Result<ColumnArray, MurrError> {
    let array = col_reader.read(&key_offsets).await?;
    Ok(ColumnArray { index, field, array })
}

async fn open_column_reader<R: DirectoryReader>(
    dtype: &DType,
    reader: Arc<R>,
    column: &crate::io::info::ColumnSegments,
) -> Result<Arc<dyn ColumnReader<R>>, MurrError> {
    match dtype {
        DType::Float32 => Ok(Arc::new(
            Float32ColumnReader::open(reader, column, &None).await?,
        )),
        DType::Utf8 => Ok(Arc::new(
            Utf8ColumnReader::open(reader, column, &None).await?,
        )),
    }
}

async fn read_segment_keys<R: DirectoryReader>(
    key_col_reader: &dyn ColumnReader<R>,
    seg_id: u32,
    num_values: u32,
) -> Result<StringArray, MurrError> {
    let offsets: Vec<KeyOffset> = (0..num_values)
        .map(|i| KeyOffset::new(i as usize, seg_id, i))
        .collect();
    let array = key_col_reader.read(&offsets).await?;
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| MurrError::TableError("key column is not StringArray".into()))
}
