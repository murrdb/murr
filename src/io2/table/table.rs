use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use log::{debug, info};

use crate::core::{DType, MurrError, TableSchema};
use crate::io2::column::float32::reader::Float32ColumnReader;
use crate::io2::column::float32::writer::Float32ColumnWriter;
use crate::io2::column::utf8::reader::Utf8ColumnReader;
use crate::io2::column::utf8::writer::Utf8ColumnWriter;
use crate::io2::column::{ColumnReader, ColumnSegmentBytes, ColumnWriter};
use crate::io2::directory::{Directory, DirectoryWriter, Reader};
use crate::io2::info::ColumnInfo;
use crate::io2::table::index::KeyIndex;
use crate::io2::table::key_offset::KeyOffset;

pub struct Table<D: Directory> {
    pub dir: Arc<D>,
    pub schema: TableSchema,
}

impl<D: Directory> Table<D> {
    pub fn new(dir: Arc<D>, schema: TableSchema) -> Arc<Self> {
        Arc::new(Table { dir, schema })
    }

    pub async fn open_reader(self: &Arc<Self>) -> Result<TableReader, MurrError> {
        let reader: Arc<dyn Reader> = Arc::new(self.dir.open_reader().await?);
        TableReader::open(self.schema.clone(), reader).await
    }

    pub async fn open_writer(self: &Arc<Self>) -> Result<TableWriter<D>, MurrError> {
        TableWriter::open(self.schema.clone(), self.dir.clone()).await
    }
}

pub struct TableReader {
    schema: TableSchema,
    reader: Arc<dyn Reader>,
    columns: HashMap<String, Box<dyn ColumnReader>>,
    index: RwLock<KeyIndex>,
}

impl TableReader {
    pub async fn open(
        schema: TableSchema,
        reader: Arc<dyn Reader>,
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

        let mut columns: HashMap<String, Box<dyn ColumnReader>> = HashMap::new();
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

        let new_reader = self.reader.reopen().await?;
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

        let mut columns: HashMap<String, Box<dyn ColumnReader>> = HashMap::new();
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

        let mut fields = Vec::with_capacity(columns.len());
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(columns.len());

        for &col_name in columns {
            let col_reader = self.columns.get(col_name).ok_or_else(|| {
                MurrError::TableError(format!("column '{}' not found", col_name))
            })?;
            let array = col_reader.read(&key_offsets).await?;

            let col_schema = self
                .schema
                .columns
                .get(col_name)
                .ok_or_else(|| {
                    MurrError::TableError(format!("column '{}' not in schema", col_name))
                })?;
            fields.push(Field::new(
                col_name,
                DataType::from(&col_schema.dtype),
                true,
            ));
            arrays.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| MurrError::ArrowError(e.to_string()))
    }
}

pub struct TableWriter<D: Directory> {
    schema: TableSchema,
    writer: D::WriterType,
}

impl<D: Directory> TableWriter<D> {
    pub async fn open(schema: TableSchema, dir: Arc<D>) -> Result<Self, MurrError> {
        let writer = dir.open_writer().await?;
        info!(
            "table writer opened: {} columns in schema",
            schema.columns.len()
        );
        Ok(TableWriter { schema, writer })
    }

    pub async fn write(&self, batch: &RecordBatch) -> Result<(), MurrError> {
        info!(
            "writing batch: {} rows, {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
        let mut segment_bytes: Vec<ColumnSegmentBytes> = Vec::new();

        for (col_name, col_schema) in &self.schema.columns {
            let col_index = batch.schema().index_of(col_name).map_err(|e| {
                MurrError::TableError(format!("column '{}' not in batch: {e}", col_name))
            })?;
            let array = batch.column(col_index).clone();

            let col_info = Arc::new(ColumnInfo {
                name: col_name.clone(),
                dtype: col_schema.dtype.clone(),
                nullable: col_schema.nullable,
            });

            let bytes = write_column(col_info, array).await?;
            debug!(
                "encoded column '{}': {} bytes",
                col_name,
                bytes.bytes.len()
            );
            segment_bytes.push(bytes);
        }

        self.writer.write(&segment_bytes).await?;
        info!("segment written successfully");
        Ok(())
    }
}

async fn open_column_reader(
    dtype: &DType,
    reader: Arc<dyn Reader>,
    column: &crate::io2::info::ColumnSegments,
) -> Result<Box<dyn ColumnReader>, MurrError> {
    match dtype {
        DType::Float32 => Ok(Box::new(
            Float32ColumnReader::open(reader, column, &None).await?,
        )),
        DType::Utf8 => Ok(Box::new(
            Utf8ColumnReader::open(reader, column, &None).await?,
        )),
    }
}

async fn write_column(
    col_info: Arc<ColumnInfo>,
    array: Arc<dyn Array>,
) -> Result<ColumnSegmentBytes, MurrError> {
    match col_info.dtype {
        DType::Float32 => {
            let writer = Float32ColumnWriter::new(col_info);
            writer.write(array).await
        }
        DType::Utf8 => {
            let writer = Utf8ColumnWriter::new(col_info);
            writer.write(array).await
        }
    }
}

async fn read_segment_keys(
    key_col_reader: &dyn ColumnReader,
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float32Array;
    use std::collections::HashMap;

    use crate::core::ColumnSchema;
    use crate::io2::directory::mem::directory::MemDirectory;
    use crate::io2::directory::Directory;
    use crate::io2::url::MemUrl;

    fn test_dir() -> Arc<MemDirectory> {
        Arc::new(MemDirectory::open(&MemUrl, "default", 4096, false))
    }

    fn test_schema() -> TableSchema {
        let mut columns = HashMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        columns.insert(
            "score".to_string(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: false,
            },
        );
        TableSchema {
            key: "key".to_string(),
            columns,
        }
    }

    fn make_batch(keys: &[&str], scores: &[f32]) -> RecordBatch {
        let key_array = Arc::new(StringArray::from(keys.to_vec())) as Arc<dyn Array>;
        let score_array = Arc::new(Float32Array::from(scores.to_vec())) as Arc<dyn Array>;
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
        ]));
        RecordBatch::try_new(schema, vec![key_array, score_array]).unwrap()
    }

    #[tokio::test]
    async fn write_and_read_roundtrip() {
        let dir = test_dir();
        let table = Table::new(dir, test_schema());

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["a", "b", "c"], &[1.0, 2.0, 3.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader.read(&["b", "a", "c"], &["score"]).await.unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 2.0);
        assert_eq!(arr.value(1), 1.0);
        assert_eq!(arr.value(2), 3.0);
    }

    #[tokio::test]
    async fn missing_keys_produce_nulls() {
        let dir = test_dir();
        let table = Table::new(dir, test_schema());

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["a", "b"], &[10.0, 20.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader
            .read(&["a", "missing", "b"], &["score"])
            .await
            .unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), 10.0);
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 20.0);
    }

    #[tokio::test]
    async fn multi_segment_last_write_wins() {
        let dir = test_dir();
        let table = Table::new(dir, test_schema());

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["a", "b"], &[1.0, 2.0]))
            .await
            .unwrap();
        writer
            .write(&make_batch(&["a", "c"], &[10.0, 30.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader
            .read(&["a", "b", "c"], &["score"])
            .await
            .unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 10.0); // overwritten by segment 1
        assert_eq!(arr.value(1), 2.0);
        assert_eq!(arr.value(2), 30.0);
    }

    #[tokio::test]
    async fn incremental_reopen() {
        let dir = test_dir();
        let table = Table::new(dir, test_schema());

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["a", "b"], &[1.0, 2.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader.read(&["a"], &["score"]).await.unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1.0);

        // Write more data
        writer
            .write(&make_batch(&["c", "a"], &[30.0, 100.0]))
            .await
            .unwrap();

        // Reopen incrementally
        let reader = reader.reopen().await.unwrap();
        let result = reader
            .read(&["a", "b", "c"], &["score"])
            .await
            .unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 100.0); // overwritten
        assert_eq!(arr.value(1), 2.0); // from old segment
        assert_eq!(arr.value(2), 30.0); // new key
    }

    #[tokio::test]
    async fn read_multiple_columns() {
        let dir = test_dir();
        let table = Table::new(dir, test_schema());

        let writer = table.open_writer().await.unwrap();
        writer
            .write(&make_batch(&["x", "y"], &[42.0, 99.0]))
            .await
            .unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader.read(&["y", "x"], &["key", "score"]).await.unwrap();

        let keys = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(keys.value(0), "y");
        assert_eq!(keys.value(1), "x");

        let scores = result
            .column(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(scores.value(0), 99.0);
        assert_eq!(scores.value(1), 42.0);
    }

    #[tokio::test]
    async fn read_empty_table() {
        let dir = test_dir();
        let table = Table::new(dir, test_schema());

        let writer = table.open_writer().await.unwrap();
        writer.write(&make_batch(&[], &[])).await.unwrap();

        let reader = table.open_reader().await.unwrap();
        let result = reader.read(&["a"], &["score"]).await.unwrap();
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(arr.len(), 1);
        assert!(arr.is_null(0));
    }
}
