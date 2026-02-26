use std::time::SystemTime;

use arrow::array::{Float32Array, StringArray};
use arrow::record_batch::RecordBatch;

use crate::core::DType;
use crate::core::MurrError;
use crate::io::directory::{Directory, LocalDirectory, SegmentInfo, TableSchema};
use crate::io::segment::WriteSegment;

use super::column::ColumnSegment;
use super::column::float32::segment::Float32Segment;
use super::column::utf8::segment::Utf8Segment;

const TABLE_JSON: &str = "table.json";

pub struct TableWriter<'a> {
    dir: &'a mut LocalDirectory,
    schema: TableSchema,
    segments: Vec<SegmentInfo>,
}

impl<'a> TableWriter<'a> {
    pub async fn create(
        schema: &TableSchema,
        dir: &'a mut LocalDirectory,
    ) -> Result<TableWriter<'a>, MurrError> {
        let index = dir.index().await?;
        if index.is_some() {
            return Err(MurrError::TableAlreadyExists(
                "table already exists in directory".to_string(),
            ));
        }

        let data = serde_json::to_vec_pretty(schema).map_err(|e| {
            MurrError::IoError(format!("serializing table schema: {}", e))
        })?;
        dir.write(TABLE_JSON, &data).await?;

        Ok(Self {
            dir,
            schema: schema.clone(),
            segments: vec![],
        })
    }

    pub async fn open(dir: &'a mut LocalDirectory) -> Result<TableWriter<'a>, MurrError> {
        let index = dir.index().await?;
        match index {
            None => Err(MurrError::TableError(
                "table does not exist in directory".to_string(),
            )),
            Some(info) => Ok(Self {
                dir,
                schema: info.schema,
                segments: info.segments,
            }),
        }
    }

    pub async fn add_segment(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<SegmentInfo, MurrError> {
        let next_id = self.segments.last().map(|s| s.id + 1).unwrap_or(0);

        let mut ws = WriteSegment::new();

        for (col_name, col_config) in &self.schema.columns {
            let col_index = batch.schema().index_of(col_name).map_err(|_| {
                MurrError::TableError(format!(
                    "column '{}' not found in RecordBatch",
                    col_name
                ))
            })?;
            let array = batch.column(col_index);

            let bytes = match col_config.dtype {
                DType::Float32 => {
                    let typed = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                        MurrError::TableError(format!(
                            "column '{}' is not a Float32Array",
                            col_name
                        ))
                    })?;
                    Float32Segment::write(col_config, typed)?
                }
                DType::Utf8 => {
                    let typed = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                        MurrError::TableError(format!(
                            "column '{}' is not a StringArray",
                            col_name
                        ))
                    })?;
                    Utf8Segment::write(col_config, typed)?
                }
            };

            ws.add_column(col_name, bytes);
        }

        let file_name = format!("{:08}.seg", next_id);
        let mut buf = Vec::new();
        ws.write(&mut buf)?;
        self.dir.write(&file_name, &buf).await?;

        let file_path = self.dir.path().join(&file_name);
        let metadata = std::fs::metadata(&file_path).map_err(|e| {
            MurrError::IoError(format!(
                "reading metadata for {}: {}",
                file_path.display(),
                e
            ))
        })?;

        let info = SegmentInfo {
            id: next_id,
            size: metadata.len() as u32,
            file_name,
            last_modified: SystemTime::now(),
        };

        self.segments.push(info.clone());
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ColumnSchema;
    use crate::io::directory::{Directory, LocalDirectory};
    use crate::io::table::reader::TableReader;
    use crate::io::table::view::TableView;
    use arrow::array::Float32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

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
                nullable: true,
            },
        );
        TableSchema { key: "key".to_string(), columns }
    }

    #[tokio::test]
    async fn test_create_writes_table_json() {
        let dir = TempDir::new().unwrap();
        let mut local = LocalDirectory::new(dir.path());
        let schema = test_schema();

        let _writer = TableWriter::create(&schema, &mut local).await.unwrap();

        // Verify table.json exists and is parseable
        let index = local.index().await.unwrap().unwrap();
        assert_eq!(index.schema, schema);
        assert_eq!(index.segments.len(), 0);
    }

    #[tokio::test]
    async fn test_create_fails_if_table_exists() {
        let dir = TempDir::new().unwrap();
        let mut local = LocalDirectory::new(dir.path());
        let schema = test_schema();

        let _writer = TableWriter::create(&schema, &mut local).await.unwrap();
        drop(_writer);

        let result = TableWriter::create(&schema, &mut local).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_fails_if_no_table() {
        let dir = TempDir::new().unwrap();
        let mut local = LocalDirectory::new(dir.path());

        let result = TableWriter::open(&mut local).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_loads_schema() {
        let dir = TempDir::new().unwrap();
        let mut local = LocalDirectory::new(dir.path());
        let schema = test_schema();

        let _writer = TableWriter::create(&schema, &mut local).await.unwrap();
        drop(_writer);

        let _writer = TableWriter::open(&mut local).await.unwrap();
    }

    #[tokio::test]
    async fn test_write_and_read_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut local = LocalDirectory::new(dir.path());
        let schema = test_schema();

        let mut writer = TableWriter::create(&schema, &mut local).await.unwrap();

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let keys: arrow::array::StringArray = ["a", "b", "c"].iter().map(|k| Some(*k)).collect();
        let scores: Float32Array = [1.0f32, 2.0, 3.0].iter().map(|v| Some(*v)).collect();
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(keys), Arc::new(scores)],
        )
        .unwrap();

        let info = writer.add_segment(&batch).await.unwrap();
        assert_eq!(info.id, 0);
        assert_eq!(info.file_name, "00000000.seg");
        drop(writer);

        // Read back via TableReader
        let index = local.index().await.unwrap().unwrap();
        let view = TableView::open(dir.path(), &index.segments).unwrap();

        let mut read_schema = HashMap::new();
        read_schema.insert(
            "score".to_string(),
            ColumnSchema {
                dtype: DType::Float32,
                nullable: true,
            },
        );

        let reader = TableReader::from_table(&view, "key", &read_schema).unwrap();
        let result = reader.get(&["b", "c", "a"], &["score"]).unwrap();

        let vals = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(vals.value(0), 2.0);
        assert_eq!(vals.value(1), 3.0);
        assert_eq!(vals.value(2), 1.0);
    }

    #[tokio::test]
    async fn test_multiple_segments_increment_id() {
        let dir = TempDir::new().unwrap();
        let mut local = LocalDirectory::new(dir.path());

        let mut columns = HashMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        let schema = TableSchema { key: "key".to_string(), columns };

        let mut writer = TableWriter::create(&schema, &mut local).await.unwrap();

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
        ]));

        let keys1: arrow::array::StringArray = ["a"].iter().map(|k| Some(*k)).collect();
        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(keys1)]).unwrap();
        let info1 = writer.add_segment(&batch1).await.unwrap();

        let keys2: arrow::array::StringArray = ["b"].iter().map(|k| Some(*k)).collect();
        let batch2 = RecordBatch::try_new(arrow_schema, vec![Arc::new(keys2)]).unwrap();
        let info2 = writer.add_segment(&batch2).await.unwrap();

        assert_eq!(info1.id, 0);
        assert_eq!(info2.id, 1);
        assert_eq!(info2.file_name, "00000001.seg");
    }
}
