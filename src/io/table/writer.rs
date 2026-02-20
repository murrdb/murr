use std::collections::HashMap;
use std::fs::File;
use std::time::SystemTime;

use arrow::array::{Float32Array, StringArray};
use arrow::record_batch::RecordBatch;

use crate::conf::{ColumnConfig, DType};
use crate::core::MurrError;
use crate::io::directory::{Directory, LocalDirectory, SegmentInfo};
use crate::io::segment::WriteSegment;

use super::column::ColumnSegment;
use super::column::float32::segment::Float32Segment;
use super::column::utf8::segment::Utf8Segment;

pub struct TableWriter<'a> {
    dir: &'a mut LocalDirectory,
    segments: Vec<SegmentInfo>,
}

impl<'a> TableWriter<'a> {
    pub async fn new(dir: &'a mut LocalDirectory) -> Result<TableWriter<'a>, MurrError> {
        let segments = dir.segments().await?;
        Ok(Self { dir, segments })
    }

    pub fn add_segment(
        &mut self,
        schema: &HashMap<String, ColumnConfig>,
        batch: &RecordBatch,
    ) -> Result<SegmentInfo, MurrError> {
        let next_id = self.segments.last().map(|s| s.id + 1).unwrap_or(0);

        let mut ws = WriteSegment::new();

        for (col_name, col_config) in schema {
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
                ref other => {
                    return Err(MurrError::TableError(format!(
                        "unsupported dtype {:?} for column '{}'",
                        other, col_name
                    )));
                }
            };

            ws.add_column(col_name, bytes);
        }

        let file_name = format!("{:08}.seg", next_id);
        let file_path = self.dir.path().join(&file_name);
        let mut file = File::create(&file_path).map_err(|e| {
            MurrError::IoError(format!("creating segment file {}: {}", file_path.display(), e))
        })?;
        ws.write(&mut file)?;

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
    use crate::io::directory::{Directory, LocalDirectory};
    use crate::io::table::reader::TableReader;
    use crate::io::table::view::TableView;
    use arrow::array::Float32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_write_and_read_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut local = LocalDirectory::new(dir.path());

        // Write a segment via TableWriter
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("score", DataType::Float32, true),
        ]));
        let keys: StringArray = ["a", "b", "c"].iter().map(|k| Some(*k)).collect();
        let scores: Float32Array = [1.0f32, 2.0, 3.0].iter().map(|v| Some(*v)).collect();
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(keys), Arc::new(scores)],
        )
        .unwrap();

        let mut schema = HashMap::new();
        schema.insert(
            "key".to_string(),
            ColumnConfig {
                dtype: DType::Utf8,
                nullable: false,
            },
        );
        schema.insert(
            "score".to_string(),
            ColumnConfig {
                dtype: DType::Float32,
                nullable: true,
            },
        );

        let mut writer = TableWriter::new(&mut local).await.unwrap();
        let info = writer.add_segment(&schema, &batch).unwrap();
        assert_eq!(info.id, 0);
        assert_eq!(info.file_name, "00000000.seg");

        // Read back via TableReader
        let infos = local.segments().await.unwrap();
        let view = TableView::open(dir.path(), &infos).unwrap();

        let mut read_schema = HashMap::new();
        read_schema.insert(
            "score".to_string(),
            ColumnConfig {
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

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
        ]));

        let mut schema = HashMap::new();
        schema.insert(
            "key".to_string(),
            ColumnConfig {
                dtype: DType::Utf8,
                nullable: false,
            },
        );

        let mut writer = TableWriter::new(&mut local).await.unwrap();

        let keys1: StringArray = ["a"].iter().map(|k| Some(*k)).collect();
        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(keys1)]).unwrap();
        let info1 = writer.add_segment(&schema, &batch1).unwrap();

        let keys2: StringArray = ["b"].iter().map(|k| Some(*k)).collect();
        let batch2 = RecordBatch::try_new(arrow_schema, vec![Arc::new(keys2)]).unwrap();
        let info2 = writer.add_segment(&schema, &batch2).unwrap();

        assert_eq!(info1.id, 0);
        assert_eq!(info2.id, 1);
        assert_eq!(info2.file_name, "00000001.seg");
    }
}
