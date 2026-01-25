use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use tokio_stream::StreamExt;

use crate::conf::TableConfig;
use crate::core::MurrError;
use crate::parquet::schema::validate_schema;

/// Converts Parquet files to a single Arrow IPC file.
///
/// Uses object_store trait for storage abstraction (works with local files and S3).
/// Streams RecordBatches to avoid loading all data into memory at once.
///
/// # Arguments
/// * `store` - ObjectStore implementation (LocalFileSystem, S3, etc.)
/// * `parquet_paths` - List of Parquet file paths to convert
/// * `output_path` - Local path for the output Arrow IPC file
/// * `table_config` - Configuration specifying expected schema
///
/// # Returns
/// * `Ok(PathBuf)` - Path to the generated Arrow IPC file
/// * `Err(MurrError)` - On validation failure, I/O error, or conversion error
pub async fn convert_parquet_to_ipc(
    store: Arc<dyn ObjectStore>,
    parquet_paths: &[ObjectPath],
    output_path: &Path,
    table_config: &TableConfig,
) -> Result<PathBuf, MurrError> {
    if parquet_paths.is_empty() {
        return Err(MurrError::ParquetError(
            "No Parquet files provided".to_string(),
        ));
    }

    log::info!(
        "Converting {} parquet files to Arrow IPC at '{}'",
        parquet_paths.len(),
        output_path.display()
    );

    let mut writer: Option<FileWriter<File>> = None;
    let mut validated_schema: Option<Arc<Schema>> = None;
    let mut total_rows: usize = 0;
    let mut total_batches: usize = 0;

    for parquet_path in parquet_paths {
        log::debug!("Processing parquet file: {}", parquet_path);

        let reader = ParquetObjectReader::new(store.clone(), parquet_path.clone());
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let parquet_schema = builder.schema().clone();

        // Validate schema on first file
        if validated_schema.is_none() {
            validate_schema(&parquet_schema, table_config)?;
            validated_schema = Some(parquet_schema.clone());

            let file = File::create(output_path)?;
            writer = Some(FileWriter::try_new(file, &parquet_schema)?);
        } else {
            // Ensure subsequent files have compatible schema
            let expected = validated_schema.as_ref().unwrap();
            if expected.as_ref() != parquet_schema.as_ref() {
                return Err(MurrError::ParquetError(format!(
                    "Schema mismatch in file '{}': expected {:?}, got {:?}",
                    parquet_path, expected, parquet_schema
                )));
            }
        }

        let mut stream = builder.build()?;
        while let Some(batch_result) = stream.next().await {
            let batch: RecordBatch = batch_result?;
            total_rows += batch.num_rows();
            total_batches += 1;

            writer.as_mut().unwrap().write(&batch)?;
        }
    }

    if let Some(mut w) = writer {
        w.finish()?;
    }

    log::info!(
        "Wrote {} batches ({} rows) to '{}'",
        total_batches,
        total_rows,
        output_path.display()
    );

    Ok(output_path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use object_store::local::LocalFileSystem;
    use parquet::arrow::ArrowWriter;
    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::TempDir;

    use crate::conf::{ColumnConfig, DType, LocalSourceConfig, SourceConfig};
    use crate::table::Table;

    fn make_config(columns: Vec<(&str, DType, bool)>) -> TableConfig {
        TableConfig {
            source: SourceConfig::Local(LocalSourceConfig {
                path: "/tmp".to_string(),
            }),
            poll_interval: Duration::from_secs(60),
            parts: 1,
            key: vec!["id".to_string()],
            columns: columns
                .into_iter()
                .map(|(name, dtype, nullable)| (name.to_string(), ColumnConfig { dtype, nullable }))
                .collect(),
        }
    }

    fn create_test_parquet_file(dir: &Path, name: &str, num_rows: usize, start_offset: usize) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Float32, true),
        ]));

        let ids: StringArray = (start_offset..start_offset + num_rows)
            .map(|i| Some(format!("key_{}", i)))
            .collect();
        let values: Float32Array = (start_offset..start_offset + num_rows)
            .map(|i| Some(i as f32))
            .collect();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(values)]).unwrap();

        let file = File::create(dir.join(name)).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_convert_single_parquet_file() {
        let input_dir = TempDir::new().unwrap();
        let output_dir = TempDir::new().unwrap();

        create_test_parquet_file(input_dir.path(), "data.parquet", 100, 0);

        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(input_dir.path()).unwrap());
        let config = make_config(vec![
            ("id", DType::Utf8, false),
            ("value", DType::Float32, true),
        ]);

        let parquet_paths = vec![ObjectPath::from("data.parquet")];
        let output_path = output_dir.path().join("output.arrow");

        let result = convert_parquet_to_ipc(store, &parquet_paths, &output_path, &config).await;

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.exists());

        // Verify the file can be opened by Table
        let table = Table::open(&output, "id").unwrap();
        let result = table.get(&["key_0", "key_50"], &["value"]).unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_convert_multiple_parquet_files() {
        let input_dir = TempDir::new().unwrap();
        let output_dir = TempDir::new().unwrap();

        create_test_parquet_file(input_dir.path(), "part_0.parquet", 50, 0);
        create_test_parquet_file(input_dir.path(), "part_1.parquet", 50, 50);

        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(input_dir.path()).unwrap());
        let config = make_config(vec![
            ("id", DType::Utf8, false),
            ("value", DType::Float32, true),
        ]);

        let parquet_paths = vec![
            ObjectPath::from("part_0.parquet"),
            ObjectPath::from("part_1.parquet"),
        ];
        let output_path = output_dir.path().join("output.arrow");

        let result = convert_parquet_to_ipc(store, &parquet_paths, &output_path, &config).await;

        assert!(result.is_ok());

        // Verify both files were merged
        let table = Table::open(result.unwrap(), "id").unwrap();
        let result = table.get(&["key_0", "key_75"], &["value"]).unwrap();
        assert_eq!(result.num_rows(), 2);

        let values = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(values.value(0), 0.0);
        assert_eq!(values.value(1), 75.0);
    }

    #[tokio::test]
    async fn test_convert_empty_paths() {
        let output_dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let config = make_config(vec![]);

        let parquet_paths: Vec<ObjectPath> = vec![];
        let output_path = output_dir.path().join("output.arrow");

        let result = convert_parquet_to_ipc(store, &parquet_paths, &output_path, &config).await;

        assert!(matches!(result, Err(MurrError::ParquetError(_))));
        assert!(result.unwrap_err().to_string().contains("No Parquet files"));
    }

    #[tokio::test]
    async fn test_convert_schema_validation_failure() {
        let input_dir = TempDir::new().unwrap();
        let output_dir = TempDir::new().unwrap();

        create_test_parquet_file(input_dir.path(), "data.parquet", 10, 0);

        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(input_dir.path()).unwrap());
        // Config expects Int64, but Parquet has Float32
        let config = make_config(vec![
            ("id", DType::Utf8, false),
            ("value", DType::Int64, true),
        ]);

        let parquet_paths = vec![ObjectPath::from("data.parquet")];
        let output_path = output_dir.path().join("output.arrow");

        let result = convert_parquet_to_ipc(store, &parquet_paths, &output_path, &config).await;

        assert!(matches!(result, Err(MurrError::ParquetError(_))));
        assert!(result.unwrap_err().to_string().contains("type mismatch"));
    }

    #[tokio::test]
    async fn test_convert_empty_config_allows_any_schema() {
        let input_dir = TempDir::new().unwrap();
        let output_dir = TempDir::new().unwrap();

        create_test_parquet_file(input_dir.path(), "data.parquet", 10, 0);

        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(input_dir.path()).unwrap());
        let mut config = make_config(vec![]);
        config.columns = HashMap::new();

        let parquet_paths = vec![ObjectPath::from("data.parquet")];
        let output_path = output_dir.path().join("output.arrow");

        let result = convert_parquet_to_ipc(store, &parquet_paths, &output_path, &config).await;

        assert!(result.is_ok());
    }
}
