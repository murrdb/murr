use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use log::{debug, info};

use crate::core::{DType, MurrError, TableSchema};
use crate::io::column::float32::writer::Float32ColumnWriter;
use crate::io::column::utf8::writer::Utf8ColumnWriter;
use crate::io::column::{ColumnSegmentBytes, ColumnWriter};
use crate::io::directory::{Directory, DirectoryWriter};
use crate::io::info::ColumnInfo;

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
