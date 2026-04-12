use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use log::{debug, info};

use crate::core::{DType, MurrError, TableSchema};
use crate::io::column::float32::Float32ColumnWriter;
use crate::io::column::float64::Float64ColumnWriter;
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
            let array = batch.column(col_index);

            let col_info = Arc::new(ColumnInfo {
                name: col_name.clone(),
                dtype: col_schema.dtype.clone(),
                nullable: col_schema.nullable,
            });

            let bytes = write_column(col_info, array.as_ref())?;
            debug!(
                "encoded column '{}': {} bytes",
                col_name,
                bytes.to_bytes().len()
            );
            segment_bytes.push(bytes);
        }

        self.writer.write(&segment_bytes).await?;
        info!("segment written successfully");
        Ok(())
    }
}

fn write_column(
    col_info: Arc<ColumnInfo>,
    array: &dyn Array,
) -> Result<ColumnSegmentBytes, MurrError> {
    match (&col_info.dtype, array.data_type()) {
        (DType::Float32, DataType::Float32) => {
            let writer = Float32ColumnWriter::new(col_info);
            writer.write(array.as_primitive())
        }
        (DType::Float64, DataType::Float64) => {
            let writer = Float64ColumnWriter::new(col_info);
            writer.write(array.as_primitive())
        }
        (DType::Utf8, DataType::Utf8) => {
            let writer = Utf8ColumnWriter::new(col_info);
            writer.write(array.as_string())
        }
        (dtype, arrow_dt) => Err(MurrError::TableError(format!(
            "dtype mismatch: schema={dtype:?}, array={arrow_dt}"
        ))),
    }
}
