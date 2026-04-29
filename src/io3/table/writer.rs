use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;

use crate::{
    core::{MurrError, TableSchema},
    io3::{
        directory::{Directory, DirectoryWriter},
        table::segment::Segment,
    },
};

pub struct TableWriter<D: Directory> {
    schema: TableSchema,
    writer: D::WriterType,
}

impl<D: Directory> TableWriter<D> {
    pub async fn open(schema: TableSchema, dir: Arc<D>) -> Result<Self, MurrError> {
        let writer = dir.open_writer().await?;
        Ok(TableWriter { schema, writer })
    }

    pub async fn write(&self, batch: &RecordBatch) -> Result<(), MurrError> {
        // Project to canonical column order so the SegmentSchema in the footer
        // matches what TableReader derives from the same TableSchema.
        let canonical: Schema = (&self.schema).into();
        let indices: Vec<usize> = canonical
            .fields()
            .iter()
            .map(|f| {
                batch
                    .schema()
                    .index_of(f.name())
                    .map_err(|e| MurrError::ArrowError(e.to_string()))
            })
            .collect::<Result<_, _>>()?;
        let ordered = batch
            .project(&indices)
            .map_err(|e| MurrError::ArrowError(e.to_string()))?;
        let segment = Segment::write(ordered, &self.schema)?;
        self.writer.write(&segment).await
    }
}
