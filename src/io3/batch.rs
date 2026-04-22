use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::DataType,
};

use crate::{
    core::{DType, MurrError},
    io3::column::{ArrayDecoder, ArrayEncoder},
    io3::{
        model::{SegmentColumnSchema, SegmentSchema},
        row::Row,
    },
};
pub struct ColumnBatch {
    pub schema: SegmentSchema,
    pub columns: Vec<ArrayRef>,
    pub row_count: usize,
}

impl ColumnBatch {
    pub fn new(batch: RecordBatch) -> Result<ColumnBatch, MurrError> {
        let fields = batch.schema().fields().clone();
        let mut columns = Vec::with_capacity(fields.len());
        let mut offset: u32 = 0;
        for (i, field) in fields.iter().enumerate() {
            let dtype = DType::try_from(field.data_type())?;
            columns.push(SegmentColumnSchema {
                index: i as u32,
                dtype,
                name: field.name().clone(),
                offset,
            });
            offset += dtype.size() as u32;
        }
        let schema = SegmentSchema::new(&columns);
        Ok(ColumnBatch {
            schema,
            columns: batch.columns().to_vec(),
            row_count: batch.num_rows(),
        })
    }
}

impl TryFrom<ColumnBatch> for RowBatch {
    type Error = MurrError;

    fn try_from(batch: ColumnBatch) -> Result<Self, Self::Error> {
        let mut row_batch = RowBatch::new(&batch.schema, batch.row_count);
        for (column, array) in batch.schema.columns.iter().zip(batch.columns.iter()) {
            match array.data_type() {
                DataType::Float32 => f32::encode_to(column, array.as_ref(), &mut row_batch)?,
                DataType::Float64 => f64::encode_to(column, array.as_ref(), &mut row_batch)?,
                DataType::Utf8 => String::encode_to(column, array.as_ref(), &mut row_batch)?,
                dt => {
                    return Err(MurrError::SegmentError(format!("unsupported dtype {dt:?}")));
                }
            }
        }
        Ok(row_batch)
    }
}

pub struct RowBatch {
    pub schema: SegmentSchema,
    pub rows: Vec<Row>,
}

impl RowBatch {
    fn new(schema: &SegmentSchema, rows: usize) -> Self {
        RowBatch {
            schema: schema.clone(),
            rows: (0..rows).map(|_| Row::new(schema)).collect(),
        }
    }
}

impl TryFrom<RowBatch> for ColumnBatch {
    type Error = MurrError;
    fn try_from(value: RowBatch) -> Result<Self, Self::Error> {
        let row_count = value.rows.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(value.schema.columns.len());
        for column in &value.schema.columns {
            let array: ArrayRef = match column.dtype {
                DType::Float32 => Arc::new(f32::decode_to(column, &value)?),
                DType::Float64 => Arc::new(f64::decode_to(column, &value)?),
                DType::Utf8 => Arc::new(String::decode_to(column, &value)?),
            };
            columns.push(array);
        }
        Ok(ColumnBatch {
            schema: value.schema,
            columns,
            row_count,
        })
    }
}
