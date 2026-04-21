use arrow::{array::Array, datatypes::DataType};

use crate::{
    core::{DType, MurrError},
    io3::column::{ArrayDecoder, ArrayEncoder},
    io3::{model::SegmentSchema, row::Row},
};
pub struct ColumnBatch {
    pub schema: SegmentSchema,
    pub columns: Vec<Box<dyn Array>>,
    pub row_count: usize,
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
        let mut columns: Vec<Box<dyn Array>> = Vec::with_capacity(value.schema.columns.len());
        for column in &value.schema.columns {
            let array: Box<dyn Array> = match column.dtype {
                DType::Float32 => Box::new(f32::decode_to(column, &value)?),
                DType::Float64 => Box::new(f64::decode_to(column, &value)?),
                DType::Utf8 => Box::new(String::decode_to(column, &value)?),
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
