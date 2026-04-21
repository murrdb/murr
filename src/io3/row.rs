// a wrapper for reading

// row layout is similar to cap'n'proto:
// [null_bitset_size: u8] [null bitset: &[u8]] [columns] [dynamic column payloads]
//
// static columns have known length and just packed together at known offsets from the SegmentSchema
// example for 4 f32 cols: [col0:f32 col1:f32 col2:f32 col3:f32] []

// dynamic cols are stored as offsets to the payloads section:
// - u32 offset has static size, so can be part of the static section
// - as it has a variable length, first 4 bytes of the payload is the length of the payload

// example for f32+utf8 column:
// [col0: f32, col1: u32] [col1_length: u32, ... col1_bytes ...]

use crate::{
    core::MurrError,
    proto::model::{DType, SegmentColumnSchema, SegmentSchema},
};
use arrow::{
    array::{Array, PrimitiveArray, StringArray},
    datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type},
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
                DataType::Utf8 => Utf8Encoder::encode_to(column, array.as_ref(), &mut row_batch)?,
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
    pub bitset_size: usize,
}

impl RowBatch {
    fn new(columns: &SegmentSchema, rows: usize) -> Self {
        todo!()
    }
}

impl TryFrom<RowBatch> for ColumnBatch {
    type Error = MurrError;
    fn try_from(value: RowBatch) -> Result<Self, Self::Error> {}
}

pub trait ArrayEncoder {
    fn encode_to(
        column: &SegmentColumnSchema,
        array: &dyn Array,
        rows: &mut RowBatch,
    ) -> Result<(), MurrError>;
}

pub trait PrimitiveArrayEncoder {
    type ArrowType: ArrowPrimitiveType;

    fn set_primitive(
        row: &mut Row,
        bitset_size: usize,
        offset: usize,
        value: &<Self::ArrowType as ArrowPrimitiveType>::Native,
    );
}

impl<T: PrimitiveArrayEncoder> ArrayEncoder for T {
    fn encode_to(
        column: &SegmentColumnSchema,
        array: &dyn Array,
        rows: &mut RowBatch,
    ) -> Result<(), MurrError> {
        let data = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T::ArrowType>>()
            .ok_or_else(|| {
                MurrError::SegmentError(format!(
                    "expected {:?}, got {:?}",
                    T::ArrowType::DATA_TYPE,
                    array.data_type()
                ))
            })?;

        let bitset_size = rows.bitset_size;
        for (index, value) in data.iter().enumerate() {
            let row = &mut rows.rows[index];
            match value {
                None => row.set_null(column.index as usize),
                Some(v) => T::set_primitive(row, bitset_size, column.offset as usize, &v),
            }
        }
        Ok(())
    }
}

impl PrimitiveArrayEncoder for f32 {
    type ArrowType = Float32Type;
    fn set_primitive(row: &mut Row, bitset_size: usize, offset: usize, value: &f32) {
        row.set_static_value(bitset_size, offset, &value.to_le_bytes());
    }
}

impl PrimitiveArrayEncoder for f64 {
    type ArrowType = Float64Type;
    fn set_primitive(row: &mut Row, bitset_size: usize, offset: usize, value: &f64) {
        row.set_static_value(bitset_size, offset, &value.to_le_bytes());
    }
}

pub struct Utf8Encoder;

impl ArrayEncoder for Utf8Encoder {
    fn encode_to(
        column: &SegmentColumnSchema,
        array: &dyn Array,
        rows: &mut RowBatch,
    ) -> Result<(), MurrError> {
        let data = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                MurrError::SegmentError(format!("expected Utf8, got {:?}", array.data_type()))
            })?;

        let bitset_size = rows.bitset_size;
        for (index, value) in data.iter().enumerate() {
            let row = &mut rows.rows[index];
            match value {
                None => row.set_null(column.index as usize),
                Some(s) => row.set_dynamic_value(bitset_size, column.offset as usize, s.as_bytes()),
            }
        }
        Ok(())
    }
}

pub struct Row {
    pub bytes: Vec<u8>,
}

impl Row {
    fn set_null(&mut self, column_index: usize) {
        // bit 0 - non-null, 1 - null; bitset lives at bytes[1..bitset_size]
        let byte = 1 + column_index / 8;
        let bit = column_index % 8;
        self.bytes[byte] |= 1 << bit;
    }

    fn set_static_value(&mut self, bitset_size: usize, byte_offset: usize, value: &[u8]) {
        let start = bitset_size + byte_offset;
        let end = start + value.len();
        self.bytes[start..end].copy_from_slice(value);
    }

    fn set_dynamic_value(&mut self, bitset_size: usize, byte_offset: usize, value: &[u8]) {
        let start = bitset_size + byte_offset;

        let payload_offset = (self.bytes.len() as u32).to_le_bytes();
        self.bytes[start..start + 4].copy_from_slice(&payload_offset);

        let len = (value.len() as u32).to_le_bytes();
        self.bytes.extend_from_slice(&len);
        self.bytes.extend_from_slice(value);
    }
}
