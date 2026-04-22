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

use crate::{core::MurrError, io3::model::SegmentSchema};

pub struct Row {
    pub bytes: Vec<u8>,
}

impl Row {
    pub fn new(schema: &SegmentSchema) -> Self {
        let row_size = schema.bitset_size as usize + schema.capacity as usize;
        let mut bytes = vec![0u8; row_size];
        bytes[0] = schema.bitset_size;
        Row { bytes }
    }
    // bit 0 - non-null, 1 - null; bitset lives at bytes[1..bitset_size]
    fn null_bit(column_index: usize) -> (usize, u8) {
        (1 + column_index / 8, (column_index % 8) as u8)
    }

    pub fn set_null(&mut self, column_index: usize) {
        let (byte, bit) = Self::null_bit(column_index);
        self.bytes[byte] |= 1 << bit;
    }

    pub fn is_null(&self, column_index: usize) -> bool {
        let (byte, bit) = Self::null_bit(column_index);
        (self.bytes[byte] >> bit) & 1 == 1
    }

    pub fn set_static_value(&mut self, bitset_size: usize, byte_offset: usize, value: &[u8]) {
        let start = bitset_size + byte_offset;
        let end = start + value.len();
        self.bytes[start..end].copy_from_slice(value);
    }

    pub fn set_dynamic_value(&mut self, bitset_size: usize, byte_offset: usize, value: &[u8]) {
        let start = bitset_size + byte_offset;

        let payload_offset = (self.bytes.len() as u32).to_le_bytes();
        self.bytes[start..start + 4].copy_from_slice(&payload_offset);

        let len = (value.len() as u32).to_le_bytes();
        self.bytes.extend_from_slice(&len);
        self.bytes.extend_from_slice(value);
    }

    fn read_u32_le(&self, at: usize) -> Result<u32, MurrError> {
        let slice = self
            .bytes
            .get(at..at + 4)
            .ok_or_else(|| MurrError::SegmentError(format!("row too short to read u32 at {at}")))?;
        let bytes: [u8; 4] = slice.try_into().map_err(|e| {
            MurrError::SegmentError(format!("row u32 slice conversion failed: {e}"))
        })?;
        Ok(u32::from_le_bytes(bytes))
    }

    pub fn get_dynamic_value(
        &self,
        bitset_size: usize,
        byte_offset: usize,
    ) -> Result<&str, MurrError> {
        let payload_offset = self.read_u32_le(bitset_size + byte_offset)? as usize;
        let len = self.read_u32_le(payload_offset)? as usize;
        let body_start = payload_offset + 4;
        let body = self
            .bytes
            .get(body_start..body_start + len)
            .ok_or_else(|| {
                MurrError::SegmentError(format!("row too short to read payload of {len} bytes"))
            })?;
        std::str::from_utf8(body).map_err(|e| MurrError::SegmentError(format!("invalid utf8: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{core::DType, io3::{batch::{ColumnBatch, RowBatch}, model::SegmentColumnSchema}};

    use super::*;
    use arrow::array::{Float32Array, StringArray};

    #[test]
    fn roundtrip_f32_utf8_with_nulls() {
        let columns = vec![
            SegmentColumnSchema {
                index: 0,
                dtype: DType::Float32,
                name: "x".into(),
                offset: 0,
            },
            SegmentColumnSchema {
                index: 1,
                dtype: DType::Utf8,
                name: "s".into(),
                offset: 4,
            },
        ];
        let schema = SegmentSchema::new(&columns);
        let f = Float32Array::from(vec![Some(1.5f32), None, Some(-3.25)]);
        let s = StringArray::from(vec![Some("hello"), Some("world"), None]);
        let batch = ColumnBatch {
            schema: schema.clone(),
            columns: vec![Arc::new(f.clone()), Arc::new(s.clone())],
            row_count: 3,
        };
        let rows: RowBatch = batch.try_into().unwrap();
        let back: ColumnBatch = rows.try_into().unwrap();
        assert_eq!(back.row_count, 3);
        assert_eq!(
            back.columns[0]
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap(),
            &f
        );
        assert_eq!(
            back.columns[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &s
        );
    }
}
