use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, UInt32Builder},
    datatypes::{DataType, Field, Schema},
};

use crate::{
    core::{IDX_COLUMN, MurrError},
    io::{
        codec::ColumnEncoder,
        schema::{SegmentColumnSchema, SegmentSchema},
    },
};

pub struct ReadRow<'a> {
    pub schema: &'a SegmentSchema,
    pub bitset: &'a [u8],
    pub values: &'a [u8],
}

impl<'a> ReadRow<'a> {
    pub fn new(schema: &'a SegmentSchema, raw: &'a [u8]) -> Self {
        let (bitset, values) = raw.split_at(schema.bitset_size);
        Self {
            schema,
            bitset,
            values,
        }
    }

    pub fn is_null(&self, column: &SegmentColumnSchema) -> bool {
        let idx = column.index as usize;
        let byte = idx / 8;
        let bit = (idx % 8) as u8;
        (self.bitset[byte] >> bit) & 1 == 1
    }

    pub fn read_static<T: bytemuck::Pod>(&self, column: &SegmentColumnSchema) -> T {
        let start = column.offset as usize;
        let end = start + std::mem::size_of::<T>();
        bytemuck::pod_read_unaligned(&self.values[start..end])
    }

    pub fn read_dynamic(&self, column: &SegmentColumnSchema) -> &[u8] {
        let slot = column.offset as usize;
        let payload_off =
            u32::from_le_bytes(self.values[slot..slot + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(
            self.values[payload_off..payload_off + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        &self.values[payload_off + 4..payload_off + 4 + len]
    }
}

/// Accumulates found rows into Arrow column builders inside `Store::read`. Stores
/// call `add_row` with the request position of each hit and skip misses; `build`
/// yields the final `RecordBatch`, led by the `IDX_COLUMN` of positions. Keeps
/// slice lifetimes bounded by the store fn frame so backends like LMDB can hold
/// a read txn across the iteration.
pub struct ReadBatchBuilder<'a> {
    segment: &'a SegmentSchema,
    columns: Vec<&'a SegmentColumnSchema>,
    idx: UInt32Builder,
    encoders: Vec<Box<dyn ColumnEncoder>>,
}

impl<'a> ReadBatchBuilder<'a> {
    /// `capacity` is the number of requested keys, an upper bound on the rows built.
    pub fn new(
        segment: &'a SegmentSchema,
        columns: Vec<&'a SegmentColumnSchema>,
        capacity: usize,
    ) -> Self {
        let encoders = columns
            .iter()
            .map(|c| c.dtype.codec().make_encoder((*c).clone(), capacity))
            .collect();
        Self {
            segment,
            columns,
            idx: UInt32Builder::with_capacity(capacity),
            encoders,
        }
    }

    pub fn add_row(&mut self, position: usize, bytes: &[u8]) -> Result<(), MurrError> {
        let row = ReadRow::new(self.segment, bytes);
        for e in &mut self.encoders {
            e.add_row(&row)?;
        }
        self.idx.append_value(position as u32);
        Ok(())
    }

    pub fn build(mut self) -> Result<RecordBatch, MurrError> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len() + 1);
        arrays.push(Arc::new(self.idx.finish()));
        arrays.extend(self.encoders.iter_mut().map(|e| e.build()));

        let mut fields: Vec<Field> = Vec::with_capacity(self.columns.len() + 1);
        fields.push(Field::new(IDX_COLUMN, DataType::UInt32, false));
        fields.extend(
            self.columns
                .iter()
                .map(|c| Field::new(&c.name, c.dtype.codec().arrow_dtype(), true)),
        );
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| MurrError::ArrowError(e.to_string()))
    }
}
