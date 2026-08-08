use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::{Field, Schema},
};

use crate::{
    core::MurrError,
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

/// Accumulates rows into Arrow column builders inside `Store::read`. Stores
/// fan raw bytes through `add_row` / `add_empty`; the builder yields the final
/// `RecordBatch` via `build`. Keeps slice lifetimes bounded by the store fn
/// frame so backends like LMDB can hold a read txn across the iteration.
pub struct ReadBatchBuilder<'a> {
    segment: &'a SegmentSchema,
    columns: Vec<&'a SegmentColumnSchema>,
    encoders: Vec<Box<dyn ColumnEncoder>>,
}

impl<'a> ReadBatchBuilder<'a> {
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
            encoders,
        }
    }

    pub fn add_row(&mut self, bytes: &[u8]) -> Result<(), MurrError> {
        let row = ReadRow::new(self.segment, bytes);
        for e in &mut self.encoders {
            e.add_row(&row)?;
        }
        Ok(())
    }

    pub fn add_empty(&mut self) -> Result<(), MurrError> {
        for e in &mut self.encoders {
            e.add_empty()?;
        }
        Ok(())
    }

    pub fn build(mut self) -> Result<RecordBatch, MurrError> {
        let arrays: Vec<ArrayRef> = self.encoders.iter_mut().map(|e| e.build()).collect();
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| Field::new(&c.name, c.dtype.codec().arrow_dtype(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| MurrError::ArrowError(e.to_string()))
    }
}
