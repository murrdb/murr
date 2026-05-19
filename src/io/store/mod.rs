use arrow::array::RecordBatch;

use crate::core::{MurrError, TableSchema};
use crate::io::row::read::ReadBatchBuilder;

pub mod manifest;
pub mod memory;
pub mod rocksdb;
pub mod snapshot;

#[cfg(test)]
pub(crate) mod test_util;

pub use manifest::Manifest;

pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KeyValue {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

pub trait Store {
    fn create_table(&mut self, table: &str, schema: &TableSchema) -> Result<(), MurrError>;
    fn write(
        &mut self,
        table: &str,
        rows: impl IntoIterator<Item = KeyValue>,
    ) -> Result<(), MurrError>;
    fn read(
        &self,
        table: &str,
        keys: &[&[u8]],
        builder: ReadBatchBuilder<'_>,
    ) -> Result<RecordBatch, MurrError>;
    fn compact(&self, table: &str) -> Result<(), MurrError>;
    fn manifest(&self) -> &Manifest;
}
