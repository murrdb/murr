use crate::core::{MurrError, TableSchema};

pub mod manifest;
pub mod memory;
pub mod rocksdb;
pub mod snapshot;

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

pub trait ReadResult {
    fn bytes(&self) -> impl Iterator<Item = Result<Option<&[u8]>, MurrError>>;
}

pub trait Store {
    type R<'a>: ReadResult
    where
        Self: 'a;
    fn create_table(&mut self, table: &str, schema: &TableSchema) -> Result<(), MurrError>;
    fn write(
        &mut self,
        table: &str,
        rows: impl IntoIterator<Item = KeyValue>,
    ) -> Result<(), MurrError>;
    fn read<'a>(&'a self, table: &str, keys: &[&[u8]]) -> Result<Self::R<'a>, MurrError>;
    fn compact(&self, table: &str) -> Result<(), MurrError>;
    fn manifest(&self) -> &Manifest;
}
