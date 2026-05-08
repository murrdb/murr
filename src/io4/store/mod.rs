use crate::core::{MurrError, TableSchema};

pub mod manifest;
pub mod memory;
pub mod rocksdb;
pub mod snapshot;

pub use manifest::Manifest;

pub trait ReadResult {
    fn bytes(&self) -> impl Iterator<Item = Result<Option<&[u8]>, MurrError>>;
}

pub trait Store {
    type R<'a>: ReadResult
    where
        Self: 'a;
    fn create_table(&mut self, table: &str, schema: &TableSchema) -> Result<(), MurrError>;
    fn write<'a>(
        &mut self,
        table: &str,
        rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    ) -> Result<(), MurrError>;
    fn read<'a>(&'a self, table: &str, keys: &[&[u8]]) -> Result<Self::R<'a>, MurrError>;
    fn manifest(&self) -> &Manifest;
}
