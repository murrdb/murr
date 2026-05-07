use crate::core::MurrError;
pub mod memory;

pub mod rocksdb;
pub mod snapshot;

pub trait ReadResult {
    fn bytes(&self) -> impl Iterator<Item = Result<Option<&[u8]>, MurrError>>;
}

pub trait Store {
    type R<'a>: ReadResult
    where
        Self: 'a;
    fn create_table(&mut self, table: &str) -> Result<(), MurrError>;
    fn write<'k, 'v>(
        &mut self,
        table: &str,
        keys: impl Iterator<Item = &'k [u8]>,
        values: impl Iterator<Item = &'v [u8]>,
    ) -> Result<(), MurrError>;
    fn read<'a>(&'a self, table: &str, keys: &[&[u8]]) -> Result<Self::R<'a>, MurrError>;
}
