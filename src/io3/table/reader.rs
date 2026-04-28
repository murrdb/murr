use std::sync::{Arc, RwLock};

use arrow::array::RecordBatch;
use hashbrown::HashMap;

use crate::{
    core::{MurrError, TableSchema},
    io3::{
        directory::DirectoryReader,
        table::{index::KeyIndex, segment::Segment},
    },
};

pub struct TableReader<R: DirectoryReader> {
    schema: TableSchema,
    reader: Arc<R>,
    segments: Vec<Option<Segment>>,
    index: RwLock<KeyIndex>,
}

impl<R: DirectoryReader> TableReader<R> {
    pub async fn open(schema: TableSchema, reader: Arc<R>) -> Result<Self, MurrError> {
        todo!()
    }
    pub async fn reopen(self) -> Result<Self, MurrError> {
        todo!()
    }
    pub async fn read(&self, keys: &[&str], columns: &[&str]) -> Result<RecordBatch, MurrError> {
        todo!()
    }
}
