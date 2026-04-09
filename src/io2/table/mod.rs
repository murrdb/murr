use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::{
    core::MurrError,
    io2::{column::ColumnReader, directory::Directory},
};

pub mod key_offset;

pub struct Table<D: Directory> {
    pub dir: Arc<D>,
}

pub struct TableReader<D: Directory> {
    pub table: Arc<Table<D>>,
    pub reader: D::ReaderType,
    pub columns: HashMap<String, Box<dyn ColumnReader<D>>>,
}

impl<D: Directory> TableReader<D> {
    async fn new(table: Arc<Table<D>>) -> Result<Self, MurrError> {
        let reader = table.dir.open_reader().await?;
        Ok(TableReader {
            table,
            reader,
            columns: HashMap::new(),
        })
    }

    async fn read(&self, _keys: &[&str], _columns: &[&str]) -> Result<RecordBatch, MurrError> {
        todo!()
    }
}

pub struct TableWriter<D: Directory> {
    pub table: Arc<Table<D>>,
    pub writer: D::WriterType,
}

impl<D: Directory> TableWriter<D> {
    async fn new(table: Arc<Table<D>>) -> Result<Self, MurrError> {
        let writer = table.dir.open_writer().await?;
        Ok(TableWriter { table, writer })
    }

    async fn write(&self, _batch: &RecordBatch) -> Result<(), MurrError> {
        todo!()
    }
}
