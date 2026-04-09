use std::collections::HashMap;

use arrow::record_batch::RecordBatch;

use crate::{
    core::MurrError,
    io2::{column::ColumnReader, directory::Directory},
};

pub mod key_offset;

pub struct Table<D: Directory> {
    pub dir: D,
}

pub struct TableReader<'a, D: Directory> {
    pub table: &'a Table<D>,
    pub reader: D::ReaderType<'a>,
    pub columns: HashMap<String, Box<dyn ColumnReader<D>>>,
}

impl<'a, D: Directory> TableReader<'a, D> {
    async fn new(table: &'a Table<D>) -> Result<Self, MurrError> {
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

pub struct TableWriter<'a, D: Directory> {
    pub table: &'a Table<D>,
    pub writer: D::WriterType<'a>,
}

impl<'a, D: Directory> TableWriter<'a, D> {
    async fn new(table: &'a Table<D>) -> Result<Self, MurrError> {
        let writer = table.dir.open_writer().await?;
        Ok(TableWriter { table, writer })
    }

    async fn write(&self, _batch: &RecordBatch) -> Result<(), MurrError> {
        todo!()
    }
}
