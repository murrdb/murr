use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;

use crate::{
    core::{MurrError, TableSchema},
    io::model::SegmentSchema,
    io4::store::Store,
};

pub struct Table<S: Store> {
    pub store: Arc<S>,
    pub table: TableSchema,
    pub segment: SegmentSchema,
    pub columns: HashMap<String, usize>,
}

impl<S: Store> Table<S> {
    fn new(store: Arc<S>, table: TableSchema) -> Self {
        todo!()
    }
    fn read(&self, keys: &[&str], columns: &[&str]) -> Result<RecordBatch, MurrError> {
        todo!()
    }
    fn write(&mut self, batch: &RecordBatch) -> Result<(), MurrError> {
        todo!()
    }
}
