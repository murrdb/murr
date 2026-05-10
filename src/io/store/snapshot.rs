use std::path::PathBuf;

use crate::core::TableSchema;

pub struct Snapshot {
    sst: Vec<PathBuf>,
    metadata: Vec<PathBuf>,
    schema: TableSchema
}

impl Snapshot {
    fn from_checkpoint(path: &PathBuf) -> Snapshot {
        todo!()
    }
}