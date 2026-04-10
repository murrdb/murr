use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use log::info;

use async_trait::async_trait;

use crate::core::{MurrError, TableSchema};
use crate::io2::directory::mem::reader::MemReader;
use crate::io2::directory::mem::writer::MemWriter;
use crate::io2::directory::{Directory, DirectoryReader, DirectoryWriter, METADATA_JSON};
use crate::io2::info::TableInfo;
use crate::io2::url::MemUrl;

pub struct MemDirectory {
    pub(crate) files: RwLock<HashMap<String, Vec<u8>>>,
    schema: TableSchema,
}

impl MemDirectory {
    pub fn segment_name(id: u32) -> String {
        format!("{:08}.seg", id)
    }
}

#[async_trait]
impl Directory for MemDirectory {
    type Location = MemUrl;
    type ReaderType = MemReader;
    type WriterType = MemWriter;

    fn create(_url: &MemUrl, _index: &str, schema: TableSchema, _page_size: u32, _direct: bool) -> Result<MemDirectory, MurrError> {
        let info = TableInfo {
            schema: schema.clone(),
            max_segment_id: 0,
            columns: HashMap::new(),
        };
        let metadata = serde_json::to_vec_pretty(&info)
            .map_err(|e| MurrError::IoError(format!("serializing metadata: {e}")))?;
        let mut files = HashMap::new();
        files.insert(METADATA_JSON.to_string(), metadata);
        info!("mem directory created");
        Ok(MemDirectory {
            files: RwLock::new(files),
            schema,
        })
    }

    fn open(_url: &MemUrl, _index: &str, _page_size: u32, _direct: bool) -> Result<MemDirectory, MurrError> {
        Err(MurrError::IoError("mem directory does not support open (no persistent storage)".to_string()))
    }

    fn list_indexes(_url: &MemUrl) -> Vec<String> {
        Vec::new()
    }

    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    async fn open_reader(self: &Arc<Self>) -> Result<Self::ReaderType, MurrError> {
        info!("mem reader opened");
        MemReader::new(Arc::clone(self)).await
    }

    async fn open_writer(self: &Arc<Self>) -> Result<Self::WriterType, MurrError> {
        info!("mem writer opened");
        MemWriter::new(Arc::clone(self)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType};

    fn test_schema() -> TableSchema {
        let mut columns = HashMap::new();
        columns.insert("key".to_string(), ColumnSchema { dtype: DType::Utf8, nullable: false });
        TableSchema { key: "key".to_string(), columns }
    }

    #[test]
    fn create_initializes_metadata() {
        let dir = MemDirectory::create(&MemUrl, "default", test_schema(), 4096, false).unwrap();
        let files = dir.files.read().unwrap();
        assert!(files.contains_key(METADATA_JSON));
    }
}
