use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use log::info;

use crate::core::{MurrError, TableSchema};
use crate::io::directory::mem::reader::MemReader;
use crate::io::directory::mem::writer::MemWriter;
use crate::io::directory::{Directory, DirectoryConfig, DirectoryReader, DirectoryWriter};
use crate::io::info::TableInfo;
use crate::io::url::MemUrl;

#[derive(Default)]
pub struct MemConfig;

impl DirectoryConfig for MemConfig {}

pub struct MemDirectory {
    pub(crate) schema: TableSchema,
    pub(crate) metadata: RwLock<TableInfo>,
    pub(crate) segments: RwLock<Vec<Option<Vec<u8>>>>,
}

#[async_trait]
impl Directory for MemDirectory {
    type Location = MemUrl;
    type ReaderType = MemReader;
    type WriterType = MemWriter;
    type ConfigType = MemConfig;

    fn create(
        _url: &MemUrl,
        _index: &str,
        schema: TableSchema,
        _config: MemConfig,
    ) -> Result<MemDirectory, MurrError> {
        info!("mem directory created");
        Ok(MemDirectory {
            schema: schema.clone(),
            metadata: RwLock::new(TableInfo { schema, segments: Vec::new() }),
            segments: RwLock::new(Vec::new()),
        })
    }

    fn open(_url: &MemUrl, _index: &str, _config: MemConfig) -> Result<MemDirectory, MurrError> {
        Err(MurrError::IoError(
            "mem directory has no persistent storage; use create()".to_string(),
        ))
    }

    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn list_indexes(_url: &MemUrl) -> Vec<String> {
        Vec::new()
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
