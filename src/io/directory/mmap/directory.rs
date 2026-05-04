use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use log::info;

use crate::core::{MurrError, TableSchema};
use crate::io::directory::mmap::reader::MMapReader;
use crate::io::directory::mmap::writer::MMapWriter;
use crate::io::directory::{Directory, DirectoryConfig, DirectoryReader, DirectoryWriter};
use crate::io::info::TableInfo;
use crate::io::url::LocalUrl;

#[derive(Default)]
pub struct MMapConfig;

impl DirectoryConfig for MMapConfig {}

pub struct MMapDirectory {
    url: LocalUrl,
    index: String,
    pub(crate) schema: TableSchema,
}

impl MMapDirectory {
    pub fn path(&self) -> PathBuf {
        self.url.path.join(&self.index)
    }

    pub fn segment_path(&self, id: u32) -> PathBuf {
        self.path().join(format!("{:08}.seg", id))
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.path().join("_metadata.json")
    }
}

#[async_trait]
impl Directory for MMapDirectory {
    type Location = LocalUrl;
    type ReaderType = MMapReader;
    type WriterType = MMapWriter;
    type ConfigType = MMapConfig;

    fn create(
        url: &LocalUrl,
        index: &str,
        schema: TableSchema,
        _config: MMapConfig,
    ) -> Result<MMapDirectory, MurrError> {
        let path = url.path.join(index);
        std::fs::create_dir_all(&path)
            .map_err(|e| MurrError::IoError(format!("creating dir {}: {e}", path.display())))?;

        let info = TableInfo {
            schema: schema.clone(),
            segments: Vec::new(),
        };
        let data = serde_json::to_vec_pretty(&info)
            .map_err(|e| MurrError::IoError(format!("serializing metadata: {e}")))?;
        let metadata_path = path.join("_metadata.json");
        std::fs::write(&metadata_path, &data)
            .map_err(|e| MurrError::IoError(format!("writing {}: {e}", metadata_path.display())))?;

        info!("mmap directory created: {}/{}", url.path.display(), index);
        Ok(MMapDirectory {
            url: url.clone(),
            index: index.to_string(),
            schema,
        })
    }

    fn open(url: &LocalUrl, index: &str, _config: MMapConfig) -> Result<MMapDirectory, MurrError> {
        let metadata_path = url.path.join(index).join("_metadata.json");
        let data = std::fs::read(&metadata_path)
            .map_err(|e| MurrError::IoError(format!("reading {}: {e}", metadata_path.display())))?;
        let info: TableInfo = serde_json::from_slice(&data)
            .map_err(|e| MurrError::IoError(format!("parsing {}: {e}", metadata_path.display())))?;

        info!("mmap directory opened: {}/{}", url.path.display(), index);
        Ok(MMapDirectory {
            url: url.clone(),
            index: index.to_string(),
            schema: info.schema,
        })
    }

    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn list_indexes(url: &LocalUrl) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(&url.path) else {
            return Vec::new();
        };
        entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect()
    }

    async fn open_reader(self: &Arc<Self>) -> Result<Self::ReaderType, MurrError> {
        info!("mmap reader opened: {}", self.path().display());
        MMapReader::new(Arc::clone(self)).await
    }

    async fn open_writer(self: &Arc<Self>) -> Result<Self::WriterType, MurrError> {
        info!("mmap writer opened: {}", self.path().display());
        MMapWriter::new(Arc::clone(self)).await
    }
}
