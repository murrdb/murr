use std::path::PathBuf;
use std::sync::Arc;

use log::info;

use async_trait::async_trait;

use crate::core::{MurrError, TableSchema};
use crate::io::directory::mmap::reader::MMapReader;
use crate::io::directory::mmap::writer::MMapWriter;
use crate::io::directory::{Directory, DirectoryReader, DirectoryWriter, METADATA_JSON};
use crate::io::info::TableInfo;
use crate::io::url::LocalUrl;

pub struct MMapDirectory {
    url: LocalUrl,
    index: String,
    schema: TableSchema,
}

impl MMapDirectory {
    pub fn path(&self) -> PathBuf {
        self.url.path.join(&self.index)
    }

    pub fn segment_path(&self, id: u32) -> PathBuf {
        self.path().join(format!("{:08}.seg", id))
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.path().join(crate::io::directory::METADATA_JSON)
    }
}

#[async_trait]
impl Directory for MMapDirectory {
    type Location = LocalUrl;
    type ReaderType = MMapReader;
    type WriterType = MMapWriter;

    fn create(url: &LocalUrl, index: &str, schema: TableSchema, _page_size: u32, _direct: bool) -> Result<MMapDirectory, MurrError> {
        let path = url.path.join(index);
        std::fs::create_dir_all(&path)
            .map_err(|e| MurrError::IoError(format!("creating dir {}: {e}", path.display())))?;

        let info = TableInfo {
            schema: schema.clone(),
            max_segment_id: 0,
            columns: std::collections::HashMap::new(),
        };
        let data = serde_json::to_vec_pretty(&info)
            .map_err(|e| MurrError::IoError(format!("serializing metadata: {e}")))?;
        let metadata_path = path.join(METADATA_JSON);
        std::fs::write(&metadata_path, &data)
            .map_err(|e| MurrError::IoError(format!("writing {}: {e}", metadata_path.display())))?;

        info!("mmap directory created: {}/{}", url.path.display(), index);
        Ok(MMapDirectory {
            url: url.clone(),
            index: index.to_string(),
            schema,
        })
    }

    fn open(url: &LocalUrl, index: &str, _page_size: u32, _direct: bool) -> Result<MMapDirectory, MurrError> {
        let path = url.path.join(index);
        let metadata_path = path.join(METADATA_JSON);
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

    fn schema(&self) -> &TableSchema {
        &self.schema
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ColumnSchema, DType};

    fn test_schema() -> TableSchema {
        let mut columns = std::collections::HashMap::new();
        columns.insert("key".to_string(), ColumnSchema { dtype: DType::Utf8, nullable: false });
        TableSchema { key: "key".to_string(), columns }
    }

    #[test]
    fn path_returns_url_path_with_index() {
        let tmp = tempfile::tempdir().unwrap();
        let url = LocalUrl { path: tmp.path().to_path_buf() };
        let dir = MMapDirectory::create(&url, "default", test_schema(), 4096, false).unwrap();
        assert_eq!(dir.path(), tmp.path().join("default"));
    }

    #[test]
    fn segment_path_zero_padded() {
        let tmp = tempfile::tempdir().unwrap();
        let url = LocalUrl { path: tmp.path().to_path_buf() };
        let dir = MMapDirectory::create(&url, "idx", test_schema(), 4096, false).unwrap();
        assert_eq!(
            dir.segment_path(0),
            tmp.path().join("idx/00000000.seg")
        );
        assert_eq!(
            dir.segment_path(42),
            tmp.path().join("idx/00000042.seg")
        );
    }

    #[test]
    fn metadata_path() {
        let tmp = tempfile::tempdir().unwrap();
        let url = LocalUrl { path: tmp.path().to_path_buf() };
        let dir = MMapDirectory::create(&url, "idx", test_schema(), 4096, false).unwrap();
        assert_eq!(
            dir.metadata_path(),
            tmp.path().join("idx/_metadata.json")
        );
    }
}
