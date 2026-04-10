use std::path::PathBuf;
use std::sync::Arc;

use log::info;

use async_trait::async_trait;

use crate::core::MurrError;
use crate::io2::directory::mmap::reader::MMapReader;
use crate::io2::directory::mmap::writer::MMapWriter;
use crate::io2::directory::{Directory, DirectoryReader, DirectoryWriter};
use crate::io2::url::LocalUrl;

pub struct MMapDirectory {
    url: LocalUrl,
    index: String,
    page_size: u32,
    direct: bool,
}

impl MMapDirectory {
    pub fn path(&self) -> PathBuf {
        self.url.path.join(&self.index)
    }

    pub fn segment_path(&self, id: u32) -> PathBuf {
        self.path().join(format!("{:08}.seg", id))
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.path().join(crate::io2::directory::METADATA_JSON)
    }
}

#[async_trait]
impl Directory for MMapDirectory {
    type Location = LocalUrl;
    type ReaderType = MMapReader;
    type WriterType = MMapWriter;

    fn open(url: &LocalUrl, index: &str, page_size: u32, direct: bool) -> MMapDirectory {
        info!(
            "mmap directory opened: {}/{}",
            url.path.display(),
            index
        );
        MMapDirectory {
            url: url.clone(),
            index: index.to_string(),
            page_size,
            direct,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_returns_url_path_with_index() {
        let url: LocalUrl = "file:///tmp/murr".parse().unwrap();
        let dir = MMapDirectory::open(&url, "default", 4096, false);
        assert_eq!(dir.path(), PathBuf::from("/tmp/murr/default"));
    }

    #[test]
    fn segment_path_zero_padded() {
        let url: LocalUrl = "file:///tmp/murr".parse().unwrap();
        let dir = MMapDirectory::open(&url, "idx", 4096, false);
        assert_eq!(
            dir.segment_path(0),
            PathBuf::from("/tmp/murr/idx/00000000.seg")
        );
        assert_eq!(
            dir.segment_path(42),
            PathBuf::from("/tmp/murr/idx/00000042.seg")
        );
    }

    #[test]
    fn metadata_path() {
        let url: LocalUrl = "file:///tmp/murr".parse().unwrap();
        let dir = MMapDirectory::open(&url, "idx", 4096, false);
        assert_eq!(
            dir.metadata_path(),
            PathBuf::from("/tmp/murr/idx/_metadata.json")
        );
    }
}
