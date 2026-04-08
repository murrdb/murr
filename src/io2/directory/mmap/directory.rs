use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::MurrError;
use crate::io2::directory::mmap::reader::MMapReader;
use crate::io2::directory::mmap::writer::MMapWriter;
use crate::io2::directory::{Directory, METADATA_JSON, TableReader, TableWriter};
use crate::io2::url::LocalUrl;

pub struct MMapDirectory {
    url: LocalUrl,
    page_size: u32,
    direct: bool,
}

impl MMapDirectory {
    pub fn path(&self) -> &Path {
        &self.url.path
    }

    pub fn segment_path(&self, id: u32) -> PathBuf {
        self.url.path.join(format!("{:08}.seg", id))
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.url.path.join(METADATA_JSON)
    }
}

impl Directory for MMapDirectory {
    type Location = LocalUrl;
    type ReaderType = MMapReader;
    type WriterType = MMapWriter;

    fn open(url: &LocalUrl, page_size: u32, direct: bool) -> MMapDirectory {
        MMapDirectory {
            url: url.clone(),
            page_size,
            direct,
        }
    }

    async fn open_reader(self: &Arc<Self>) -> Result<Self::ReaderType, MurrError> {
        MMapReader::new(Arc::clone(self)).await
    }

    async fn open_writer(self: &Arc<Self>) -> Result<Self::WriterType, MurrError> {
        MMapWriter::new(Arc::clone(self)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_returns_url_path() {
        let url: LocalUrl = "file:///tmp/murr".parse().unwrap();
        let dir = MMapDirectory::open(&url, 4096, false);
        assert_eq!(dir.path(), Path::new("/tmp/murr"));
    }

    #[test]
    fn segment_path_zero_padded() {
        let url: LocalUrl = "file:///tmp/murr".parse().unwrap();
        let dir = MMapDirectory::open(&url, 4096, false);
        assert_eq!(
            dir.segment_path(0),
            PathBuf::from("/tmp/murr/00000000.seg")
        );
        assert_eq!(
            dir.segment_path(42),
            PathBuf::from("/tmp/murr/00000042.seg")
        );
    }

    #[test]
    fn metadata_path() {
        let url: LocalUrl = "file:///tmp/murr".parse().unwrap();
        let dir = MMapDirectory::open(&url, 4096, false);
        assert_eq!(
            dir.metadata_path(),
            PathBuf::from("/tmp/murr/_metadata.json")
        );
    }
}
