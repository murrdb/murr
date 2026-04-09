use std::collections::HashMap;
use std::sync::RwLock;

use crate::core::MurrError;
use crate::io2::directory::mem::reader::MemReader;
use crate::io2::directory::mem::writer::MemWriter;
use crate::io2::directory::{Directory, METADATA_JSON, Reader, Writer};
use crate::io2::url::MemUrl;

pub struct MemDirectory {
    pub(crate) files: RwLock<HashMap<String, Vec<u8>>>,
}

impl MemDirectory {
    pub fn segment_name(id: u32) -> String {
        format!("{:08}.seg", id)
    }
}

impl Directory for MemDirectory {
    type Location = MemUrl;
    type ReaderType<'a> = MemReader<'a>;
    type WriterType<'a> = MemWriter<'a>;

    fn open(_url: &MemUrl, _page_size: u32, _direct: bool) -> MemDirectory {
        MemDirectory {
            files: RwLock::new(HashMap::new()),
        }
    }

    async fn open_reader(&self) -> Result<Self::ReaderType<'_>, MurrError> {
        MemReader::new(self).await
    }

    async fn open_writer(&self) -> Result<Self::WriterType<'_>, MurrError> {
        MemWriter::new(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_empty_directory() {
        let dir = MemDirectory::open(&MemUrl, 4096, false);
        let files = dir.files.read().unwrap();
        assert!(files.is_empty());
    }
}
