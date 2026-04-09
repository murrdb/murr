use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use log::info;

use async_trait::async_trait;

use crate::core::MurrError;
use crate::io2::directory::mem::reader::MemReader;
use crate::io2::directory::mem::writer::MemWriter;
use crate::io2::directory::{Directory, Reader, Writer};
use crate::io2::url::MemUrl;

pub struct MemDirectory {
    pub(crate) files: RwLock<HashMap<String, Vec<u8>>>,
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

    fn open(_url: &MemUrl, _page_size: u32, _direct: bool) -> MemDirectory {
        info!("mem directory opened");
        MemDirectory {
            files: RwLock::new(HashMap::new()),
        }
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

    #[test]
    fn open_creates_empty_directory() {
        let dir = MemDirectory::open(&MemUrl, 4096, false);
        let files = dir.files.read().unwrap();
        assert!(files.is_empty());
    }
}
