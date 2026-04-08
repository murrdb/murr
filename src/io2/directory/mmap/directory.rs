use url::Url;

use crate::io2::directory::Directory;
use crate::io2::directory::mmap::reader::MMapReader;
use crate::io2::directory::mmap::writer::MMapWriter;

pub struct MMapDirectory {
    page_size: u32,
    direct: bool,
    url: Url,
}

impl Directory for MMapDirectory {
    type ReaderType = MMapReader;
    type WriterType = MMapWriter;

    fn open(url: &Url, page_size: u32, direct: bool) -> MMapDirectory {
        MMapDirectory {
            page_size,
            direct,
            url: url.clone(),
        }
    }

    async fn open_reader() -> Self::ReaderType {
        todo!()
    }

    async fn open_writer() -> Self::WriterType {
        todo!()
    }
}
