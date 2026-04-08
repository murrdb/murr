use std::{collections::HashMap, sync::Arc};

use memmap2::Mmap;
use url::Url;

use crate::{
    core::MurrError,
    io2::{
        bytes::FromBytes,
        directory::{SegmentReadRequest, TableReader, mmap::directory::MMapDirectory},
        info::TableInfo,
    },
};

pub struct MMapReader {
    dir: Arc<MMapDirectory>,
    mmaps: Vec<Option<Mmap>>,
}

impl TableReader for MMapReader {
    type D = MMapDirectory;

    async fn new(dir: Arc<Self::D>) -> Self {
        MMapReader {
            dir: dir,
            mmaps: vec![],
        }
    }
    async fn info() -> Result<TableInfo, MurrError> {
        todo!()
    }

    async fn read<T, C: FromBytes<T>>(
        requests: &[SegmentReadRequest],
    ) -> Result<Vec<T>, MurrError> {
        todo!()
    }
}
