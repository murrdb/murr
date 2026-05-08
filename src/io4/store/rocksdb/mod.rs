use rocksdb::{DBPinnableSlice, Error};

use crate::core::MurrError;
use crate::io4::store::ReadResult;

pub mod config;
pub mod plain;

const MANIFEST_FILE: &str = "manifest.json";
pub struct MultiGetResult<'a> {
    pub(crate) values: Vec<Result<Option<DBPinnableSlice<'a>>, Error>>,
}

impl ReadResult for MultiGetResult<'_> {
    fn bytes(&self) -> impl Iterator<Item = Result<Option<&[u8]>, MurrError>> {
        self.values.iter().map(|r| match r {
            Ok(opt) => Ok(opt.as_deref()),
            Err(e) => Err(MurrError::IoError(e.to_string())),
        })
    }
}
