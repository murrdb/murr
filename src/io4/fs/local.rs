use std::path::PathBuf;

use crate::{
    core::MurrError,
    io4::fs::{File, Filesystem, RequestResult, url::LocalURL},
};

pub struct LocalFS {}

impl Filesystem for LocalFS {
    type U = LocalURL;
    async fn list(&self, path: &LocalURL) -> Result<Vec<File<LocalURL>>, MurrError> {
        todo!()
    }

    async fn upload(
        &self,
        local_path: &PathBuf,
        remote_path: &LocalURL,
    ) -> Result<RequestResult, MurrError> {
        todo!()
    }
    async fn download(
        &self,
        remote_path: &LocalURL,
        local_path: PathBuf,
    ) -> Result<RequestResult, MurrError> {
        todo!()
    }
}
