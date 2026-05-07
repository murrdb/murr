use std::{fmt::Display, path::PathBuf};

pub mod local;
pub mod url;
use crate::{core::MurrError, io4::fs::url::URL};

pub struct File<U: URL> {
    pub path: U,
    pub size: u64,
    pub last_modified: u64,
}

pub struct RequestResult {
    pub took_millis: u64,
    pub bytes_per_sec: u64,
}

pub trait Filesystem {
    type U: URL;
    async fn list(&self, path: &Self::U) -> Result<Vec<File<Self::U>>, MurrError>;
    async fn upload(
        &self,
        local_path: &PathBuf,
        remote_path: &Self::U,
    ) -> Result<RequestResult, MurrError>;
    async fn download(
        &self,
        remote_path: &Self::U,
        local_path: PathBuf,
    ) -> Result<RequestResult, MurrError>;
}
