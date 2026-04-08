use std::path::PathBuf;

pub trait URL {}

#[derive(Debug, Clone)]
pub struct LocalUrl {
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct S3Url {
    pub bucket: String,
    pub prefix: String,
}

impl URL for S3Url {}
impl URL for LocalUrl {}
