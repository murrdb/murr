use std::path::PathBuf;

pub trait URL {
    fn to_str(&self) -> &str;
}

pub struct LocalURL {
    pub path: PathBuf,
}

impl URL for LocalURL {
    fn to_str(&self) -> &str {
        todo!()
    }
}

pub struct S3URL {
    pub bucket: String,
    pub prefix: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

impl URL for S3URL {
    fn to_str(&self) -> &str {
        todo!()
    }
}
