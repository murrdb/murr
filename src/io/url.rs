use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use crate::core::MurrError;

pub trait Url {}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalUrl {
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
pub struct S3Url {
    pub bucket: String,
    pub prefix: String,
}

pub struct MemUrl;

impl Url for S3Url {}
impl Url for LocalUrl {}
impl Url for MemUrl {}

impl FromStr for LocalUrl {
    type Err = MurrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rest = s
            .strip_prefix("file://")
            .ok_or_else(|| MurrError::IoError(format!("expected file:// scheme, got: {s}")))?;
        if rest.is_empty() {
            return Err(MurrError::IoError("file:// URL has empty path".to_string()));
        }
        Ok(LocalUrl {
            path: PathBuf::from(rest),
        })
    }
}

impl FromStr for S3Url {
    type Err = MurrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rest = s
            .strip_prefix("s3://")
            .ok_or_else(|| MurrError::IoError(format!("expected s3:// scheme, got: {s}")))?;
        let (bucket, prefix) = rest
            .split_once('/')
            .ok_or_else(|| MurrError::IoError(format!("s3:// URL missing prefix: {s}")))?;
        if bucket.is_empty() {
            return Err(MurrError::IoError("s3:// URL has empty bucket".to_string()));
        }
        Ok(S3Url {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        })
    }
}

impl fmt::Display for LocalUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "file://{}", self.path.display())
    }
}

impl fmt::Display for S3Url {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_url_parse_absolute() {
        let url: LocalUrl = "file:///tmp/murr/data".parse().unwrap();
        assert_eq!(url.path, PathBuf::from("/tmp/murr/data"));
    }

    #[test]
    fn local_url_parse_relative() {
        let url: LocalUrl = "file://data/tables".parse().unwrap();
        assert_eq!(url.path, PathBuf::from("data/tables"));
    }

    #[test]
    fn local_url_parse_wrong_scheme() {
        let err = "s3://bucket/prefix".parse::<LocalUrl>();
        assert!(err.is_err());
    }

    #[test]
    fn local_url_parse_empty_path() {
        let err = "file://".parse::<LocalUrl>();
        assert!(err.is_err());
    }

    #[test]
    fn local_url_display_roundtrip_absolute() {
        let url: LocalUrl = "file:///tmp/murr".parse().unwrap();
        assert_eq!(url.to_string(), "file:///tmp/murr");
    }

    #[test]
    fn local_url_display_roundtrip_relative() {
        let url: LocalUrl = "file://data/tables".parse().unwrap();
        assert_eq!(url.to_string(), "file://data/tables");
    }

    #[test]
    fn s3_url_parse() {
        let url: S3Url = "s3://my-bucket/path/to/data".parse().unwrap();
        assert_eq!(url.bucket, "my-bucket");
        assert_eq!(url.prefix, "path/to/data");
    }

    #[test]
    fn s3_url_parse_empty_prefix() {
        let url: S3Url = "s3://my-bucket/".parse().unwrap();
        assert_eq!(url.bucket, "my-bucket");
        assert_eq!(url.prefix, "");
    }

    #[test]
    fn s3_url_parse_wrong_scheme() {
        let err = "file:///tmp".parse::<S3Url>();
        assert!(err.is_err());
    }

    #[test]
    fn s3_url_parse_empty_bucket() {
        let err = "s3:///prefix".parse::<S3Url>();
        assert!(err.is_err());
    }

    #[test]
    fn s3_url_parse_no_prefix() {
        let err = "s3://bucket-only".parse::<S3Url>();
        assert!(err.is_err());
    }

    #[test]
    fn s3_url_display_roundtrip() {
        let url: S3Url = "s3://bucket/prefix/path".parse().unwrap();
        assert_eq!(url.to_string(), "s3://bucket/prefix/path");
    }
}
