use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::conf::path::resolve_cache_dir;
use crate::io::store::rocksdb::block::BlockConfig;
use crate::io::store::rocksdb::plain::PlainConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfig {
    #[serde(default = "default_path")]
    pub path: PathBuf,
    #[serde(default, flatten)]
    pub backend: BackendConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BackendConfig {
    Mmap(PlainConfig),
    Block(BlockConfig),
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: default_path(),
            backend: BackendConfig::default(),
        }
    }
}

impl Default for BackendConfig {
    fn default() -> Self {
        BackendConfig::Mmap(PlainConfig::default())
    }
}

fn default_path() -> PathBuf {
    resolve_cache_dir()
        .expect("failed to resolve cache dir — set storage.path or MURR_STORAGE_PATH")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_mmap() {
        let config = StorageConfig::default();
        assert!(matches!(config.backend, BackendConfig::Mmap(_)));
        assert!(!config.path.as_os_str().is_empty());
        assert_eq!(config.path.file_name().unwrap(), "murr");
    }

    #[test]
    fn parses_mmap_yaml() {
        let yaml = "
path: /custom/path
mmap:
  bloom_bits_per_key: 20
";
        let cfg: StorageConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.path, PathBuf::from("/custom/path"));
        match cfg.backend {
            BackendConfig::Mmap(p) => assert_eq!(p.bloom_bits_per_key, 20),
            _ => panic!("expected mmap"),
        }
    }

    #[test]
    fn parses_block_yaml() {
        let yaml = "
path: /custom/path
block:
  block_size: 8192
";
        let cfg: StorageConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.path, PathBuf::from("/custom/path"));
        match cfg.backend {
            BackendConfig::Block(b) => assert_eq!(b.block_size, 8192),
            _ => panic!("expected block"),
        }
    }
}
