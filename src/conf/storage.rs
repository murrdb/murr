use serde::{Deserialize, Serialize};

use crate::io::directory::mmap::directory::MMapConfig;
use crate::io::directory::mem::directory::MemConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackendConfig {
    Mmap(MMapConfig),
    Mem(MemConfig),
}

impl Default for BackendConfig {
    fn default() -> Self {
        BackendConfig::Mmap(MMapConfig::default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default)]
    pub backend: BackendConfig,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_default_cache_dir_resolves() {
        let config = StorageConfig::default();
        let BackendConfig::Mmap(mmap_cfg) = config.backend else {
            panic!("default should be mmap");
        };
        assert!(!mmap_cfg.cache_dir.as_os_str().is_empty());
        assert_eq!(mmap_cfg.cache_dir.file_name().unwrap(), "murr");
    }

    #[test]
    fn test_explicit_cache_dir_preserved() {
        let config = StorageConfig {
            backend: BackendConfig::Mmap(MMapConfig::new(PathBuf::from("/custom/path"))),
        };
        let BackendConfig::Mmap(mmap_cfg) = config.backend else {
            panic!("should be mmap");
        };
        assert_eq!(mmap_cfg.cache_dir, PathBuf::from("/custom/path"));
    }
}
