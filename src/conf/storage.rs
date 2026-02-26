use std::path::{Path, PathBuf};

use log::info;
use serde::{Deserialize, Serialize};

use crate::core::MurrError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default = "StorageConfig::default_cache_dir")]
    pub cache_dir: PathBuf,
}

impl StorageConfig {
    fn default_cache_dir() -> PathBuf {
        resolve_cache_dir()
            .expect("failed to resolve cache dir — set storage.cache_dir or MURR_STORAGE_CACHE__DIR")
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            cache_dir: Self::default_cache_dir(),
        }
    }
}

fn is_dir_writable(path: &Path) -> bool {
    if !path.is_dir() {
        return false;
    }
    let probe = path.join(".murr_write_probe");
    let ok = std::fs::write(&probe, b"").is_ok();
    let _ = std::fs::remove_file(&probe);
    ok
}

fn resolve_cache_dir() -> Result<PathBuf, MurrError> {
    let candidates: Vec<PathBuf> = vec![
        std::env::current_dir().unwrap_or_default(),
        PathBuf::from("/var/lib/murr"),
        PathBuf::from("/data"),
        std::env::temp_dir(),
    ];

    let mut errors: Vec<String> = Vec::new();

    for parent in &candidates {
        if parent.as_os_str().is_empty() {
            continue;
        }
        if !is_dir_writable(parent) {
            errors.push(format!("{}: not writable", parent.display()));
            continue;
        }
        let murr_dir = parent.join("murr");
        if murr_dir.is_dir() {
            if is_dir_writable(&murr_dir) {
                info!("Using cache dir: {}", murr_dir.display());
                return Ok(murr_dir);
            }
            errors.push(format!("{}: exists but not writable", murr_dir.display()));
            continue;
        }
        match std::fs::create_dir_all(&murr_dir) {
            Ok(_) => {
                info!("Using cache dir: {}", murr_dir.display());
                return Ok(murr_dir);
            }
            Err(e) => {
                errors.push(format!("{}: failed to create: {e}", murr_dir.display()));
            }
        }
    }

    Err(MurrError::ConfigParsingError(format!(
        "no writable cache directory found. Tried: {}",
        errors.join("; ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_cache_dir_resolves() {
        let config = StorageConfig::default();
        assert!(!config.cache_dir.as_os_str().is_empty());
        assert_eq!(config.cache_dir.file_name().unwrap(), "murr");
    }

    #[test]
    fn test_explicit_cache_dir_preserved() {
        let config = StorageConfig {
            cache_dir: PathBuf::from("/custom/path"),
        };
        assert_eq!(config.cache_dir, PathBuf::from("/custom/path"));
    }
}
