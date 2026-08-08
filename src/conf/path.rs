use std::path::{Path, PathBuf};

use log::info;

use crate::core::MurrError;

const MURR_DIR: &str = "murr";

pub fn is_dir_writable(path: &Path) -> bool {
    if !path.is_dir() {
        return false;
    }
    let probe = path.join(".murr_write_probe");
    let ok = std::fs::write(&probe, b"").is_ok();
    let _ = std::fs::remove_file(&probe);
    ok
}

pub fn resolve_cache_dir() -> Result<PathBuf, MurrError> {
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
        let murr_dir = parent.join(MURR_DIR);
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
