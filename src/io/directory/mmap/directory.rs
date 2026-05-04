use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use log::info;
use serde::{Deserialize, Serialize};

use crate::core::{MurrError, TableSchema};
use crate::io::directory::mmap::reader::MMapReader;
use crate::io::directory::mmap::writer::MMapWriter;
use crate::io::directory::{Directory, DirectoryConfig, DirectoryReader, DirectoryWriter};
use crate::io::info::TableInfo;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MMapConfig {
    #[serde(default = "MMapConfig::default_cache_dir")]
    pub cache_dir: PathBuf,
}

impl MMapConfig {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    fn default_cache_dir() -> PathBuf {
        resolve_cache_dir()
            .expect("failed to resolve cache dir — set storage.backend.cache_dir or MURR_STORAGE_BACKEND__CACHE__DIR")
    }
}

impl Default for MMapConfig {
    fn default() -> Self {
        Self::new(Self::default_cache_dir())
    }
}

impl DirectoryConfig for MMapConfig {}

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

pub struct MMapDirectory {
    root: PathBuf,
    index: String,
    pub(crate) schema: TableSchema,
}

impl MMapDirectory {
    pub fn path(&self) -> PathBuf {
        self.root.join(&self.index)
    }

    pub fn segment_path(&self, id: u32) -> PathBuf {
        self.path().join(format!("{:08}.seg", id))
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.path().join(Self::METADATA_JSON)
    }
}

#[async_trait]
impl Directory for MMapDirectory {
    type ReaderType = MMapReader;
    type WriterType = MMapWriter;
    type ConfigType = MMapConfig;

    fn create(index: &str, schema: TableSchema, config: MMapConfig) -> Result<Self, MurrError> {
        let path = config.cache_dir.join(index);
        std::fs::create_dir_all(&path)
            .map_err(|e| MurrError::IoError(format!("creating dir {}: {e}", path.display())))?;

        let info = TableInfo { schema: schema.clone(), segments: Vec::new() };
        let data = serde_json::to_vec_pretty(&info)
            .map_err(|e| MurrError::IoError(format!("serializing metadata: {e}")))?;
        let metadata_path = path.join(Self::METADATA_JSON);
        std::fs::write(&metadata_path, &data)
            .map_err(|e| MurrError::IoError(format!("writing {}: {e}", metadata_path.display())))?;

        info!("mmap directory created: {}/{}", config.cache_dir.display(), index);
        Ok(MMapDirectory { root: config.cache_dir, index: index.to_string(), schema })
    }

    fn open(index: &str, config: MMapConfig) -> Result<Self, MurrError> {
        let metadata_path = config.cache_dir.join(index).join(Self::METADATA_JSON);
        let data = std::fs::read(&metadata_path)
            .map_err(|e| MurrError::IoError(format!("reading {}: {e}", metadata_path.display())))?;
        let info: TableInfo = serde_json::from_slice(&data)
            .map_err(|e| MurrError::IoError(format!("parsing {}: {e}", metadata_path.display())))?;

        info!("mmap directory opened: {}/{}", config.cache_dir.display(), index);
        Ok(MMapDirectory { root: config.cache_dir, index: index.to_string(), schema: info.schema })
    }

    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn list_indexes(config: &MMapConfig) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(&config.cache_dir) else {
            return Vec::new();
        };
        entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect()
    }

    async fn open_reader(self: &Arc<Self>) -> Result<Self::ReaderType, MurrError> {
        info!("mmap reader opened: {}", self.path().display());
        MMapReader::new(Arc::clone(self)).await
    }

    async fn open_writer(self: &Arc<Self>) -> Result<Self::WriterType, MurrError> {
        info!("mmap writer opened: {}", self.path().display());
        MMapWriter::new(Arc::clone(self)).await
    }
}
