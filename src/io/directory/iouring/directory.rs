use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use log::info;

use crate::core::{MurrError, TableSchema};
use crate::io::directory::iouring::IoUringConfig;
use crate::io::directory::iouring::pool::IoUringPool;
use crate::io::directory::iouring::reader::IoUringReader;
use crate::io::directory::iouring::writer::IoUringWriter;
use crate::io::directory::{Directory, DirectoryReader, DirectoryWriter};
use crate::io::info::TableInfo;

pub struct IoUringDirectory {
    root: PathBuf,
    index: String,
    pub(crate) schema: TableSchema,
    pub(crate) cfg: IoUringConfig,
    pool: OnceLock<Arc<IoUringPool>>,
}

impl IoUringDirectory {
    pub fn path(&self) -> PathBuf {
        self.root.join(&self.index)
    }

    pub fn segment_path(&self, id: u32) -> PathBuf {
        self.path().join(format!("{:08}.seg", id))
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.path().join(Self::METADATA_JSON)
    }

    /// Get-or-create the shared io_uring worker pool. First reader on this
    /// directory pays the spawn cost; subsequent readers (and reopens) reuse
    /// the same pool.
    pub(crate) fn pool(&self) -> Result<Arc<IoUringPool>, MurrError> {
        if let Some(pool) = self.pool.get() {
            return Ok(Arc::clone(pool));
        }
        let pool = Arc::new(IoUringPool::new(self.cfg.clone())?);
        match self.pool.set(Arc::clone(&pool)) {
            Ok(()) => Ok(pool),
            Err(_) => Ok(Arc::clone(self.pool.get().expect("set or already set"))),
        }
    }
}

#[async_trait]
impl Directory for IoUringDirectory {
    type ReaderType = IoUringReader;
    type WriterType = IoUringWriter;
    type ConfigType = IoUringConfig;

    fn create(index: &str, schema: TableSchema, config: IoUringConfig) -> Result<Self, MurrError> {
        let path = config.cache_dir.join(index);
        std::fs::create_dir_all(&path)
            .map_err(|e| MurrError::IoError(format!("creating dir {}: {e}", path.display())))?;

        let info = TableInfo {
            schema: schema.clone(),
            segments: Vec::new(),
        };
        let data = serde_json::to_vec_pretty(&info)
            .map_err(|e| MurrError::IoError(format!("serializing metadata: {e}")))?;
        let metadata_path = path.join(Self::METADATA_JSON);
        std::fs::write(&metadata_path, &data).map_err(|e| {
            MurrError::IoError(format!("writing {}: {e}", metadata_path.display()))
        })?;

        info!(
            "iouring directory created: {}/{}",
            config.cache_dir.display(),
            index
        );
        Ok(IoUringDirectory {
            root: config.cache_dir.clone(),
            index: index.to_string(),
            schema,
            cfg: config,
            pool: OnceLock::new(),
        })
    }

    fn open(index: &str, config: IoUringConfig) -> Result<Self, MurrError> {
        let metadata_path = config.cache_dir.join(index).join(Self::METADATA_JSON);
        let data = std::fs::read(&metadata_path)
            .map_err(|e| MurrError::IoError(format!("reading {}: {e}", metadata_path.display())))?;
        let info: TableInfo = serde_json::from_slice(&data)
            .map_err(|e| MurrError::IoError(format!("parsing {}: {e}", metadata_path.display())))?;

        info!(
            "iouring directory opened: {}/{}",
            config.cache_dir.display(),
            index
        );
        Ok(IoUringDirectory {
            root: config.cache_dir.clone(),
            index: index.to_string(),
            schema: info.schema,
            cfg: config,
            pool: OnceLock::new(),
        })
    }

    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn list_indexes(config: &IoUringConfig) -> Vec<String> {
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
        info!("iouring reader opened: {}", self.path().display());
        IoUringReader::new(Arc::clone(self)).await
    }

    async fn open_writer(self: &Arc<Self>) -> Result<Self::WriterType, MurrError> {
        info!("iouring writer opened: {}", self.path().display());
        IoUringWriter::new(Arc::clone(self)).await
    }
}
