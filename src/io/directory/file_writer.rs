use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::core::{MurrError, TableSchema};
use crate::io::info::{SegmentInfo, TableInfo};

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

pub(crate) fn atomic_write(path: &Path, data: &[u8]) -> Result<(), MurrError> {
    let tmp = tmp_path(path);
    let mut file = File::create(&tmp)
        .map_err(|e| MurrError::IoError(format!("creating {}: {e}", tmp.display())))?;
    file.write_all(data)
        .map_err(|e| MurrError::IoError(format!("writing {}: {e}", tmp.display())))?;
    file.sync_all()
        .map_err(|e| MurrError::IoError(format!("syncing {}: {e}", tmp.display())))?;
    drop(file);
    std::fs::rename(&tmp, path).map_err(|e| {
        MurrError::IoError(format!(
            "renaming {} to {}: {e}",
            tmp.display(),
            path.display()
        ))
    })?;
    Ok(())
}

fn load_existing_info(metadata_path: &Path) -> Option<TableInfo> {
    std::fs::read(metadata_path)
        .ok()
        .and_then(|data| serde_json::from_slice(&data).ok())
}

pub(crate) fn next_segment_id(metadata_path: &Path) -> u32 {
    load_existing_info(metadata_path)
        .map(|info| info.segments.len() as u32)
        .unwrap_or(0)
}

pub(crate) fn append_segment_info(
    metadata_path: &Path,
    schema: &TableSchema,
    seg: SegmentInfo,
) -> Result<(), MurrError> {
    let mut info = load_existing_info(metadata_path).unwrap_or_else(|| TableInfo {
        schema: schema.clone(),
        segments: Vec::new(),
    });
    info.segments.push(seg);
    let data = serde_json::to_vec_pretty(&info)
        .map_err(|e| MurrError::IoError(format!("serializing metadata: {e}")))?;
    atomic_write(metadata_path, &data)
}
