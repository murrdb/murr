use std::path::Path;

use ouroboros::self_referencing;

use crate::core::MurrError;
use crate::io::directory::{SegmentInfo, TableSchema};

use super::reader::TableReader;
use super::view::TableView;

#[self_referencing]
pub struct CachedTable {
    view: TableView,
    #[borrows(view)]
    #[covariant]
    reader: TableReader<'this>,
}

impl CachedTable {
    pub fn open(
        dir_path: &Path,
        schema: &TableSchema,
        segments: &[SegmentInfo],
        old: Option<CachedTable>,
    ) -> Result<Self, MurrError> {
        let existing = match old {
            Some(cached) => cached.into_heads().view.into_segments(),
            None => Vec::new(),
        };

        let view = TableView::open(dir_path, segments, existing)?;

        CachedTableTryBuilder {
            view,
            reader_builder: |view: &TableView| {
                TableReader::from_table(view, &schema.key, &schema.columns)
            },
        }
        .try_build()
    }

    pub fn get(&self, keys: &[&str], columns: &[&str]) -> Result<arrow::record_batch::RecordBatch, MurrError> {
        self.borrow_reader().get(keys, columns)
    }
}
