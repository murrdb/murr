use std::path::Path;
use std::sync::Arc;

use ouroboros::self_referencing;

use crate::core::MurrError;
use crate::io::directory::{SegmentInfo, TableSchema};

use super::index::KeyIndex;
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
        let (existing, previous_index) = match old {
            Some(cached) => {
                let idx: Arc<KeyIndex> = cached.borrow_reader().index().clone();
                let segs = cached.into_heads().view.into_segments();
                (segs, Some(idx))
            }
            None => (Vec::new(), None),
        };

        let view = TableView::open(dir_path, segments, existing)?;

        CachedTableTryBuilder {
            view,
            reader_builder: |view: &TableView| {
                TableReader::from_table(view, &schema.key, &schema.columns, previous_index)
            },
        }
        .try_build()
    }

    pub fn get(
        &self,
        keys: &[&str],
        columns: &[&str],
    ) -> Result<arrow::record_batch::RecordBatch, MurrError> {
        self.borrow_reader().get(keys, columns)
    }
}
