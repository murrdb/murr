use std::sync::Arc;

use crate::io2::directory::mmap::directory::MMapDirectory;
use crate::io2::table::{Table, TableReader};

pub(crate) struct TableState {
    pub table: Arc<Table<MMapDirectory>>,
    pub reader: Option<TableReader>,
}
