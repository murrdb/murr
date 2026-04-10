use std::sync::Arc;

use crate::io::directory::mmap::directory::MMapDirectory;
use crate::io::table::{Table, TableReader};

pub(crate) struct TableState {
    pub table: Arc<Table<MMapDirectory>>,
    pub reader: Option<TableReader>,
}
