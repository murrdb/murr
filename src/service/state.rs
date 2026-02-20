use crate::io::directory::{LocalDirectory, TableSchema};
use crate::io::table::CachedTable;

pub(crate) struct TableState {
    pub dir: LocalDirectory,
    pub schema: TableSchema,
    pub cached: Option<CachedTable>,
}
