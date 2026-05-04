use std::sync::Arc;

use crate::io::directory::Directory;
use crate::io::table::reader::TableReader;

pub(crate) struct TableState<D: Directory> {
    pub dir: Arc<D>,
    pub reader: Option<TableReader<D::ReaderType>>,
}
