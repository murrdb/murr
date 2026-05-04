use std::sync::Arc;

use crate::io3::directory::Directory;
use crate::io3::table::reader::TableReader;

pub(crate) struct TableState<D: Directory> {
    pub dir: Arc<D>,
    pub reader: Option<TableReader<D::ReaderType>>,
}
