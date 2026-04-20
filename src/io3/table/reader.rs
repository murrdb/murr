use std::sync::{Arc, RwLock};

use hashbrown::HashMap;

use crate::{
    core::TableSchema,
    io3::{directory::DirectoryReader, table::index::KeyIndex},
};

pub struct TableReader<R: DirectoryReader> {
    schema: TableSchema,
    reader: Arc<R>,
    //columns: HashMap<String, Arc<dyn ColumnReader<R>>>,
    index: RwLock<KeyIndex>,
}
