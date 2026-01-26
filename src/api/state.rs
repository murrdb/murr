use std::sync::Arc;

use crate::manager::TableManager;

/// Shared application state for HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<TableManager>,
}
