use std::sync::Arc;

use crate::old::manager::TableManager;

/// Shared application state for HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<TableManager>,
}
