mod convert;
mod error;
mod handlers;

use std::sync::Arc;

use axum::routing::{get, post, put};
use axum::Router;

use crate::core::MurrError;
use crate::service::MurrService;

pub struct MurrApi {
    service: Arc<MurrService>,
}

impl MurrApi {
    pub fn new(service: MurrService) -> Self {
        Self {
            service: Arc::new(service),
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/openapi.json", get(handlers::openapi))
            .route("/health", get(handlers::health))
            .route("/api/v1/table", get(handlers::list_tables))
            .route("/api/v1/table/{name}/schema", get(handlers::get_schema))
            .route("/api/v1/table/{name}", put(handlers::create_table))
            .route("/api/v1/table/{name}/fetch", post(handlers::fetch))
            .route("/api/v1/table/{name}/write", put(handlers::write_table))
            .with_state(self.service.clone())
    }

    pub async fn serve(self, addr: &str) -> Result<(), MurrError> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| MurrError::IoError(format!("binding to {addr}: {e}")))?;
        axum::serve(listener, self.router())
            .await
            .map_err(|e| MurrError::IoError(format!("serving: {e}")))?;
        Ok(())
    }
}
