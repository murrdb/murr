mod convert;
mod error;
mod handlers;

use std::sync::Arc;

use crate::core::MurrError;
use crate::io::store::Store;
use crate::service::MurrService;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};
use axum::serve::ListenerExt;
use log::info;

pub struct MurrHttpService<S: Store> {
    service: Arc<MurrService<S>>,
}

impl<S: Store> MurrHttpService<S> {
    pub fn new(service: Arc<MurrService<S>>) -> Self {
        Self { service }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/openapi.json", get(handlers::openapi))
            .route("/health", get(handlers::health))
            .route("/api/v1/table", get(handlers::list_tables::<S>))
            .route("/api/v1/table/{name}/schema", get(handlers::get_schema::<S>))
            .route("/api/v1/table/{name}", put(handlers::create_table::<S>))
            .route("/api/v1/table/{name}/fetch", post(handlers::fetch::<S>))
            .route("/api/v1/table/{name}/write", put(handlers::write_table::<S>))
            .layer(DefaultBodyLimit::max(
                self.service.config().server.http.max_payload_size,
            ))
            .with_state(self.service.clone())
    }

    pub async fn serve(self) -> Result<(), MurrError> {
        let addr = self.service.config().server.http.addr();
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| MurrError::IoError(format!("binding to {addr}: {e}")))?
            .tap_io(|stream| {
                stream.set_nodelay(true).ok();
            });
        info!("Listening for HTTP requests on {addr}");
        axum::serve(listener, self.router())
            .await
            .map_err(|e| MurrError::IoError(format!("serving: {e}")))?;
        info!("HTTP server stopped");
        Ok(())
    }
}
