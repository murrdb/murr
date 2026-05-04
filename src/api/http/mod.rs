mod convert;
mod error;
mod handlers;
mod json;

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};
use axum::serve::ListenerExt;
use axum::Router;

use crate::core::MurrError;
use crate::io::directory::Directory;
use crate::service::MurrService;

pub struct MurrHttpService<D: Directory> {
    service: Arc<MurrService<D>>,
}

impl<D: Directory + 'static> MurrHttpService<D> {
    pub fn new(service: Arc<MurrService<D>>) -> Self {
        Self { service }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/openapi.json", get(handlers::openapi))
            .route("/health", get(handlers::health))
            .route("/api/v1/table", get(handlers::list_tables::<D>))
            .route("/api/v1/table/{name}/schema", get(handlers::get_schema::<D>))
            .route("/api/v1/table/{name}", put(handlers::create_table::<D>))
            .route("/api/v1/table/{name}/fetch", post(handlers::fetch::<D>))
            .route("/api/v1/table/{name}/write", put(handlers::write_table::<D>))
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
            .tap_io(|stream| { stream.set_nodelay(true).ok(); });
        axum::serve(listener, self.router())
            .await
            .map_err(|e| MurrError::IoError(format!("serving: {e}")))?;
        Ok(())
    }
}
