mod convert;
mod error;
mod handlers;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};
use axum::Router;

use crate::core::MurrError;
use crate::service::MurrService;

pub struct MurrHttpService {
    service: Arc<MurrService>,
}

impl MurrHttpService {
    pub fn new(service: Arc<MurrService>) -> Self {
        Self { service }
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
            .layer(DefaultBodyLimit::max(
                self.service.config().server.http.max_payload_size,
            ))
            .with_state(self.service.clone())
    }

    pub async fn serve(self) -> Result<(), MurrError> {
        let addr = self.service.config().server.http.addr();
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| MurrError::IoError(format!("parsing addr {addr}: {e}")))?;

        let socket = socket2::Socket::new(
            socket2::Domain::for_address(socket_addr),
            socket2::Type::STREAM,
            None,
        )
        .map_err(|e| MurrError::IoError(format!("creating socket: {e}")))?;
        socket
            .set_nodelay(true)
            .map_err(|e| MurrError::IoError(format!("set_nodelay: {e}")))?;
        socket
            .set_reuse_address(true)
            .map_err(|e| MurrError::IoError(format!("set_reuse_address: {e}")))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| MurrError::IoError(format!("set_nonblocking: {e}")))?;
        socket
            .bind(&socket_addr.into())
            .map_err(|e| MurrError::IoError(format!("binding to {addr}: {e}")))?;
        socket
            .listen(1024)
            .map_err(|e| MurrError::IoError(format!("listen: {e}")))?;

        let listener = tokio::net::TcpListener::from_std(socket.into())
            .map_err(|e| MurrError::IoError(format!("from_std listener: {e}")))?;

        axum::serve(listener, self.router())
            .await
            .map_err(|e| MurrError::IoError(format!("serving: {e}")))?;
        Ok(())
    }
}
