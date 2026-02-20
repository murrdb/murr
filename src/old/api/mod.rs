mod error;
mod handlers;
mod response;
mod state;
mod types;

use axum::Router;
use axum::routing::{get, post};

pub use state::AppState;

/// Create the API router with all routes.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health_handler))
        .route("/v1/{table}/_fetch", post(handlers::fetch_handler))
        .with_state(state)
}
