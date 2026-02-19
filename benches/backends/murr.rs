//! Murr backend for benchmarks.

use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, header};
use bytes::Bytes;
use http_body_util::BodyExt;
use tower::ServiceExt;

use murr::api::{AppState, create_router};
use murr::manager::TableManager;
use murr::testutil::setup_benchmark_table;

use super::{BenchBackend, BenchConfig};

/// Murr backend that benchmarks the full HTTP API stack.
pub struct MurrBackend {
    app_state: Option<AppState>,
    _temp_dir: Option<tempfile::TempDir>,
}

impl MurrBackend {
    pub fn new() -> Self {
        Self {
            app_state: None,
            _temp_dir: None,
        }
    }
}

#[async_trait]
impl BenchBackend for MurrBackend {
    /// Returns raw Arrow IPC bytes (wire format).
    type Result = Bytes;

    fn name(&self) -> &'static str {
        "murr"
    }

    async fn init(&mut self, config: &BenchConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (state, temp_dir) = setup_benchmark_table(&config.table_name, config.num_rows).await;

        let manager = Arc::new(TableManager::new(temp_dir.path().join("data")));
        manager.insert(config.table_name.clone(), state).await;

        self.app_state = Some(AppState { manager });
        self._temp_dir = Some(temp_dir);

        Ok(())
    }

    async fn fetch(
        &self,
        keys: &[String],
        columns: &[String],
    ) -> Result<Self::Result, Box<dyn Error + Send + Sync>> {
        let state = self.app_state.as_ref().unwrap();
        let app = create_router(state.clone());

        let request_body = serde_json::json!({
            "keys": keys,
            "columns": columns,
        })
        .to_string();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/bench_table/_fetch")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/vnd.apache.arrow.stream")
            .body(Body::from(request_body))?;

        let response = app.oneshot(request).await?;
        let body = response.into_body().collect().await?.to_bytes();

        Ok(body)
    }

    async fn cleanup(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.app_state = None;
        self._temp_dir = None;
        Ok(())
    }
}
