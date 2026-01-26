//! Redis blob backend for benchmarks.
//!
//! Stores features as packed f32 blobs: key -> bytes([f32; num_columns]).

use std::error::Error;

use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::redis::Redis;

use super::testdata::{generate_bench_data, pack_floats};
use super::{BenchBackend, BenchConfig};

/// Redis backend using packed f32 blobs for all features.
pub struct RedisBlobBackend {
    container: Option<ContainerAsync<Redis>>,
    conn: Option<MultiplexedConnection>,
}

impl RedisBlobBackend {
    pub fn new() -> Self {
        Self {
            container: None,
            conn: None,
        }
    }
}

#[async_trait]
impl BenchBackend for RedisBlobBackend {
    /// Returns packed f32 blobs per key.
    type Result = Vec<Vec<u8>>;

    fn name(&self) -> &'static str {
        "redis_blob"
    }

    async fn init(&mut self, config: &BenchConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Start Redis container
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let client = redis::Client::open(format!("redis://{}:{}", host, port))?;
        let mut conn = client.get_multiplexed_async_connection().await?;

        // Generate and load test data using pipelining
        let data = generate_bench_data(config.num_rows, config.num_columns);

        // Load in batches to avoid huge pipelines
        const BATCH_SIZE: usize = 10_000;
        for chunk in data.chunks(BATCH_SIZE) {
            let mut pipe = redis::pipe();
            for (key, values) in chunk {
                let blob = pack_floats(values);
                pipe.set::<_, _>(key, blob);
            }
            pipe.query_async::<()>(&mut conn).await?;
        }

        self.container = Some(container);
        self.conn = Some(conn);

        Ok(())
    }

    async fn fetch(
        &self,
        keys: &[String],
        _columns: &[String],
    ) -> Result<Self::Result, Box<dyn Error + Send + Sync>> {
        let mut conn = self.conn.clone().unwrap();

        // MGET for batch retrieval
        let results: Vec<Option<Vec<u8>>> = conn.mget(keys).await?;

        let bytes: Vec<Vec<u8>> = results
            .into_iter()
            .map(|opt| opt.unwrap_or_default())
            .collect();

        Ok(bytes)
    }

    async fn cleanup(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Drop connection first
        self.conn = None;
        // Container will be cleaned up when dropped
        if let Some(container) = self.container.take() {
            container.stop().await?;
        }
        Ok(())
    }
}
