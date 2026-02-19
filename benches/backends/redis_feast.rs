//! Redis Feast-style backend for benchmarks.
//!
//! Stores features as Redis hashes: key -> HSET { col_0: f32, col_1: f32, ... }.

use std::collections::HashMap;
use std::error::Error;

use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

use super::testdata::{bench_column_names, generate_bench_data};
use super::{BenchBackend, BenchConfig};

/// Redis backend using Feast-style hashes (field per feature).
pub struct RedisFeastBackend {
    container: Option<ContainerAsync<Redis>>,
    conn: Option<MultiplexedConnection>,
    column_names: Vec<String>,
}

impl RedisFeastBackend {
    pub fn new() -> Self {
        Self {
            container: None,
            conn: None,
            column_names: Vec::new(),
        }
    }
}

#[async_trait]
impl BenchBackend for RedisFeastBackend {
    /// Returns feature maps per key.
    type Result = Vec<HashMap<String, f32>>;

    fn name(&self) -> &'static str {
        "redis_feast"
    }

    async fn init(&mut self, config: &BenchConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Start Redis container
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let client = redis::Client::open(format!("redis://{}:{}", host, port))?;
        let mut conn = client.get_multiplexed_async_connection().await?;

        // Generate test data
        let data = generate_bench_data(config.num_rows, config.num_columns);
        let columns = bench_column_names(config.num_columns);

        // Load using HSET with pipelining
        const BATCH_SIZE: usize = 10_000;
        for chunk in data.chunks(BATCH_SIZE) {
            let mut pipe = redis::pipe();
            for (key, values) in chunk {
                // Build field-value pairs: [(col_0, val_0), (col_1, val_1), ...]
                let fields: Vec<(&str, f32)> = columns
                    .iter()
                    .zip(values.iter())
                    .map(|(col, val)| (col.as_str(), *val))
                    .collect();
                pipe.hset_multiple::<_, _, _>(key, &fields);
            }
            pipe.query_async::<()>(&mut conn).await?;
        }

        self.container = Some(container);
        self.conn = Some(conn);
        self.column_names = columns;

        Ok(())
    }

    async fn fetch(
        &self,
        keys: &[String],
        columns: &[String],
    ) -> Result<Self::Result, Box<dyn Error + Send + Sync>> {
        let mut conn = self.conn.clone().unwrap();

        // Use pipelining for batch HMGET
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.hget::<_, _>(key, columns);
        }

        let results: Vec<Vec<Option<f32>>> = pipe.query_async(&mut conn).await?;

        let feature_maps: Vec<HashMap<String, f32>> = results
            .into_iter()
            .map(|values| {
                columns
                    .iter()
                    .zip(values.into_iter())
                    .filter_map(|(col, opt_val)| opt_val.map(|v| (col.clone(), v)))
                    .collect()
            })
            .collect();

        Ok(feature_maps)
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
