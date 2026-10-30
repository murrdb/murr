//! Bench-side data prep: schema + deterministic batch + per-iteration key generation.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use murr::core::{ColumnSchema, DTypeName, TableSchema};

use super::data::generate_batch;

pub struct Dataset {
    num_rows: usize,
    num_cols: usize,
    table_schema: TableSchema,
    arrow_schema: Arc<Schema>,
    batch: RecordBatch,
}

impl Dataset {
    pub fn new(num_rows: usize, num_cols: usize) -> Self {
        let mut columns = IndexMap::new();
        columns.insert(
            "key".to_string(),
            ColumnSchema {
                dtype: DTypeName::Utf8,
                nullable: false,
                key: true,
                strict: true,
            },
        );
        for i in 0..num_cols {
            columns.insert(
                format!("col_{}", i),
                ColumnSchema {
                    dtype: DTypeName::Float32,
                    nullable: false,
                    key: false,
                    strict: true,
                },
            );
        }
        let table_schema = TableSchema { columns };
        let arrow_schema = Arc::new(Schema::from(&table_schema));
        let batch = generate_batch(&arrow_schema, num_rows);
        Self {
            num_rows,
            num_cols,
            table_schema,
            arrow_schema,
            batch,
        }
    }

    pub fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    pub fn arrow_schema(&self) -> &Arc<Schema> {
        &self.arrow_schema
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn column_names(&self) -> Vec<String> {
        (0..self.num_cols).map(|i| format!("col_{}", i)).collect()
    }

    pub fn batches(&self, chunk: usize) -> impl Iterator<Item = RecordBatch> + '_ {
        let total = self.num_rows;
        let chunk = chunk.max(1);
        (0..total).step_by(chunk).map(move |offset| {
            let len = chunk.min(total - offset);
            self.batch.slice(offset, len)
        })
    }

    /// Random lookup keys, of which a `hit_rate` fraction exist in the table. A miss is a
    /// stored id with a prefix, so it has the same length as a hit and can never match.
    pub fn generate_keys(&self, count: usize, seed: u64, hit_rate: f64) -> Vec<String> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..count)
            .map(|_| {
                let id = rng.random_range(0..self.num_rows);
                if rng.random_bool(hit_rate) {
                    id.to_string()
                } else {
                    format!("x{id}")
                }
            })
            .collect()
    }
}
