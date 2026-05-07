use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TableFormat {
    #[default]
    BlockBased,
    Plain,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RocksDBConfig {
    #[serde(default)]
    pub table_format: TableFormat,
    /// PlainTable: bloom filter bits per key (built into the table). 0 disables.
    #[serde(default = "default_plain_bloom_bits")]
    pub plain_bloom_bits_per_key: i32,
    /// PlainTable: hash table utilization ratio (lower = fewer collisions, more space).
    #[serde(default = "default_data_block_hash_ratio")]
    pub plain_hash_table_ratio: f64,
    /// PlainTable: stride between sparse-index entries (lower = faster lookup, more memory).
    #[serde(default = "default_plain_index_sparseness")]
    pub plain_index_sparseness: usize,
    /// PlainTable: store the full index in the file instead of rebuilding at open.
    #[serde(default)]
    pub plain_store_index_in_file: bool,
    #[serde(default)]
    pub bloom_filter_bits_per_key: Option<f64>,
    #[serde(default = "default_true")]
    pub whole_key_filtering: bool,
    #[serde(default = "default_block_size")]
    pub block_size: usize,
    #[serde(default)]
    pub block_cache_mb: usize,
    #[serde(default)]
    pub cache_index_and_filter_blocks: bool,
    #[serde(default)]
    pub pin_l0_filter_and_index_blocks: bool,
    #[serde(default = "default_block_restart_interval")]
    pub block_restart_interval: i32,
    #[serde(default = "default_true")]
    pub data_block_hash_index: bool,
    #[serde(default = "default_data_block_hash_ratio")]
    pub data_block_hash_ratio: f64,
    #[serde(default = "default_true")]
    pub mmap_reads: bool,
    /// Open SST files with O_DIRECT, bypassing the OS page cache. Mutually exclusive with `mmap_reads`.
    #[serde(default)]
    pub use_direct_reads: bool,
    #[serde(default = "default_true")]
    pub async_io: bool,
    #[serde(default)]
    pub verify_checksums: bool,
    /// `true` → `batched_multi_get_cf_opt` (SST-format-aware fast path, requires sorted input).
    /// `false` → `multi_get_cf_opt` (generic path, no sort required).
    #[serde(default = "default_true")]
    pub batched_multi_get: bool,
    /// Only used when `batched_multi_get = true`. Pre-sort keys client-side and tell RocksDB to
    /// skip its internal sort. Ignored for the generic multi_get path.
    #[serde(default = "default_true")]
    pub sorted_input: bool,
}

fn default_true() -> bool {
    true
}
fn default_block_size() -> usize {
    512
}
fn default_block_restart_interval() -> i32 {
    8
}
fn default_data_block_hash_ratio() -> f64 {
    0.75
}
fn default_plain_bloom_bits() -> i32 {
    10
}
fn default_plain_index_sparseness() -> usize {
    16
}
