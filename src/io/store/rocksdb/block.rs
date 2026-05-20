use rocksdb::{BlockBasedOptions, Cache, DataBlockIndexType, Options};
use serde::{Deserialize, Serialize};

use crate::io::store::rocksdb::ReadMethod;
use crate::io::store::rocksdb::plain::{
    default_data_block_hash_ratio, default_disable_auto_compactions,
    default_target_file_size_base, default_write_buffer_size,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockConfig {
    /// Bloom filter bits per key. None disables.
    #[serde(default)]
    pub bloom_filter_bits_per_key: Option<f64>,
    #[serde(default = "default_true")]
    pub whole_key_filtering: bool,
    #[serde(default = "default_block_size")]
    pub block_size: usize,
    /// LRU block cache size in MiB. 0 disables.
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
    /// Open SST files with O_DIRECT, bypassing the OS page cache.
    /// Mutually exclusive with `mmap_reads`.
    #[serde(default)]
    pub use_direct_reads: bool,
    #[serde(default = "default_true")]
    pub async_io: bool,
    #[serde(default)]
    pub verify_checksums: bool,
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,
    #[serde(default = "default_target_file_size_base")]
    pub target_file_size_base: u64,
    #[serde(default = "default_disable_auto_compactions")]
    pub disable_auto_compactions: bool,
    #[serde(default = "default_block_read_method")]
    pub read_method: ReadMethod,
}

impl Default for BlockConfig {
    fn default() -> Self {
        Self {
            bloom_filter_bits_per_key: None,
            whole_key_filtering: true,
            block_size: default_block_size(),
            block_cache_mb: 0,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            block_restart_interval: default_block_restart_interval(),
            data_block_hash_index: true,
            data_block_hash_ratio: default_data_block_hash_ratio(),
            mmap_reads: true,
            use_direct_reads: false,
            async_io: true,
            verify_checksums: false,
            write_buffer_size: default_write_buffer_size(),
            target_file_size_base: default_target_file_size_base(),
            disable_auto_compactions: default_disable_auto_compactions(),
            read_method: default_block_read_method(),
        }
    }
}

fn default_block_read_method() -> ReadMethod {
    ReadMethod::MultiGetSorted
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

impl From<&BlockConfig> for Options {
    fn from(config: &BlockConfig) -> Self {
        let mut bbt = BlockBasedOptions::default();
        bbt.set_block_size(config.block_size);
        bbt.set_block_restart_interval(config.block_restart_interval);
        bbt.set_whole_key_filtering(config.whole_key_filtering);
        bbt.set_cache_index_and_filter_blocks(config.cache_index_and_filter_blocks);
        bbt.set_pin_l0_filter_and_index_blocks_in_cache(config.pin_l0_filter_and_index_blocks);
        if let Some(bits) = config.bloom_filter_bits_per_key {
            bbt.set_bloom_filter(bits, false);
        }
        if config.data_block_hash_index {
            bbt.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
            bbt.set_data_block_hash_ratio(config.data_block_hash_ratio);
        }
        if config.block_cache_mb > 0 {
            let cache = Cache::new_lru_cache(config.block_cache_mb << 20);
            bbt.set_block_cache(&cache);
        }

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_allow_mmap_reads(config.mmap_reads);
        opts.set_use_direct_reads(config.use_direct_reads);
        opts.set_write_buffer_size(config.write_buffer_size);
        opts.set_target_file_size_base(config.target_file_size_base);
        opts.set_disable_auto_compactions(config.disable_auto_compactions);
        opts.set_block_based_table_factory(&bbt);
        opts
    }
}
