use rocksdb::{
    KeyEncodingType, MemtableFactory, Options, PlainTableFactoryOptions, SliceTransform,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlainConfig {
    #[serde(default = "default_bloom_bits")]
    pub bloom_bits_per_key: i32,
    /// PlainTable: hash table utilization ratio (lower = fewer collisions, more space).
    #[serde(default = "default_data_block_hash_ratio")]
    pub hash_table_ratio: f64,
    /// PlainTable: stride between sparse-index entries (lower = faster lookup, more memory).
    #[serde(default = "default_index_sparseness")]
    pub index_sparseness: usize,
    /// PlainTable: store the full index in the file instead of rebuilding at open.
    #[serde(default)]
    pub store_index_in_file: bool,
    #[serde(default)]
    pub huge_page_tlb_size: usize,
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,
    #[serde(default = "default_target_file_size_base")]
    pub target_file_size_base: u64,
    #[serde(default = "default_disable_auto_compactions")]
    pub disable_auto_compactions: bool,
}

impl Default for PlainConfig {
    fn default() -> Self {
        Self {
            bloom_bits_per_key: default_bloom_bits(),
            hash_table_ratio: default_data_block_hash_ratio(),
            index_sparseness: default_index_sparseness(),
            store_index_in_file: false,
            huge_page_tlb_size: 0,
            write_buffer_size: default_write_buffer_size(),
            target_file_size_base: default_target_file_size_base(),
            disable_auto_compactions: default_disable_auto_compactions(),
        }
    }
}

fn default_bloom_bits() -> i32 {
    16
}
fn default_index_sparseness() -> usize {
    4
}
pub(super) fn default_data_block_hash_ratio() -> f64 {
    0.75
}
pub(super) fn default_write_buffer_size() -> usize {
    256 * 1024 * 1024
}
pub(super) fn default_target_file_size_base() -> u64 {
    1024 * 1024 * 1024
}
pub(super) fn default_disable_auto_compactions() -> bool {
    false
}

impl From<&PlainConfig> for Options {
    fn from(config: &PlainConfig) -> Self {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_allow_mmap_reads(true);
        opts.set_prefix_extractor(SliceTransform::create_noop());
        opts.set_memtable_factory(MemtableFactory::Vector);
        opts.set_write_buffer_size(config.write_buffer_size);
        opts.set_target_file_size_base(config.target_file_size_base);
        opts.set_disable_auto_compactions(config.disable_auto_compactions);
        opts.set_plain_table_factory(&PlainTableFactoryOptions {
            user_key_length: 0,
            bloom_bits_per_key: config.bloom_bits_per_key,
            hash_table_ratio: config.hash_table_ratio,
            index_sparseness: config.index_sparseness,
            huge_page_tlb_size: config.huge_page_tlb_size,
            encoding_type: KeyEncodingType::Plain,
            full_scan_mode: false,
            store_index_in_file: config.store_index_in_file,
        });
        opts
    }
}
