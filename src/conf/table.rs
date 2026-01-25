use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DType {
    Utf8,
    Int16,
    Int32,
    Int64,
    Uint16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ColumnConfig {
    pub dtype: DType,
    #[serde(default = "ColumnConfig::default_nullable")]
    pub nullable: bool,
}

impl ColumnConfig {
    pub fn default_nullable() -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct S3SourceConfig {
    pub bucket: String,
    pub prefix: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default = "S3SourceConfig::default_region")]
    pub region: String,
}

impl S3SourceConfig {
    pub fn default_region() -> String {
        String::from("us-east-1")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LocalSourceConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub enum SourceConfig {
    #[serde(rename = "s3")]
    S3(S3SourceConfig),
    #[serde(rename = "local")]
    Local(LocalSourceConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    pub source: SourceConfig,

    #[serde(
        with = "humantime_serde",
        default = "TableConfig::default_poll_interval"
    )]
    pub poll_interval: Duration,
    #[serde(default = "TableConfig::default_parts")]
    pub parts: u32,
    pub key: Vec<String>,
    #[serde(default = "TableConfig::default_columns")]
    pub columns: HashMap<String, ColumnConfig>,
}

impl TableConfig {
    fn default_poll_interval() -> Duration {
        Duration::from_mins(1)
    }
    fn default_parts() -> u32 {
        8
    }
    fn default_columns() -> HashMap<String, ColumnConfig> {
        HashMap::new()
    }
}

pub type TablesConfig = HashMap<String, TableConfig>;
