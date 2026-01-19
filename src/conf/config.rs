use std::collections::HashMap;

use crate::{
    conf::{ServerConfig, TablesConfig},
    core::{
        CliArgs,
        MurrError::{self, ConfigParsingError},
    },
};
use config::Config as CConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "Config::default_server")]
    pub server: ServerConfig,
    pub tables: TablesConfig,
}

impl Config {
    pub fn from_file(file_path: &str) -> Result<Config, MurrError> {
        let content = std::fs::read_to_string(file_path)?;
        return Config::from_str(&content);
    }
    pub fn from_str(yaml_str: &str) -> Result<Config, MurrError> {
        let config = CConfig::builder()
            .add_source(config::File::from_str(yaml_str, config::FileFormat::Yaml))
            .build()
            .map_err(|e| ConfigParsingError(e.to_string()))?
            .try_deserialize::<Config>()
            .map_err(|e| ConfigParsingError(e.to_string()))?;
        return Ok(config);
    }

    pub fn from_args(args: &CliArgs) -> Result<Config, MurrError> {
        match &args.config {
            Some(config_path) => Config::from_file(config_path),
            None => Ok(Config::default()),
        }
    }

    fn default_server() -> ServerConfig {
        ServerConfig::default()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            tables: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::conf::{
        ColumnConfig, DType, LocalSourceConfig, S3SourceConfig, SourceConfig, TableConfig,
    };

    use super::*;

    #[test]
    fn load_simple() {
        let conf = Config::from_file("tests/fixtures/config/simple.yml");
        assert_eq!(
            conf,
            Ok(Config {
                server: ServerConfig {
                    host: String::from("localhost"),
                    port: 8080,
                    data_dir: String::from("/var/lib/murr")
                },
                tables: HashMap::from([(
                    String::from("clicks"),
                    TableConfig {
                        source: SourceConfig::Local(LocalSourceConfig {
                            path: String::from("/data")
                        }),
                        key: vec![String::from("id")],
                        poll_interval: Duration::from_mins(1),
                        parts: 8,
                        columns: HashMap::new()
                    }
                )])
            })
        )
    }

    #[test]
    fn load_full() {
        let conf = Config::from_file("tests/fixtures/config/full.yml");
        assert_eq!(
            conf,
            Ok(Config {
                server: ServerConfig {
                    host: String::from("localhost"),
                    port: 8080,
                    data_dir: String::from("/var/lib/murr")
                },
                tables: HashMap::from([(
                    String::from("clicks"),
                    TableConfig {
                        source: SourceConfig::S3(S3SourceConfig {
                            bucket: String::from("bucket"),
                            prefix: String::from("prefix"),
                            endpoint: Option::from(String::from("https://minio:8080")),
                            region: String::from("us-east-1")
                        }),
                        key: vec![String::from("id")],
                        poll_interval: Duration::from_mins(1),
                        parts: 8,
                        columns: HashMap::from([
                            (
                                String::from("id"),
                                ColumnConfig {
                                    dtype: DType::Utf8,
                                    nullable: false
                                }
                            ),
                            (
                                String::from("clicks7"),
                                ColumnConfig {
                                    dtype: DType::Float32,
                                    nullable: true
                                }
                            )
                        ])
                    }
                )])
            })
        )
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.server.host, "localhost");
        assert_eq!(config.server.port, 8080);
    }
}
