use crate::{
    conf::{ServerConfig, StorageConfig},
    core::{
        CliArgs,
        MurrError::{self, ConfigParsingError},
    },
};
use config::Config as CConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

impl Config {
    pub fn from_args(args: &CliArgs) -> Result<Config, MurrError> {
        let mut builder = CConfig::builder();

        if let Some(config_path) = &args.config {
            builder = builder.add_source(config::File::with_name(config_path));
        }

        builder = builder.add_source(
            config::Environment::with_prefix("MURR")
                .separator("_")
                .try_parsing(true),
        );

        builder
            .build()
            .map_err(|e| ConfigParsingError(e.to_string()))?
            .try_deserialize::<Config>()
            .map_err(|e| ConfigParsingError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.server.http.host, "0.0.0.0");
        assert_eq!(config.server.http.port, 8080);
        assert_eq!(config.server.grpc.host, "0.0.0.0");
        assert_eq!(config.server.grpc.port, 8081);
    }

    #[test]
    fn test_config_from_args_no_file() {
        let args = CliArgs { config: None };
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.server.http.port, 8080);
        assert_eq!(config.server.grpc.port, 8081);
    }

    #[test]
    fn test_config_unknown_field_rejected() {
        let args = CliArgs {
            config: Some("nonexistent.yaml".to_string()),
        };
        let result = Config::from_args(&args);
        assert!(result.is_err());
    }
}
