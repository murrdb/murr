use crate::{
    conf::ServerConfig,
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
}

impl Config {
    pub fn from_file(file_path: &str) -> Result<Config, MurrError> {
        let content = std::fs::read_to_string(file_path)?;
        Config::from_str(&content)
    }
    pub fn from_str(yaml_str: &str) -> Result<Config, MurrError> {
        let config = CConfig::builder()
            .add_source(config::File::from_str(yaml_str, config::FileFormat::Yaml))
            .build()
            .map_err(|e| ConfigParsingError(e.to_string()))?
            .try_deserialize::<Config>()
            .map_err(|e| ConfigParsingError(e.to_string()))?;
        Ok(config)
    }

    pub fn from_args(args: &CliArgs) -> Result<Config, MurrError> {
        match &args.config {
            Some(config_path) => Config::from_file(config_path),
            None => Ok(Config::default()),
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.server.host, "localhost");
        assert_eq!(config.server.port, 8080);
    }
}
