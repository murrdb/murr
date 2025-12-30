use crate::{
    conf::Server,
    core::MurrError::{self, ConfigParsingError},
};
use config::Config as CConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: Server,
}

impl Config {
    pub fn from_str(toml_str: &str) -> Result<Config, MurrError> {
        let config = CConfig::builder()
            .add_source(config::File::from_str(toml_str, config::FileFormat::Toml))
            .build()
            .map_err(|e| ConfigParsingError(e.to_string()))?
            .try_deserialize::<Config>()
            .map_err(|e| ConfigParsingError(e.to_string()))?;
        return Ok(config);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_correct_toml() {
        let toml = r#"
        [server]
        host = "127.0.0.1"
        port = 3000
        "#;
        let conf = Config::from_str(toml);
        assert_eq!(
            conf,
            Ok(Config {
                server: Server {
                    host: String::from("127.0.0.1"),
                    port: 3000
                }
            })
        );
    }
}
