use crate::core::MurrError::{self, ConfigParsingError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Server {
    #[serde(default = "Server::default_host")]
    pub host: String,
    #[serde(default = "Server::default_port")]
    pub port: u16,
}
impl Server {
    fn default_port() -> u16 {
        8080
    }

    fn default_host() -> String {
        String::from("localhost")
    }
}
