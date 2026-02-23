use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "ServerConfig::default_host")]
    pub host: String,
    #[serde(default = "ServerConfig::default_port")]
    pub port: u16,
    #[serde(default = "ServerConfig::default_flight_port")]
    pub flight_port: u16,
    #[serde(default = "ServerConfig::default_dir")]
    pub data_dir: String,
}
impl ServerConfig {
    fn default_port() -> u16 {
        8080
    }

    fn default_flight_port() -> u16 {
        8081
    }

    fn default_host() -> String {
        String::from("localhost")
    }

    fn default_dir() -> String {
        String::from("/var/lib/murr")
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: Self::default_host(),
            port: Self::default_port(),
            flight_port: Self::default_flight_port(),
            data_dir: Self::default_dir(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_default() {
        let server = ServerConfig::default();
        assert_eq!(server.host, "localhost");
        assert_eq!(server.port, 8080);
        assert_eq!(server.flight_port, 8081);
    }
}
