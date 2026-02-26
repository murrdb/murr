use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    #[serde(default = "HttpConfig::default_host")]
    pub host: String,
    #[serde(default = "HttpConfig::default_port")]
    pub port: u16,
}

impl HttpConfig {
    fn default_host() -> String {
        String::from("0.0.0.0")
    }

    fn default_port() -> u16 {
        8080
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            host: Self::default_host(),
            port: Self::default_port(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    #[serde(default = "GrpcConfig::default_host")]
    pub host: String,
    #[serde(default = "GrpcConfig::default_port")]
    pub port: u16,
}

impl GrpcConfig {
    fn default_host() -> String {
        String::from("0.0.0.0")
    }

    fn default_port() -> u16 {
        8081
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            host: Self::default_host(),
            port: Self::default_port(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub grpc: GrpcConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_defaults() {
        let http = HttpConfig::default();
        assert_eq!(http.host, "0.0.0.0");
        assert_eq!(http.port, 8080);
        assert_eq!(http.addr(), "0.0.0.0:8080");
    }

    #[test]
    fn test_grpc_defaults() {
        let grpc = GrpcConfig::default();
        assert_eq!(grpc.host, "0.0.0.0");
        assert_eq!(grpc.port, 8081);
        assert_eq!(grpc.addr(), "0.0.0.0:8081");
    }

    #[test]
    fn test_server_defaults() {
        let server = ServerConfig::default();
        assert_eq!(server.http.port, 8080);
        assert_eq!(server.grpc.port, 8081);
    }
}
