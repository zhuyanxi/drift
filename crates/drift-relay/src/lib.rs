use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayConfig {
    pub bind_address: String,
    pub port: u16,
    pub room_timeout_secs: u64,
    pub max_connections: u32,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0".into(),
            port: 9009,
            room_timeout_secs: 600,
            max_connections: 1024,
        }
    }
}
