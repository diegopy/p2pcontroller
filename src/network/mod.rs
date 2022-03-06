use std::time::Duration;

use serde::{de, Deserialize, Deserializer};

pub mod controller;
pub mod peer;

#[derive(Debug, Deserialize, Clone, Eq, PartialEq)]
pub struct Config {
    pub target_outgoing_connections: u32,
    pub max_incoming_connections: u32,
    pub max_simultaneous_outgoing_connection_attempts: u32,
    pub max_simultaneous_incoming_connection_attempts: u32,
    pub max_idle_peers: u32,
    pub max_banned_peers: u32,
    #[serde(
        rename = "peer_file_dump_interval_seconds",
        deserialize_with = "deserialize_interval"
    )]
    pub peer_file_dump_interval: Duration,
    #[serde(
        rename = "connection_timeout_seconds",
        deserialize_with = "deserialize_interval"
    )]
    pub connection_timeout: Duration,
    #[serde(
        rename = "connector_wait_seconds",
        deserialize_with = "deserialize_interval"
    )]
    pub connector_wait: Duration,
}

pub fn deserialize_interval<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let secs = u64::deserialize(deserializer)?;
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;
    use std::time::Duration;

    use super::Config;

    #[test]
    fn config_format() -> Result<()> {
        let cfg_json = json!({
            "target_outgoing_connections": 1,
            "max_incoming_connections": 2,
            "max_simultaneous_outgoing_connection_attempts": 3,
            "max_simultaneous_incoming_connection_attempts": 4,
            "max_idle_peers": 5,
            "max_banned_peers": 6,
            "peer_file_dump_interval_seconds": 7,
            "connection_timeout_seconds": 8,
            "connector_wait_seconds": 9,
        });
        let expected = Config {
            target_outgoing_connections: 1,
            max_incoming_connections: 2,
            max_simultaneous_outgoing_connection_attempts: 3,
            max_simultaneous_incoming_connection_attempts: 4,
            max_idle_peers: 5,
            max_banned_peers: 6,
            peer_file_dump_interval: Duration::from_secs(7),
            connection_timeout: Duration::from_secs(8),
            connector_wait: Duration::from_secs(9),
        };
        let cfg: Config = serde_json::from_value(cfg_json)?;
        assert_eq!(expected, cfg);
        Ok(())
    }
}
