use std::{fs, path::Path, sync::Arc};

use serde::{Deserialize, Serialize};

pub struct Config {
    inner: Arc<ConfigInner>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ConfigInner {
    total_transactions: usize,
    connection_threads: usize,
    connections: usize,
    duration: u64,
    ethereum_rpc_url: String,
    rpc_url: String,
    request_timeout: u64,
    chain_id: u64,
    rollup_id: String,
    signing_keys: Vec<String>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl Clone for Config {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Config {
    pub fn total_transactions(&self) -> usize {
        self.inner.total_transactions
    }

    pub fn connection_threads(&self) -> usize {
        self.inner.connection_threads
    }

    pub fn connections(&self) -> usize {
        self.inner.connections
    }

    pub fn duration(&self) -> u64 {
        self.inner.duration
    }

    pub fn ethereum_rpc_url(&self) -> &str {
        &self.inner.ethereum_rpc_url
    }

    pub fn rpc_url(&self) -> &str {
        &self.inner.rpc_url
    }

    pub fn request_timeout(&self) -> u64 {
        self.inner.request_timeout
    }

    pub fn chain_id(&self) -> u64 {
        self.inner.chain_id
    }

    pub fn rollup_id(&self) -> &str {
        &self.inner.rollup_id
    }

    pub fn signing_keys(&self) -> &Vec<String> {
        &self.inner.signing_keys
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let config_string = fs::read_to_string(path).map_err(ConfigError::Open)?;
        let config_inner: ConfigInner =
            toml::from_str(&config_string).map_err(ConfigError::Parse)?;

        Ok(Self {
            inner: config_inner.into(),
        })
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Open(std::io::Error),
    Parse(toml::de::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ConfigError {}
