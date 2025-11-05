use radius_sdk::kvstore::{KvStoreError};

#[derive(Debug)]
pub enum Error {
    Config(crate::types::ConfigError),
    Database(radius_sdk::kvstore::KvStoreError),
    RpcServerError(radius_sdk::json_rpc::server::RpcServerError),
    RpcClientError(radius_sdk::json_rpc::client::RpcClientError),

    LoadConfigOption(std::io::Error),
    ParseTomlString(toml::de::Error),
    RemoveConfigDirectory,
    CreateConfigDirectory,
    CreateConfigFile,
    CreatePrivateKeyFile,
    HexDecodeError,

    NotFound,
}

unsafe impl Send for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

impl From<KvStoreError> for Error {
    fn from(err: KvStoreError) -> Self {
        Self::Database(err)
    }
}

impl From<crate::types::ConfigError> for Error {
    fn from(value: crate::types::ConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<radius_sdk::json_rpc::server::RpcServerError> for Error {
    fn from(value: radius_sdk::json_rpc::server::RpcServerError) -> Self {
        Self::RpcServerError(value)
    }
}

impl From<radius_sdk::json_rpc::client::RpcClientError> for Error {
    fn from(value: radius_sdk::json_rpc::client::RpcClientError) -> Self {
        Self::RpcClientError(value)
    }
}
