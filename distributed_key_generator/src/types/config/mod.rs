mod config_option;
mod config_path;

use std::{fs, path::PathBuf};

pub use config_option::*;
pub use config_path::*;
use radius_sdk::signature::{Address, ChainType, PrivateKeySigner};

pub const DEFAULT_HOME_PATH: &str = ".radius";
pub const DATABASE_DIR_NAME: &str = "database";
pub const CONFIG_FILE_NAME: &str = "Config.toml";
pub const SIGNING_KEY: &str = "signing_key";

const DEFAULT_EXTERNAL_RPC_URL: &str = "http://127.0.0.1:3000";
const DEFAULT_INTERNAL_RPC_URL: &str = "http://127.0.0.1:4000";
const DEFAULT_CLUSTER_RPC_URL: &str = "http://127.0.0.1:5000";

const DEFAULT_RADIUS_FOUNDATION_ADDRESS: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
const DEFAULT_CHAIN_TYPE: &str = "ethereum";

const DEFAULT_PARTIAL_KEY_GENERATION_CYCLE: u64 = 5;
const DEFAULT_PARTIAL_KEY_AGGREGATION_CYCLE: u64 = 4;

#[derive(Clone)]
pub struct Config {
    path: PathBuf,

    external_rpc_url: String,
    internal_rpc_url: String,
    cluster_rpc_url: String,
    seed_cluster_rpc_url: Option<String>,

    signer: PrivateKeySigner,

    radius_foundation_address: Address,
    chain_type: ChainType,

    partial_key_generation_cycle: u64,
    partial_key_aggregation_cycle: u64,
}

impl Config {
    pub fn load(config_option: &mut ConfigOption) -> Result<Self, ConfigError> {
        let config_path = match config_option.path.as_mut() {
            Some(config_path) => config_path.clone(),
            None => {
                let config_path: PathBuf = ConfigPath::default().as_ref().into();
                config_option.path = Some(config_path.clone());
                config_path
            }
        };

        // Read config file
        let config_file_path = config_path.join(CONFIG_FILE_NAME);
        let config_string = fs::read_to_string(config_file_path).map_err(ConfigError::Load)?;

        // Parse String to TOML String
        let config_file: ConfigOption =
            toml::from_str(&config_string).map_err(ConfigError::Parse)?;

        // Merge configs from CLI input
        let merged_config_option = config_file.merge(config_option);

        let chain_type = merged_config_option.chain_type.unwrap().try_into().unwrap();

        // Read signing key
        let signing_key_path = config_path.join(SIGNING_KEY);
        let signer =
            PrivateKeySigner::from_str(chain_type, &fs::read_to_string(signing_key_path).unwrap())
                .unwrap();

        Ok(Config {
            path: config_path,
            external_rpc_url: merged_config_option.external_rpc_url.unwrap(),
            internal_rpc_url: merged_config_option.internal_rpc_url.unwrap(),
            cluster_rpc_url: merged_config_option.cluster_rpc_url.unwrap(),
            seed_cluster_rpc_url: merged_config_option.seed_cluster_rpc_url.clone(),
            signer,
            radius_foundation_address: Address::from_str(
                chain_type,
                &merged_config_option.radius_foundation_address.unwrap(),
            )
            .unwrap(),
            chain_type,

            partial_key_generation_cycle: merged_config_option
                .partial_key_generation_cycle
                .unwrap(),
            partial_key_aggregation_cycle: merged_config_option
                .partial_key_aggregation_cycle
                .unwrap(),
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn database_path(&self) -> PathBuf {
        self.path.join(DATABASE_DIR_NAME)
    }

    pub fn signer(&self) -> &PrivateKeySigner {
        &self.signer
    }

    pub fn radius_foundation_address(&self) -> &Address {
        &self.radius_foundation_address
    }

    pub fn chain_type(&self) -> &ChainType {
        &self.chain_type
    }

    pub fn address(&self) -> &Address {
        self.signer().address()
    }

    pub fn external_rpc_url(&self) -> &String {
        &self.external_rpc_url
    }

    pub fn internal_rpc_url(&self) -> &String {
        &self.internal_rpc_url
    }

    pub fn partial_key_generation_cycle(&self) -> u64 {
        self.partial_key_generation_cycle
    }

    pub fn partial_key_aggregation_cycle(&self) -> u64 {
        self.partial_key_aggregation_cycle
    }

    pub fn cluster_rpc_url(&self) -> &String {
        &self.cluster_rpc_url
    }

    pub fn seed_cluster_rpc_url(&self) -> &Option<String> {
        &self.seed_cluster_rpc_url
    }

    pub fn external_port(&self) -> Result<String, ConfigError> {
        Ok(self
            .external_rpc_url()
            .split(':')
            .last()
            .ok_or(ConfigError::InvalidExternalPort)?
            .to_string())
    }

    pub fn cluster_port(&self) -> Result<String, ConfigError> {
        Ok(self
            .cluster_rpc_url()
            .split(':')
            .last()
            .ok_or(ConfigError::InvalidClusterPort)?
            .to_string())
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Load(std::io::Error),
    Parse(toml::de::Error),
    RemoveConfigDirectory(std::io::Error),
    CreateConfigDirectory(std::io::Error),
    CreateConfigFile(std::io::Error),
    CreatePrivateKeyFile(std::io::Error),

    InvalidExternalPort,
    InvalidClusterPort,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ConfigError {}
