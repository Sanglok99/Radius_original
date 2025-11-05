use std::path::PathBuf;

use clap::Parser;
use serde::{Deserialize, Serialize};

use super::{
    config_path::ConfigPath, DEFAULT_CHAIN_TYPE, DEFAULT_CLUSTER_RPC_URL, DEFAULT_EXTERNAL_RPC_URL,
    DEFAULT_INTERNAL_RPC_URL, DEFAULT_PARTIAL_KEY_AGGREGATION_CYCLE,
    DEFAULT_PARTIAL_KEY_GENERATION_CYCLE, DEFAULT_RADIUS_FOUNDATION_ADDRESS,
};

#[derive(Debug, Deserialize, Parser, Serialize)]
pub struct ConfigOption {
    #[doc = "Set the configuration file path to load from"]
    #[clap(long = "path")]
    pub path: Option<PathBuf>,

    #[doc = "Set the external rpc url"]
    #[clap(long = "external-rpc-url")]
    pub external_rpc_url: Option<String>,

    #[doc = "Set the internal rpc url"]
    #[clap(long = "internal-rpc-url")]
    pub internal_rpc_url: Option<String>,

    #[doc = "Set the cluster rpc url"]
    #[clap(long = "cluster-rpc-url")]
    pub cluster_rpc_url: Option<String>,

    #[doc = "Set the seed cluster rpc url"]
    #[clap(long = "seed-cluster-rpc-url")]
    pub seed_cluster_rpc_url: Option<String>,

    #[doc = "Set the radius foundation address"]
    #[clap(long = "radius-foundation-address")]
    pub radius_foundation_address: Option<String>,

    #[doc = "Set the chain type (for verifying signature for foundation address)"]
    #[clap(long = "chain-type")]
    pub chain_type: Option<String>,

    #[doc = "Set partial key generation cycle"]
    #[clap(long = "partial-key-generation-cycle")]
    pub partial_key_generation_cycle: Option<u64>,

    #[doc = "Set partial key aggregation cycle"]
    #[clap(long = "partial-key-aggregation-cycle")]
    pub partial_key_aggregation_cycle: Option<u64>,
}

impl Default for ConfigOption {
    fn default() -> Self {
        Self {
            path: Some(ConfigPath::default().as_ref().into()),

            external_rpc_url: Some(DEFAULT_EXTERNAL_RPC_URL.into()),
            internal_rpc_url: Some(DEFAULT_INTERNAL_RPC_URL.into()),
            cluster_rpc_url: Some(DEFAULT_CLUSTER_RPC_URL.into()),
            seed_cluster_rpc_url: None,
            radius_foundation_address: Some(DEFAULT_RADIUS_FOUNDATION_ADDRESS.into()),
            chain_type: Some(DEFAULT_CHAIN_TYPE.into()),
            partial_key_generation_cycle: Some(DEFAULT_PARTIAL_KEY_GENERATION_CYCLE),
            partial_key_aggregation_cycle: Some(DEFAULT_PARTIAL_KEY_AGGREGATION_CYCLE),
        }
    }
}

impl ConfigOption {
    pub fn get_toml_string(&self) -> String {
        let mut toml_string = String::new();

        set_toml_comment(&mut toml_string, "Set external rpc url");
        set_toml_name_value(&mut toml_string, "external_rpc_url", &self.external_rpc_url);

        set_toml_comment(&mut toml_string, "Set internal rpc url");
        set_toml_name_value(&mut toml_string, "internal_rpc_url", &self.internal_rpc_url);

        set_toml_comment(&mut toml_string, "Set cluster rpc url");
        set_toml_name_value(&mut toml_string, "cluster_rpc_url", &self.cluster_rpc_url);

        set_toml_comment(&mut toml_string, "Set seed cluster rpc url");
        set_toml_name_value(
            &mut toml_string,
            "seed_cluster_rpc_url",
            &self.seed_cluster_rpc_url,
        );

        set_toml_comment(&mut toml_string, "Set the radius foundation address");
        set_toml_name_value(
            &mut toml_string,
            "radius_foundation_address",
            &self.radius_foundation_address,
        );

        set_toml_comment(
            &mut toml_string,
            "Set the chain type (for verifying signature for foundation address)",
        );
        set_toml_name_value(&mut toml_string, "chain_type", &self.chain_type);

        set_toml_comment(&mut toml_string, "Set partial key generation cycle");
        set_toml_name_value(
            &mut toml_string,
            "partial_key_generation_cycle",
            &self.partial_key_generation_cycle,
        );

        set_toml_comment(&mut toml_string, "Set partial key aggregation cycle");
        set_toml_name_value(
            &mut toml_string,
            "partial_key_aggregation_cycle",
            &self.partial_key_aggregation_cycle,
        );

        toml_string
    }

    pub fn merge(mut self, other: &ConfigOption) -> Self {
        if other.path.is_some() {
            self.path.clone_from(&other.path);
        }

        if other.external_rpc_url.is_some() {
            self.external_rpc_url.clone_from(&other.external_rpc_url);
        }

        if other.internal_rpc_url.is_some() {
            self.internal_rpc_url.clone_from(&other.internal_rpc_url);
        }

        if other.cluster_rpc_url.is_some() {
            self.cluster_rpc_url.clone_from(&other.cluster_rpc_url);
        }

        if other.seed_cluster_rpc_url.is_some() {
            self.seed_cluster_rpc_url
                .clone_from(&other.seed_cluster_rpc_url);
        }

        if other.radius_foundation_address.is_some() {
            self.radius_foundation_address
                .clone_from(&other.radius_foundation_address);
        }

        if other.chain_type.is_some() {
            self.chain_type.clone_from(&other.chain_type);
        }

        if other.partial_key_generation_cycle.is_some() {
            self.partial_key_aggregation_cycle
                .clone_from(&other.partial_key_aggregation_cycle);
        }

        if other.partial_key_aggregation_cycle.is_some() {
            self.partial_key_aggregation_cycle
                .clone_from(&other.partial_key_aggregation_cycle);
        }

        self
    }
}

fn set_toml_comment(toml_string: &mut String, comment: &'static str) {
    let comment = format!("# {}\n", comment);

    toml_string.push_str(&comment);
}

fn set_toml_name_value<T>(toml_string: &mut String, name: &'static str, value: &Option<T>)
where
    T: std::fmt::Debug,
{
    let name_value = match value {
        Some(value) => format!("{} = {:?}\n\n", name, value),
        None => format!("# {} = {:?}\n\n", name, value),
    };

    toml_string.push_str(&name_value);
}
