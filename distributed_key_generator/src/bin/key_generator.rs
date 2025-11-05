use std::str::FromStr;

use clap::{Parser, Subcommand};
use distributed_key_generation::{
    error::{self, Error},
    rpc::{
        cluster::{self, GetKeyGeneratorList, GetKeyGeneratorRpcUrlListResponse},
        external, internal,
    },
    state::AppState,
    task::single_key_generator::run_single_key_generator,
    types::*,
};
use radius_sdk::{
    json_rpc::{
        client::{Id, RpcClient},
        server::{RpcParameter, RpcServer},
    },
    kvstore::KvStoreBuilder,
};
pub use serde::{Deserialize, Serialize};
use skde::{setup, BigUint};
use tokio::task::JoinHandle;

#[derive(Debug, Deserialize, Parser, Serialize)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub fn init() -> Self {
        Cli::parse()
    }
}

#[derive(Subcommand, Debug, Deserialize, Serialize)]
pub enum Commands {
    /// Initializes a node
    Init {
        #[clap(flatten)]
        config_path: Box<ConfigPath>,
    },

    /// Starts the node
    Start {
        #[clap(flatten)]
        config_option: Box<ConfigOption>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt().init();

    let mut cli = Cli::init();

    match cli.command {
        Commands::Init { ref config_path } => ConfigPath::init(config_path)?,
        Commands::Start {
            ref mut config_option,
        } => {
            // Load the configuration from the path
            let config = Config::load(config_option)?;

            tracing::info!(
                "Successfully loaded the configuration file at {:?}.",
                config.path(),
            );

            // TODO: remove this values
            const PRIME_P: &str = "8155133734070055735139271277173718200941522166153710213522626777763679009805792017274916613411023848268056376687809186180768200590914945958831360737612803";
            const PRIME_Q: &str = "13379153270147861840625872456862185586039997603014979833900847304743997773803109864546170215161716700184487787472783869920830925415022501258643369350348243";
            const GENERATOR: &str = "4";
            const TIME_PARAM_T: u32 = 2;
            const MAX_KEY_GENERATOR_NUMBER: u32 = 2;

            let time = 2_u32.pow(TIME_PARAM_T);
            let p = BigUint::from_str(PRIME_P).expect("Invalid PRIME_P");
            let q = BigUint::from_str(PRIME_Q).expect("Invalid PRIME_Q");
            let g = BigUint::from_str(GENERATOR).expect("Invalid GENERATOR");
            let max_key_generator_number = BigUint::from(MAX_KEY_GENERATOR_NUMBER);

            let skde_params = setup(time, p, q, g, max_key_generator_number);

            // Initialize the database
            KvStoreBuilder::default()
                .set_default_lock_timeout(5000)
                .set_txn_lock_timeout(5000)
                .build(config.database_path())
                .map_err(error::Error::Database)?
                .init();

            KeyGeneratorList::initialize().map_err(error::Error::Database)?;
            KeyId::initialize().map_err(error::Error::Database)?;

            tracing::info!(
                "Successfully initialized the database at {:?}.",
                config.database_path(),
            );

            if let Some(seed_rpc_url) = config.seed_cluster_rpc_url() {
                // Follow
                // Initialize the cluster RPC server
                let rpc_client: RpcClient = RpcClient::new()?;

                let response: GetKeyGeneratorRpcUrlListResponse = rpc_client
                    .request(
                        seed_rpc_url,
                        GetKeyGeneratorList::method(),
                        &GetKeyGeneratorList,
                        Id::Null,
                    )
                    .await?;

                let key_generator_list: KeyGeneratorList =
                    response.key_generator_rpc_url_list.into();

                key_generator_list.put()?;
            }

            // Initialize an application-wide state instance
            let app_state = AppState::new(config, skde_params);

            if app_state.config().seed_cluster_rpc_url().is_none() {
                // Leader
                // Run the single key generator task
                run_single_key_generator(app_state.clone());
            }

            // Initialize the internal RPC server
            initialize_internal_rpc_server(&app_state).await?;

            // Initialize the cluster RPC server
            initialize_cluster_rpc_server(&app_state).await?;

            // Initialize the external RPC server.
            let server_handle = initialize_external_rpc_server(&app_state).await?;

            server_handle.await.unwrap();
        }
    }

    Ok(())
}

async fn initialize_internal_rpc_server(app_state: &AppState) -> Result<(), Error> {
    let internal_rpc_url = app_state.config().internal_rpc_url().to_string();

    // Initialize the internal RPC server.
    let internal_rpc_server = RpcServer::new(app_state.clone())
        .register_rpc_method::<internal::AddKeyGenerator>()?
        .init(app_state.config().internal_rpc_url().to_string())
        .await
        .map_err(error::Error::RpcServerError)?;

    tracing::info!(
        "Successfully started the internal RPC server: {}",
        internal_rpc_url
    );

    tokio::spawn(async move {
        internal_rpc_server.stopped().await;
    });

    Ok(())
}

async fn initialize_cluster_rpc_server(app_state: &AppState) -> Result<(), Error> {
    let cluster_rpc_url = anywhere(&app_state.config().cluster_port()?);

    let key_generator_rpc_server = RpcServer::new(app_state.clone())
        .register_rpc_method::<cluster::GetKeyGeneratorList>()?
        .register_rpc_method::<cluster::SyncKeyGenerator>()?
        .register_rpc_method::<cluster::SyncAggregatedKey>()?
        .register_rpc_method::<cluster::SyncPartialKey>()?
        .register_rpc_method::<cluster::RunGeneratePartialKey>()?
        .init(cluster_rpc_url.clone())
        .await
        .map_err(error::Error::RpcServerError)?;

    tracing::info!(
        "Successfully started the cluster RPC server: {}",
        cluster_rpc_url
    );

    tokio::spawn(async move {
        key_generator_rpc_server.stopped().await;
    });

    Ok(())
}

async fn initialize_external_rpc_server(app_state: &AppState) -> Result<JoinHandle<()>, Error> {
    let external_rpc_url = anywhere(&app_state.config().external_port()?);

    // Initialize the external RPC server.
    let external_rpc_server = RpcServer::new(app_state.clone())
        .register_rpc_method::<external::GetEncryptionKey>()?
        .register_rpc_method::<external::GetDecryptionKey>()?
        .register_rpc_method::<external::GetLatestEncryptionKey>()?
        .register_rpc_method::<external::GetLatestKeyId>()?
        .register_rpc_method::<external::GetSkdeParams>()?
        .init(external_rpc_url.clone())
        .await
        .map_err(error::Error::RpcServerError)?;

    tracing::info!(
        "Successfully started the external RPC server: {}",
        external_rpc_url
    );

    let server_handle = tokio::spawn(async move {
        external_rpc_server.stopped().await;
    });

    Ok(server_handle)
}

pub fn anywhere(port: &str) -> String {
    format!("0.0.0.0:{}", port)
}
