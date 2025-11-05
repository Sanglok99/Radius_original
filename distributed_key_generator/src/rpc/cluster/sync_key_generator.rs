use radius_sdk::{
    json_rpc::server::{RpcError, RpcParameter},
    signature::Address,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    state::AppState,
    types::{KeyGenerator, KeyGeneratorList},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncKeyGenerator {
    // signature: Signature, // TODO: Uncomment this code
    message: SyncKeyGeneratorMessage,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SyncKeyGeneratorMessage {
    address: Address,
    cluster_rpc_url: String,
    external_rpc_url: String,
}

impl RpcParameter<AppState> for SyncKeyGenerator {
    type Response = ();

    fn method() -> &'static str {
        "sync_key_generator"
    }

    async fn handler(self, _context: AppState) -> Result<Self::Response, RpcError> {
        info!(
            "Sync key generator - address: {:?} / cluster_rpc_url: {:?} / external_rpc_url: {:?}",
            self.message.address.as_hex_string(),
            self.message.cluster_rpc_url,
            self.message.external_rpc_url
        );

        // TODO: Uncomment this code
        // self.signature.verify_signature(
        //     serialize_to_bincode(&self.message)?.as_slice(),
        //     context.config().radius_foundation_address().as_slice(),
        //     context.config().chain_type().clone(),
        // )?;

        let key_generator = KeyGenerator::new(
            self.message.address.clone(),
            self.message.cluster_rpc_url.clone(),
            self.message.external_rpc_url.clone(),
        );

        let key_generator_list = KeyGeneratorList::get()?;
        if key_generator_list.contains(&key_generator) {
            return Ok(());
        }

        KeyGeneratorList::apply(|key_generator_list| {
            key_generator_list.insert(key_generator);
        })?;

        Ok(())
    }
}
