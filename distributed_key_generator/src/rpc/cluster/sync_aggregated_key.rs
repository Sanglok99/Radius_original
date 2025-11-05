use radius_sdk::{
    json_rpc::server::{RpcError, RpcParameter},
    signature::Address,
};
use serde::{Deserialize, Serialize};
use skde::{
    delay_encryption::solve_time_lock_puzzle,
    key_aggregation::{aggregate_key, AggregatedKey as SkdeAggregatedKey},
};

use crate::rpc::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncAggregatedKey {
    pub key_id: KeyId,
    pub aggregated_key: SkdeAggregatedKey,
    pub participant_addresses: Vec<Address>,
}

impl RpcParameter<AppState> for SyncAggregatedKey {
    type Response = ();

    fn method() -> &'static str {
        "sync_aggregated_key"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        let skde_params = context.skde_params().clone();

        let partial_key_address_list =
            PartialKeyAddressList::get_or(self.key_id, PartialKeyAddressList::default)?;

        let partial_key_list = partial_key_address_list.get_partial_key_list(self.key_id)?;

        let skde_aggregated_key = aggregate_key(&skde_params, &partial_key_list);
        let aggregated_key = AggregatedKey::new(skde_aggregated_key.clone());
        aggregated_key.put(self.key_id)?;

        tracing::info!(
            "Completed to generate encryption key - key id: {:?} / encryption key: {:?}",
            self.key_id,
            skde_aggregated_key.u
        );

        tokio::spawn(async move {
            let decryption_key =
                solve_time_lock_puzzle(&skde_params, &skde_aggregated_key).unwrap();
            let decryption_key = DecryptionKey::new(decryption_key.sk.clone());

            decryption_key.put(self.key_id).unwrap();
            tracing::info!(
                "Complete to get decryption key - key_id: {:?} / decryption key: {:?}",
                self.key_id,
                decryption_key
            );
        });

        Ok(())
    }
}
