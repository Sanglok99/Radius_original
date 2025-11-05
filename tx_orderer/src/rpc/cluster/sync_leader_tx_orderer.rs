use std::time::{SystemTime, UNIX_EPOCH};

use radius_sdk::json_rpc::server::ProcessPriority;

use super::LeaderChangeMessage;
use crate::rpc::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncLeaderTxOrderer {
    pub leader_change_message: LeaderChangeMessage,
    pub rollup_signature: Signature,

    pub batch_number: u64,
    pub transaction_order: u64,

    pub provided_batch_number: u64,
    pub provided_transaction_order: i64,
}

impl RpcParameter<AppState> for SyncLeaderTxOrderer {
    type Response = ();

    fn method() -> &'static str {
        "sync_leader_tx_orderer"
    }

    fn priority(&self) -> ProcessPriority {
        ProcessPriority::High
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        println!("=== SyncLeaderTxOrderer 시작 ==="); // test code

        println!("self.rollup_signature: {:?}", self.rollup_signature); // test code
        println!("self.batch_number: {:?}", self.batch_number); // test code
        println!("self.transaction_order: {:?}", self.transaction_order); // test code
        println!("self.provided_batch_number: {:?}", self.provided_batch_number); // test code
        println!("self.provided_transaction_order: {:?}", self.provided_transaction_order); // test code
        println!("self.leader_change_message: {:?}", self.leader_change_message); // test code
        println!("self.leader_change_message.platform_block_height: {:?}", self.leader_change_message.platform_block_height); // test code
        println!("self.leader_change_message.current_leader_tx_orderer_address: {:?}", self.leader_change_message.current_leader_tx_orderer_address); // test code
        println!("self.leader_change_message.next_leader_tx_orderer_address: {:?}", self.leader_change_message.next_leader_tx_orderer_address); // test code
        
        let rollup_id = self.leader_change_message.rollup_id.clone();

        let start_sync_leader_tx_orderer_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        let rollup = Rollup::get(&rollup_id).map_err(|e| {
            tracing::error!("Failed to retrieve rollup: {:?}", e);
            Error::RollupNotFound
        })?;

        println!("= rollup initialization ="); // test code
        println!("rollup.platform: {:?}", rollup.platform); // test code
        println!("rollup.liveness_service_provider: {:?}", rollup.liveness_service_provider); // test code
        println!("rollup.cluster_id: {:?}", rollup.cluster_id); // test code
        println!("self.leader_change_message.platform_block_height: {:?}", self.leader_change_message.platform_block_height); // test code

        let cluster = Cluster::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
            self.leader_change_message.platform_block_height,
        )?;

        println!("= cluster initialization ="); // test code
        println!("cluster.tx_orderer_address: {:?}", cluster.tx_orderer_address); // test code
        println!("cluster.rollup_id_list: {:?}", cluster.rollup_id_list); // test code
        println!("cluster.tx_orderer_rpc_infos: {:?}", cluster.tx_orderer_rpc_infos); // test code
        println!("cluster.block_margin: {:?}", cluster.block_margin); // test code

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;
        let tx_orderer_address = signer.address().clone();
        let is_leader =
            tx_orderer_address == self.leader_change_message.next_leader_tx_orderer_address;

        println!("is_leader: {:?}", is_leader); // test code
        println!("signer.address() value: {:?}", signer.address()); // test code
        println!("self.leader_change_message.next_leader_tx_orderer_address: {:?}", self.leader_change_message.next_leader_tx_orderer_address); // test code

        let leader_tx_orderer_rpc_info = cluster
            .get_tx_orderer_rpc_info(&self.leader_change_message.next_leader_tx_orderer_address)
            .ok_or_else(|| {
                tracing::error!(
                    "TxOrderer RPC info not found for address {:?}",
                    self.leader_change_message.next_leader_tx_orderer_address
                );
                Error::TxOrdererInfoNotFound
            })?;

        println!("= leader_tx_orderer_rpc_info initialization ="); // test code
        println!("leader_tx_orderer_rpc_info.cluster_rpc_url: {:?}", leader_tx_orderer_rpc_info.cluster_rpc_url); // test code
        println!("leader_tx_orderer_rpc_info.external_rpc_url: {:?}", leader_tx_orderer_rpc_info.external_rpc_url); // test code
        println!("leader_tx_orderer_rpc_info.tx_orderer_address: {:?}", leader_tx_orderer_rpc_info.tx_orderer_address); // test code

        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )?;

        println!("= mut_cluster_metadata initialization ="); // test code
        println!("mut_cluster_metadata.platform_block_height: {:?}", mut_cluster_metadata.platform_block_height); // test code
        println!("mut_cluster_metadata.is_leader: {:?}", mut_cluster_metadata.is_leader); // test code
        println!("mut_cluster_metadata.leader_tx_orderer_rpc_info: {:?}", mut_cluster_metadata.leader_tx_orderer_rpc_info); // test code

        mut_cluster_metadata.platform_block_height =
            self.leader_change_message.platform_block_height;
        mut_cluster_metadata.is_leader = is_leader;
        mut_cluster_metadata.leader_tx_orderer_rpc_info = Some(leader_tx_orderer_rpc_info.clone());
        mut_cluster_metadata.epoch = Some(mut_cluster_metadata.epoch.unwrap_or(0) + 1); // new code

        println!("= mut_cluster_metadata update ="); // test code
        println!("mut_cluster_metadata.platform_block_height: {:?}", mut_cluster_metadata.platform_block_height); // test code
        println!("mut_cluster_metadata.is_leader: {:?}", mut_cluster_metadata.is_leader); // test code
        println!("mut_cluster_metadata.leader_tx_orderer_rpc_info: {:?}", mut_cluster_metadata.leader_tx_orderer_rpc_info); // test code

        mut_cluster_metadata.update()?;

        let mut mut_rollup_metadata = RollupMetadata::get_mut(&rollup_id)?;

        println!("= mut_rollup_metadata initialization ="); // test code
        println!("mut_rollup_metadata.batch_number: {:?}", mut_rollup_metadata.batch_number); // test code
        println!("mut_rollup_metadata.transaction_order: {:?}", mut_rollup_metadata.transaction_order); // test code
        println!("mut_rollup_metadata.provided_batch_number: {:?}", mut_rollup_metadata.provided_batch_number); // test code
        println!("mut_rollup_metadata.provided_transaction_order: {:?}", mut_rollup_metadata.provided_transaction_order); // test code

        mut_rollup_metadata.batch_number = self.batch_number;
        mut_rollup_metadata.transaction_order = self.transaction_order;
        mut_rollup_metadata.provided_batch_number = self.provided_batch_number;
        mut_rollup_metadata.provided_transaction_order = self.provided_transaction_order;

        println!("= mut_rollup_metadata update ="); // test code
        println!("mut_rollup_metadata.batch_number: {:?}", mut_rollup_metadata.batch_number); // test code
        println!("mut_rollup_metadata.transaction_order: {:?}", mut_rollup_metadata.transaction_order); // test code
        println!("mut_rollup_metadata.provided_batch_number: {:?}", mut_rollup_metadata.provided_batch_number); // test code
        println!("mut_rollup_metadata.provided_transaction_order: {:?}", mut_rollup_metadata.provided_transaction_order); // test code

        mut_rollup_metadata.update()?;

        let end_sync_leader_tx_orderer_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        tracing::info!(
            "sync_leader_tx_orderer - total take time: {:?} / self: {:?}",
            end_sync_leader_tx_orderer_time - start_sync_leader_tx_orderer_time,
            self
        );

        println!("=== SyncLeaderTxOrderer 종료 ==="); // test code

        Ok(())
    }
}
