use super::LeaderChangeMessage;
use crate::rpc::{cluster::sync_leader_tx_orderer, prelude::*};

use super::send_end_signal_to_epoch_leader; // new code

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SetLeaderTxOrderer {
    pub leader_change_message: LeaderChangeMessage,
    pub rollup_signature: Signature,
}

impl RpcParameter<AppState> for SetLeaderTxOrderer {
    type Response = ();

    fn method() -> &'static str {
        "set_leader_tx_orderer"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        println!("===== ⚙️⚙️⚙️⚙️⚙️ SetLeaderTxOrderer handler() 시작 ⚙️⚙️⚙️⚙️⚙️ ====="); // test code

        let rollup_id = self.leader_change_message.rollup_id.clone();

        let rollup_metadata = match RollupMetadata::get(&rollup_id) {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::error!(
                    "Failed to get rollup metadata - rollup_id: {:?} / error: {:?}",
                    rollup_id,
                    err,
                );

                return Ok(());
            }
        };

        let rollup = Rollup::get(&rollup_id)?;

        let cluster = Cluster::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
            self.leader_change_message.platform_block_height,
        )?;

        let leader_tx_orderer_rpc_info = cluster
            .get_tx_orderer_rpc_info(&self.leader_change_message.next_leader_tx_orderer_address)
            .ok_or_else(|| {
                tracing::error!(
                    "TxOrderer RPC info not found for address {:?}",
                    self.leader_change_message.next_leader_tx_orderer_address
                );
                Error::TxOrdererInfoNotFound
            })?;

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;

        let tx_orderer_address = signer.address().clone();

        let is_next_leader =
            tx_orderer_address == self.leader_change_message.next_leader_tx_orderer_address;

        println!("is_next_leader: {:?}", is_next_leader); // test code

        // 🚀🚀🚀🚀🚀 mut_cluster_metadata synchronization start 🚀🚀🚀🚀🚀
        // 📌 platform_block_height ✅
        // 📌 is_leader ✅
        // 📌 leader_tx_orderer_rpc_info ✅
        // 📌 epoch ✅
        // 📌 epoch_node_bitmap -> no need to synchronize
        // 📌 epoch_leader_map ✅

        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )?;

        println!("🚀🚀🚀🚀🚀 mut_cluster_metadata before update 🚀🚀🚀🚀🚀"); // test code
        println!("mut_cluster_metadata.platform_block_height: {:?}", mut_cluster_metadata.platform_block_height); // test code
        println!("mut_cluster_metadata.is_leader: {:?}", mut_cluster_metadata.is_leader); // test code
        println!("mut_cluster_metadata.leader_tx_orderer_rpc_info: {:?}", mut_cluster_metadata.leader_tx_orderer_rpc_info); // test code
        println!("💡mut_cluster_metadata.epoch(업데이트 전): {:?}", mut_cluster_metadata.epoch); // test code
        println!("mut_cluster_metadata.epoch_node_bitmap: {:?}", mut_cluster_metadata.epoch_node_bitmap); // test code
        println!("mut_cluster_metadata.epoch_leader_map: {:?}", mut_cluster_metadata.epoch_leader_map); // test code
        println!("🚀🚀🚀🚀🚀 mut_cluster_metadata before update 🚀🚀🚀🚀🚀"); // test code

        mut_cluster_metadata.platform_block_height =
            self.leader_change_message.platform_block_height; // 🚩 platform_block_height 
        mut_cluster_metadata.is_leader = is_next_leader; // 🚩 is_leader 
        mut_cluster_metadata.leader_tx_orderer_rpc_info = Some(leader_tx_orderer_rpc_info.clone()); // 🚩 leader_tx_orderer_rpc_info 

        // === new code start ===
        let old_epoch = match mut_cluster_metadata.epoch {
            Some(epoch) => epoch,
            None => {
                tracing::error!("Cannot assign an old epoch — the epoch in ClusterMetadata is missing for some reason.");
                return Ok(());
            }
        };

        mut_cluster_metadata.epoch = Some(old_epoch + 1); // 🚩 epoch 

        let new_epoch = match mut_cluster_metadata.epoch{
            Some(epoch) => epoch,
            None => {
                tracing::error!("Cannot assign an old epoch — the epoch in ClusterMetadata is missing for some reason.");
                return Ok(());
            }
        };

        let new_leader_tx_orderer_address = mut_cluster_metadata.leader_tx_orderer_rpc_info.as_ref().unwrap().tx_orderer_address.to_string();

        mut_cluster_metadata.epoch_leader_map.insert(new_epoch, new_leader_tx_orderer_address); // 🚩 epoch_leader_map

        println!("💫💫💫💫💫 mut_cluster_metadata after update 💫💫💫💫💫"); // test code
        println!("mut_cluster_metadata.platform_block_height: {:?}", mut_cluster_metadata.platform_block_height); // test code
        println!("mut_cluster_metadata.is_leader: {:?}", mut_cluster_metadata.is_leader); // test code
        println!("mut_cluster_metadata.leader_tx_orderer_rpc_info: {:?}", mut_cluster_metadata.leader_tx_orderer_rpc_info); // test code
        println!("💡mut_cluster_metadata.epoch(업데이트 후): {:?}", mut_cluster_metadata.epoch); // test code
        println!("mut_cluster_metadata.epoch_node_bitmap: {:?}", mut_cluster_metadata.epoch_node_bitmap); // test code
        println!("mut_cluster_metadata.epoch_leader_map: {:?}", mut_cluster_metadata.epoch_leader_map); // test code
        println!("💫💫💫💫💫 mut_cluster_metadata after update 💫💫💫💫💫"); // test code

        // === new code end ===
        // 💫💫💫💫💫 mut_cluster_metadata synchronization end 💫💫💫💫💫

        let signer = context.get_signer(rollup.platform).await?;
        let current_tx_orderer_address = signer.address();

        // === new code start ===
        let epoch_leader_rpc_url = leader_tx_orderer_rpc_info
            .cluster_rpc_url
            .clone()
            .unwrap_or_default();

        // === new code end ===

        sync_leader_tx_orderer(
            context.clone(),
            cluster,
            // current_tx_orderer_address, // old code(not used)
            self.leader_change_message.clone(),
            self.rollup_signature,
            rollup_metadata.batch_number,
            rollup_metadata.transaction_order,
            rollup_metadata.provided_batch_number,
            rollup_metadata.provided_transaction_order,
            rollup_metadata.provided_epoch,
            rollup_metadata.completed_batch_number,
            &self.leader_change_message.current_leader_tx_orderer_address,
            old_epoch,
            new_epoch,
            epoch_leader_rpc_url.clone(),
        )
        .await;

        send_end_signal_to_epoch_leader(
            context.clone(),
            self.leader_change_message.rollup_id.clone(),
            old_epoch,
            epoch_leader_rpc_url,
        );

        mut_cluster_metadata.update()?;

        println!("===== ⚙️⚙️⚙️⚙️⚙️ SetLeaderTxOrderer handler() 종료(노드 주소: {:?}) ⚙️⚙️⚙️⚙️⚙️ =====", tx_orderer_address); // test code

        Ok(())
    }
}
