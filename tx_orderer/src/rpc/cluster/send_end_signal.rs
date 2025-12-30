use crate::rpc::prelude::*;

use radius_sdk::signature::Address;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendEndSignal {
    pub rollup_id: RollupId,
    pub epoch: u64,
    pub sender_address: Address,
}

impl RpcParameter<AppState> for SendEndSignal {
    type Response = ();

    fn method() -> &'static str {
        "send_end_signal"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        println!("===== 📤📤📤📤📤 SendEndSignal handler() 시작 📤📤📤📤📤 ====="); // test code

        println!("epoch: {:?}", self.epoch); // test code
        println!("sender_address: {:?}", self.sender_address); // test code

        let rollup = Rollup::get(&self.rollup_id).map_err(|e| {
            tracing::error!("Failed to retrieve rollup: {:?}", e);
            Error::RollupNotFound
        })?;

        let cluster_metadata = ClusterMetadata::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        ).map_err(|e| {
            tracing::error!(
                "Failed to retrieve cluster metadata for rollup_id: {:?}, cluster_id: {:?}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                e
            );
            e
        })?;

        let cluster = Cluster::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
            cluster_metadata.platform_block_height,
        ).map_err(|e| {
            tracing::error!(
                "Failed to retrieve cluster for rollup_id: {:?}, cluster_id: {:?}, platform_block_height: {}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                cluster_metadata.platform_block_height,
                e
            );
            e
        })?;

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;
        let tx_orderer_address = signer.address().clone();

        // 현재 노드의 cluster RPC URL 가져오기
        let current_node_rpc_info = cluster.get_tx_orderer_rpc_info(&tx_orderer_address)
            .ok_or_else(|| {
                tracing::error!(
                    "Failed to get RPC info for current node. tx_orderer_address: {:?}",
                    tx_orderer_address
                );
                Error::TxOrdererInfoNotFound
            })?;

        /*
        let current_node_cluster_rpc_url = current_node_rpc_info.cluster_rpc_url.clone()
            .ok_or_else(|| {
                tracing::error!(
                    "Cluster RPC URL not found for current node. tx_orderer_address: {:?}",
                    tx_orderer_address
                );
                Error::GeneralError("Cluster RPC URL not found".into())
            })?;
        */

        // println!("current_node_cluster_rpc_url: {:?}", current_node_cluster_rpc_url); // test code
        println!("tx_orderer_address: {:?}", tx_orderer_address); // test code
        println!("epoch's leader: {:?}", cluster_metadata.epoch_leader_map.get(&self.epoch).unwrap_or(&"".to_string())); // test code

        // 현재 노드가 epoch의 leader인지 확인(❗address❗로 비교)
        if cluster_metadata.epoch_leader_map.get(&self.epoch).unwrap_or(&"".to_string()) != &tx_orderer_address.to_string() {
            tracing::error!(
                "Received end_signal but current node is not the epoch's leader. rollup_id: {:?}, epoch: {}, sender_address: {:?}",
                self.rollup_id,
                self.epoch,
                self.sender_address
            );
            return Err(Error::GeneralError("Not a leader node".into()).into());
        }
        
        let node_index = cluster.tx_orderer_rpc_infos.iter().find_map(|(index, info)| {
            if info.tx_orderer_address == self.sender_address {
                Some(*index)
            } else {
                None
            }
        }).ok_or_else(|| {
            tracing::error!(
                "Failed to find node index for sender_address: {:?} in cluster. rollup_id: {:?}, epoch: {}",
                self.sender_address,
                self.rollup_id,
                self.epoch
            );
            Error::GeneralError("Sender address not found in cluster".into())
        })?;

        // epoch_node_bitmap 업데이트
        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        ).map_err(|e| {
            tracing::error!(
                "Failed to get mutable cluster metadata for rollup_id: {:?}, cluster_id: {:?}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                e
            );
            e
        })?;

        // 노드 인덱스에 대응되는 비트 설정
        mut_cluster_metadata.set_node_bit(self.epoch, node_index);
        
        // 모든 노드가 시그널을 보냈는지 확인
        let total_nodes = cluster.tx_orderer_rpc_infos.len();
        if mut_cluster_metadata.all_nodes_sent_signal(self.epoch, total_nodes) {
            // CanProvideEpochInfo에 epoch 추가
            CanProvideEpochInfo::add_completed_epoch(&self.rollup_id, self.epoch).map_err(|e| {
                tracing::error!(
                    "Failed to add completed epoch to CanProvideEpochInfo. rollup_id: {:?}, epoch: {}, error: {:?}",
                    self.rollup_id,
                    self.epoch,
                    e
                );
                e
            })?;
        }

        println!("epoch completed: (epoch: {:?}, completed: {:?})", self.epoch, mut_cluster_metadata.all_nodes_sent_signal(self.epoch, total_nodes)); // test code

        mut_cluster_metadata.update().map_err(|e| {
            tracing::error!(
                "Failed to update cluster metadata. rollup_id: {:?}, cluster_id: {:?}, epoch: {}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                self.epoch,
                e
            );
            e
        })?;

        println!("===== 📤📤📤📤📤 SendEndSignal handler() 종료(노드 주소: {:?}) 📤📤📤📤📤 =====", tx_orderer_address); // test code

        Ok(())
    }
}