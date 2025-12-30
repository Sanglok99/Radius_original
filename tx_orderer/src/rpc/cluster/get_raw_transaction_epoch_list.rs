use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::LeaderChangeMessage;
use crate::rpc::prelude::*;

use radius_sdk::{json_rpc::client::Priority, signature::Address};
use tokio::{sync::mpsc::UnboundedReceiver, time::Instant};

use super::SyncLeaderTxOrderer;
use crate::{
    rpc::{
        cluster::{GetOrderCommitmentInfo, GetOrderCommitmentInfoResponse},
        prelude::*,
    },
    task::{send_transaction_list_to_mev_searcher, MevTargetTransaction},
};

use super::send_end_signal_to_epoch_leader; // new code
use super::get_last_valid_completed_epoch; // new code
use super::my_extract_raw_transactions; // new code
use super::get_last_valid_transaction_order; // new code
use super::my_fetch_and_append_transactions; // new code

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetRawTransactionEpochList {
    pub rollup_id: RollupId,
    pub rollup_signature: Signature,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetRawTransactionEpochListResponse {
    pub raw_transaction_list: Vec<String>,
}

impl RpcParameter<AppState> for GetRawTransactionEpochList {
    type Response = GetRawTransactionEpochListResponse;

    fn method() -> &'static str {
        "get_raw_transaction_epoch_list"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        println!("===== 🗂️🗂️🗂️🗂️🗂️ GetRawTransactionEpochList handler() 시작 🗂️🗂️🗂️🗂️🗂️ ====="); // test code

        let start_get_raw_transaction_epoch_list_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        let mut raw_transaction_epoch_list = Vec::new();

        let rollup_id = self.rollup_id.clone();

        let rollup_metadata = match RollupMetadata::get(&rollup_id) {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::error!(
                    "Failed to get rollup metadata - rollup_id: {:?} / error: {:?}",
                    rollup_id,
                    err,
                );

                return Ok(GetRawTransactionEpochListResponse {
                    raw_transaction_list: Vec::new(),
                });
            }
        };

        let mut epoch = rollup_metadata.provided_epoch;

        if let Ok(can_provide_epoch) = CanProvideEpochInfo::get(&rollup_id) {
            let completed_epoch_list = &can_provide_epoch.completed_epoch;
            epoch = match get_last_valid_completed_epoch(completed_epoch_list, rollup_metadata.provided_epoch) {
                Ok(last_valid_epoch) => last_valid_epoch,
                Err(err) => {
                    tracing::error!("Failed to get epoch - rollup_id: {:?} / error: {:?}", rollup_id, err);
                    return Ok(GetRawTransactionEpochListResponse {
                        raw_transaction_list: Vec::new(),
                    });
                }
            };
        } else {
            tracing::error!("Failed to get can_provide_epoch - rollup_id: {:?}", rollup_id);
            return Ok(GetRawTransactionEpochListResponse {
                raw_transaction_list: Vec::new(),
            });
        }

        println!("💡epoch(CanProvideEpochInfo에서 받아온 값): {:?}", epoch); // test code

        let rollup = Rollup::get(&rollup_id)?;
        
        println!("last_completed_batch_number: {:?}", rollup_metadata.completed_batch_number); // test code

        let last_completed_batch_number = rollup_metadata.completed_batch_number; 

        let mut current_completed_batch_number = last_completed_batch_number; 
        let mut current_provided_batch_number = last_completed_batch_number + 1; 

        println!("current_completed_batch_number(Batch 순회 전): {:?}", current_completed_batch_number); // test code
        println!("current_provided_batch_number(Batch 순회 전): {:?}", current_provided_batch_number); // test code

        let mut current_provided_transaction_order = -1;
        
        let mut iteration_count = 0; // test code

        while let Ok(batch) = Batch::get(&rollup_id, current_provided_batch_number as u64) { // current_provided_batch_number is i64, but Batch::get requires u64. This variable is always a non-negative integer so this won't cause an error.
            println!("= {:?}th interation =", iteration_count); // test code

            let mut transactions_in_batch = 0;
            raw_transaction_epoch_list.extend(my_extract_raw_transactions(
                batch,
                epoch,
                &mut transactions_in_batch,
            ));

            if transactions_in_batch == 0 { // All transactions in the batch have been processed
                current_completed_batch_number += 1;    
            }
            
            current_provided_batch_number += 1;
            current_provided_transaction_order = -1;

            iteration_count += 1; // test code
        }

        println!("current_provided_batch_number(Batch 순회 후): {:?}", current_provided_batch_number); // test code
        println!("current_provided_transaction_order(Batch 순회 후): {:?}", current_provided_transaction_order); // test code

        if let Ok(can_provide_transaction_info) = CanProvideTransactionInfo::get(&rollup_id) {
            if let Some(can_provide_transaction_orderers) = can_provide_transaction_info
                .can_provide_transaction_orders_per_batch
                .get(&(current_provided_batch_number as u64))
            {
                let valid_end_transaction_order = get_last_valid_transaction_order(
                    can_provide_transaction_orderers,
                    current_provided_transaction_order,
                );
        
                my_fetch_and_append_transactions(
                    &rollup_id,
                    current_provided_batch_number as u64,
                    (current_provided_transaction_order + 1) as u64,
                    valid_end_transaction_order,
                    &mut raw_transaction_epoch_list,
                    &epoch,
                )?;
            }
        }

        let mut mut_rollup_metadata = RollupMetadata::get_mut(&rollup_id)?;

        let mut batch_number_list_to_delete = Vec::new();
        for batch_number in (last_completed_batch_number + 1)..current_completed_batch_number {
            batch_number_list_to_delete.push(batch_number);
        }

        mut_rollup_metadata.provided_batch_number = current_provided_batch_number as u64;
        mut_rollup_metadata.provided_transaction_order = current_provided_transaction_order;

        mut_rollup_metadata.completed_batch_number = current_completed_batch_number; // new code
        mut_rollup_metadata.provided_epoch = epoch; // new code

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;

        let tx_orderer_address = signer.address().clone();

        let cluster_metadata = ClusterMetadata::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )
        .map_err(|error| {
            tracing::error!("Failed to get cluster metadata: {:?}", error);
            Error::ClusterMetadataNotFound
        })?;

        if cluster_metadata.is_leader == false {
            println!("*** if mut_cluster_metadata.is_leader == false ***"); // test code

            if let Some(current_leader_tx_orderer_rpc_info) =
                cluster_metadata.leader_tx_orderer_rpc_info.clone()
            {
                let current_leader_tx_orderer_cluster_rpc_url = current_leader_tx_orderer_rpc_info
                    .cluster_rpc_url
                    .clone()
                    .unwrap();

                println!("current_leader_tx_orderer_cluster_rpc_url: {:?}", current_leader_tx_orderer_cluster_rpc_url); // test code
                println!("current_leader_tx_orderer_rpc_info: {:?}", current_leader_tx_orderer_rpc_info); // test code

                let parameter = GetOrderCommitmentInfo {
                    rollup_id: self.rollup_id.clone(),
                };

                match context
                    .rpc_client()
                    .request_with_priority::<&GetOrderCommitmentInfo, GetOrderCommitmentInfoResponse>(
                        current_leader_tx_orderer_cluster_rpc_url.clone(),
                        GetOrderCommitmentInfo::method(),
                        &parameter,
                        Id::Null,
                        Priority::High,
                    )
                    .await
                {
                    Ok(response) => {
                        tracing::info!(
                          "Get order commitment info - current leader external rpc response: {:?}",
                          response
                        );

                        mut_rollup_metadata.batch_number = response.batch_number;
                        mut_rollup_metadata.transaction_order = response.transaction_order;
                    }
                    Err(error) => {
                        tracing::error!(
                            "Get order commitment info - current leader external rpc error: {:?}",
                            error
                        );
                    }
                }
            } else {
                tracing::warn!(
                    "Current leader tx orderer RPC info not found for address {:?}",
                    cluster_metadata.leader_tx_orderer_rpc_info.clone().unwrap().tx_orderer_address
                );
            }
        }

        let end_get_raw_transaction_epoch_list_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        tracing::info!(
            "get_raw_transaction_epoch_list - total take time: {:?}",
            end_get_raw_transaction_epoch_list_time - start_get_raw_transaction_epoch_list_time
        );

        let shared_channel_infos = context.shared_channel_infos();
        let mev_searcher_infos = MevSearcherInfos::get_or(MevSearcherInfos::default).unwrap();

        send_transaction_list_to_mev_searcher(
            &rollup_id,
            raw_transaction_epoch_list.clone(),
            shared_channel_infos,
            &mev_searcher_infos,
        );

        let ip_list = mev_searcher_infos.get_ip_list_by_rollup_id(&rollup_id);
        let receivers: Vec<Arc<tokio::sync::Mutex<UnboundedReceiver<MevTargetTransaction>>>> = {
            let map = shared_channel_infos.lock().unwrap();
            ip_list
                .iter()
                .filter_map(|ip| map.get(ip).map(|(_, rx)| Arc::clone(rx)))
                .collect()
        };

        let collected_mev_target_transaction = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let mut sub_tasks = vec![];

        for receiver in receivers {
            let collected_clone = Arc::clone(&collected_mev_target_transaction);
            let rx = Arc::clone(&receiver);

            let sub_task = tokio::spawn(async move {
                let deadline = Instant::now() + Duration::from_millis(5000);

                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => {}
                    maybe_mev_target_transaction = async {
                        let mut guard = rx.lock().await;
                        guard.recv().await
                    } => {
                        if let Some(mev_target_transaction) = maybe_mev_target_transaction {
                            tracing::info!("Received mev target transaction: {:?}", mev_target_transaction);
                            collected_clone.lock().await.push(mev_target_transaction);
                        }
                    }
                }
            });

            sub_tasks.push(sub_task);
        }

        let _ = futures::future::join_all(sub_tasks).await;

        {
            let result = collected_mev_target_transaction.lock().await;
            tracing::info!("Collected mev target transactions: {:?}", *result);

            for mev_target_transaction in result.iter() {
                raw_transaction_epoch_list
                    .extend(mev_target_transaction.backrunning_transaction_list.clone());
            }
        }

        println!("===== 🗂️🗂️🗂️🗂️🗂️ GetRawTransactionEpochList handler() 종료(노드 주소: {:?}) 🗂️🗂️🗂️🗂️🗂️ =====", tx_orderer_address); // test code

        Ok(GetRawTransactionEpochListResponse {
            raw_transaction_list: raw_transaction_epoch_list,
        })
    }
}