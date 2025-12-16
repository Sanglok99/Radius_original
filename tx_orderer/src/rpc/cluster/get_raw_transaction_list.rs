use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetRawTransactionList {
    pub leader_change_message: LeaderChangeMessage,
    pub rollup_signature: Signature,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LeaderChangeMessage {
    pub rollup_id: RollupId,
    pub executor_address: Address,
    pub platform_block_height: u64,

    pub current_leader_tx_orderer_address: Address,
    pub next_leader_tx_orderer_address: Address,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignMessage {
    pub rollup_id: RollupId,
    pub executor_address: String,
    pub platform_block_height: u64,

    pub current_leader_tx_orderer_address: Address,
    pub next_leader_tx_orderer_address: Address,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetRawTransactionListResponse {
    pub raw_transaction_list: Vec<String>,
}

impl RpcParameter<AppState> for GetRawTransactionList {
    type Response = GetRawTransactionListResponse;

    fn method() -> &'static str {
        "get_raw_transaction_list"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        println!("=== GetRawTransactionList 시작 ==="); // test code

        let start_get_raw_transaction_list_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        let mut raw_transaction_list = Vec::new();

        let rollup_id = self.leader_change_message.rollup_id.clone();
        println!("LeaderChangeMessage rollup_id: {:?}", rollup_id); // test code

        let rollup_metadata = match RollupMetadata::get(&rollup_id) {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::error!(
                    "Failed to get rollup metadata - rollup_id: {:?} / error: {:?}",
                    rollup_id,
                    err,
                );

                return Ok(GetRawTransactionListResponse {
                    raw_transaction_list: Vec::new(),
                });
            }
        };

        // === new code start ===
        let mut epoch = rollup_metadata.provided_epoch;

        if let Ok(can_provide_epoch) = CanProvideEpochInfo::get(&rollup_id) {
            let completed_epoch_list = &can_provide_epoch.completed_epoch;
            epoch = match get_last_valid_completed_epoch(completed_epoch_list, rollup_metadata.provided_epoch) {
                Ok(last_valid_epoch) => last_valid_epoch,
                Err(err) => {
                    tracing::error!("Failed to get epoch - rollup_id: {:?} / error: {:?}", rollup_id, err);
                    return Ok(GetRawTransactionListResponse {
                        raw_transaction_list: Vec::new(),
                    });
                }
            };
        } else {
            tracing::error!("Failed to get can_provide_epoch - rollup_id: {:?}", rollup_id);
            return Ok(GetRawTransactionListResponse {
                raw_transaction_list: Vec::new(),
            });
        }
        // === new code end ===

        let rollup = Rollup::get(&rollup_id)?;
        // println!("Rollup: {:?}", rollup); // test code
        // println!("rollup_id comparison - LeaderChangeMessage: {:?}, Rollup: {:?}, same: {}", rollup_id, rollup.rollup_id, rollup_id == rollup.rollup_id); // test code

        // let start_batch_number = rollup_metadata.provided_batch_number; // old code
        let last_completed_batch_number = rollup_metadata.completed_batch_number; // new code
        // let mut current_provided_batch_number = start_batch_number; // old code
        let mut current_provided_batch_number = last_completed_batch_number + 1; // new code
        let mut current_completed_batch_number = last_completed_batch_number; // new code
        let mut current_provided_transaction_order = rollup_metadata.provided_transaction_order;

        // println!("= after initialization ="); // test code
        // println!("start_batch_number: {:?}", start_batch_number); // test code
        // println!("current_provided_batch_number: {:?}", current_provided_batch_number); // test code
        // println!("current_provided_transaction_order: {:?}", current_provided_transaction_order); // test code

        // old code
        /*
        let mut i = 0; // test code

        while let Ok(batch) = Batch::get(&rollup_id, current_provided_batch_number) {
            println!("= {:?}th interation =", i); // test code

            let start_transaction_order = if current_provided_batch_number == start_batch_number {
                println!("if"); // test code
                current_provided_transaction_order + 1
            } else {
                println!("else"); // test code
                0
            };
            // println!("start_transaction_order: {:?}", start_transaction_order); // test code

            raw_transaction_list.extend(extract_raw_transactions(
                batch,
                start_transaction_order as u64,
            ));

            current_provided_batch_number += 1;
            current_provided_transaction_order = -1;

            i += 1; // test code
            println!("current_provided_batch_number: {:?}", current_provided_batch_number); // test code
            println!("current_provided_transaction_order: {:?}", current_provided_transaction_order); // test code
        }
        */
        
        // === new code start ===
        while let Ok(batch) = Batch::get(&rollup_id, current_provided_batch_number) {
            // println!("= {:?}th interation =", i); // test code

            let mut transactions_in_batch = 0;
            raw_transaction_list.extend(my_extract_raw_transactions(
                batch,
                epoch,
                &mut transactions_in_batch,
            ));

            if transactions_in_batch == 0 { // All transactions in the batch have been processed
                current_completed_batch_number += 1;    
            }
            
            current_provided_batch_number += 1;
            current_provided_transaction_order = -1;
            // i += 1; // test code
            // println!("current_provided_batch_number: {:?}", current_provided_batch_number); // test code
        }
        // === new code end ===

        // println!("= after while loop ="); // test code
        // println!("current_provided_batch_number: {:?}", current_provided_batch_number); // test code
        // println!("current_provided_transaction_order: {:?}", current_provided_transaction_order); // test code
        
        // old code
        if let Ok(can_provide_transaction_info) = CanProvideTransactionInfo::get(&rollup_id) {
            if let Some(can_provide_transaction_orderers) = can_provide_transaction_info
                .can_provide_transaction_orders_per_batch
                .get(&current_provided_batch_number)
            {
                let valid_end_transaction_order = get_last_valid_transaction_order(
                    can_provide_transaction_orderers,
                    current_provided_transaction_order,
                );
                // println!("valid_end_transaction_order: {:?}", valid_end_transaction_order); // test code

                fetch_and_append_transactions(
                    &rollup_id,
                    current_provided_batch_number,
                    (current_provided_transaction_order + 1) as u64,
                    valid_end_transaction_order,
                    &mut raw_transaction_list,
                )?;

                current_provided_transaction_order = valid_end_transaction_order;

                if current_provided_transaction_order
                    == rollup.max_transaction_count_per_batch as i64 - 1
                {
                    // println!("if current_provided_transaction_order == rollup.max_transaction_count_per_batch as i64 - 1"); // test code
                    current_provided_batch_number += 1;
                    current_provided_transaction_order = -1;
                }
            }
        }

        // === new code start ===
        if let Ok(can_provide_transaction_info) = CanProvideTransactionInfo::get(&rollup_id) {
            if let Some(can_provide_transaction_orderers) = can_provide_transaction_info
                .can_provide_transaction_orders_per_batch
                .get(&current_provided_batch_number)
            {
                let valid_end_transaction_order = get_last_valid_transaction_order(
                    can_provide_transaction_orderers,
                    current_provided_transaction_order,
                );
        
                my_fetch_and_append_transactions(
                    &rollup_id,
                    current_provided_batch_number,
                    (current_provided_transaction_order + 1) as u64,
                    valid_end_transaction_order,
                    &mut raw_transaction_list,
                )?;
        
                current_provided_transaction_order = valid_end_transaction_order;
        
                if current_provided_transaction_order
                    == rollup.max_transaction_count_per_batch as i64 - 1
                {
                    current_provided_batch_number += 1;
                    current_provided_transaction_order = -1;
                }
            }
        }
        // === new code end ===

        let cluster = Cluster::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
            self.leader_change_message.platform_block_height,
        )?;

        println!("self.leader_change_message.platform_block_height: {:?}", self.leader_change_message.platform_block_height); // test code

        let mut mut_rollup_metadata = RollupMetadata::get_mut(&rollup_id)?;

        let mut batch_number_list_to_delete = Vec::new();
        for batch_number in (last_completed_batch_number + 1)..current_completed_batch_number {
            batch_number_list_to_delete.push(batch_number);
        }

        mut_rollup_metadata.provided_batch_number = current_provided_batch_number;
        mut_rollup_metadata.provided_transaction_order = current_provided_transaction_order;

        mut_rollup_metadata.completed_batch_number = current_completed_batch_number; // new code
        mut_rollup_metadata.provided_epoch = epoch; // new code

        let leader_tx_orderer_rpc_info = cluster
            .get_tx_orderer_rpc_info(&self.leader_change_message.next_leader_tx_orderer_address)
            .ok_or_else(|| {
                tracing::error!(
                    "TxOrderer RPC info not found for address {:?}",
                    self.leader_change_message.next_leader_tx_orderer_address
                );
                Error::TxOrdererInfoNotFound
            })?;

        /*
        println!("leader_tx_orderer_rpc_info: {:?}", leader_tx_orderer_rpc_info); // test code
        println!("leader_tx_orderer_rpc_info.cluster_rpc_url: {:?}", leader_tx_orderer_rpc_info.cluster_rpc_url); // test code
        println!("leader_tx_orderer_rpc_info.external_rpc_url: {:?}", leader_tx_orderer_rpc_info.external_rpc_url); // test code
        println!("leader_tx_orderer_rpc_info.tx_orderer_address: {:?}", leader_tx_orderer_rpc_info.tx_orderer_address); // test code
        */
        // println!("=== rollup.platform value: {:?} ===", rollup.platform); // test code. This shows Ethereum/Holesky/Local
        // tracing::info!("rollup.platform value: {:?}", rollup.platform); // test code. This shows Ethereum/Holesky/Local
        
        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;

        let tx_orderer_address = signer.address().clone();

        println!("signer.address() value: {:?}", signer.address()); // test code
        println!("self.leader_change_message.next_leader_tx_orderer_address: {:?}", self.leader_change_message.next_leader_tx_orderer_address); // test code

        let is_next_leader =
            tx_orderer_address == self.leader_change_message.next_leader_tx_orderer_address;

        println!("is_next_leader: {:?}", is_next_leader); // test code
        
        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )?;

        println!("= mut_cluster_metadata initialization ="); // test code
        println!("mut_cluster_metadata.cluster_id: {:?}", mut_cluster_metadata.cluster_id); // test code
        println!("mut_cluster_metadata.platform_block_height: {:?}", mut_cluster_metadata.platform_block_height); // test code
        println!("mut_cluster_metadata.is_leader: {:?}", mut_cluster_metadata.is_leader); // test code
        println!("mut_cluster_metadata.leader_tx_orderer_rpc_info: {:?}", mut_cluster_metadata.leader_tx_orderer_rpc_info); // test code

        if mut_cluster_metadata.is_leader == false {
            println!("*** if mut_cluster_metadata.is_leader == false ***"); // test code

            if let Some(current_leader_tx_orderer_rpc_info) =
                mut_cluster_metadata.leader_tx_orderer_rpc_info.clone()
            {
                let current_leader_tx_orderer_cluster_rpc_url = current_leader_tx_orderer_rpc_info
                    .cluster_rpc_url
                    .clone()
                    .unwrap();

                println!("current_leader_tx_orderer_cluster_rpc_url: {:?}", current_leader_tx_orderer_cluster_rpc_url); // test code
                println!("current_leader_tx_orderer_rpc_info: {:?}", current_leader_tx_orderer_rpc_info); // test code

                let parameter = GetOrderCommitmentInfo {
                    rollup_id: self.leader_change_message.rollup_id.clone(),
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
                    self.leader_change_message.current_leader_tx_orderer_address
                );
            }
        }

        mut_cluster_metadata.platform_block_height =
            self.leader_change_message.platform_block_height;
        mut_cluster_metadata.is_leader = is_next_leader;
        mut_cluster_metadata.leader_tx_orderer_rpc_info = Some(leader_tx_orderer_rpc_info.clone());

        println!("= mut_cluster_metadata update ="); // test code
        println!("mut_cluster_metadata.cluster_id: {:?}", mut_cluster_metadata.cluster_id); // test code
        println!("mut_cluster_metadata.platform_block_height: {:?}", mut_cluster_metadata.platform_block_height); // test code
        println!("mut_cluster_metadata.is_leader: {:?}", mut_cluster_metadata.is_leader); // test code
        println!("mut_cluster_metadata.leader_tx_orderer_rpc_info: {:?}", mut_cluster_metadata.leader_tx_orderer_rpc_info); // test code

        let signer = context.get_signer(rollup.platform).await?;
        let current_tx_orderer_address = signer.address();

        sync_leader_tx_orderer(
            context.clone(),
            cluster,
            current_tx_orderer_address,
            self.leader_change_message.clone(),
            self.rollup_signature,
            mut_rollup_metadata.batch_number,
            mut_rollup_metadata.transaction_order,
            mut_rollup_metadata.provided_batch_number,
            mut_rollup_metadata.provided_transaction_order,
            &self.leader_change_message.current_leader_tx_orderer_address.clone(), // new code
        )
        .await;

        mut_cluster_metadata.update()?;
        let _ = mut_rollup_metadata.update().map_err(|error| {
            tracing::error!(
                "rollup_metadata update error - rollup id: {:?}, error: {:?}",
                self.leader_change_message.rollup_id,
                error
            );
        });

        let end_get_raw_transaction_list_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        tracing::info!(
            "get_raw_transaction_list - total take time: {:?}",
            end_get_raw_transaction_list_time - start_get_raw_transaction_list_time
        );

        let shared_channel_infos = context.shared_channel_infos();
        let mev_searcher_infos = MevSearcherInfos::get_or(MevSearcherInfos::default).unwrap();

        send_transaction_list_to_mev_searcher(
            &rollup_id,
            raw_transaction_list.clone(),
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
                raw_transaction_list
                    .extend(mev_target_transaction.backrunning_transaction_list.clone());
            }
        }

        Ok(GetRawTransactionListResponse {
            raw_transaction_list,
        })
    }
}

pub async fn sync_leader_tx_orderer(
    context: AppState,
    cluster: Cluster,
    current_tx_orderer_address: &Address,
    leader_change_message: LeaderChangeMessage,
    rollup_signature: Signature,
    batch_number: u64,
    transaction_order: u64,
    provided_batch_number: u64,
    provided_transaction_order: i64,
    current_leader_tx_orderer_address: &Address, // new code
) {
    println!("=== sync_leader_tx_orderer 시작 ==="); // test code
    println!("next_leader_tx_orderer_rpc_info.tx_orderer_address: {}", &leader_change_message.next_leader_tx_orderer_address); // test code
    println!("current_tx_orderer_address: {}", current_tx_orderer_address); // test code
    println!("current_leader_tx_orderer_address: {}", current_leader_tx_orderer_address); // test code

    let mut other_cluster_rpc_url_list = cluster.get_other_cluster_rpc_url_list();
    if other_cluster_rpc_url_list.is_empty() {
        tracing::info!("No cluster RPC URLs available for synchronization");
        return;
    }

    if let Some(next_leader_tx_orderer_rpc_info) =
        cluster.get_tx_orderer_rpc_info(&leader_change_message.next_leader_tx_orderer_address)
    {
        println!("if let Some(next_leader_tx_orderer_rpc_info) == true"); // test code

        let next_leader_tx_orderer_cluster_rpc_url = next_leader_tx_orderer_rpc_info
            .cluster_rpc_url
            .clone()
            .unwrap();

        println!("next_leader_tx_orderer_cluster_rpc_url: {:?}", next_leader_tx_orderer_cluster_rpc_url); // test code

        // Filter out the next leader's cluster URL from the list
        other_cluster_rpc_url_list = other_cluster_rpc_url_list
            .into_iter()
            .filter(|rpc_url| rpc_url != &next_leader_tx_orderer_cluster_rpc_url)
            .collect();

        let parameter = SyncLeaderTxOrderer {
            leader_change_message,
            rollup_signature,
            batch_number,
            transaction_order,
            provided_batch_number,
            provided_transaction_order,
        };

        // if next_leader_tx_orderer_rpc_info.tx_orderer_address != current_tx_orderer_address { // old code
        if next_leader_tx_orderer_rpc_info.tx_orderer_address != current_leader_tx_orderer_address { // new code
            println!("if next_leader_tx_orderer_rpc_info.tx_orderer_address != current_leader_tx_orderer_address"); // test code

            // Directly request the next leader tx_orderer to sync
            let start_sync_leader_tx_order_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();

            let _result: Result<(), radius_sdk::json_rpc::client::RpcClientError> = context
                .rpc_client()
                .request_with_priority(
                    next_leader_tx_orderer_cluster_rpc_url.clone(),
                    SyncLeaderTxOrderer::method(),
                    &parameter,
                    Id::Null,
                    Priority::High,
                )
                .await;

            let end_sync_leader_tx_order_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();

            tracing::info!(
                "SyncLeaderTxOrderer - start: {:?} / end: {:?} / gap: {:?} / next_leader_tx_orderer_cluster_rpc_url: {:?}, parameter: {:?}",
                start_sync_leader_tx_order_time,
                end_sync_leader_tx_order_time,
                end_sync_leader_tx_order_time - start_sync_leader_tx_order_time,
                next_leader_tx_orderer_cluster_rpc_url,
                parameter
            );

            // Fire and forget to the rest of the cluster nodes asynchronously
            let urls = other_cluster_rpc_url_list.clone();
            tokio::spawn(async move {
                let _ = context
                    .rpc_client()
                    .fire_and_forget_multicast(
                        urls,
                        SyncLeaderTxOrderer::method(),
                        &parameter,
                        Id::Null,
                    )
                    .await;
            });
        }

        println!("=== sync_leader_tx_orderer 종료 ==="); // test code
    } else {
        println!("else"); // test code
        
        tracing::error!(
            "Next leader tx orderer RPC info not found for address {:?}",
            leader_change_message.next_leader_tx_orderer_address
        );

        println!("=== sync_leader_tx_orderer 종료 ==="); // test code
    }
}

fn extract_raw_transactions(batch: Batch, start_transaction_order: u64) -> Vec<String> {
    batch
        .raw_transaction_list
        .into_iter()
        .enumerate()
        .filter_map(|(i, transaction)| {
            if (i as u64) >= start_transaction_order {
                Some(match transaction {
                    RawTransaction::Eth(EthRawTransaction { raw_transaction, .. }) => raw_transaction, // new code
                    // RawTransaction::Eth(EthRawTransaction(data)) => data,
                    RawTransaction::EthBundle(EthRawBundleTransaction(data)) => data,
                })
            } else {
                None
            }
        })
        .collect()
}

// === new code start ===
fn my_extract_raw_transactions(batch: Batch, epoch: u64, transactions_in_batch: &mut i32) -> Vec<String> {
    batch
        .raw_transaction_list
        .into_iter()
        .filter_map(|transaction| {
            match transaction {
                RawTransaction::Eth(eth_tx) => {
                    match eth_tx.epoch {
                        Some(tx_epoch) if tx_epoch >= epoch => {
                            *transactions_in_batch += 1; // Unprocessed transactions still in the batch
                            Some(eth_tx.raw_transaction)
                        }
                        _ => None,
                    }
                }
                RawTransaction::EthBundle(_) => None,
            }          
        })
        .collect()
}

fn get_last_valid_completed_epoch(
    completed_epoch: &BTreeSet<u64>,
    provided_epoch: u64,
) -> Result<u64, Error> {
    let mut last_valid_epoch = provided_epoch;

    for &epoch in completed_epoch {
        if epoch == last_valid_epoch + 1 {
            last_valid_epoch += 1;
        } else if epoch > last_valid_epoch {
            break;
        }
    }

    Ok(last_valid_epoch)
}
// === new code end ===

fn get_last_valid_transaction_order(
    can_provide_transaction_orders: &BTreeSet<u64>,
    provided_transaction_order: i64,
) -> i64 {
    // println!("=== get_last_valid_transaction_order 시작 ==="); // test code
    
    let mut last_valid_transaction_order = provided_transaction_order;

    // println!("last_valid_transaction_order(before iteration): {:?}", last_valid_transaction_order); // test code
    
    // let mut iteration_count = 0; // test code

    for &transaction_order in can_provide_transaction_orders {
        // iteration_count += 1; // test code

        let transaction_order = transaction_order as i64;

        if transaction_order == last_valid_transaction_order + 1 {
            last_valid_transaction_order += 1;
        } else if transaction_order > last_valid_transaction_order {
            // println!("[{:?}] transaction_order > last_valid_transaction_order", transaction_order); // test code
            break;
        }
    }

    // println!("last_valid_transaction_order(after iteration): {:?}", last_valid_transaction_order); // test code
    // println!("iteration_count: {:?}", iteration_count); // test code

    // println!("=== get_last_valid_transaction_order 종료 ==="); // test code

    last_valid_transaction_order as i64
}

fn fetch_and_append_transactions(
    rollup_id: &RollupId,
    batch_number: u64,
    start_transaction_order: u64,
    last_valid_transaction_order: i64,
    raw_transaction_list: &mut Vec<String>,
) -> Result<(), RpcError> {
    if last_valid_transaction_order < start_transaction_order as i64 {
        return Ok(());
    }

    for transaction_order in
        start_transaction_order..=last_valid_transaction_order.try_into().unwrap()
    {
        let (raw_transaction, _) =
            RawTransactionModel::get(rollup_id, batch_number, transaction_order)?;
        let raw_transaction = match raw_transaction {
            RawTransaction::Eth(EthRawTransaction { raw_transaction, .. }) => raw_transaction, // new code
            // RawTransaction::Eth(EthRawTransaction(data)) => data, // old code
            RawTransaction::EthBundle(EthRawBundleTransaction(data)) => data,
        };
        raw_transaction_list.push(raw_transaction);
    }
    Ok(())
}

fn my_fetch_and_append_transactions(
    rollup_id: &RollupId,
    batch_number: u64,
    start_transaction_order: u64,
    last_valid_transaction_order: i64,
    raw_transaction_list: &mut Vec<String>,
) -> Result<(), RpcError> {
    if last_valid_transaction_order < start_transaction_order as i64 {
        return Ok(());
    }

    for transaction_order in
        start_transaction_order..=last_valid_transaction_order.try_into().unwrap()
    {
        let (raw_transaction, _) =
            RawTransactionModel::get(rollup_id, batch_number, transaction_order)?;
        let raw_transaction = match raw_transaction {
            RawTransaction::Eth(EthRawTransaction { raw_transaction, .. }) => raw_transaction, // new code
            // RawTransaction::Eth(EthRawTransaction(data)) => data, // old code
            RawTransaction::EthBundle(EthRawBundleTransaction(data)) => data,
        };
        raw_transaction_list.push(raw_transaction);
    }
    Ok(())
}