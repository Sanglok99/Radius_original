use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use radius_sdk::signature::Address;

use crate::{
    rpc::{
        cluster::{BatchCreationMessage, SyncBatchCreation, SyncRawTransaction},
        external::issue_order_commitment,
        prelude::*,
    },
    task::finalize_batch,
    types::*,
};

/// Wall times for a single `send_raw_transaction` handler on one node; **milliseconds** since Unix epoch.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionHandlerTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionRollupMetadataTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionRollupMetadataGetMutTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionAfterRollupMetadataUpdateTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionRedirectToLeaderTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionResponse {
    pub order_commitment: OrderCommitment,
    pub handler_timings: SendRawTransactionHandlerTimings,
    pub rollup_metadata_timings: SendRawTransactionRollupMetadataTimings,
    pub rollup_metadata_get_mut_timings: SendRawTransactionRollupMetadataGetMutTimings,
    pub after_rollup_metadata_update_timings: Option<SendRawTransactionAfterRollupMetadataUpdateTimings>,
    pub redirect_to_leader_timings: Option<SendRawTransactionRedirectToLeaderTimings>,
    pub leader_rollup_metadata_timings: Option<SendRawTransactionRollupMetadataTimings>,
    pub leader_rollup_metadata_get_mut_timings: Option<SendRawTransactionRollupMetadataGetMutTimings>,
    pub leader_after_rollup_metadata_update_timings: Option<SendRawTransactionAfterRollupMetadataUpdateTimings>,
}

fn now_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransaction {
    pub rollup_id: RollupId,
    pub raw_transaction: RawTransaction,
}

impl RpcParameter<AppState> for SendRawTransaction {
    type Response = SendRawTransactionResponse;

    fn method() -> &'static str {
        "send_raw_transaction"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        let rollup = Rollup::get(&self.rollup_id)?;

        let handler_start_ms = now_epoch_ms(); // test code

        let rollup_metadata_start_ms = now_epoch_ms(); // test code

        let mut mut_rollup_metadata = RollupMetadata::get_mut(&self.rollup_id)?;
        
        let rollup_metadata_end_ms = now_epoch_ms(); // test code

        let rollup_metadata_get_mut_start_ms = now_epoch_ms(); // test code

        let cluster_metadata = ClusterMetadata::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )
        .map_err(|error| {
            tracing::error!("Failed to get cluster metadata: {:?}", error);
            Error::ClusterMetadataNotFound
        })?;

        if cluster_metadata.is_leader {
            let cluster = Cluster::get(
                rollup.platform,
                rollup.liveness_service_provider,
                &rollup.cluster_id,
                cluster_metadata.platform_block_height,
            )
            .map_err(|error| {
                tracing::error!("Failed to get cluster: {:?}", error);
                Error::ClusterNotFound
            })?;

            let batch_number = mut_rollup_metadata.batch_number;
            let transaction_order = mut_rollup_metadata.transaction_order;
            let transaction_hash = self.raw_transaction.raw_transaction_hash();

            RawTransactionModel::put_with_transaction_hash(
                &self.rollup_id,
                &transaction_hash,
                self.raw_transaction.clone(),
                true,
            )?;

            RawTransactionModel::put(
                &self.rollup_id,
                batch_number,
                transaction_order,
                self.raw_transaction.clone(),
                true,
            )?;

            let merkle_tree = context.merkle_tree_manager().get(&self.rollup_id).await?;
            let (_, pre_merkle_path) = merkle_tree.add_data(transaction_hash.as_ref()).await;
            drop(merkle_tree);

            mut_rollup_metadata.transaction_order += 1;
            CanProvideTransactionInfo::add_can_provide_transaction_orders(
                &self.rollup_id,
                batch_number,
                vec![transaction_order],
            )?;

            let is_updated = mut_rollup_metadata.check_and_update_batch_info();

            mut_rollup_metadata.update()?;

            let rollup_metadata_get_mut_end_ms = now_epoch_ms(); // test code

            let after_rollup_metadata_update_start_ms = now_epoch_ms(); // test code

            if is_updated {
                context
                    .merkle_tree_manager()
                    .insert(&self.rollup_id, MerkleTree::new())
                    .await;

                finalize_batch(context.clone(), &self.rollup_id, batch_number);
            }

            let order_commitment = issue_order_commitment(
                context.clone(),
                rollup.platform,
                self.rollup_id.clone(),
                rollup.order_commitment_type,
                transaction_hash.clone(),
                batch_number,
                transaction_order,
                pre_merkle_path,
            )
            .await?;

            order_commitment.put(&self.rollup_id, batch_number, transaction_order)?;

            sync_raw_transaction(
                context.clone(),
                cluster,
                self.rollup_id,
                batch_number,
                transaction_order,
                self.raw_transaction.clone(),
                order_commitment.clone(),
                true,
            );

            let builder_rpc_url = context.config().builder_rpc_url.clone();
            let cloned_rpc_client = context.rpc_client();

            if builder_rpc_url.is_some() {
                match self.raw_transaction {
                    RawTransaction::Eth(eth_raw_transaction) => {
                        let params = serde_json::json!([
                            eth_raw_transaction.0,
                            batch_number,
                            transaction_order
                        ]);

                        let transaction_hash: String = cloned_rpc_client
                            .request(
                                &builder_rpc_url.unwrap(),
                                "eth_sendRawTransaction",
                                &params,
                                Id::Null,
                            )
                            .await
                            .map_err(|error| {
                                tracing::error!("Failed to send raw transaction: {:?}", error);
                                Error::RpcClient(error)
                            })?;
                    }
                    RawTransaction::EthBundle(_eth_bundle_raw_transaction) => {
                        unimplemented!("EthBundle raw transaction is not supported yet");
                    }
                }
            }

            let order_commitment = match rollup.order_commitment_type {
                OrderCommitmentType::TransactionHash => OrderCommitment::Single(
                    SingleOrderCommitment::TransactionHash(TransactionHashOrderCommitment::new(
                        transaction_hash.as_string(),
                    )),
                ),
                OrderCommitmentType::Sign => order_commitment,
            };

            let after_rollup_metadata_update_end_ms = now_epoch_ms(); // test code

            let handler_end_ms = now_epoch_ms(); // test code

            Ok(SendRawTransactionResponse {
                order_commitment,
                handler_timings: SendRawTransactionHandlerTimings {
                    start_ms: handler_start_ms,
                    end_ms: handler_end_ms,
                },
                rollup_metadata_timings: SendRawTransactionRollupMetadataTimings {
                    start_ms: rollup_metadata_start_ms,
                    end_ms: rollup_metadata_end_ms,
                },
                rollup_metadata_get_mut_timings: SendRawTransactionRollupMetadataGetMutTimings {
                    start_ms: rollup_metadata_get_mut_start_ms,
                    end_ms: rollup_metadata_get_mut_end_ms,
                },
                after_rollup_metadata_update_timings: Some(SendRawTransactionAfterRollupMetadataUpdateTimings {
                    start_ms: after_rollup_metadata_update_start_ms,
                    end_ms: after_rollup_metadata_update_end_ms,
                }),
                redirect_to_leader_timings: None,
                leader_rollup_metadata_timings: None,
                leader_rollup_metadata_get_mut_timings: None,
                leader_after_rollup_metadata_update_timings: None,
            })
        } else {
            drop(mut_rollup_metadata);

            let rollup_metadata_get_mut_end_ms = now_epoch_ms(); // test code

            let redirect_to_leader_start_ms = now_epoch_ms(); // test code

            match cluster_metadata.leader_tx_orderer_rpc_info {
                Some(leader_tx_orderer_rpc_info) => {
                    let leader_external_rpc_url = leader_tx_orderer_rpc_info
                        .external_rpc_url
                        .clone()
                        .ok_or(Error::EmptyLeaderClusterRpcUrl)?;

                    match context
                        .rpc_client()
                        .request::<&SendRawTransaction, SendRawTransactionResponse>(
                            leader_external_rpc_url,
                            SendRawTransaction::method(),
                            &self,
                            Id::Null,
                        )
                        .await
                    {
                        Ok(response) => {
                            let redirect_to_leader_end_ms = now_epoch_ms(); // test code

                            let leader_rollup_metadata_timings = response.rollup_metadata_timings;
                            let leader_rollup_metadata_get_mut_timings = response.rollup_metadata_get_mut_timings;
                            let leader_after_rollup_metadata_update_timings = response.after_rollup_metadata_update_timings;

                            let handler_end_ms = now_epoch_ms(); // test code

                            Ok(SendRawTransactionResponse {
                                order_commitment: response.order_commitment,
                                handler_timings: SendRawTransactionHandlerTimings {
                                    start_ms: handler_start_ms,
                                    end_ms: handler_end_ms,
                                },
                                rollup_metadata_timings: SendRawTransactionRollupMetadataTimings {
                                    start_ms: rollup_metadata_start_ms,
                                    end_ms: rollup_metadata_end_ms,
                                },
                                rollup_metadata_get_mut_timings: SendRawTransactionRollupMetadataGetMutTimings {
                                    start_ms: rollup_metadata_get_mut_start_ms,
                                    end_ms: rollup_metadata_get_mut_end_ms,
                                },
                                after_rollup_metadata_update_timings: None,
                                redirect_to_leader_timings: Some(SendRawTransactionRedirectToLeaderTimings {
                                    start_ms: redirect_to_leader_start_ms,
                                    end_ms: redirect_to_leader_end_ms,
                                }),
                                leader_rollup_metadata_timings: Some(leader_rollup_metadata_timings),
                                leader_rollup_metadata_get_mut_timings: Some(leader_rollup_metadata_get_mut_timings),
                                leader_after_rollup_metadata_update_timings: leader_after_rollup_metadata_update_timings,
                            })
                        }
                        Err(error) => {
                            tracing::error!(
                                "Send raw transaction - leader external rpc error: {:?}",
                                error
                            );
                            Err(error.into())
                        }
                    }
                }
                None => {
                    tracing::error!("Send raw transaction - leader tx orderer rpc info is None");
                    return Err(Error::EmptyLeader)?;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn sync_raw_transaction(
    context: AppState,
    cluster: Cluster,
    rollup_id: RollupId,
    batch_number: u64,
    transaction_order: u64,
    raw_transaction: RawTransaction,
    order_commitment: OrderCommitment,
    is_direct_sent: bool,
) {
    tokio::spawn(async move {
        let other_cluster_rpc_url_list = cluster.get_other_cluster_rpc_url_list();
        if other_cluster_rpc_url_list.is_empty() {
            return;
        }

        let sync_raw_transaction = SyncRawTransaction {
            rollup_id,
            batch_number,
            transaction_order,
            raw_transaction,
            order_commitment: order_commitment,
            is_direct_sent,
        };

        context
            .rpc_client()
            .fire_and_forget_multicast(
                other_cluster_rpc_url_list,
                SyncRawTransaction::method(),
                &sync_raw_transaction,
                Id::Null,
            )
            .await
    });
}

#[allow(clippy::too_many_arguments)]
pub fn sync_batch_creation(
    context: AppState,
    cluster: Cluster,
    platform: Platform,
    rollup_id: RollupId,
    batch_number: u64,
    batch_commitment: [u8; 32],
    batch_creator_signature: Signature,
) {
    tokio::spawn(async move {
        /*
        tracing::info!(
            "Sync batch creation - rollup_id: {:?} / batch_number: {:?}",
            rollup_id,
            batch_number
        );
        */

        let other_cluster_rpc_url_list = cluster.get_other_cluster_rpc_url_list();
        if other_cluster_rpc_url_list.is_empty() {
            return;
        }

        let batch_creation_massage = BatchCreationMessage {
            rollup_id: rollup_id.clone(),
            batch_number,
            batch_commitment,
            batch_creator_signature,
        };
        let leader_tx_orderer_signature = match context
            .get_signer(platform)
            .await
            .map_err(|e| tracing::error!("Failed to get signer: {}", e))
            .and_then(|signer| {
                signer
                    .sign_message(&batch_creation_massage)
                    .map_err(|e| tracing::error!("Failed to sign message: {}", e))
            }) {
            Ok(signature) => signature,
            Err(_) => return,
        };

        let sync_batch_creation = SyncBatchCreation {
            batch_creation_massage,
            leader_tx_orderer_signature,
        };

        context
            .rpc_client()
            .fire_and_forget_multicast(
                other_cluster_rpc_url_list.clone(),
                SyncBatchCreation::method(),
                &sync_batch_creation,
                Id::Null,
            )
            .await
    });
}
