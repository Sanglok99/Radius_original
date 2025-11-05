use crate::{
    rpc::{
        cluster::{BatchCreationMessage, SyncBatchCreation, SyncRawTransaction},
        external::issue_order_commitment,
        prelude::*,
    },
    task::finalize_batch,
    types::*,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransaction {
    pub rollup_id: RollupId,
    pub raw_transaction: RawTransaction,
}

impl RpcParameter<AppState> for SendRawTransaction {
    type Response = OrderCommitment;

    fn method() -> &'static str {
        "send_raw_transaction"
    }
    async fn handler(mut self, context: AppState) -> Result<Self::Response, RpcError> { // new code
    // async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> { // old code

        // === test code begin ===
        println!("=== SendRawTransaction 시작 ===");
        println!("Rollup ID: {}", self.rollup_id);
        println!("Transaction Type: {:?}", self.raw_transaction);
        // === test code end ===

        let rollup = Rollup::get(&self.rollup_id)?;

        let mut mut_rollup_metadata = RollupMetadata::get_mut(&self.rollup_id)?;

        println!("Rollup Max Transaction Count Per Batch: {}", mut_rollup_metadata.max_transaction_count_per_batch); // test code

        let cluster_metadata = ClusterMetadata::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )
        .map_err(|error| {
            tracing::error!("Failed to get cluster metadata: {:?}", error);
            Error::ClusterMetadataNotFound
        })?;

        // === new code start ===
        match &mut self.raw_transaction {
            RawTransaction::Eth(eth_tx) => {
                if eth_tx.epoch.is_none() && eth_tx.current_leader_tx_orderer_address.is_none() { // if the transaction is from the client
                    // set the epoch
                    eth_tx.set_epoch(cluster_metadata.epoch.unwrap_or(0)); 
                    
                    // set the current leader tx orderer address in the raw transaction
                    eth_tx.current_leader_tx_orderer_address = Some(cluster_metadata.leader_tx_orderer_rpc_info.clone().unwrap().tx_orderer_address.as_hex_string());
                    
                    println!("Enhanced transaction - Epoch: {:?}, Leader: {:?}", eth_tx.epoch, eth_tx.current_leader_tx_orderer_address); // test code
                }
                else {
                    println!("(Transaction is not from the client) Enhanced transaction - Epoch: {:?}, Leader: {:?}", eth_tx.epoch, eth_tx.current_leader_tx_orderer_address); // test code
                }
            }
            RawTransaction::EthBundle(_) => {}
        }

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;

        println!("signer.address(): {:?}", signer.address()); // test code

        let tx_orderer_address = signer.address().clone().as_hex_string(); // address of the current tx orderer
        
        // get the current leader tx orderer address from the raw transaction
        let raw_transaction_current_leader_tx_orderer_address = match &self.raw_transaction { 
            RawTransaction::Eth(eth_tx) => eth_tx.current_leader_tx_orderer_address.clone(),
            RawTransaction::EthBundle(_) => None,
        };

        let mut is_current_leader = false;
        if raw_transaction_current_leader_tx_orderer_address.is_some() {
            if raw_transaction_current_leader_tx_orderer_address.clone().unwrap() == tx_orderer_address {
                is_current_leader = true;
            } else {
                is_current_leader = false;
            }
        }
        else {
            println!("⚠️raw_transaction_current_leader_tx_orderer_address is None"); // test code
            is_current_leader = cluster_metadata.is_leader;
        }

        println!("raw_transaction_current_leader_tx_orderer_address: {:?}", raw_transaction_current_leader_tx_orderer_address.clone().unwrap()); // test code
        println!("tx_orderer_address: {:?}", tx_orderer_address); // test code
        println!("is_current_leader: {:?}", is_current_leader); // test code

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
        // === new code end ===

        // if cluster_metadata.is_leader { // old code
        if is_current_leader { // new code
            // === test code begin ===
            println!("=== Leader Node ===");
            println!("Cluster ID: {}", rollup.cluster_id);
            println!("Platform: {:?}", rollup.platform);
            println!("Liveness Service Provider: {:?}", rollup.liveness_service_provider);
            println!("Platform Block Height: {}", cluster_metadata.platform_block_height);
            // === test code end ===

            /* // old code
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
            */

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

            if is_updated {
                context
                    .merkle_tree_manager()
                    .insert(&self.rollup_id, MerkleTree::new())
                    .await;

                finalize_batch(context.clone(), &self.rollup_id, batch_number);
            }

            println!("Order Commitment Type: {:?}", rollup.order_commitment_type);

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
                match self.raw_transaction.clone() {
                    RawTransaction::Eth(eth_raw_transaction) => {
                        let params = serde_json::json!([
                            eth_raw_transaction.raw_transaction, // new code
                            // eth_raw_transaction.0, // old code
                            batch_number,
                            transaction_order
                        ]);

                        let _transaction_hash: String = cloned_rpc_client
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

            match rollup.order_commitment_type {
                OrderCommitmentType::TransactionHash => Ok(OrderCommitment::Single(
                    SingleOrderCommitment::TransactionHash(TransactionHashOrderCommitment::new(
                        transaction_hash.as_string(),
                    )),
                )),
                OrderCommitmentType::Sign => Ok(order_commitment),
            }
        } else {
            println!("=== SendRawTransaction 리더 아님 ===");

            drop(mut_rollup_metadata);

            match cluster_metadata.leader_tx_orderer_rpc_info {
                Some(leader_tx_orderer_rpc_info) => {
                    let leader_external_rpc_url = leader_tx_orderer_rpc_info
                        .external_rpc_url
                        .clone()
                        .ok_or(Error::EmptyLeaderClusterRpcUrl)?;

                    match context
                        .rpc_client()
                        .request(
                            leader_external_rpc_url,
                            SendRawTransaction::method(),
                            &self,
                            Id::Null,
                        )
                        .await
                    {
                        Ok(response) => Ok(response),
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

        println!("=== Sync Raw Transaction Info ===");
        println!("Batch Number: {}", sync_raw_transaction.batch_number);
        println!("Transaction Order: {}", sync_raw_transaction.transaction_order);

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
        tracing::info!(
            "Sync batch creation - rollup_id: {:?} / batch_number: {:?}",
            rollup_id,
            batch_number
        );

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
