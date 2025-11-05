use std::time::Duration;

use radius_sdk::{
    json_rpc::{
        client::{Id, RpcClient},
        server::RpcParameter,
    },
    signature::Address,
};
use skde::{
    delay_encryption::solve_time_lock_puzzle,
    key_aggregation::{aggregate_key, AggregatedKey as SkdeAggregatedKey},
};
use tokio::time::sleep;

use crate::{
    rpc::cluster::{RunGeneratePartialKey, SyncAggregatedKey},
    state::AppState,
    types::*,
};

pub fn run_single_key_generator(context: AppState) {
    tokio::spawn(async move {
        let partial_key_generation_cycle = context.config().partial_key_generation_cycle();
        let partial_key_aggregation_cycle = context.config().partial_key_aggregation_cycle();

        loop {
            sleep(Duration::from_secs(partial_key_generation_cycle)).await;
            let context = context.clone();

            let mut key_id = KeyId::get_mut().unwrap();
            let current_key_id = key_id.clone();
            key_id.increase_key_id();
            key_id.update().unwrap();

            run_generate_partial_key(current_key_id);

            tokio::spawn(async move {
                sleep(Duration::from_secs(partial_key_aggregation_cycle)).await;
                let skde_params = context.skde_params().clone();

                let partial_key_address_list =
                    PartialKeyAddressList::get_or(current_key_id, PartialKeyAddressList::default)
                        .unwrap();

                let participant_addresses = partial_key_address_list.to_vec();
                let partial_key_list = partial_key_address_list
                    .get_partial_key_list(current_key_id)
                    .unwrap();

                let skde_aggregated_key = aggregate_key(&skde_params, &partial_key_list);

                let aggregated_key = AggregatedKey::new(skde_aggregated_key.clone());
                aggregated_key.put(current_key_id).unwrap();

                tracing::info!(
                    "Completed to generate encryption key - key id: {:?} / encryption key: {:?}",
                    current_key_id,
                    skde_aggregated_key.u
                );

                sync_aggregated_key(
                    current_key_id,
                    skde_aggregated_key.clone(),
                    participant_addresses,
                    context.config().signer().address(),
                );

                let secure_key =
                    solve_time_lock_puzzle(&skde_params, &skde_aggregated_key).unwrap();
                let decryption_key = DecryptionKey::new(secure_key.sk.clone());
                decryption_key.put(current_key_id).unwrap();

                tracing::info!(
                    "Complete to get decryption key - key_id: {:?} / decryption key: {:?}",
                    current_key_id,
                    decryption_key
                );
            });
        }
    });
}

pub fn run_generate_partial_key(key_id: KeyId) {
    let all_key_generator_rpc_url_list = KeyGeneratorList::get()
        .unwrap()
        .get_all_key_generator_rpc_url_list();

    tokio::spawn(async move {
        let parameter = RunGeneratePartialKey { key_id };

        let rpc_client = RpcClient::new().unwrap();
        rpc_client
            .multicast(
                all_key_generator_rpc_url_list,
                RunGeneratePartialKey::method(),
                &parameter,
                Id::Null,
            )
            .await
            .unwrap();
    });
}

pub fn sync_aggregated_key(
    key_id: KeyId,
    aggregated_key: SkdeAggregatedKey,
    participant_addresses: Vec<Address>,
    my_address: &Address,
) {
    let other_key_generator_rpc_url_list = KeyGeneratorList::get()
        .unwrap()
        .get_other_key_generator_rpc_url_list(my_address);

    tokio::spawn(async move {
        let parameter = SyncAggregatedKey {
            key_id,
            aggregated_key,
            participant_addresses,
        };

        let rpc_client = RpcClient::new().unwrap();
        rpc_client
            .multicast(
                other_key_generator_rpc_url_list,
                SyncAggregatedKey::method(),
                &parameter,
                Id::Null,
            )
            .await
            .unwrap();
    });
}
