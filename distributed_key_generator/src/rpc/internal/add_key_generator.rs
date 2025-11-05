use radius_sdk::signature::Address;

use crate::rpc::{cluster::SyncKeyGenerator, prelude::*};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AddKeyGenerator {
    // signature: Signature, // TODO: Uncomment this code
    message: AddKeyGeneratorMessage,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AddKeyGeneratorMessage {
    address: Address,
    cluster_rpc_url: String,
    external_rpc_url: String,
}

impl RpcParameter<AppState> for AddKeyGenerator {
    type Response = ();

    fn method() -> &'static str {
        "add_key_generator"
    }

    async fn handler(self, _context: AppState) -> Result<Self::Response, RpcError> {
        tracing::info!(
            "Add distributed key generation - address: {:?} / cluster_rpc_url: {:?} / external_rpc_url: {:?}",
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

        let key_generator_address_list = KeyGeneratorList::get()?;
        if key_generator_address_list.contains(&key_generator) {
            return Ok(());
        }

        KeyGeneratorList::apply(|key_generator_list| {
            key_generator_list.insert(key_generator);
        })?;

        sync_key_generator(self);

        Ok(())
    }
}

pub fn sync_key_generator(add_key_generator: AddKeyGenerator) {
    let other_key_generator_rpc_url_list = KeyGeneratorList::get()
        .unwrap()
        .get_all_key_generator_rpc_url_list();

    tokio::spawn(async move {
        tracing::info!(
            "Sync distributed key generation - address: {:?} / cluster_rpc_url: {:?} / rpc_client_count: {:?}",
            add_key_generator.message.address.as_hex_string(),
            add_key_generator.message.cluster_rpc_url,
            other_key_generator_rpc_url_list.len()
        );

        let rpc_client = RpcClient::new().unwrap();
        rpc_client
            .multicast(
                other_key_generator_rpc_url_list,
                SyncKeyGenerator::method(),
                &add_key_generator,
                Id::Null,
            )
            .await
            .unwrap();
    });
}
