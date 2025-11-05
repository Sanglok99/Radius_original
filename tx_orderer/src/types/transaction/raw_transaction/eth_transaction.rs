use ethers_core::types as eth_types;

use crate::{error::Error, types::prelude::*};

#[derive(Clone, Debug, Deserialize, Serialize)]
// === new code start ===
pub struct EthRawTransaction {
    pub raw_transaction: String,
    #[serde(default)]
    pub epoch: Option<u64>, // None is required by the clients
    #[serde(default)]
    pub current_leader_tx_orderer_address: Option<String>, // None is required by the clients
}
// === new code end ===

// pub struct EthRawTransaction(pub String); // old code

// === new code start ===
impl Default for EthRawTransaction {
    fn default() -> Self {
        Self {
            raw_transaction: "".to_string(),
            epoch: None,
            current_leader_tx_orderer_address: None,
        }
    }
}
// === new code end ===

/*
// old code
impl Default for EthRawTransaction {
    fn default() -> Self {
        Self("".to_string())
    }
}
*/

// === new code start ===
impl From<String> for EthRawTransaction {
    fn from(value: String) -> Self {
        Self {
            raw_transaction: value,
            epoch: None,
            current_leader_tx_orderer_address: None,
        }
    }
}
// === new code end ===

/*
// old code
impl From<String> for EthRawTransaction {
    fn from(value: String) -> Self {
        Self(value)
    }
}
*/

impl EthRawTransaction {
    pub fn raw_transaction_hash(&self) -> RawTransactionHash {
        let decoded_transaction = decode_rlp_transaction(&self.raw_transaction).unwrap(); // new code
        // let decoded_transaction = decode_rlp_transaction(&self.0).unwrap(); // old code

        let transaction_hash = const_hex::encode_prefixed(decoded_transaction.hash);

        RawTransactionHash::from(transaction_hash)
    }

    pub fn rollup_transaction(&self) -> Result<eth_types::Transaction, Error> {
        decode_rlp_transaction(&self.raw_transaction).map_err(|_| Error::InvalidTransaction) // new code
        // decode_rlp_transaction(&self.0).map_err(|_| Error::InvalidTransaction) // old code
    }

    // === new code start ===
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = Some(epoch);
    }
    // === new code end ===
}
