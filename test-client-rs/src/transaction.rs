use alloy::primitives::FixedBytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub enum Transaction {
    EthRaw(EthRawTransaction),
    Raw(RawTransaction),
    Encrypted(RawTransaction),
}

impl Transaction {
    pub fn eth_raw_transaction(encoded_transaction: String) -> Self {
        Self::EthRaw(EthRawTransaction::new(encoded_transaction))
    }

    pub fn raw_transaction(rollup_id: String, encoded_transaction: String) -> Self {
        Self::Raw(RawTransaction::new(rollup_id, encoded_transaction))
    }

    pub fn encrypted_transaction(rollup_id: String, encoded_transaction: String) -> Self {
        Self::Encrypted(RawTransaction::new(rollup_id, encoded_transaction))
    }
}

#[derive(Clone, Debug, Serialize)]
// === new code start ===
pub struct EthRawTransaction {
    pub raw_transaction: String,
    #[serde(default)]
    pub epoch: Option<u64>, // None is required by the clients
    #[serde(default)]
    pub current_leader_tx_orderer_address: Option<String>, // None is required by the clients
}
// === new code end ===

// pub struct EthRawTransaction(Vec<String>); // old code

// === new code start ===
impl EthRawTransaction {
    pub fn new(encoded_transaction: String) -> Self {
        Self {
            raw_transaction: encoded_transaction,
            epoch: None,
            current_leader_tx_orderer_address: None,
        }
    }
}
// === new code end ===

/*
// old code
impl EthRawTransaction {
    pub fn new(encoded_transaction: String) -> Self {
        Self(vec![encoded_transaction])
    }
}
*/

#[derive(Clone, Debug, Serialize)]
pub struct RawTransaction {
    rollup_id: String,
    raw_transaction: RawTransactionData,
}

// 제안: Postman 스키마에 맞게 data를 객체로
#[derive(Clone, Debug, Serialize)]
struct RawTransactionData {
    #[serde(rename = "type")]
    transaction_type: String,
    data: EthData,
}

#[derive(Clone, Debug, Serialize)]
struct EthData {
    raw_transaction: String,
    #[serde(default)]
    epoch: Option<u64>, // None is required by the clients
    #[serde(default)]
    current_leader_tx_orderer_address: Option<String>, // None is required by the clients
}

impl RawTransaction {
    pub fn new(rollup_id: String, encoded_transaction: String) -> Self {
        Self {
            rollup_id,
            raw_transaction: RawTransactionData {
                transaction_type: "eth".to_owned(),
                data: EthData {
                    raw_transaction: encoded_transaction,
                    epoch: None,
                    current_leader_tx_orderer_address: None,
                },
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize)]

pub enum TransactionResponse {
    TransactionHash(FixedBytes<32>),
    OrderCommitment(serde_json::Value),
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrderCommitment {
    pub data: OrderCommitmentData,
    pub signature: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OrderCommitmentData {
    pub rollup_id: String,
    pub block_height: u64,
    pub transaction_order: u64,
    pub pre_merkle_path: Vec<String>,
}
