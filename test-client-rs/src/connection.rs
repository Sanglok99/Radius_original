use std::sync::Arc;

use alloy::primitives::FixedBytes;
use radius_sdk::json_rpc::client::{Id, RpcClient};
use tokio::{
    sync::{mpsc, Mutex},
    time::Instant,
};

use crate::{
    config::Config,
    statistics::Statistics,
    transaction::{EthRawTransaction, RawTransaction, Transaction, TransactionResponse},
};

pub type Sender = mpsc::Sender<Transaction>;
pub type Receiver = Arc<Mutex<mpsc::Receiver<Transaction>>>;

pub fn connection_channel(size: usize) -> (Sender, Receiver) {
    let (sender, receiver) = mpsc::channel(size);
    let receiver = Arc::new(Mutex::new(receiver));

    (sender, receiver)
}

pub struct Connection {
    config: Config,
    statistics: Statistics,
    rpc_client: RpcClient,
    receiver: Receiver,
}

impl Connection {
    pub fn new(
        config: Config,
        statistics: Statistics,
        receiver: Receiver,
    ) -> Result<Self, ConnectionError> {
        let rpc_client = RpcClient::builder()
            .request_timeout(config.request_timeout() * 1000)
            .build()
            .map_err(ConnectionError::InitRpcClient)?;

        Ok(Self {
            config,
            statistics,
            rpc_client,
            receiver,
        })
    }

    pub async fn init(self) {
        loop {
            let mut receiver = self.receiver.lock().await;
            if let Some(transaction) = receiver.recv().await {
                drop(receiver);

                let time_start = Instant::now();
                self.statistics.sent().await;
                match self.send_transaction(transaction).await {
                    Ok(response) => {
                        let response_time = time_start.elapsed().as_millis();
                        self.statistics.succeed(response, response_time).await;
                    }
                    Err(error) => {
                        tracing::error!("{}", error);
                        let response_time = time_start.elapsed().as_millis();
                        self.statistics.failed(error, response_time).await;
                    }
                }
            } else {
                break;
            }
        }
    }

    pub async fn send_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<TransactionResponse, ConnectionError> {
        match transaction {
            Transaction::EthRaw(transaction) => self.send_eth_raw_transaction(transaction).await,
            Transaction::Raw(transaction) => self.send_raw_transaction(transaction).await,
            Transaction::Encrypted(transaction) => {
                self.send_encrypted_transaction(transaction).await
            }
        }
    }

    pub async fn send_eth_raw_transaction(
        &self,
        transaction: EthRawTransaction,
    ) -> Result<TransactionResponse, ConnectionError> {
        match self
            .rpc_client
            .request::<EthRawTransaction, FixedBytes<32>>(
                self.config.rpc_url(),
                "eth_sendRawTransaction",
                transaction,
                Id::Null,
            )
            .await
        {
            Ok(response) => Ok(TransactionResponse::TransactionHash(response)),
            Err(error) => {
                tracing::error!("{}", error);

                Err(ConnectionError::Request(
                    Method::SendEthRawTransaction,
                    error,
                ))
            }
        }
    }

    pub async fn send_raw_transaction(
        &self,
        transaction: RawTransaction,
    ) -> Result<TransactionResponse, ConnectionError> {
        match self
            .rpc_client
            .request::<RawTransaction, serde_json::Value>(
                self.config.rpc_url(),
                "send_raw_transaction",
                transaction,
                Id::Null,
            )
            .await
        {
            Ok(response) => Ok(TransactionResponse::OrderCommitment(response)),
            Err(error) => Err(ConnectionError::Request(Method::SendRawTransaction, error)),
        }
    }

    pub async fn send_encrypted_transaction(
        &self,
        transaction: RawTransaction,
    ) -> Result<TransactionResponse, ConnectionError> {
        match self
            .rpc_client
            .request::<RawTransaction, serde_json::Value>(
                self.config.rpc_url(),
                "send_encrypted_transaction",
                transaction,
                Id::Null,
            )
            .await
        {
            Ok(response) => Ok(TransactionResponse::OrderCommitment(response)),
            Err(error) => Err(ConnectionError::Request(
                Method::SendEncryptedTransaction,
                error,
            )),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    InitRpcClient(radius_sdk::json_rpc::client::RpcClientError),
    Request(Method, radius_sdk::json_rpc::client::RpcClientError),
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ConnectionError {}

#[derive(Debug)]
pub enum Method {
    SendEthRawTransaction,
    SendRawTransaction,
    SendEncryptedTransaction,
}
