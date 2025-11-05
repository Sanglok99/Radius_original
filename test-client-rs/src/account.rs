use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use alloy::{
    network::{Ethereum, EthereumWallet},
    primitives::Address,
    providers::{
        fillers::{FillProvider, JoinFill, WalletFiller},
        Identity, ProviderBuilder, RootProvider, WalletProvider,
    },
    signers::local::PrivateKeySigner,
    transports::http::{Client, Http},
};
use radius_sdk::json_rpc::client::{BatchRequest, RpcClient};

use crate::config::Config;

pub type Accounts = Arc<Vec<Account>>;

pub struct Account {
    inner: Arc<AccountInner>,
}

struct AccountInner {
    config: Config,
    provider: FillProvider<
        JoinFill<Identity, WalletFiller<EthereumWallet>>,
        RootProvider<Http<Client>>,
        Http<Client>,
        Ethereum,
    >,
    nonce: AtomicU64,
}

impl Clone for Account {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Account {
    pub async fn from_config(config: &Config) -> Result<Accounts, AccountError> {
        if config.signing_keys().is_empty() {
            return Err(AccountError::EmptySigningKey);
        }

        let accounts: Vec<Self> = config
            .signing_keys()
            .iter()
            .map(|signing_key| Self::new(config.clone(), signing_key))
            .collect::<Result<Vec<Self>, AccountError>>()?;

        let rpc_client = RpcClient::builder()
            .request_timeout(config.request_timeout() * 1000)
            .build()
            .map_err(AccountError::InitRpcClient)?;

        let mut batch_request = BatchRequest::new();
        accounts
            .iter()
            .enumerate()
            .try_for_each(|(index, account)| {
                let parameter: Vec<String> =
                    vec![account.address().to_string(), "latest".to_owned()];
                batch_request
                    .push("eth_getTransactionCount", &parameter, index as i64)
                    .map_err(AccountError::BuildBatchRequest)
            })?;

        let batch_response = rpc_client
            .batch_request(config.ethereum_rpc_url(), &batch_request)
            .await
            .map_err(AccountError::BatchRequest)?;

        for (response, account) in batch_response.into_iter().zip(accounts.iter()) {
            let nonce_string: String = response.parse().map_err(AccountError::BatchResponse)?;

            let nonce =
                u64::from_str_radix(&nonce_string[2..], 16).map_err(AccountError::ParseNonce)?;
            account.set_nonce(nonce);
        }

        Ok(Arc::new(accounts))
    }

    pub fn new(config: Config, signing_key: impl AsRef<str>) -> Result<Self, AccountError> {
        let signer =
            PrivateKeySigner::from_str(signing_key.as_ref()).map_err(AccountError::Signer)?;
        let wallet = EthereumWallet::new(signer);
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .on_http(config.rpc_url().parse().map_err(AccountError::Provider)?);

        let nonce = AtomicU64::new(0);

        Ok(Self {
            inner: AccountInner {
                config,
                provider,
                nonce,
            }
            .into(),
        })
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn address(&self) -> Address {
        self.inner.provider.wallet().default_signer().address()
    }

    fn set_nonce(&self, nonce: u64) {
        self.inner.nonce.store(nonce, Ordering::SeqCst);
    }

    pub fn nonce(&self) -> u64 {
        self.inner.nonce.load(Ordering::SeqCst)
    }

    pub fn fetch_add_nonce(&self) -> u64 {
        self.inner.nonce.fetch_add(1, Ordering::SeqCst)
    }

    pub fn wallet(&self) -> &EthereumWallet {
        self.inner.provider.wallet()
    }
}

#[derive(Debug)]
pub enum AccountError {
    EmptySigningKey,
    ParseNonce(std::num::ParseIntError),
    Signer(alloy::signers::local::LocalSignerError),
    Provider(url::ParseError),
    InitRpcClient(radius_sdk::json_rpc::client::RpcClientError),
    BuildBatchRequest(radius_sdk::json_rpc::client::RpcClientError),
    BatchRequest(radius_sdk::json_rpc::client::RpcClientError),
    BatchResponse(radius_sdk::json_rpc::client::RpcClientError),
}

impl std::fmt::Display for AccountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for AccountError {}
