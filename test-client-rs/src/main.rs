use std::{env, time::Duration};

use alloy::primitives::TxKind;
use test_client_rs::{
    self,
    account::{Account, Accounts},
    config::Config,
    connection::{connection_channel, Connection, ConnectionError},
    statistics::Statistics,
    transaction::Transaction,
};
use tokio::{task::JoinHandle, time::interval};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();

    let arguments: Vec<String> = env::args().skip(1).collect();
    let config_path = arguments
        .get(0)
        .expect("Provide the configuration file path.")
        .to_owned();
    let config = Config::open(&config_path)?;

    // Panics if the connection threads exceeds the number of cores available.
    let connection_threads = config.connection_threads();
    let available_threads = std::thread::available_parallelism()
        .expect("Failed to get the available threads.")
        .get();
    if connection_threads > available_threads {
        panic!(
            "The sum of generator and connection threads cannot exceed the available threads({}).",
            available_threads
        );
    }

    // Initialize the async runtime.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(connection_threads)
        .max_blocking_threads(1)
        .build()?;

    // Initialize accounts from signing keys and set their nonces.
    let accounts = runtime.block_on(Account::from_config(&config))?;
    tracing::info!("Initialized {} accounts.", config.signing_keys().len());
    accounts.iter().for_each(|account| {
        tracing::info!(
            "Address: {:?}\t Nonce: {}",
            account.address(),
            account.nonce()
        )
    });

    // Initialize the transaction queues.
    let (sender, receiver) = connection_channel(config.total_transactions());

    // Prefill the connection queue with transactions.
    for _ in 0..config.total_transactions() {
        let transaction = runtime.block_on(raw_transaction(accounts.clone()));
        sender.blocking_send(transaction)?;
    }

    // Initialize the statistics.
    let statistics = Statistics::new(config.total_transactions());

    // Initialize connections.
    let connections = (0..config.connections())
        .map(|_| Connection::new(config.clone(), statistics.clone(), receiver.clone()))
        .collect::<Result<Vec<Connection>, ConnectionError>>()?;
    let connection_handles: Vec<JoinHandle<()>> = connections
        .into_iter()
        .map(|connection| runtime.spawn(connection.init()))
        .collect();
    tracing::info!("Initialized {} connections.", config.connections());

    // Initialize the ticker.
    runtime.block_on({
        let duration = config.duration();

        async move {
            let mut interval = interval(Duration::from_secs(1));
            interval.tick().await;

            for second in 0..duration {
                interval.tick().await;
                tracing::info!("{} seconds passed..", second + 1);
            }

            drop(sender);
        }
    });

    for connection in connection_handles.into_iter() {
        connection.abort();
    }

    statistics.print_stats();

    Ok(())
}

#[allow(unused)]
async fn raw_transaction(accounts: Accounts) -> Transaction {
    use alloy::{
        eips::eip2718::Encodable2718, network::TransactionBuilder, primitives::U256,
        rpc::types::TransactionRequest,
    };
    use rand::seq::SliceRandom;

    let from = accounts.get(0).unwrap();
    let to = accounts.choose(&mut rand::thread_rng()).unwrap();

    let transaction = TransactionRequest {
        chain_id: Some(to.config().chain_id()),
        to: Some(TxKind::Call(to.address())),
        nonce: Some(from.fetch_add_nonce()),
        gas: Some(21_000),
        gas_price: Some(1_000_000_000),
        value: Some(U256::from(1)),
        ..Default::default()
    };

    let envelope = transaction.build(from.wallet()).await.unwrap();
    let encoded_transaction = const_hex::encode_prefixed(envelope.encoded_2718());

    Transaction::raw_transaction(from.config().rollup_id().to_owned(), encoded_transaction)
}

#[allow(unused)]
async fn encrypted_transaction(accounts: Accounts) -> Transaction {
    use alloy::{
        eips::eip2718::Encodable2718, network::TransactionBuilder, primitives::U256,
        rpc::types::TransactionRequest,
    };
    use rand::seq::SliceRandom;

    let from = accounts.get(0).unwrap();
    let to = accounts.choose(&mut rand::thread_rng()).unwrap();

    let transaction = TransactionRequest {
        chain_id: Some(to.config().chain_id()),
        to: Some(TxKind::Call(to.address())),
        nonce: Some(from.fetch_add_nonce()),
        gas: Some(21_000),
        gas_price: Some(1_000_000_000),
        value: Some(U256::from(1)),
        ..Default::default()
    };

    let envelope = transaction.build(from.wallet()).await.unwrap();
    let encoded_transaction = const_hex::encode_prefixed(envelope.encoded_2718());

    Transaction::encrypted_transaction(from.config().rollup_id().to_owned(), encoded_transaction)
}
