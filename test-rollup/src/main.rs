use std::{
    fs::OpenOptions,
    io::{self, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use reqwest::Client;
use serde_json::json;
use tokio::time::sleep;
use tracing::{error, info};

struct TeeWriter {
    file: Arc<Mutex<std::fs::File>>,
}

impl Write for TeeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut stdout = io::stdout();
        stdout.write_all(buf)?;

        let mut file = self.file.lock().unwrap();
        file.write_all(buf)?;

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()?;

        let mut file = self.file.lock().unwrap();
        file.flush()
    }
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("test-rollup.log")
        .expect("failed to open log file");

    let file = Arc::new(Mutex::new(file));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_writer({
            let file = Arc::clone(&file);
            move || TeeWriter {
                file: Arc::clone(&file),
            }
        })
        .init();
}

#[tokio::main]
async fn main() {
    init_logging();

    let client = Client::new();

    // let platform_url = "http://14.32.133.68:8545"; // old code
    let platform_url = "http://127.0.0.1:8545"; // new code

    let executor_address = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"; // old code
    // let executor_address = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"; // new code

    // let rollup_id = "rollup_id_2"; // old code
    let rollup_id = "radius_rollup"; // new code

    // === cross-server test code start ===
    let rpc_urls = [
        "http://165.194.35.15:11103", // sys5(TX_ORDERER)
        "http://165.194.35.11:11103", // sys2(TX_ORDERER_2)
        "http://165.194.35.11:11106", // sys2(TX_ORDERER_3)
        "http://165.194.35.14:11103", // sys4(TX_ORDERER_4)
        "http://165.194.35.14:11106", // sys4(TX_ORDERER_5)
    ];
    // === cross-server test code end ===

    // === cross-server test code start ===
    let tx_orderer_addresses = [
        "0xa0Ee7A142d267C1f36714E4a8F75612F20a79720", // sys5(TX_ORDERER)
        "0xcd3B766CCDd6AE721141F452C550Ca635964ce71", // sys2(TX_ORDERER_2)
        "0x2546BcD3c84621e976D8185a91A922aE77ECEc30", // sys2(TX_ORDERER_3)
        "0xbDA5747bFD65F08deb54cb465eB87D40e51B197E", // sys4(TX_ORDERER_4)
        "0xdD2FD4581271e230360230F9337D5c0430Bf44C0", // sys4(TX_ORDERER_5)
    ];
    // === cross-server test code end ===

    let l1_block_generation_interval = 12;
    let block_generation_interval = 3;

    let mut rollup_block_height = 1;
    let mut block_generation_count = 0;
    let mut cumulative_tx_count: usize = 0;

    let get_platform_block_height = json!({
        "jsonrpc":"2.0",
        "method":"eth_blockNumber",
        "params": [],
        "id":1
    });

    let response = client
        .post(platform_url)
        .json(&get_platform_block_height)
        .send()
        .await
        .unwrap();

    let response = response.json::<serde_json::Value>().await.unwrap();

    if let Some(hex_str) = response["result"].as_str() {
        match u64::from_str_radix(hex_str.trim_start_matches("0x"), 16) {
            Ok(mut platform_block_height) => loop {
                let current_leader_tx_orderer_index =
                    rollup_block_height % tx_orderer_addresses.len();
                let next_leader_tx_orderer_index =
                    (current_leader_tx_orderer_index + 1) % tx_orderer_addresses.len();

                info!(
                    "Current leader tx orderer address: {}\nnext leader tx orderer address: {}",
                    tx_orderer_addresses[current_leader_tx_orderer_index],
                    tx_orderer_addresses[next_leader_tx_orderer_index]
                );

                let request_body = json!({
                    "jsonrpc": "2.0",
                    "method": "get_raw_transaction_list",
                    "params": {
                        "leader_change_message": {
                            "rollup_id": rollup_id,
                            "executor_address": executor_address,
                            "platform_block_height": platform_block_height - 3,
                            "current_leader_tx_orderer_address": tx_orderer_addresses[current_leader_tx_orderer_index],
                            "next_leader_tx_orderer_address": tx_orderer_addresses[next_leader_tx_orderer_index],
                        },
                        "rollup_signature": "0xc6bA578acFF1eA914A6a727b2F20776eB4ad61EE333333333333333333333333c6bA578acFF1eA914A6a727b2F20776eB4ad61EE33333333333333333333333333"
                    },
                    "id": 1
                });

                match client
                    .post(rpc_urls[current_leader_tx_orderer_index])
                    .json(&request_body)
                    .send()
                    .await
                {
                    Ok(response) => {
                        let response = response.json::<serde_json::Value>().await.unwrap();

                        let tx_list_len = response["result"]["raw_transaction_list"]
                            .as_array()
                            .map(|arr| arr.len())
                            .unwrap_or(0);

                        cumulative_tx_count += tx_list_len;

                        info!(
                            "raw_transaction_list 길이: {}, 누적 합: {}",
                            tx_list_len, cumulative_tx_count
                        );

                        if let Ok(pretty) = serde_json::to_string_pretty(&response) {
                            info!("Response\n{}", pretty);
                        } else {
                            info!(?response, "Response");
                        }

                        rollup_block_height += 1;
                    }
                    Err(e) => error!(%e, "Request failed"),
                }

                if block_generation_count
                    == l1_block_generation_interval / block_generation_interval
                {
                    block_generation_count = 0;
                    platform_block_height += 1;
                }

                block_generation_count += 1;
                sleep(Duration::from_secs(block_generation_interval)).await;
            },
            Err(e) => error!(%e, "Failed to convert hex to u64"),
        }
    }
}