# tx_orderer/scripts/execute/env.sh

#!/bin/bash
SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
PROJECT_ROOT_PATH="$( cd $SCRIPT_PATH/../.. >/dev/null 2>&1 ; pwd -P )"

BIN_FILE_NAME="tx_orderer"
BIN_PATH="$PROJECT_ROOT_PATH/scripts/${BIN_FILE_NAME}_3"

if [ -z "$1" ]; then
  echo "Error: No argument supplied. Usage: $0 <data_suffix>"
  exit 1
fi

DATA_PATH=$PROJECT_ROOT_PATH/data_$1
CONFIG_FILE_PATH=$DATA_PATH/Config.toml
PRIVATE_KEY_PATH=$DATA_PATH/signing_key

# for execute_2 only
cp $PROJECT_ROOT_PATH/target/release/$BIN_FILE_NAME $PROJECT_ROOT_PATH/scripts/${BIN_FILE_NAME}_3 # _ 뒤에 tx_orderer 번호 붙이기

# Copy the new version's binary to the scripts directory
# Use a temporary name first to avoid "Text file busy" error if binary is running
if [[ -f "$PROJECT_ROOT_PATH/target/release/$BIN_FILE_NAME" ]]; then
  cp $PROJECT_ROOT_PATH/target/release/$BIN_FILE_NAME $PROJECT_ROOT_PATH/scripts
fi

# Check if the binary exists
if [[ ! -f "$BIN_PATH" ]]; then
    echo "Error: TxOrderer binary not found at $BIN_PATH"
    echo "Please run this command 'cp $PROJECT_ROOT_PATH/target/release/$BIN_FILE_NAME $PROJECT_ROOT_PATH/scripts"
    exit 1
fi

# Operating tx_orderer private key
TX_ORDERER_PRIVATE_KEY="0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba" # 5

# TxOrderer
TX_ORDERER_INTERNAL_RPC_URL="http://127.0.0.1:4002" # Internal IP - Please change this IP.
TX_ORDERER_CLUSTER_RPC_URL="http://165.194.35.15:5002"  # External IP - Please change this IP.
TX_ORDERER_EXTERNAL_RPC_URL="http://165.194.35.15:3002" # External IP - Please change this IP.

# DKG (for ENCRYPTED_TRANSACTION_TYPE=skde)
DISTRIBUTED_KEY_GENERATOR_EXTERNAL_RPC_URL="http://165.194.35.15:11002" # Please change this distribured key generator (external) rpc url.

# Seeder
SEEDER_EXTERNAL_RPC_URL="http://165.194.35.15:10002" # Please change this seeder (external) rpc url.

# Reward Manager
REWARD_MANAGER_EXTERNAL_RPC_URL="http://165.194.35.15:6100" # Please change this reward manager (external) rpc url.

# Builder
# BUILDER_EXTERNAL_RPC_URL="http://127.0.0.1:7200" # Please change this builder (external) rpc url.