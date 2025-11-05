#!/bin/bash

set -e

echo "🚀 Starting TX Orderer..."

#######################################
# Utility Functions
#######################################
replace_env_var() {
  local file=$1
  local key=$2
  local value=$3
  sed -i "s|^${key}=.*|${key}=${value}|" "$file"
}

ensure_env_file() {
  local example_path=$1
  local target_path=$2

  if [[ ! -f "$target_path" ]]; then
    echo "📄 Creating $target_path from example..."
    cp "$example_path" "$target_path"
  fi
}

if [ "$TX_ORDERER_MODE" = "init" ]; then
    #######################################
    # 1. Prepare Execution env
    #######################################
    TX_ORDERER_EXECUTE_ENV_PATH="./scripts/execute/env.sh"
    ensure_env_file "./scripts/execute/env_example.sh" "$TX_ORDERER_EXECUTE_ENV_PATH" 
    replace_env_var "$TX_ORDERER_EXECUTE_ENV_PATH" "TX_ORDERER_PRIVATE_KEY" "$TX_ORDERER_PRIVATE_KEY"
    replace_env_var "$TX_ORDERER_EXECUTE_ENV_PATH" "TX_ORDERER_INTERNAL_RPC_URL" "$TX_ORDERER_INTERNAL_RPC_URL"
    replace_env_var "$TX_ORDERER_EXECUTE_ENV_PATH" "TX_ORDERER_CLUSTER_RPC_URL" "$TX_ORDERER_CLUSTER_RPC_URL"
    replace_env_var "$TX_ORDERER_EXECUTE_ENV_PATH" "TX_ORDERER_EXTERNAL_RPC_URL" "$TX_ORDERER_EXTERNAL_RPC_URL"
    replace_env_var "$TX_ORDERER_EXECUTE_ENV_PATH" "DISTRIBUTED_KEY_GENERATOR_EXTERNAL_RPC_URL" "$DISTRIBUTED_KEY_GENERATOR_EXTERNAL_RPC_URL"
    replace_env_var "$TX_ORDERER_EXECUTE_ENV_PATH" "SEEDER_EXTERNAL_RPC_URL" "$SEEDER_EXTERNAL_RPC_URL"
    replace_env_var "$TX_ORDERER_EXECUTE_ENV_PATH" "REWARD_MANAGER_EXTERNAL_RPC_URL" "$REWARD_MANAGER_EXTERNAL_RPC_URL"

    if [ -n "$BUILDER_EXTERNAL_RPC_URL" ]; then
        replace_env_var "$TX_ORDERER_EXECUTE_ENV_PATH" "# BUILDER_EXTERNAL_RPC_URL" "$BUILDER_EXTERNAL_RPC_URL"
    fi

    #######################################
    # 2. Prepare RPC call env
    #######################################
    TX_ORDERER_RPC_CALL_ENV_PATH="./scripts/rpc-call/env.sh"
    ensure_env_file "./scripts/rpc-call/env_example.sh" "$TX_ORDERER_RPC_CALL_ENV_PATH" 
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "TX_ORDERER_INTERNAL_RPC_URL" "$TX_ORDERER_INTERNAL_RPC_URL"

    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "LIVENESS_PLATFORM" "$LIVENESS_PLATFORM"
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "LIVENESS_SERVICE_PROVIDER" "$LIVENESS_SERVICE_PROVIDER"
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "LIVENESS_RPC_URL" "$LIVENESS_RPC_URL"
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "LIVENESS_WS_URL" "$LIVENESS_WS_URL"
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS" \""$LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS"\"
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "CLUSTER_ID" "$CLUSTER_ID"

    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "VALIDATION_PLATFORM" "$VALIDATION_PLATFORM"
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "VALIDATION_SERVICE_PROVIDER" "$VALIDATION_SERVICE_PROVIDER"
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "VALIDATION_RPC_URL" "$VALIDATION_RPC_URL"
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "VALIDATION_WS_URL" "$VALIDATION_WS_URL"
    replace_env_var "$TX_ORDERER_RPC_CALL_ENV_PATH" "VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS" "$VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS"

    echo "All environment files are prepared."

    echo "🚀 Running tx orderer Initialization..."
    ./scripts/execute/01_init_tx_orderer.sh

    echo "✅ Environment variables applied successfully."
    ./scripts/execute/02_run_tx_orderer.sh &
    
    sleep 5
    ./scripts/rpc-call/11_add_sequencing_info.sh
    
    sleep 5
    ./scripts/rpc-call/12_add_symbiotic_validation_info.sh
    
    sleep 5
    ./scripts/rpc-call/13_add_cluster.sh

    tail -f /dev/null

elif [ "$TX_ORDERER_MODE" = "run" ]; then
    echo "🚀 Running tx orderer..."
    ./scripts/execute/02_run_tx_orderer.sh &

    tail -f /dev/null
else
    echo "❌ Invalid TX_ORDERER_MODE: ${TX_ORDERER_MODE}"
    exit 1
fi