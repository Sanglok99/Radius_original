#!/bin/bash

set -e

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

if [ "$SECURE_RPC_PROVIDER_MODE" = "init" ]; then
    #######################################
    # 1. Prepare Execution env
    ######################################
    SECURE_RPC_EXECUTE_ENV_PATH="./scripts/execute/env.sh"
    ensure_env_file "./scripts/execute/env_example.sh" "$SECURE_RPC_EXECUTE_ENV_PATH"
    replace_env_var "$SECURE_RPC_EXECUTE_ENV_PATH" "SECURE_RPC_EXTERNAL_RPC_URL" "$SECURE_RPC_EXTERNAL_RPC_URL"
    replace_env_var "$SECURE_RPC_EXECUTE_ENV_PATH" "SECURE_RPC_EXTERNAL_WS_URL" "$SECURE_RPC_EXTERNAL_WS_URL"

    replace_env_var "$SECURE_RPC_EXECUTE_ENV_PATH" "ROLLUP_ID" "$ROLLUP_ID"
    replace_env_var "$SECURE_RPC_EXECUTE_ENV_PATH" "ROLLUP_RPC_URL" "$ROLLUP_RPC_URL"
    replace_env_var "$SECURE_RPC_EXECUTE_ENV_PATH" "ROLLUP_WS_URL" "$ROLLUP_WS_URL"

    replace_env_var "$SECURE_RPC_EXECUTE_ENV_PATH" "TX_ORDERER_EXTERNAL_RPC_URL_LIST" "$TX_ORDERER_EXTERNAL_RPC_URL_LIST"

    replace_env_var "$SECURE_RPC_EXECUTE_ENV_PATH" "ENCRYPTED_TRANSACTION_TYPE" "$ENCRYPTED_TRANSACTION_TYPE"
    
    replace_env_var "$SECURE_RPC_EXECUTE_ENV_PATH" "DISTRIBUTED_KEY_GENERATOR_EXTERNAL_RPC_URL" "$DISTRIBUTED_KEY_GENERATOR_EXTERNAL_RPC_URL"

    echo "All environment files are prepared."

    echo "🚀 Running Secure RPC Provider Initialization..."
    ./scripts/execute/01_init_secure_rpc.sh

    echo "✅ Environment variables applied successfully."
    
    ./scripts/execute/02_run_secure_rpc.sh &

    tail -f /dev/null

elif [ "$SECURE_RPC_PROVIDER_MODE" = "run" ]; then
    echo "🚀 Running Secure RPC Provider..."
    ./scripts/execute/02_run_secure_rpc.sh &

    tail -f /dev/null

else
    echo "❌ Invalid MODE: ${SECURE_RPC_PROVIDER_MODE}"
    exit 1
fi