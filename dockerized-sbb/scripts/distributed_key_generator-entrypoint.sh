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

#######################################
# 3. Run Mode Handling
#######################################
if [ "$DISTRIBUTED_KEY_GENERATOR_MODE" = "init" ]; then
    #######################################
    # 1. Prepare Execution env
    #######################################
    DISTRIBUTED_KEY_GENERATOR_EXECUTE_ENV_PATH="./scripts/execute/env.sh"
    ensure_env_file "./scripts/execute/env_example.sh" "$DISTRIBUTED_KEY_GENERATOR_EXECUTE_ENV_PATH"
    replace_env_var "$DISTRIBUTED_KEY_GENERATOR_EXECUTE_ENV_PATH" "KEY_GENERATOR_INTERNAL_RPC_URL" "$DISTRIBUTED_KEY_GENERATOR_INTERNAL_RPC_URL"
    replace_env_var "$DISTRIBUTED_KEY_GENERATOR_EXECUTE_ENV_PATH" "KEY_GENERATOR_CLUSTER_RPC_URL" "$DISTRIBUTED_KEY_GENERATOR_CLUSTER_RPC_URL"
    replace_env_var "$DISTRIBUTED_KEY_GENERATOR_EXECUTE_ENV_PATH" "KEY_GENERATOR_EXTERNAL_RPC_URL" "$DISTRIBUTED_KEY_GENERATOR_EXTERNAL_RPC_URL"
    replace_env_var "$DISTRIBUTED_KEY_GENERATOR_EXECUTE_ENV_PATH" "KEY_GENERATOR_PRIVATE_KEY" "$DISTRIBUTED_KEY_GENERATOR_PRIVATE_KEY"

    DISTRIBUTED_KEY_GENERATOR_RPC_CALL_ENV_PATH="./scripts/rpc-call/env.sh" 
    ensure_env_file "./scripts/rpc-call/env_example.sh" "$DISTRIBUTED_KEY_GENERATOR_RPC_CALL_ENV_PATH"
    replace_env_var "$DISTRIBUTED_KEY_GENERATOR_RPC_CALL_ENV_PATH" "KEY_GENERATOR_INTERNAL_RPC_URL" "$DISTRIBUTED_KEY_GENERATOR_INTERNAL_RPC_URL"
    replace_env_var "$DISTRIBUTED_KEY_GENERATOR_RPC_CALL_ENV_PATH" "KEY_GENERATOR_CLUSTER_RPC_URL" "$DISTRIBUTED_KEY_GENERATOR_CLUSTER_RPC_URL"
    replace_env_var "$DISTRIBUTED_KEY_GENERATOR_RPC_CALL_ENV_PATH" "KEY_GENERATOR_EXTERNAL_RPC_URL" "$DISTRIBUTED_KEY_GENERATOR_EXTERNAL_RPC_URL"
    replace_env_var "$DISTRIBUTED_KEY_GENERATOR_RPC_CALL_ENV_PATH" "KEY_GENERATOR_ADDRESS" "$DISTRIBUTED_KEY_GENERATOR_ADDRESS"

    echo "All environment files are prepared."

    echo "🚀 Running Key Generator Initialization..."
    ./scripts/execute/01_init_key_generator.sh

    echo "✅ Environment variables applied successfully."
    ./scripts/execute/02_run_key_generator.sh &

    sleep 5
    
    ./scripts/rpc-call/10_initialize.sh > /dev/null 2>&1

    tail -f /dev/null

elif [ "$DISTRIBUTED_KEY_GENERATOR_MODE" = "run" ]; then
    echo "🚀 Running Key Generator..."
    ./scripts/execute/02_run_key_generator.sh &

    tail -f /dev/null

else
    echo "❌ Invalid MODE: ${DISTRIBUTED_KEY_GENERATOR_MODE}"
    exit 1
fi