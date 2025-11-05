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

######################################
# 3. Run Mode Handling
######################################
if [ "$SEEDER_MODE" = "init" ]; then
    #######################################
    # 1. Prepare Execution env
    #######################################
    SEEDER_EXECUTE_ENV_PATH="./scripts/execute/env.sh"
    ensure_env_file "./scripts/execute/env_example.sh" "$SEEDER_EXECUTE_ENV_PATH" 
    replace_env_var "$SEEDER_EXECUTE_ENV_PATH" "SEEDER_INTERNAL_RPC_URL" "$SEEDER_INTERNAL_RPC_URL"
    replace_env_var "$SEEDER_EXECUTE_ENV_PATH" "SEEDER_EXTERNAL_RPC_URL" "$SEEDER_EXTERNAL_RPC_URL"

    #######################################
    # 2. Prepare RPC Call env
    #######################################
    SEEDER_RPC_CALL_ENV_PATH="./scripts/rpc-call/env.sh" 
    ensure_env_file "./scripts/rpc-call/env_example.sh" "$SEEDER_RPC_CALL_ENV_PATH"
    replace_env_var "$SEEDER_RPC_CALL_ENV_PATH" "SEEDER_INTERNAL_RPC_URL" "$SEEDER_INTERNAL_RPC_URL"
    replace_env_var "$SEEDER_RPC_CALL_ENV_PATH" "LIVENESS_PLATFORM" "$LIVENESS_PLATFORM"
    replace_env_var "$SEEDER_RPC_CALL_ENV_PATH" "LIVENESS_SERVICE_PROVIDER" "$LIVENESS_SERVICE_PROVIDER"
    replace_env_var "$SEEDER_RPC_CALL_ENV_PATH" "LIVENESS_RPC_URL" "$LIVENESS_RPC_URL"
    replace_env_var "$SEEDER_RPC_CALL_ENV_PATH" "LIVENESS_WS_URL" "$LIVENESS_WS_URL"
    replace_env_var "$SEEDER_RPC_CALL_ENV_PATH" "LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS" "$LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS"

    echo "All environment files are prepared."

    echo "🚀 Running Seeder Initialization..."
    ./scripts/execute/01_init_seeder.sh
    echo "✅ Environment variables applied successfully."

    ./scripts/execute/02_run_seeder.sh &
    
    sleep 5

     ./scripts/rpc-call/10_initialize.sh > /dev/null 2>&1

    tail -f /dev/null

elif [ "$SEEDER_MODE" = "run" ]; then
    echo "🚀 Running Seeder..."
    ./scripts/execute/02_run_seeder.sh &

    tail -f /dev/null

else
    echo "❌ Invalid MODE: ${SEEDER_MODE}"
    exit 1
fi
