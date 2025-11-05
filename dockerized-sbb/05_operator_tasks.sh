#!/bin/bash


PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE_PATH="$PROJECT_ROOT_PATH/.operator_env"
UTIL_FILE_PATH="$PROJECT_ROOT_PATH/util.sh"

source $UTIL_FILE_PATH

foundry_check_and_build

if [ ! -f "$ENV_FILE_PATH" ]; then
  echo "Error: $ENV_FILE_PATH file not found"
  exit 1
fi

source $ENV_FILE_PATH

# Function to check a condition and execute a command only if needed
check_and_execute() {
    local check_command=$1
    local expected_output=$2
    local send_command=$3
    local description=$4

    echo "Checking: $description"
    result=$(eval "$check_command")

    if [[ "$result" == "$expected_output" ]]; then
        echo "✅ Already set: $description"
    else
        echo "❌ Check failed. Executing: $description"
        eval "$send_command"
    fi
}

echo $OPERATOR_REGISTRY_CONTRACT_ADDRESS
echo $VALIDATION_RPC_URL
echo $OPERATOR_ADDRESS
# 1. Register Operator
check_and_execute \
    "cast call $OPERATOR_REGISTRY_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL 'isEntity(address who)(bool)' $OPERATOR_ADDRESS" \
    "true" \
    "cast send $OPERATOR_REGISTRY_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL --private-key $OPERATOR_PRIVATE_KEY 'registerOperator()'" \
    "Registering Operator"

# 2. Opt-in to Vault
check_and_execute \
    "cast call $OPERATOR_VAULT_OPT_IN_SERVICE_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL 'isOptedIn(address who, address where)(bool)' $OPERATOR_ADDRESS $DEFAULT_VAULT_CONTRACT_ADDRESS" \
    "true" \
    "cast send $OPERATOR_VAULT_OPT_IN_SERVICE_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL --private-key $OPERATOR_PRIVATE_KEY 'optIn(address vault)' $DEFAULT_VAULT_CONTRACT_ADDRESS" \
    "Opt-in to Vault"

# 3. Opt-in to Network
check_and_execute \
    "cast call $OPERATOR_NETWORK_OPT_IN_SERVICE_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL 'isOptedIn(address who, address where)(bool)' $OPERATOR_ADDRESS $NETWORK_ADDRESS" \
    "true" \
    "cast send $OPERATOR_NETWORK_OPT_IN_SERVICE_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL --private-key $OPERATOR_PRIVATE_KEY 'optIn(address network)' $NETWORK_ADDRESS" \
    "Opt-in to Network"

# 4. Register Tx_Orderer
check_and_execute \
    "cast call $LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS --rpc-url $LIVENESS_RPC_URL 'isTxOrdererRegistered(string clusterId, address txOrderer)(bool)' $CLUSTER_ID $TX_ORDERER_ADDRESS" \
    "true" \
    "cast send $LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS --rpc-url $LIVENESS_RPC_URL --private-key $TX_ORDERER_PRIVATE_KEY 'registerTxOrderer(string clusterId)' $CLUSTER_ID" \
    "Registering Tx_Orderer"

echo "✅ All necessary steps have been completed."
