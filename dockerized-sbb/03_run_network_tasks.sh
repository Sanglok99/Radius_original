#!/bin/bash

set -e  # Exit on error

PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE_PATH="$PROJECT_ROOT_PATH/.network_env"
UTIL_FILE_PATH="$PROJECT_ROOT_PATH/util.sh"

source $UTIL_FILE_PATH

foundry_check_and_build

if [ ! -f "$ENV_FILE_PATH" ]; then
  echo "Error: $ENV_FILE_PATH file not found"
  exit 1
fi

source $ENV_FILE_PATH

create_cluster_and_rollup() {
    cluster_list=$(cast call "$LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS" "getAllClusterIds()(string[])" --rpc-url "$LIVENESS_RPC_URL")
    if echo "$cluster_list" | grep -q "$CLUSTER_ID"; then
      echo "Cluster '$CLUSTER_ID' already exists."
    else
      set +e
      RESULT=$(cast send "$LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS" --rpc-url "$LIVENESS_RPC_URL" --private-key "$CLUSTER_OWNER_PRIVATE_KEY" \
        "initializeCluster(string clusterId, uint256 maxSequencerNumber)" "$CLUSTER_ID" "$MAX_TX_ORDERER_NUMBER" 2>&1)
      IS_EXIT=$?
      set -e

      if [[ $IS_EXIT -ne 0 ]]; then
        echo "initializeCluster error"
        echo "$RESULT"
        exit 1
      else
        echo "Complete initializeCluster."
      fi
    fi

    if cast call "$LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS" "getRollup(string,string)((string,address,string,string,string,address[],(string,string,address)))" \
      "$CLUSTER_ID" "$ROLLUP_ID" --rpc-url "$LIVENESS_RPC_URL" 2>&1 | grep -q "execution reverted"; then

      RESULT=$(cast send "$LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS" --rpc-url "$LIVENESS_RPC_URL" --private-key "$CLUSTER_OWNER_PRIVATE_KEY" \
        "addRollup(string,(string,address,string,string,string,address,(string,string,address)))" \
        "$CLUSTER_ID" "($ROLLUP_ID,$ROLLUP_OWNER_ADDRESS,$ROLLUP_TYPE,$ENCRYPTED_TRANSACTION_TYPE,$ORDER_COMMITMENT_TYPE,$EXECUTOR_ADDRESS,($VALIDATION_PLATFORM,$VALIDATION_SERVICE_PROVIDER,$VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS))"  2>&1)
      IS_EXIT=$?
      set -e

      if [[ $IS_EXIT -ne 0 ]]; then
        echo "addRollup error"
        echo "$RESULT"
        exit 1
      else
        echo "Complete addRollup."
      fi
    else
      echo "Rollup '$ROLLUP_ID' already exists."
    fi
}

register_network_and_middleware() {

  IS_REGISTERED=$(cast call "$NETWORK_REGISTRY_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" "isEntity(address)(bool)" "$NETWORK_ADDRESS")
  if [[ "$IS_REGISTERED" == "false" ]]; then
    echo "Registering network..."
    RESULT=$(cast send "$NETWORK_REGISTRY_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" --private-key "$NETWORK_PRIVATE_KEY" "registerNetwork()" 2>&1)
    IS_EXIT=$?
    set -e

    if [[ $IS_EXIT -ne 0 ]]; then
      echo "Register network error occurred:"
      echo "$RESULT"
      exit 1
    else
      echo "Complete register network."
    fi
  else
    echo "Network already registered."
  fi

  IS_SET=$(cast call "$NETWORK_MIDDLEWARE_SERVICE_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" "middleware(address)(address)" "$NETWORK_ADDRESS")
  if [[ "$IS_SET" == "0x0000000000000000000000000000000000000000" ]]; then
    echo "Setting middleware..."
    RESULT=$(cast send "$NETWORK_MIDDLEWARE_SERVICE_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" --private-key "$NETWORK_PRIVATE_KEY" \
      "setMiddleware(address middlewareAddress)" "$VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS" 2>&1)
    IS_EXIT=$?
    set -e

    if [[ $IS_EXIT -ne 0 ]]; then
      echo "Set middleware error occurred:"
      echo "$RESULT"
      exit 1
    else
      echo "Complete set middleware."
    fi
  else
    echo "Middleware already set."
  fi
}

register_operator() {
  IS_REGISTERED=$(cast call "$OPERATOR_REGISTRY_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" "isEntity(address who)(bool)" "$OPERATOR_ADDRESS")

  if [[ "$IS_REGISTERED" == "false" ]]; then
    echo "❌ Skipping registerOperator...(not opt-in)"
  else
    REGISTRY_CONTRACT_ADDRESS=$(cast call "$VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" "registry()(address)")
    
    operators=$(cast call $REGISTRY_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL \
    "getCurrentOperatorInfos()((address, address, (address, uint256)[])[])")

    if echo "$operators" | grep -q "$OPERATOR_ADDRESS"; then
        echo "The operator is already registered"
    else
        $(cast send $VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL --private-key $NETWORK_PRIVATE_KEY \
        "registerOperator(address operatorAddress, address txOrdererAddress)" $OPERATOR_ADDRESS $TX_ORDERER_ADDRESS)

        echo "Completed registering the operator"
    fi    
  fi
}

register_token() {
  REGISTRY_CONTRACT_ADDRESS=$(cast call "$VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" "registry()(address)")

  IS_REGISTERED=$(cast call "$REGISTRY_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" "isActiveToken(address)(bool)" "$DEFAULT_TOKEN_CONTRACT_ADDRESS")
  if [[ "$IS_REGISTERED" == "false" ]]; then
    echo "Registering token..."
    RESULT=$(cast send "$VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" --private-key "$NETWORK_PRIVATE_KEY" \
      "registerToken(address token)" "$DEFAULT_TOKEN_CONTRACT_ADDRESS" 2>&1)
    IS_EXIT=$?
    set -e

    if [[ $IS_EXIT -ne 0 ]]; then
      echo "Register token error occurred:"
      echo "$RESULT"
      exit 1
    else
      echo "Complete register token."
    fi
  else
    echo "Token already registered."
  fi
}

register_vault() {
  REGISTRY_CONTRACT_ADDRESS=$(cast call "$VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" "registry()(address)")

  IS_REGISTERED=$(cast call "$REGISTRY_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" "isActiveVault(address vault)(bool)" "$DEFAULT_VAULT_CONTRACT_ADDRESS")

  if [[ "$IS_REGISTERED" == "false" ]]; then
    echo "Registering vault..."
    RESULT=$(cast send "$VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" --private-key "$NETWORK_PRIVATE_KEY" \
      "registerVault(address vault, address stakerRewards, address operatorRewards, address slasher)" "$DEFAULT_VAULT_CONTRACT_ADDRESS" "$DEFAULT_STAKER_REWARD_CONTRACT_ADDRESS" "$DEFAULT_OPERATOR_REWARD_CONTRACT_ADDRESS" "$DEFAULT_SLASHER_CONTRACT_ADDRESS" 2>&1)
    IS_EXIT=$?
    set -e

    if [[ $IS_EXIT -ne 0 ]]; then
      echo "Register vault error occurred:"
      echo "$RESULT"
      exit 1
    else
      echo "Complete register vault."
    fi
  else
    echo "Vault already registered."
  fi
}

set_max_network_limit() {
  set +e
  RESULT=$(cast send "$DEFAULT_DELEGATOR_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" --private-key "$NETWORK_PRIVATE_KEY" \
    "setMaxNetworkLimit(uint96 identifier, uint256 amount)" 0 "$MAX_NETWORK_LIMIT" 2>&1)
  IS_EXIT=$?
  set -e

  if [[ $IS_EXIT -ne 0 ]]; then
    if echo "$RESULT" | grep -q "execution reverted"; then
      echo "Already set the max network limit (network)"
    else
      echo "Unexpected error occurred:"
      echo "$RESULT"
      exit 1
    fi
  else
    echo "Complete set max network limit ($MAX_NETWORK_LIMIT)."
  fi
}

#######################################
# Menu
#######################################
echo "========================"
echo " MENU"
echo "========================"
echo "0) Exit"
echo "1) Initialize Cluster & Add rollup"
echo "2) Register network and set middleware"
echo "3) Register operator"
echo "4) Register token"
echo "5) Register vault"
echo "6) Set max network limit"
echo "7) Execute all process (1 ~ 6)"
echo "------------------------"
read -p "-> Please choose number: " choice

case $choice in
  1)
    create_cluster_and_rollup
    ;;
  2)
    register_network_and_middleware
    ;;
  3)
    register_operator
    ;;
  4)
    register_token
    ;;
  5)
    register_vault
    ;;
  6)
    set_max_network_limit
    ;;
  7)
    create_cluster_and_rollup
    register_network_and_middleware
    register_operator
    register_token
    register_vault
    set_max_network_limit
    ;;
  0)
    echo "Exited"
    exit 0
    ;;
  *)
    echo "Wrong number"
    exit 1
    ;;
esac
