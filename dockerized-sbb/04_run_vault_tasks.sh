#!/bin/bash

set -e  # Exit on error

PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE_PATH="$PROJECT_ROOT_PATH/.vault_env"
UTIL_FILE_PATH="$PROJECT_ROOT_PATH/util.sh"

source $UTIL_FILE_PATH

foundry_check_and_build

if [ ! -f "$ENV_FILE_PATH" ]; then
  echo "Error: $ENV_FILE_PATH file not found"
  exit 1
fi

source $ENV_FILE_PATH

#######################################
# Helper functions
#######################################
set_network_limit() {
  set +e
  RESULT=$(cast send "$DEFAULT_DELEGATOR_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" --private-key "$DEFAULT_VAULT_CONTRACT_OWNER_PRIVATE_KEY" \
    "setNetworkLimit(bytes32 subnetwork, uint256 amount)" "$SUBNETWORK" "$NETWORK_LIMIT" 2>&1)
  IS_EXIT=$?
  set -e

  if [[ $IS_EXIT -ne 0 ]]; then
    if echo "$RESULT" | grep -q "execution reverted"; then
      max_network_limit=$(cast call "$DEFAULT_DELEGATOR_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" \
      "maxNetworkLimit(bytes32 subnetwork)(uint256 maxNetworkLimit)" "$SUBNETWORK" 2>/dev/null)
      
      echo "  MAX_NETWORK_LIMIt: ${max_network_limit} / NETWORK_LIMIT: ${NETWORK_LIMIT}"
    else
      echo "Unexpected error occurred:"
      echo "$RESULT"
      exit 1
    fi
  else
    echo "Complete set network limit ($NETWORK_LIMIT)."
  fi
}

set_network_share() {
  source "$ENV_FILE_PATH" "$EXPORTED_ENV_PATH"

  set +e
  RESULT=$(cast send "$DEFAULT_DELEGATOR_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" --private-key "$DEFAULT_VAULT_CONTRACT_OWNER_PRIVATE_KEY" \
    "setOperatorNetworkShares(bytes32 subnetwork, address operator, uint256 shares)" "$SUBNETWORK" "$OPERATOR_ADDRESS" "$DELEGATE_AMOUNT" 2>&1)
  IS_EXIT=$?
  set -e

  if [[ $IS_EXIT -ne 0 ]]; then
    if echo "$RESULT" | grep -q "execution reverted"; then
      echo "Already set the network share"
    else
      echo "Set operator network share error"
      echo "$RESULT"
      exit 1
    fi
  else
    echo "Complete set network share."
  fi

  cast call "$DEFAULT_DELEGATOR_CONTRACT_ADDRESS" --rpc-url "$VALIDATION_RPC_URL" \
    "stake(bytes32 subnetwork, address operator)(uint256)" "$SUBNETWORK" "$OPERATOR_ADDRESS"
}

#######################################
# Menu
#######################################
echo "========================"
echo " MENU"
echo "========================"
echo "0) Exit"
echo "1) Set network limit (vault)"
echo "2) Set network share (vault)"
echo "------------------------"
read -p "-> Please choose number: " choice

case $choice in
  1)
    set_network_limit
    ;;
  2)
    set_network_share
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
