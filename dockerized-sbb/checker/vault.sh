#!/bin/bash

VALIDATION_RPC_URL="http://35.189.33.95:8545"

NETWORK_ADDRESS="0x976EA74026E726554dB657fA54763abd0C3a0aa9"
SUBNETWORK=$NETWORK_ADDRESS"000000000000000000000000"

VAULTS=("VAULT_1")
VAULT_CONTRACT_ADDRESES=("0x5046E6fF3B288E4776999e6B2a901fc36F1820D9")
DELEGATOR_CONTRACT_ADDRESSES=("0x046ECDa2E3B6861A4e11afE5A2eb7aE22A550f0b")
TOKEN_CONTRACT_ADDRESSES=("0x8464135c8F25Da09e49BC8782676a84730C318bC")
NETWORK_MAX_LIMITS=("4725000000")

for i in "${!VAULTS[@]}"; do
  name="${VAULTS[$i]}"
  vault="${VAULT_CONTRACT_ADDRESES[$i]}"
  delegator="${DELEGATOR_CONTRACT_ADDRESSES[$i]}"
  token="${TOKEN_CONTRACT_ADDRESSES[$i]}"
  max_limit="${NETWORK_MAX_LIMITS[$i]}"

  echo "Name: $name"
  echo "  VAULT Address: $vault"
  echo "  DELEGATOR Address: $delegator"
  echo "  TOKEN Address: $token"
  echo "  SUBNETWORK: $SUBNETWORK"

  if [ -n "$delegator" ]; then
    max_network_limit=$(cast call "$delegator" --rpc-url "$VALIDATION_RPC_URL" \
      "maxNetworkLimit(bytes32)(uint256)" "$SUBNETWORK" 2>/dev/null)
    echo "  NETWORK_MAX_LIMIT: ${max_network_limit:-Error fetching data}"

    total_operator_network_shares=$(cast call "$delegator" --rpc-url "$VALIDATION_RPC_URL" \
      "totalOperatorNetworkShares(bytes32)(uint256)" "$SUBNETWORK" 2>/dev/null)
    echo "  Total operator network shares: ${total_operator_network_shares:-Error fetching data}"
  fi

  echo ""
done