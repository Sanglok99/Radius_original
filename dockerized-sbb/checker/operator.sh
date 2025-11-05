#!/bin/bash

##########################################################################################

RPC_URL="http://35.189.33.95:8545"

VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS="0x1CfD8455F189c56a4FBd81EB7D4118DB04616BA8"
LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS="0xbdEd0D2bf404bdcBa897a74E6657f1f12e5C6fb6"

CLUSTER_ID="radius_cluster"

NETWORK_ADDRESS="0x976EA74026E726554dB657fA54763abd0C3a0aa9"
SUBNETWORK=$NETWORK_ADDRESS"000000000000000000000000"

OPERATOR_REGISTRY_CONTRACT_ADDRESS="0xDc64a140Aa3E981100a9becA4E685f962f0cF6C9"
OPERATOR_NETWORK_OPT_IN_SERVICE_CONTRACT_ADDRESS="0x8A791620dd6260079BF849Dc5567aDC3F2FdC318"
OPERATOR_VAULT_OPT_IN_SERVICE_CONTRACT_ADDRESS="0x2279B7A0a67DB372996a5FaB50D91eAA73d2eBe6"

# VAULT Info (name|vault address|delegator address)
vault_info_list=(
  "VAULT_1|0x5046E6fF3B288E4776999e6B2a901fc36F1820D9|0x046ECDa2E3B6861A4e11afE5A2eb7aE22A550f0b"
)

# TOKEN Info (name|address)
tokens=(
  "STETH|0x8464135c8F25Da09e49BC8782676a84730C318bC"
)

# TEAM Info (name|operator address|tx_orderer address)
teams=(
  "ROLLUP_1|0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f|0xa0Ee7A142d267C1f36714E4a8F75612F20a79720"
)

# VAULT mapping (team name|vault name)
team_vaults=(
  "ROLLUP_1|VAULT_1"
)

##########################################################################################

check_team() {
  local team_name=$1
  local operator_address=$2
  local tx_orderer=$3

  echo "=============================="
  echo "team: $team_name"
  echo "=============================="

  result1=$(cast call $LIVENESS_SERVICE_MANAGER_CONTRACT_ADDRESS --rpc-url $RPC_URL \
    "isTxOrdererRegistered(string clusterId, address operating)(bool)" $CLUSTER_ID $tx_orderer)
  echo "1. Check register tx orderer - ('$CLUSTER_ID', $tx_orderer): $result1"

  result2=$(cast call $OPERATOR_REGISTRY_CONTRACT_ADDRESS --rpc-url $RPC_URL \
    "isEntity(address who)(bool)" $operator_address)
  echo "2. Check register operator - ($operator_address): $result2"

  result3=$(cast call $OPERATOR_NETWORK_OPT_IN_SERVICE_CONTRACT_ADDRESS --rpc-url $RPC_URL \
    "isOptedIn(address who, address where)(bool)" $operator_address $NETWORK_ADDRESS)
  echo "3. Check Optin to network - ($operator_address, $NETWORK_ADDRESS): $result3"

  echo "4. Check Optin to Vaults and operator network shares"

  for mapping in "${team_vaults[@]}"; do
    IFS="|" read -r team vault_name <<< "$mapping"
    if [ "$team" == "$team_name" ]; then

      for vault_info in "${vault_info_list[@]}"; do
        IFS="|" read -r vn vault_address delegator_address <<< "$vault_info"
        if [ "$vn" == "$vault_name" ]; then

          result_vault=$(cast call $OPERATOR_VAULT_OPT_IN_SERVICE_CONTRACT_ADDRESS --rpc-url $RPC_URL \
            "isOptedIn(address who, address where)(bool)" $operator_address $vault_address)
          echo " - $vault_name ($vault_address): $result_vault"

          result_share=$(cast call $delegator_address --rpc-url $RPC_URL \
            "operatorNetworkShares(bytes32 subnetwork, address operator)(uint256)" $SUBNETWORK $operator_address)
          echo "   * Share amount - $result_share"
        fi
      done
    fi
  done

  echo "6. Check register operator in middleware contract"
  REGISTRY_CONTRACT_ADDRESS=$(cast call "$VALIDATION_SERVICE_MANAGER_CONTRACT_ADDRESS" --rpc-url "$RPC_URL" "registry()(address)")

  cast_output=$(cast call $REGISTRY_CONTRACT_ADDRESS --rpc-url $RPC_URL \
    "getCurrentOperatorInfos()((address, address, (address, uint256)[])[])")

  echo "cast_output" $cast_output

  if echo "$cast_output" | grep -q "$operator_address"; then
    echo "   * It exists in the middleware contract."

    echo "7. Check staking amount for each token"
    for token_entry in "${tokens[@]}"; do
      IFS="|" read -r token_name token_address <<< "$token_entry"

      staking_amount=$(cast call $REGISTRY_CONTRACT_ADDRESS --rpc-url $RPC_URL \
        "getCurrentOperatorTokenStake(address operator, address token)(uint256)" \
        $operator_address $token_address)

      echo "   * $token_name ($token_address) - $staking_amount"
    done
  else
    echo "Address $operator_address does not exist in the middleware contract."
  fi

  echo ""
}

for team in "${teams[@]}"; do
  IFS="|" read -r name operator operating <<< "$team"
  check_team "$name" "$operator" "$operating"
done