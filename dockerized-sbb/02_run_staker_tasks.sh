#!/bin/bash

set -e  # Exit on error

PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE_PATH="$PROJECT_ROOT_PATH/.staker_env"
UTIL_FILE_PATH="$PROJECT_ROOT_PATH/util.sh"

source $UTIL_FILE_PATH

foundry_check_and_build

if [ ! -f "$ENV_FILE_PATH" ]; then
  echo "Error: $ENV_FILE_PATH file not found"
  exit 1
fi

source $ENV_FILE_PATH

#######################################
# Menu
#######################################
echo "========================"
echo " MENU"
echo "========================"
echo "0) Exit"
echo "1) Get token (for testing)"
echo "2) Stake"
echo "------------------------"
read -p "-> Please choose number: " choice

case $choice in
  1)
    cast send $DEFAULT_TOKEN_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL --private-key $DEFAULT_TOKEN_CONTRACT_OWNER_PRIVATE_KEY \
    "transfer(address,uint256)" $STAKER_ADDRESS $DEPOSIT_AMOUNT

    cast call $DEFAULT_TOKEN_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL \
    "balanceOf(address)(uint256)" $STAKER_ADDRESS
    ;;
  2)
    cast send $DEFAULT_TOKEN_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL --private-key $STAKER_PRIVATE_KEY \
    "approve(address spender, uint256 value)(bool)" $DEFAULT_COLLATERAL_CONTRACT_ADDRESS $DEPOSIT_AMOUNT 

    cast call $DEFAULT_TOKEN_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL \
    "allowance(address,address)(uint256)" $STAKER_ADDRESS $DEFAULT_COLLATERAL_CONTRACT_ADDRESS

    cast send $DEFAULT_COLLATERAL_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL --private-key $STAKER_PRIVATE_KEY \
    "deposit(address recipient, uint256 amount)(uint256)" $STAKER_ADDRESS $DEPOSIT_AMOUNT 

    cast call $DEFAULT_TOKEN_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL \
    "balanceOf(address)(uint256)" $DEFAULT_COLLATERAL_CONTRACT_ADDRESS

    cast send $DEFAULT_COLLATERAL_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL --private-key $STAKER_PRIVATE_KEY \
    "approve(address spender, uint256 value)(bool)" $DEFAULT_VAULT_CONTRACT_ADDRESS $DEPOSIT_AMOUNT

    cast send $DEFAULT_VAULT_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL --private-key $STAKER_PRIVATE_KEY \
    "deposit(address onBehalfOf, uint256 amount)(uint256 depositedAmount, uint256 mintedShares)" $STAKER_ADDRESS $DEPOSIT_AMOUNT

    cast call $DEFAULT_VAULT_CONTRACT_ADDRESS --rpc-url $VALIDATION_RPC_URL \
    "activeSharesOf(address)(uint256)" $STAKER_ADDRESS
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
