#!/bin/bash

set -e  # Exit on error

PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE_PATH="$PROJECT_ROOT_PATH/.blockchain_env"
UTIL_FILE_PATH="$PROJECT_ROOT_PATH/util.sh"
DEPLOYED_INFO_PATH="$PROJECT_ROOT_PATH/deployed_info.sh"

if [ ! -f "$ENV_FILE_PATH" ]; then
  echo "Error: $ENV_FILE_PATH file not found"
  exit 1
fi

source $ENV_FILE_PATH
source $UTIL_FILE_PATH

foundry_check_and_build
jq_check_and_build

#######################################
# Replace env variables in a target script
#######################################
replace_env_vars() {
  local source_file=$1
  local target_file=$2
  local temp_file
  temp_file=$(mktemp)

  while IFS= read -r line || [ -n "$line" ]; do
    if [[ $line =~ ^([A-Za-z_]+)=(.*)$ ]]; then
      key="${BASH_REMATCH[1]}"
      value=$(grep -E "^$key=" "$source_file" | cut -d= -f2-)
      echo "$key=${value:-${BASH_REMATCH[2]}}" >> "$temp_file"
    else
      echo "$line" >> "$temp_file"
    fi
  done < "$target_file"

  mv "$temp_file" "$target_file"
}

#######################################
# Menu
#######################################
echo "========================"
echo " MENU"
echo "========================"
echo "0) Exit"
echo "1) Deploy Contracts"
echo "2) Start blockchain"
echo "------------------------"
read -p "-> Please choose number: " choice

case $choice in
  1)
    ENV_SCRIPT="$PROJECT_ROOT_PATH/$REPO_DIR/utils/env.sh"
    
    if [ ! -d "$REPO_DIR" ]; then
      git clone --branch "$BRANCH" --single-branch "$REPO_URL" "$REPO_DIR"
    fi
    
    cd "$REPO_DIR"

    replace_env_vars "$ENV_FILE_PATH" "$ENV_SCRIPT"

    make build-contracts
    make deploy-all
    
    "$PROJECT_ROOT_PATH/$REPO_DIR/utils/state/export_env.sh" > "$DEPLOYED_INFO_PATH"
    ;;
  2)
    REPO_DIR="symbiotic-middleware-contract"
    cd "$REPO_DIR"

    [ -f "$DEPLOYED_INFO_PATH" ] && source "$DEPLOYED_INFO_PATH" && make start || echo "Not exist env file ($DEPLOYED_INFO_PATH)"
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
