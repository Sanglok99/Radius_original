#!/bin/bash
SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source $SCRIPT_PATH/env.sh

# Safety check function for DATA_PATH
check_data_path_safety() {
    local path="$1"
    
    # Check if DATA_PATH is empty or unset
    if [ -z "$path" ]; then
        echo "ERROR: DATA_PATH is empty or not set!"
        echo "This is a safety check to prevent accidental deletion of important directories."
        exit 1
    fi
    
    # Normalize the path (resolve symlinks and get absolute path)
    local normalized_path=$(readlink -f "$path" 2>/dev/null || echo "$path")
    
    # Check if path is root directory
    if [ "$normalized_path" = "/" ]; then
        echo "ERROR: DATA_PATH is set to root directory (/)!"
        echo "This would delete the entire filesystem. Aborting for safety."
        exit 1
    fi
    
    # Check if path is home directory
    if [ "$normalized_path" = "$HOME" ] || [ "$normalized_path" = "${HOME%/}" ]; then
        echo "ERROR: DATA_PATH is set to home directory ($HOME)!"
        echo "This would delete your home directory. Aborting for safety."
        exit 1
    fi
    
    # Check if path contains "data" in the name (expected pattern)
    # Allows: /data, /data/, /data_1, /data_2, etc.
    if [[ ! "$normalized_path" =~ /data(/|$|_) ]]; then
        echo "WARNING: DATA_PATH does not contain 'data' in its path: $normalized_path"
        echo "Expected pattern: .../data, .../data/, or .../data_*"
        read -p "Do you want to continue? (yes/no): " confirm
        if [ "$confirm" != "yes" ]; then
            echo "Aborted by user."
            exit 1
        fi
    fi
    
    # Check if path is too short (less than 5 characters) - likely a mistake
    if [ ${#normalized_path} -lt 5 ]; then
        echo "ERROR: DATA_PATH is suspiciously short: $normalized_path"
        echo "This might be a configuration error. Aborting for safety."
        exit 1
    fi
    
    echo "Safety check passed. DATA_PATH: $normalized_path"
}

# Perform safety check before any deletion
check_data_path_safety "$DATA_PATH"

mkdir -p $DATA_PATH

# Safe removal with additional verification
if [ -d "$DATA_PATH" ]; then
    echo "Cleaning up existing data in $DATA_PATH..."
    # Use find instead of rm -rf for safer deletion
    find "$DATA_PATH" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
else
    echo "DATA_PATH directory does not exist yet. Will be created."
fi

echo "Initialize tx_orderer" 

$BIN_PATH init --path $DATA_PATH

sed -i.temp "s|0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80|$TX_ORDERER_PRIVATE_KEY|g" $PRIVATE_KEY_PATH

sed -i.temp "s|internal_rpc_url = \"http://127.0.0.1:4000\"|internal_rpc_url = \"$TX_ORDERER_INTERNAL_RPC_URL\"|g" $CONFIG_FILE_PATH
sed -i.temp "s|cluster_rpc_url = \"http://127.0.0.1:5000\"|cluster_rpc_url = \"$TX_ORDERER_CLUSTER_RPC_URL\"|g" $CONFIG_FILE_PATH
sed -i.temp "s|external_rpc_url = \"http://127.0.0.1:3000\"|external_rpc_url = \"$TX_ORDERER_EXTERNAL_RPC_URL\"|g" $CONFIG_FILE_PATH

sed -i.temp "s|distributed_key_generation_rpc_url = \"http://127.0.0.1:7100\"|distributed_key_generation_rpc_url = \"$DISTRIBUTED_KEY_GENERATOR_EXTERNAL_RPC_URL\"|g" $CONFIG_FILE_PATH

sed -i.temp "s|seeder_rpc_url = \"http://127.0.0.1:6000\"|seeder_rpc_url = \"$SEEDER_EXTERNAL_RPC_URL\"|g" $CONFIG_FILE_PATH

sed -i.temp "s|reward_manager_rpc_url = \"http://127.0.0.1:6100\"|reward_manager_rpc_url = \"$REWARD_MANAGER_EXTERNAL_RPC_URL\"|g" $CONFIG_FILE_PATH

if [ -n "$BUILDER_EXTERNAL_RPC_URL" ]; then
    sed -i.temp "s|# builder_rpc_url = None|builder_rpc_url = \"$BUILDER_EXTERNAL_RPC_URL\"|g" $CONFIG_FILE_PATH
fi


rm $CONFIG_FILE_PATH.temp
rm $PRIVATE_KEY_PATH.temp