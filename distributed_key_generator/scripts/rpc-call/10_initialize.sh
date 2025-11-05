#!/bin/bash
SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source $SCRIPT_PATH/env.sh

curl --location $KEY_GENERATOR_INTERNAL_RPC_URL \
--header 'Content-Type: application/json' \
--data '{
    "jsonrpc": "2.0",
    "method": "add_key_generator",
    "params": {
        "message": {
            "address": "'"$KEY_GENERATOR_ADDRESS"'",
            "cluster_rpc_url": "'"$KEY_GENERATOR_CLUSTER_RPC_URL"'",
            "external_rpc_url": "'"$KEY_GENERATOR_EXTERNAL_RPC_URL"'"
        }
    },
    "id": 1
}'

echo ""