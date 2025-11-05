#!/bin/bash
SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
source $SCRIPT_PATH/../env.sh

curl --location $KEY_GENERATOR_CLUSTER_RPC_URL \
--header 'Content-Type: application/json' \
--data '{
    "jsonrpc": "2.0",
    "method": "get_key_generator_list",
    "params": {},
    "id": 1
}'

echo ""