#!/bin/bash

set -e  # Exit on error

PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
      value=$(grep -E "^export $key=" "$source_file" | cut -d= -f2-)
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
echo "1) Staker"
echo "2) Network"
echo "3) Vault"
echo "4) Operator"
echo "5) Dev"
echo "------------------------"
read -p "-> Please choose number: " choice

DEPLOYED_INFO_PATH="$PROJECT_ROOT_PATH/deployed_info.sh"

case $choice in
  1)
    SOURCE_ENV_FILE_PATH="$PROJECT_ROOT_PATH/env_templates/.staker_env_template"
    TARGET_ENV_FILE_PATH="$PROJECT_ROOT_PATH/.staker_env"

    cp $SOURCE_ENV_FILE_PATH $TARGET_ENV_FILE_PATH

    replace_env_vars "$DEPLOYED_INFO_PATH" "$TARGET_ENV_FILE_PATH"
    ;;
  2)
    SOURCE_ENV_FILE_PATH="$PROJECT_ROOT_PATH/env_templates/.network_env_template"
    TARGET_ENV_FILE_PATH="$PROJECT_ROOT_PATH/.network_env"

    cp $SOURCE_ENV_FILE_PATH $TARGET_ENV_FILE_PATH

    replace_env_vars "$DEPLOYED_INFO_PATH" "$TARGET_ENV_FILE_PATH"
    ;;
  3)
    SOURCE_ENV_FILE_PATH="$PROJECT_ROOT_PATH/env_templates/.vault_env_template"
    TARGET_ENV_FILE_PATH="$PROJECT_ROOT_PATH/.vault_env"

    cp $SOURCE_ENV_FILE_PATH $TARGET_ENV_FILE_PATH

    replace_env_vars "$DEPLOYED_INFO_PATH" "$TARGET_ENV_FILE_PATH"
    ;;
  4)
    SOURCE_ENV_FILE_PATH="$PROJECT_ROOT_PATH/env_templates/.operator_env_template"
    TARGET_ENV_FILE_PATH="$PROJECT_ROOT_PATH/.operator_env"

    cp $SOURCE_ENV_FILE_PATH $TARGET_ENV_FILE_PATH

    replace_env_vars "$DEPLOYED_INFO_PATH" "$TARGET_ENV_FILE_PATH"
    ;;
  5)
    SOURCE_ENV_FILE_PATH="$PROJECT_ROOT_PATH/env_templates/.dev_env_template"
    TARGET_ENV_FILE_PATH="$PROJECT_ROOT_PATH/.dev_env"

    cp $SOURCE_ENV_FILE_PATH $TARGET_ENV_FILE_PATH

    replace_env_vars "$DEPLOYED_INFO_PATH" "$TARGET_ENV_FILE_PATH"
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
