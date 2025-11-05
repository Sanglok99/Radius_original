#!/bin/bash

set -e  # Exit on error

jq_check_and_build() {
  if command -v jq &> /dev/null; then
    echo "✅ jq is already installed: $(jq --version)"
  else

    OS_TYPE=$(uname)

    if [ "$OS_TYPE" = "Darwin" ]; then
        echo "Detected macOS. Installing jq via Homebrew..."

        # brew 설치 여부 확인
        if ! command -v brew &> /dev/null; then
            echo "❌ Homebrew not found. Please install Homebrew first: https://brew.sh"
            exit 1
        fi

        brew install jq

    elif [ "$OS_TYPE" = "Linux" ]; then
        echo "🐧 Detected Linux. Installing jq via apt..."

        # sudo 필수
        if ! command -v sudo &> /dev/null; then
            echo "❌ sudo not found. Please install sudo or run as root."
            exit 1
        fi

        sudo apt-get update
        sudo apt-get install -y jq
    else
        echo "❌ Unsupported OS: $OS_TYPE"
        exit 1
    fi
  fi
}

foundry_check_and_build() {
  # Step 1: Check if `cast` is installed
  if ! command -v cast &> /dev/null; then
      echo "⚠️ Foundry's 'cast' command is not installed. Installing Foundry..."
      curl -L https://foundry.paradigm.xyz | bash

      export PATH="$HOME/.foundry/bin:$PATH"
  fi

  # Step 2: Check if `cast` version matches the required nightly version
  REQUIRED_VERSION="cast 0.2.0 (5b7e4cb"

  CURRENT_VERSION=$(cast --version)

  echo "🔍 Current version: $CURRENT_VERSION"
  echo "$REQUIRED_VERSION"
  
  if [[ "$CURRENT_VERSION" != "$REQUIRED_VERSION"* ]]; then
      echo "⚠️ Incorrect Foundry version detected: $CURRENT_VERSION"
      echo "Updating Foundry to required version: $REQUIRED_VERSION..."
      OS_TYPE=$(uname)

      if [ "$OS_TYPE" = "Darwin" ]; then
          foundryup -v "nightly-5b7e4cb3c882b28f3c32ba580de27ce7381f415a"
      elif [ "$OS_TYPE" = "Linux" ]; then
          foundryup -i "nightly-5b7e4cb3c882b28f3c32ba580de27ce7381f415a"
      else
          echo "❌ Unknown OS: $OS_TYPE"
      fi
  else
      echo "✅ Foundry is already at the required version. Skipping update..."
  fi
}