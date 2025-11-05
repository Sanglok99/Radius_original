#!/bin/bash

set -e

PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Services
ALL_SERVICES=("seeder" "distributed_key_generator" "tx_orderer" "secure-rpc-provider")

# ===== Utility functions =====
build_service() {
  echo "🔨 Building $1..."
  docker-compose build "$1"
  echo "✅ Done building $1."
}

run_service() {
  echo "🚀 Running $1..."
  docker-compose up -d "$1"
  echo "✅ $1 is running."
}

show_service_menu() {
  SERVICE=$1
  SERVICE_NAME=$(echo "$SERVICE" | tr '_' ' ' | sed -E 's/\b(.)/\u\1/g')

  echo ""
  echo "======== $SERVICE_NAME ========"
  echo "1) Build $SERVICE_NAME"
  echo "2) Run $SERVICE_NAME"
  echo "0) Back to previous menu"
  echo "-----------------------------"
  read -p "-> Choose an action: " action

  case $action in
    1) build_service "$SERVICE" ;;
    2) run_service "$SERVICE" ;;
    0) return ;;
    *) echo "❌ Invalid input";;
  esac
}

run_all_services() {
  for svc in "${ALL_SERVICES[@]}"; do
    run_service "$svc"
  done
}

build_and_run_all_services() {
  for svc in "${ALL_SERVICES[@]}"; do
    build_service "$svc"
    run_service "$svc"
  done
}

# ===== Role-specific Menus =====
operator_mode() {
  while true; do
    echo ""
    echo "========================"
    echo " TX_ORDERER - OPERATOR "
    echo "========================"
    echo "0) Exit"
    echo "1) Build Tx Orderer"
    echo "2) Run Tx Orderer"
    echo "------------------------"
    read -p "-> Choose an option: " op_choice

    case $op_choice in
      1) build_service "tx_orderer" ;;
      2) run_service "tx_orderer" ;;
      0)
        echo "👋 Exiting Operator Mode."
        exit 0
        ;;
      *) echo "❌ Invalid option. Try again." ;;
    esac
  done
}

developer_mode() {
  while true; do
    echo ""
    echo "============================="
    echo " DOCKER SERVICE ORCHESTRATOR"
    echo "============================="
    echo "0) Exit"
    echo "1) Seeder"
    echo "2) Distributed Key Generator"
    echo "3) Tx Orderer"
    echo "4) Secure RPC Provider"
    echo "5) Run All Services (no build)"
    echo "6) Build & Run All Services"
    echo "-----------------------------"
    read -p "-> Choose a service or action: " choice

    case $choice in
      1) show_service_menu "seeder" ;;
      2) show_service_menu "distributed_key_generator" ;;
      3) show_service_menu "tx_orderer" ;;
      4) show_service_menu "secure-rpc-provider" ;;
      5) run_all_services ;;
      6) build_and_run_all_services ;;
      0)
        echo "👋 Exiting Developer Mode."
        exit 0
        ;;
      *) echo "❌ Invalid option. Try again." ;;
    esac
  done
}

# ===== Initial Role Selection =====
echo ""
echo "=============================="
echo " SELECT YOUR OPERATION MODE"
echo "=============================="
echo "0) Exit"
echo "1) Run Tx Orderer as Operator"
echo "2) Run All Services as Developer"
echo "------------------------------"
read -p "-> Choose your mode: " mode_choice

case $mode_choice in
  1) operator_mode ;;
  2) developer_mode ;;
  0)
    echo "👋 Exiting."
    exit 0
    ;;
  *)
    echo "❌ Invalid mode. Try again."
    ;;
esac
