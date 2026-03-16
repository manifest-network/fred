#!/usr/bin/env bash
# dev-init.sh — Register provider & SKUs on-chain, generate dev config files.
#
# Adapted from manifest-deploy/roles/chain-bootstrap/tasks/main.yml.
# Requires a running local chain and the manifestd binary.
#
# Usage:
#   bash scripts/dev-init.sh
#
# All settings are configurable via environment variables (see defaults below).

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (override via environment)
# ---------------------------------------------------------------------------
MANIFESTD="${MANIFESTD:-manifestd}"
CHAIN_HOME="${CHAIN_HOME:-$HOME/.manifest}"
CHAIN_ID="${CHAIN_ID:-manifest-ledger-beta}"
KEYRING_BACKEND="${KEYRING_BACKEND:-test}"
KEY_NAME="${KEY_NAME:-acc0}"
NODE="${NODE:-http://localhost:26657}"
GAS_PRICES="${GAS_PRICES:-0.025umfx}"
PWR_DENOM="${PWR_DENOM:-factory/manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj/upwr}"
API_URL="${API_URL:-https://localhost:8080}"
CALLBACK_SECRET="${CALLBACK_SECRET:-$(openssl rand -hex 32)}"

# Repo root (script lives in scripts/)
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# SKU definitions: name and price (price in PWR per hour, divisible by 3600)
SKU_NAMES=("docker-micro" "docker-small" "docker-medium" "docker-large")
SKU_PRICES=(3600000 7200000 14400000 28800000)

# Common flags for manifestd commands
COMMON_FLAGS=(
  --home "$CHAIN_HOME"
  --keyring-backend "$KEYRING_BACKEND"
)
TX_FLAGS=(
  "${COMMON_FLAGS[@]}"
  --chain-id "$CHAIN_ID"
  --node "$NODE"
  --gas auto
  --gas-adjustment 1.5
  --gas-prices "$GAS_PRICES"
  --broadcast-mode sync
  --output json
  --yes
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m OK\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33mWARN\033[0m %s\n' "$*"; }
die()   { printf '\033[1;31mERROR\033[0m %s\n' "$*" >&2; exit 1; }

# Run a manifestd tx command and validate the response code is 0.
run_tx() {
  local output
  output=$("$MANIFESTD" "$@" 2>&1) || die "manifestd command failed: $output"
  local code
  code=$(echo "$output" | jq -r '.code // 0' 2>/dev/null) || code=""
  if [[ "$code" != "0" && "$code" != "" ]]; then
    die "Transaction failed (code $code): $output"
  fi
  echo "$output"
}

# ---------------------------------------------------------------------------
# 1. Derive provider address
# ---------------------------------------------------------------------------
info "Deriving provider address from key '$KEY_NAME'"
PROVIDER_ADDRESS=$("$MANIFESTD" keys show "$KEY_NAME" "${COMMON_FLAGS[@]}" --address) \
  || die "Failed to derive address. Is the key '$KEY_NAME' in the keyring at $CHAIN_HOME?"
ok "Provider address: $PROVIDER_ADDRESS"

# ---------------------------------------------------------------------------
# 2. Wait for chain to sync
# ---------------------------------------------------------------------------
info "Waiting for chain to sync at $NODE ..."
for i in $(seq 1 30); do
  if status=$(curl -sf "$NODE/status" 2>/dev/null); then
    catching_up=$(echo "$status" | jq -r '.result.sync_info.catching_up')
    if [[ "$catching_up" == "false" ]]; then
      ok "Chain is synced"
      break
    fi
  fi
  if [[ $i -eq 30 ]]; then
    die "Chain did not sync within 150 seconds"
  fi
  sleep 5
done

# ---------------------------------------------------------------------------
# 3. Check if provider already exists
# ---------------------------------------------------------------------------
info "Checking if provider is already registered"
provider_json=$("$MANIFESTD" query sku provider-by-address "$PROVIDER_ADDRESS" \
  --node "$NODE" --output json 2>/dev/null) || provider_json="{}"

provider_count=$(echo "$provider_json" | jq '.providers | length' 2>/dev/null) || provider_count=0
existing_uuid=$(echo "$provider_json" | jq -r '.providers[0].uuid // empty' 2>/dev/null) || existing_uuid=""

# ---------------------------------------------------------------------------
# 4. Register provider if needed
# ---------------------------------------------------------------------------
if [[ -n "$existing_uuid" ]]; then
  ok "Provider already registered (uuid: $existing_uuid)"
else
  info "Registering provider on-chain"
  run_tx tx sku create-provider \
    "$PROVIDER_ADDRESS" "$PROVIDER_ADDRESS" \
    --api-url "$API_URL" \
    --from "$KEY_NAME" \
    "${TX_FLAGS[@]}" >/dev/null
  ok "Provider registration tx submitted, waiting for confirmation ..."
  sleep 7
fi

# ---------------------------------------------------------------------------
# 5. Query provider UUID (with retries)
# ---------------------------------------------------------------------------
info "Querying provider UUID"
PROVIDER_UUID=""
for i in $(seq 1 10); do
  provider_json=$("$MANIFESTD" query sku provider-by-address "$PROVIDER_ADDRESS" \
    --node "$NODE" --output json 2>/dev/null) || provider_json="{}"
  PROVIDER_UUID=$(echo "$provider_json" | jq -r '.providers[0].uuid // empty' 2>/dev/null) || PROVIDER_UUID=""
  if [[ -n "$PROVIDER_UUID" ]]; then
    break
  fi
  if [[ $i -eq 10 ]]; then
    die "Failed to get provider UUID after 10 attempts"
  fi
  sleep 6
done
ok "Provider UUID: $PROVIDER_UUID"

# ---------------------------------------------------------------------------
# 6. Query existing SKUs and create missing ones
# ---------------------------------------------------------------------------
info "Querying existing SKUs for provider"
skus_json=$("$MANIFESTD" query sku skus-by-provider "$PROVIDER_UUID" \
  --node "$NODE" --output json 2>/dev/null) || skus_json='{"skus":[]}'

# Build list of existing SKU names
existing_sku_names=$(echo "$skus_json" | jq -r '.skus[].name' 2>/dev/null) || existing_sku_names=""

created_any=false
for idx in "${!SKU_NAMES[@]}"; do
  name="${SKU_NAMES[$idx]}"
  price="${SKU_PRICES[$idx]}"

  if echo "$existing_sku_names" | grep -qx "$name"; then
    ok "SKU '$name' already exists"
    continue
  fi

  info "Creating SKU '$name' (price: ${price}${PWR_DENOM})"
  run_tx tx sku create-sku \
    "$PROVIDER_UUID" "$name" 1 "${price}${PWR_DENOM}" \
    --from "$KEY_NAME" \
    "${TX_FLAGS[@]}" >/dev/null
  ok "SKU '$name' tx submitted"
  created_any=true

  # Sleep between SKU creations to avoid sequence errors
  if [[ $idx -lt $(( ${#SKU_NAMES[@]} - 1 )) ]]; then
    sleep 7
  fi
done

if [[ "$created_any" == "true" ]]; then
  info "Waiting for SKU transactions to confirm ..."
  sleep 7
fi

# ---------------------------------------------------------------------------
# 7. Query all SKU UUIDs and build mapping
# ---------------------------------------------------------------------------
info "Querying final SKU UUIDs"
for i in $(seq 1 10); do
  skus_json=$("$MANIFESTD" query sku skus-by-provider "$PROVIDER_UUID" \
    --node "$NODE" --output json 2>/dev/null) || skus_json='{"skus":[]}'
  sku_count=$(echo "$skus_json" | jq '.skus | length' 2>/dev/null) || sku_count=0
  if [[ "$sku_count" -ge "${#SKU_NAMES[@]}" ]]; then
    break
  fi
  if [[ $i -eq 10 ]]; then
    die "Expected ${#SKU_NAMES[@]} SKUs but found $sku_count"
  fi
  sleep 6
done

# Build associative arrays: name→uuid and uuid→name
declare -A SKU_UUID_BY_NAME
declare -A SKU_NAME_BY_UUID
while IFS=$'\t' read -r uuid name; do
  SKU_UUID_BY_NAME["$name"]="$uuid"
  SKU_NAME_BY_UUID["$uuid"]="$name"
done < <(echo "$skus_json" | jq -r '.skus[] | [.uuid, .name] | @tsv')

for name in "${SKU_NAMES[@]}"; do
  if [[ -z "${SKU_UUID_BY_NAME[$name]:-}" ]]; then
    die "SKU '$name' not found on-chain after creation"
  fi
  ok "SKU $name => ${SKU_UUID_BY_NAME[$name]}"
done

# ---------------------------------------------------------------------------
# 8. Generate docker-backend.yaml
# ---------------------------------------------------------------------------
DOCKER_BACKEND_FILE="$REPO_ROOT/docker-backend.yaml"
info "Writing $DOCKER_BACKEND_FILE"

cat > "$DOCKER_BACKEND_FILE" <<YAML
# Docker Backend Configuration
#
# Generated by scripts/dev-init.sh — do not commit.
# See docker-backend.example.yaml for all available options.

name: docker
listen_addr: ":9001"
docker_host: "unix:///var/run/docker.sock"

# Resource pool
total_cpu_cores: 8.0
total_memory_mb: 16384
total_disk_mb: 102400

# SKU Mapping - map on-chain SKU UUIDs to local profiles
sku_mapping:
  "${SKU_UUID_BY_NAME[docker-micro]}": "docker-micro"
  "${SKU_UUID_BY_NAME[docker-small]}": "docker-small"
  "${SKU_UUID_BY_NAME[docker-medium]}": "docker-medium"
  "${SKU_UUID_BY_NAME[docker-large]}": "docker-large"

# SKU profiles
sku_profiles:
  docker-micro:
    cpu_cores: 0.25
    memory_mb: 256
  docker-small:
    cpu_cores: 0.5
    memory_mb: 512
    disk_mb: 1024
  docker-medium:
    cpu_cores: 1.0
    memory_mb: 1024
  docker-large:
    cpu_cores: 4.0
    memory_mb: 4096
    disk_mb: 10240

# Allowed registries
allowed_registries:
  - "docker.io"
  - "ghcr.io"

# Callback configuration
callback_secret: "$CALLBACK_SECRET"
callback_insecure_skip_verify: true
callback_db_path: "callbacks.db"
callback_max_age: 24h

# Failure diagnostics persistence
diagnostics_db_path: "diagnostics.db"
diagnostics_max_age: 168h

# Release history persistence
releases_db_path: "releases.db"
releases_max_age: 2160h  # 90 days

# External address for container port mappings
host_address: "127.0.0.1"

# Volume management
volume_data_path: "/mnt/docker-btrfs/volumes"

# Timeouts
provision_timeout: 10m
image_pull_timeout: 5m
container_create_timeout: 30s
container_start_timeout: 30s
container_stop_timeout: 30s
startup_verify_duration: 5s
reconcile_interval: 5m
YAML

ok "Wrote $DOCKER_BACKEND_FILE"

# ---------------------------------------------------------------------------
# 9. Generate config.mock.yaml
# ---------------------------------------------------------------------------
CONFIG_DOCKER_FILE="$REPO_ROOT/config.docker.yaml"
info "Writing $CONFIG_DOCKER_FILE"

cat > "$CONFIG_DOCKER_FILE" <<YAML
# Manifest Provider Daemon Configuration
#
# Generated by scripts/dev-init.sh — do not commit.

production_mode: false
log_level: "info"

# Chain configuration
chain_id: "$CHAIN_ID"
grpc_endpoint: "localhost:9090"
websocket_url: "ws://localhost:26657/websocket"

# Provider identification
provider_uuid: "$PROVIDER_UUID"
provider_address: "$PROVIDER_ADDRESS"

# Keyring configuration
keyring_backend: "$KEYRING_BACKEND"
keyring_dir: "$CHAIN_HOME/"
key_name: "$KEY_NAME"

# API server
api_listen_addr: ":8080"

# TLS (self-signed for dev)
tls_cert_file: "$REPO_ROOT/cert.pem"
tls_key_file: "$REPO_ROOT/key.pem"

# Backend
backends:
  - name: docker
    url: "http://localhost:9001"
    timeout: 30s
    default: true

# Callbacks
callback_base_url: "https://localhost:8080"
callback_secret: "$CALLBACK_SECRET"

# Reconciliation
reconciliation_interval: "5m"

# Token replay protection
token_tracker_db_path: "$REPO_ROOT/tokens.db"

# Payload store
payload_store_db_path: "$REPO_ROOT/payloads.db"

# Transaction configuration
gas_limit: 1500000
gas_price: 0
fee_denom: "umfx"

# Graceful shutdown timeout
shutdown_timeout: "30s"
YAML

ok "Wrote $CONFIG_DOCKER_FILE"

# ---------------------------------------------------------------------------
# 10. Summary
# ---------------------------------------------------------------------------
echo ""
info "Dev environment initialized!"
echo ""
echo "  Provider address : $PROVIDER_ADDRESS"
echo "  Provider UUID    : $PROVIDER_UUID"
echo "  SKU mapping:"
for name in "${SKU_NAMES[@]}"; do
  printf '    %-16s => %s\n' "$name" "${SKU_UUID_BY_NAME[$name]}"
done
echo ""
echo "  Generated files:"
echo "    $DOCKER_BACKEND_FILE"
echo "    $CONFIG_DOCKER_FILE"
echo ""
echo "  Start the backend and providerd:"
echo "    ./docker-backend --config docker-backend.yaml"
echo "    ./providerd --config config.docker.yaml"
echo ""
