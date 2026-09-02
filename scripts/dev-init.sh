#!/usr/bin/env bash
# dev-init.sh — Register provider & SKUs on-chain, generate dev config files.
#
# Adapted from manifest-deploy/roles/chain-bootstrap/tasks/main.yml.
# Requires a running local chain plus manifestd, jq, curl, openssl, and findmnt.
#
# Usage:
#   bash scripts/dev-init.sh
#
# Documented settings below may be overridden with environment variables.

set -euo pipefail
# Generated YAML contains callback HMAC authority and the TLS private key may be
# created below. Make every new file private regardless of the caller's umask.
umask 077

# ---------------------------------------------------------------------------
# Configuration (override via environment)
# ---------------------------------------------------------------------------
MANIFESTD="${MANIFESTD:-manifestd}"
CHAIN_HOME="${CHAIN_HOME:-$HOME/.manifest}"
CHAIN_ID="${CHAIN_ID:-manifest-ledger-beta}"
KEYRING_BACKEND="${KEYRING_BACKEND:-test}"
KEY_NAME="${KEY_NAME:-acc0}"
NODE="${NODE:-http://localhost:26657}"
GRPC_ENDPOINT="${GRPC_ENDPOINT:-localhost:9090}"
WEBSOCKET_URL="${WEBSOCKET_URL:-ws://localhost:26657/websocket}"
GAS_PRICES="${GAS_PRICES:-0.025umfx}"
PWR_DENOM="${PWR_DENOM:-factory/manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj/upwr}"
API_URL="${API_URL:-https://localhost:8080}"
CALLBACK_BASE_URL="${CALLBACK_BASE_URL:-https://localhost:8080}"
DOCKER_BACKEND_LISTEN_ADDR="${DOCKER_BACKEND_LISTEN_ADDR:-:9001}"
DOCKER_BACKEND_URL="${DOCKER_BACKEND_URL:-http://localhost:9001}"
CALLBACK_SECRET="${CALLBACK_SECRET:-}"  # random default resolved after helpers (needs openssl)

# Physical repo root (script lives in scripts/). Fresh placement confirmation
# canonicalizes the existing parent directory, so keep the printed path on the
# same physical-path basis even when the checkout was reached through a symlink.
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
DOCKER_BACKEND_FILE="$REPO_ROOT/docker-backend.yaml"
CONFIG_DOCKER_FILE="$REPO_ROOT/config.docker.yaml"
PLACEMENT_DB_PATH="$REPO_ROOT/placements.db"
TOKEN_DB_PATH="$REPO_ROOT/tokens.db"
PAYLOAD_DB_PATH="$REPO_ROOT/payloads.db"
CALLBACK_DB_PATH="$REPO_ROOT/callbacks.db"
RELEASES_DB_PATH="$REPO_ROOT/releases.db"
RETENTION_DB_PATH="$REPO_ROOT/retention.db"
DIAGNOSTICS_DB_PATH="$REPO_ROOT/diagnostics.db"

# Volume data path. Fred writes managed volume subdirs under this path.
# Must be on a filesystem supported by fred's quota backends (btrfs, xfs
# with project quotas, or zfs). The default lives under $HOME so dev
# setups don't need sudo; for production-shaped layouts override with
# e.g. VOLUME_DATA_PATH=/var/lib/fred/volumes.
VOLUME_DATA_PATH="${VOLUME_DATA_PATH:-$HOME/fred-volumes}"
# Optional explicit active mount containing VOLUME_DATA_PATH. When omitted,
# the script derives the mount boundary with findmnt after creating the data
# directory. docker-backend verifies this mount remains present at runtime.
VOLUME_MOUNT_PATH="${VOLUME_MOUNT_PATH:-}"

# TLS cert/key paths used by providerd. The script generates a self-signed
# cert only when neither exists (and errors if exactly one is present).
CERT_FILE="${CERT_FILE:-$REPO_ROOT/cert.pem}"
KEY_FILE="${KEY_FILE:-$REPO_ROOT/key.pem}"
# Subject Alternative Names baked into the dev cert. Override for
# remote test setups (e.g. CERT_SAN="DNS:my.host,IP:10.0.0.5").
CERT_SAN="${CERT_SAN:-DNS:localhost,IP:127.0.0.1}"

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

CERT_TMP=""
KEY_TMP=""
DOCKER_BACKEND_TMP=""
CONFIG_DOCKER_TMP=""
cleanup_dev_init_temps() {
  for temporary_path in "$CERT_TMP" "$KEY_TMP" "$DOCKER_BACKEND_TMP" "$CONFIG_DOCKER_TMP"; do
    if [[ -n "$temporary_path" ]]; then
      rm -f -- "$temporary_path"
    fi
  done
}
trap cleanup_dev_init_temps EXIT

private_temp_for() {
  local target="$1"
  local parent base
  parent="$(dirname -- "$target")"
  base="$(basename -- "$target")"
  mktemp "$parent/.${base}.dev-init.XXXXXX"
}

# Publish by hard-linking a private same-directory temporary file. link(2)
# atomically refuses every existing destination, including a dangling symlink;
# unlike a checked-then-cat sequence, there is no overwrite race.
publish_private_no_replace() {
  local source="$1"
  local target="$2"
  chmod 600 -- "$source" || die "Failed to make generated file private: $source"
  ln -- "$source" "$target" \
    || die "Refusing to overwrite path created during initialization: $target"
  rm -f -- "$source" || die "Failed to remove publication temporary file: $source"
}

json_quote() {
  jq -cn --arg value "$1" '$value'
}

# Keep this byte-oriented to match hmacauth.MinSecretLength. Bash's ordinary
# ${#value} is locale-sensitive and can count Unicode characters instead.
validate_callback_secret() {
  local byte_length
  byte_length="$(printf '%s' "$CALLBACK_SECRET" | wc -c)"
  if (( byte_length < 32 )); then
    die "CALLBACK_SECRET must be at least 32 bytes (got $byte_length)"
  fi
}

# This script creates a fresh local authority; it is deliberately not a
# credential-rotation or recovery command. Refuse to overwrite prior generated
# configuration or authoritative state, including remnants of a partial run.
for existing_path in \
  "$DOCKER_BACKEND_FILE" \
  "$CONFIG_DOCKER_FILE" \
  "$PLACEMENT_DB_PATH" \
  "$TOKEN_DB_PATH" \
  "$PAYLOAD_DB_PATH" \
  "$CALLBACK_DB_PATH" \
  "$RELEASES_DB_PATH" \
  "$RETENTION_DB_PATH" \
  "$DIAGNOSTICS_DB_PATH" \
  "$CALLBACK_DB_PATH.storage-identity-anchor.json" \
  "$VOLUME_DATA_PATH/.fred-backend-storage-identity.json"; do
  if [[ -e "$existing_path" || -L "$existing_path" ]]; then
    die "fresh dev initialization refused: $existing_path already exists. Use the existing environment's normal startup path; do not rotate or recreate its authority with dev-init.sh"
  fi
done

# Default CALLBACK_SECRET to a random value. openssl is also required later to
# generate the dev TLS cert; resolve it here (after die is defined) so a missing
# openssl gives a clear message instead of an opaque set -e abort at the top.
if [[ -z "$CALLBACK_SECRET" ]]; then
  command -v openssl >/dev/null || die "openssl not found — install it, or set CALLBACK_SECRET (and pre-create CERT_FILE/KEY_FILE to skip cert generation)"
  CALLBACK_SECRET="$(openssl rand -hex 32)"
fi
validate_callback_secret

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
# 8. Ensure volume data path exists
# ---------------------------------------------------------------------------
# docker-backend rejects a volume_data_path containing whitespace; fail fast
# here rather than at backend startup (e.g. a $HOME that contains spaces).
if [[ "$VOLUME_DATA_PATH" == *[[:space:]]* ]]; then
  die "VOLUME_DATA_PATH must not contain whitespace (got: '$VOLUME_DATA_PATH') — docker-backend rejects whitespace volume paths"
fi
info "Ensuring volume data path exists: $VOLUME_DATA_PATH"
if [[ ! -d "$VOLUME_DATA_PATH" ]]; then
  mkdir -p -- "$VOLUME_DATA_PATH" \
    || die "Failed to create $VOLUME_DATA_PATH (set VOLUME_DATA_PATH= to a writable location)"
  ok "Created $VOLUME_DATA_PATH"
else
  ok "Already present"
fi
# docker-backend (same user in dev) creates per-volume subdirs here, which needs
# write + execute (search) permission — fail fast if the path isn't usable.
[[ -w "$VOLUME_DATA_PATH" && -x "$VOLUME_DATA_PATH" ]] \
  || die "$VOLUME_DATA_PATH is not writable/searchable by $(id -un) — fix its permissions or set VOLUME_DATA_PATH= to a writable location"

# Canonicalize the data root after it exists, then bind it to the active mount
# that contains it. A plain parent directory is not sufficient evidence: the
# backend refuses startup if the declared mount later disappears.
VOLUME_DATA_PATH="$(cd "$VOLUME_DATA_PATH" && pwd -P)" \
  || die "Failed to resolve the physical VOLUME_DATA_PATH"
command -v findmnt >/dev/null \
  || die "findmnt not found — install util-linux to prove the active mount containing VOLUME_DATA_PATH"
DETECTED_VOLUME_MOUNT_PATH="$(findmnt -n -o TARGET -T "$VOLUME_DATA_PATH")" \
  || die "Failed to determine the active mount containing $VOLUME_DATA_PATH"
DETECTED_VOLUME_MOUNT_PATH="$(cd "$DETECTED_VOLUME_MOUNT_PATH" && pwd -P)" \
  || die "Failed to resolve detected active mount '$DETECTED_VOLUME_MOUNT_PATH'"
if [[ -n "$VOLUME_MOUNT_PATH" ]]; then
  VOLUME_MOUNT_PATH="$(cd "$VOLUME_MOUNT_PATH" && pwd -P)" \
    || die "VOLUME_MOUNT_PATH must name an existing active mount (got: '$VOLUME_MOUNT_PATH')"
  if [[ "$VOLUME_MOUNT_PATH" != "$DETECTED_VOLUME_MOUNT_PATH" ]]; then
    die "VOLUME_MOUNT_PATH must equal the active mount containing VOLUME_DATA_PATH (configured: '$VOLUME_MOUNT_PATH', detected: '$DETECTED_VOLUME_MOUNT_PATH')"
  fi
else
  VOLUME_MOUNT_PATH="$DETECTED_VOLUME_MOUNT_PATH"
fi
if [[ "$VOLUME_MOUNT_PATH" == *[[:space:]]* ]]; then
  die "VOLUME_MOUNT_PATH must not contain whitespace (got: '$VOLUME_MOUNT_PATH') — docker-backend rejects whitespace mount paths"
fi
ok "Volume mount path: $VOLUME_MOUNT_PATH"

# ---------------------------------------------------------------------------
# 9. Ensure TLS cert + key exist
# ---------------------------------------------------------------------------
info "Ensuring TLS cert exists: $CERT_FILE"
cert_present=false
key_present=false
[[ -e "$CERT_FILE" || -L "$CERT_FILE" ]] && cert_present=true
[[ -e "$KEY_FILE" || -L "$KEY_FILE" ]] && key_present=true
if [[ "$cert_present" == "true" && "$key_present" == "true" ]]; then
  [[ -f "$CERT_FILE" && -f "$KEY_FILE" ]] \
    || die "CERT_FILE and KEY_FILE must resolve to regular files; dangling or non-file paths are refused"
  ok "Cert and key already present"
elif [[ "$cert_present" == "true" || "$key_present" == "true" ]]; then
  die "One of $CERT_FILE / $KEY_FILE exists but the other is missing — remove the stray file and re-run, or set CERT_FILE/KEY_FILE to a fresh pair"
else
  command -v openssl >/dev/null || die "openssl not found; install it or pre-create $CERT_FILE and $KEY_FILE"
  CERT_TMP="$(private_temp_for "$CERT_FILE")" \
    || die "Failed to create a private certificate temporary beside $CERT_FILE"
  KEY_TMP="$(private_temp_for "$KEY_FILE")" \
    || die "Failed to create a private key temporary beside $KEY_FILE"
  if ! ssl_err=$(openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -keyout "$KEY_TMP" -out "$CERT_TMP" \
    -subj /CN=localhost \
    -addext "subjectAltName=$CERT_SAN" 2>&1); then
    die "openssl cert generation failed: $ssl_err"
  fi
  publish_private_no_replace "$CERT_TMP" "$CERT_FILE"
  CERT_TMP=""
  publish_private_no_replace "$KEY_TMP" "$KEY_FILE"
  KEY_TMP=""
  ok "Generated $CERT_FILE (SAN: $CERT_SAN)"
fi

# ---------------------------------------------------------------------------
# 10. Generate docker-backend.yaml
# ---------------------------------------------------------------------------
info "Writing $DOCKER_BACKEND_FILE"

DOCKER_BACKEND_LISTEN_ADDR_JSON="$(json_quote "$DOCKER_BACKEND_LISTEN_ADDR")" \
  || die "Failed to encode DOCKER_BACKEND_LISTEN_ADDR"
CALLBACK_SECRET_JSON="$(json_quote "$CALLBACK_SECRET")" \
  || die "Failed to encode CALLBACK_SECRET"
VOLUME_DATA_PATH_JSON="$(json_quote "$VOLUME_DATA_PATH")" \
  || die "Failed to encode VOLUME_DATA_PATH"
VOLUME_MOUNT_PATH_JSON="$(json_quote "$VOLUME_MOUNT_PATH")" \
  || die "Failed to encode VOLUME_MOUNT_PATH"
CALLBACK_DB_PATH_JSON="$(json_quote "$CALLBACK_DB_PATH")" \
  || die "Failed to encode callback database path"
RELEASES_DB_PATH_JSON="$(json_quote "$RELEASES_DB_PATH")" \
  || die "Failed to encode release database path"
RETENTION_DB_PATH_JSON="$(json_quote "$RETENTION_DB_PATH")" \
  || die "Failed to encode retention database path"
DIAGNOSTICS_DB_PATH_JSON="$(json_quote "$DIAGNOSTICS_DB_PATH")" \
  || die "Failed to encode diagnostics database path"
SKU_DOCKER_MICRO_JSON="$(json_quote "${SKU_UUID_BY_NAME[docker-micro]}")" \
  || die "Failed to encode docker-micro SKU UUID"
SKU_DOCKER_SMALL_JSON="$(json_quote "${SKU_UUID_BY_NAME[docker-small]}")" \
  || die "Failed to encode docker-small SKU UUID"
SKU_DOCKER_MEDIUM_JSON="$(json_quote "${SKU_UUID_BY_NAME[docker-medium]}")" \
  || die "Failed to encode docker-medium SKU UUID"
SKU_DOCKER_LARGE_JSON="$(json_quote "${SKU_UUID_BY_NAME[docker-large]}")" \
  || die "Failed to encode docker-large SKU UUID"
DOCKER_BACKEND_TMP="$(private_temp_for "$DOCKER_BACKEND_FILE")" \
  || die "Failed to create a private Docker config temporary"

cat >| "$DOCKER_BACKEND_TMP" <<YAML
# Docker Backend Configuration
#
# Generated by scripts/dev-init.sh — do not commit.
# See docker-backend.example.yaml for all available options.

name: docker
listen_addr: $DOCKER_BACKEND_LISTEN_ADDR_JSON
docker_host: "unix:///var/run/docker.sock"

# Resource pool
total_cpu_cores: 8.0
total_memory_mb: 16384
total_disk_mb: 102400

# SKU Mapping - map on-chain SKU UUIDs to local profiles
sku_mapping:
  $SKU_DOCKER_MICRO_JSON: "docker-micro"
  $SKU_DOCKER_SMALL_JSON: "docker-small"
  $SKU_DOCKER_MEDIUM_JSON: "docker-medium"
  $SKU_DOCKER_LARGE_JSON: "docker-large"

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
callback_secret: $CALLBACK_SECRET_JSON
# Local dev only: production_mode stays false, which permits the insecure
# skip-verify below. Setting production_mode: true makes the backend reject it
# at startup (ENG-321). Never ship a production config with skip-verify enabled.
production_mode: false
callback_insecure_skip_verify: true
callback_db_path: $CALLBACK_DB_PATH_JSON
callback_max_age: 24h

# Failure diagnostics persistence
diagnostics_db_path: $DIAGNOSTICS_DB_PATH_JSON
diagnostics_max_age: 168h

# Release history persistence
releases_db_path: $RELEASES_DB_PATH_JSON
releases_max_age: 2160h  # 90 days

# Retained-data authority (required even when retention is disabled)
retention_db_path: $RETENTION_DB_PATH_JSON

# External address for container port mappings
host_address: "127.0.0.1"

# Volume management
volume_data_path: $VOLUME_DATA_PATH_JSON
volume_mount_path: $VOLUME_MOUNT_PATH_JSON

# Timeouts
provision_timeout: 10m
image_pull_timeout: 5m
container_create_timeout: 30s
container_start_timeout: 30s
container_stop_timeout: 30s
startup_verify_duration: 5s
reconcile_interval: 5m
YAML

publish_private_no_replace "$DOCKER_BACKEND_TMP" "$DOCKER_BACKEND_FILE"
DOCKER_BACKEND_TMP=""
ok "Wrote $DOCKER_BACKEND_FILE"

# ---------------------------------------------------------------------------
# 11. Generate config.docker.yaml
# ---------------------------------------------------------------------------
info "Writing $CONFIG_DOCKER_FILE"

CHAIN_ID_JSON="$(json_quote "$CHAIN_ID")" || die "Failed to encode CHAIN_ID"
GRPC_ENDPOINT_JSON="$(json_quote "$GRPC_ENDPOINT")" || die "Failed to encode GRPC_ENDPOINT"
WEBSOCKET_URL_JSON="$(json_quote "$WEBSOCKET_URL")" || die "Failed to encode WEBSOCKET_URL"
PROVIDER_UUID_JSON="$(json_quote "$PROVIDER_UUID")" || die "Failed to encode provider UUID"
PROVIDER_ADDRESS_JSON="$(json_quote "$PROVIDER_ADDRESS")" || die "Failed to encode provider address"
KEYRING_BACKEND_JSON="$(json_quote "$KEYRING_BACKEND")" || die "Failed to encode KEYRING_BACKEND"
KEYRING_DIR_JSON="$(json_quote "$CHAIN_HOME/")" || die "Failed to encode keyring directory"
KEY_NAME_JSON="$(json_quote "$KEY_NAME")" || die "Failed to encode KEY_NAME"
CERT_FILE_JSON="$(json_quote "$CERT_FILE")" || die "Failed to encode CERT_FILE"
KEY_FILE_JSON="$(json_quote "$KEY_FILE")" || die "Failed to encode KEY_FILE"
DOCKER_BACKEND_URL_JSON="$(json_quote "$DOCKER_BACKEND_URL")" \
  || die "Failed to encode DOCKER_BACKEND_URL"
CALLBACK_BASE_URL_JSON="$(json_quote "$CALLBACK_BASE_URL")" \
  || die "Failed to encode CALLBACK_BASE_URL"
TOKEN_DB_PATH_JSON="$(json_quote "$TOKEN_DB_PATH")" || die "Failed to encode token database path"
PAYLOAD_DB_PATH_JSON="$(json_quote "$PAYLOAD_DB_PATH")" || die "Failed to encode payload database path"
PLACEMENT_DB_PATH_JSON="$(json_quote "$PLACEMENT_DB_PATH")" \
  || die "Failed to encode placement database path"
CONFIG_DOCKER_TMP="$(private_temp_for "$CONFIG_DOCKER_FILE")" \
  || die "Failed to create a private providerd config temporary"

cat >| "$CONFIG_DOCKER_TMP" <<YAML
# Manifest Provider Daemon Configuration
#
# Generated by scripts/dev-init.sh — do not commit.

production_mode: false
log_level: "info"

# Chain configuration
chain_id: $CHAIN_ID_JSON
grpc_endpoint: $GRPC_ENDPOINT_JSON
websocket_url: $WEBSOCKET_URL_JSON

# Provider identification
provider_uuid: $PROVIDER_UUID_JSON
provider_address: $PROVIDER_ADDRESS_JSON

# Keyring configuration
keyring_backend: $KEYRING_BACKEND_JSON
keyring_dir: $KEYRING_DIR_JSON
key_name: $KEY_NAME_JSON

# API server
api_listen_addr: ":8080"

# CORS (optional)
# Allowed origins for browser-based clients (e.g. the manifest-admin SPA).
# Defaults to ["*"] (all origins) when omitted. Set to an empty list
# (cors_origins: []) to disable CORS middleware — fred logs a startup
# warning and browsers will block cross-origin requests. Non-browser
# clients (CLI, server-to-server) are unaffected. Only GET/POST are
# allowed; permitted headers are Authorization and Content-Type;
# credentials are disabled.
# cors_origins:
#   - "http://localhost:5173"

# TLS (self-signed for dev)
tls_cert_file: $CERT_FILE_JSON
tls_key_file: $KEY_FILE_JSON

# Backend
backends:
  - name: docker
    url: $DOCKER_BACKEND_URL_JSON
    timeout: 30s
    default: true
    hmac_secret: $CALLBACK_SECRET_JSON

# Callbacks
callback_base_url: $CALLBACK_BASE_URL_JSON

# Reconciliation
reconciliation_interval: "5m"

# Token replay protection
token_tracker_db_path: $TOKEN_DB_PATH_JSON

# Payload store
payload_store_db_path: $PAYLOAD_DB_PATH_JSON

# Durable lease-to-backend placement authority
placement_store_db_path: $PLACEMENT_DB_PATH_JSON

# Transaction configuration
gas_limit: 1500000
gas_price: 0
fee_denom: "umfx"

# Graceful shutdown timeout
shutdown_timeout: "30s"
YAML

publish_private_no_replace "$CONFIG_DOCKER_TMP" "$CONFIG_DOCKER_FILE"
CONFIG_DOCKER_TMP=""
ok "Wrote $CONFIG_DOCKER_FILE"

# ---------------------------------------------------------------------------
# 12. Summary
# ---------------------------------------------------------------------------
echo ""
info "Dev environment initialized!"
echo ""
echo "  Provider address : $PROVIDER_ADDRESS"
echo "  Provider UUID    : $PROVIDER_UUID"
echo "  Volume data path : $VOLUME_DATA_PATH"
echo "  Volume mount path: $VOLUME_MOUNT_PATH"
echo "  TLS cert         : $CERT_FILE"
echo "  TLS key          : $KEY_FILE"
echo "  SKU mapping:"
for name in "${SKU_NAMES[@]}"; do
  printf '    %-16s => %s\n' "$name" "${SKU_UUID_BY_NAME[$name]}"
done
echo ""
echo "  Generated files:"
echo "    $DOCKER_BACKEND_FILE"
echo "    $CONFIG_DOCKER_FILE"
echo ""
printf '  In each terminal, first run: cd %q\n' "$REPO_ROOT"
echo ""
echo "  Complete the fresh, offline authority bootstrap (keep tenant ingress stopped):"
echo "    ./build/docker-backend -config docker-backend.yaml -initialize-storage-identity new"
echo ""
echo "  Then keep the normal backend running in terminal A:"
echo "    ./build/docker-backend -config docker-backend.yaml"
echo ""
echo "  With terminal A running and the backend still empty, initialize placement in terminal B:"
echo "    fresh_confirmation=\"\$(./build/placement-preflight -config config.docker.yaml -print-fresh-confirmation -expected-backends '[\"docker\"]')\""
echo "    ./build/placement-preflight -config config.docker.yaml -initialize-fresh \\"
echo "      -expected-backends '[\"docker\"]' \\"
echo "      -confirm-insecure-chain 'I ACCEPT UNAUTHENTICATED CHAIN EVIDENCE FOR LOCAL DEVELOPMENT' \\"
echo '      -confirm-quiesced "$fresh_confirmation"'
echo ""
echo "  Only after placement-preflight ends with INITIALIZED_FOR_CUTOVER, start providerd:"
echo "    ./build/providerd --config config.docker.yaml"
echo ""
