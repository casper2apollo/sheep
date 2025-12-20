#!/usr/bin/env bash
set -euo pipefail

# Entrypoint script to write Kafka Producer and Consumer property files

# Load .env file if present
if [ -f ".env" ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^[[:space:]]*#' .env | xargs)
  echo "[entrypoint] Loaded .env file"
fi

echo "[entrypoint] Generating Kafka property files from environment variables..."
echo " - Producer properties: ${KAFKA_PRODUCER_PROPERTIES_PATH:-<unset>}"
echo

# write key=value pair only if value is non-empty and not "None"
write_prop() {
  local key="$1"
  local value="${2:-}"
  local out_file="$3"
  if [ -n "${value}" ] && [ "${value}" != "None" ]; then
    echo "${key}=${value}" >> "${out_file}"
  fi
}

# resolve project root (directory containing this script, then ..)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# defaults if not set
: "${KAFKA_PRODUCER_PROPERTIES_PATH:=src/config/kafka_producer.properties}"

# resolve absolute/relative paths safely
resolve_path() {
  local p="$1"
  if [[ "$p" = /* ]]; then
    echo "$p"
  else
    echo "${PROJECT_ROOT}/${p}"
  fi
}

producer_path="$(resolve_path "${KAFKA_PRODUCER_PROPERTIES_PATH}")"

# ensure parent dirs exist
mkdir -p "$(dirname "${producer_path}")"

# safety: refuse to write to a directory
if [ -d "${producer_path}" ]; then
  echo "[entrypoint] ERROR: producer properties path is a directory: ${producer_path}" >&2
  exit 1
fi

# ---
# Generate PRODUCER config
# ---
echo "[producer]" > "${producer_path}"

write_prop "bootstrap_servers"   "${KAFKA_PRODUCER_BOOTSTRAP_SERVERS:-}"            "${producer_path}"
write_prop "topic"               "${KAFKA_OUTPUT_TOPIC:-}"                          "${producer_path}"
write_prop "acks"                "${KAFKA_PRODUCER_ACKS:-all}"                      "${producer_path}"
write_prop "compression_type"    "${KAFKA_PRODUCER_COMPRESSION_TYPE:-gzip}"         "${producer_path}"
write_prop "linger_ms"           "${KAFKA_PRODUCER_LINGER_MS:-5}"                   "${producer_path}"
write_prop "retries"             "${KAFKA_PRODUCER_RETRIES:-3}"                     "${producer_path}"
write_prop "client_id"           "${KAFKA_PRODUCER_CLIENT_ID:-}"                    "${producer_path}"
write_prop "security_protocol"   "${KAFKA_PRODUCER_SECURITY_PROTOCOL:-PLAINTEXT}"   "${producer_path}"
write_prop "sasl_mechanism"      "${KAFKA_PRODUCER_SASL_MECHANISM:-}"               "${producer_path}"
write_prop "sasl_plain_username" "${KAFKA_PRODUCER_SASL_PLAIN_USERNAME:-}"          "${producer_path}"
write_prop "sasl_plain_password" "${KAFKA_PRODUCER_SASL_PLAIN_PASSWORD:-}"          "${producer_path}"

echo
echo "[entrypoint] Kafka configuration files created successfully."
echo "  - ${producer_path}"
echo

# only exec if command provided
if [ "$#" -gt 0 ]; then
  exec "$@"
fi
