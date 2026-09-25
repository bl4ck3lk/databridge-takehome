#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

uv sync --extra dev

(umask 077 && touch .env)
chmod 600 .env

# Append a setting only when .env lacks it, so a rerun never replaces a trusted value.
ensure_env() {
  if ! grep -q "^$1=" .env; then
    printf '%s="%s"\n' "$1" "$2" >> .env
  fi
}

if ! grep -q '^DATABRIDGE_ENCRYPTION_KEY=' .env; then
  key="$(uv run python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
  [ -n "$key" ] || exit 1
  ensure_env DATABRIDGE_ENCRYPTION_KEY "$key"
fi
ensure_env DATABRIDGE_DB_PATH "$PWD/state/databridge.sqlite3"
ensure_env DATABRIDGE_KNOWN_HOSTS "$PWD/known_hosts"

mkdir -p sftp_data
docker compose up -d

if [ ! -s known_hosts ]; then
  hosts_temp="$(mktemp "${TMPDIR:-/tmp}/databridge-known-hosts.XXXXXX")"
  attempt=0
  host_ready=0
  while [ "$attempt" -lt 20 ]; do
    if ssh-keyscan -T 2 -p 2222 127.0.0.1 > "$hosts_temp" 2>/dev/null && [ -s "$hosts_temp" ]; then
      host_ready=1
      break
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  if [ "$host_ready" -ne 1 ]; then
    rm -f "$hosts_temp"
    echo "Could not read the local SFTP server host key after 20 attempts" >&2
    exit 1
  fi
  mv "$hosts_temp" known_hosts
fi
chmod 600 known_hosts

make check
make integration
make smoke
exec make run
