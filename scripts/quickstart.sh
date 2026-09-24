#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

uv sync --extra dev

if [ ! -f .env ]; then
  env_temp="$(mktemp .env.XXXXXX)"
  if ! uv run python -c 'from cryptography.fernet import Fernet; print("DATABRIDGE_ENCRYPTION_KEY=" + Fernet.generate_key().decode())' > "$env_temp" || [ ! -s "$env_temp" ]; then
    rm -f "$env_temp"
    exit 1
  fi
  mv "$env_temp" .env
fi
chmod 600 .env

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
