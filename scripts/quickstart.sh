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
ensure_env DATABRIDGE_SFTP_PORT 2222
# The fixture's user takes this UID so the bind-mounted sftp_data stays writable on Linux.
ensure_env DATABRIDGE_SFTP_UID "$(id -u)"
# One Compose project per checkout path, so checkouts never share a container or host keys.
ensure_env COMPOSE_PROJECT_NAME "databridge-$(printf '%s' "$PWD" | cksum | cut -d ' ' -f 1)"

set -a
. ./.env
set +a

mkdir -p sftp_data
docker compose up -d
sh scripts/trust_sftp_fixture.sh "$DATABRIDGE_KNOWN_HOSTS" "$DATABRIDGE_SFTP_PORT"

make check
make integration
make smoke
exec make run
