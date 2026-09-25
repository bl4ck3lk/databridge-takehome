#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

. scripts/env_file.sh
. scripts/sftp_fixture_user.sh

uv sync --extra dev

(umask 077 && touch .env)
chmod 600 .env

if ! grep -q '^DATABRIDGE_ENCRYPTION_KEY=' .env; then
  key="$(uv run python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
  [ -n "$key" ] || exit 1
  ensure_env .env DATABRIDGE_ENCRYPTION_KEY "$key"
fi
ensure_env .env DATABRIDGE_DB_PATH "$PWD/state/databridge.sqlite3"
ensure_env .env DATABRIDGE_KNOWN_HOSTS "$PWD/known_hosts"
ensure_env .env DATABRIDGE_SFTP_PORT 2222
# One Compose project per checkout path, so checkouts never share a container or host keys.
ensure_env .env COMPOSE_PROJECT_NAME "databridge-$(printf '%s' "$PWD" | cksum | cut -d ' ' -f 1)"

prepare_fixture_data .env sftp_data
docker compose up -d
sh scripts/trust_sftp_fixture.sh \
  "$(read_env .env DATABRIDGE_KNOWN_HOSTS)" "$(read_env .env DATABRIDGE_SFTP_PORT)"

make check
make integration
make smoke
exec make run
