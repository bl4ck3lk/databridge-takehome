#!/bin/sh
# Trust the local Docker SFTP fixture's host keys under 127.0.0.1 and localhost.
# Usage: trust_sftp_fixture.sh KNOWN_HOSTS PORT
#
# The first run scans 127.0.0.1 once: trust on first use, for this local test container only.
# localhost is then trusted with the keys already trusted for 127.0.0.1, never by a new scan,
# and a rerun leaves an existing file unchanged.
set -eu

known_hosts="$1"
port="$2"
if [ "$port" = 22 ]; then
  address=127.0.0.1
  alias=localhost
else
  address="[127.0.0.1]:$port"
  alias="[localhost]:$port"
fi

(umask 077 && touch "$known_hosts")
chmod 600 "$known_hosts"

if ! ssh-keygen -F "$address" -f "$known_hosts" > /dev/null; then
  scanned="$(mktemp "${TMPDIR:-/tmp}/databridge-known-hosts.XXXXXX")"
  attempt=0
  # Keep key lines only; ssh-keyscan also prints "#" banner comments.
  until ssh-keyscan -T 2 -p "$port" 127.0.0.1 2> /dev/null | grep -v '^#' > "$scanned"; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 20 ]; then
      rm -f "$scanned"
      echo "Could not read the local SFTP server host key on port $port after 20 attempts" >&2
      exit 1
    fi
    sleep 1
  done
  cat "$scanned" >> "$known_hosts"
  rm -f "$scanned"
fi

if ! ssh-keygen -F "$alias" -f "$known_hosts" > /dev/null; then
  ssh-keygen -F "$address" -f "$known_hosts" | grep -v '^#' | sed "s/^[^ ]*/$alias/" >> "$known_hosts"
fi
