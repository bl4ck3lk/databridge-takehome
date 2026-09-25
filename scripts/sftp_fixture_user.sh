# The SFTP fixture's user and its bind-mounted data directory. Load env_file.sh first.

# The image's sshd refuses every UID 0 login (PermitRootLogin no), so a root host keeps this
# UID, the image's default in docker-compose.yml.
FIXTURE_ROOT_UID=1001

# prepare_fixture_data ENV_FILE DIR: store the fixture user's UID in ENV_FILE when it lacks one,
# then create DIR for the bind mount. The fixture user takes the host user's UID, so the bind
# mount stays writable on Linux; on a root host it keeps its own UID and root hands it DIR.
prepare_fixture_data() {
  fixture_host_uid="$(id -u)"
  if [ "$fixture_host_uid" = 0 ]; then
    fixture_uid="$FIXTURE_ROOT_UID"
  else
    fixture_uid="$fixture_host_uid"
  fi
  ensure_env "$1" DATABRIDGE_SFTP_UID "$fixture_uid" || return 1
  fixture_uid="$(read_env "$1" DATABRIDGE_SFTP_UID)"
  if [ "$fixture_uid" = 0 ]; then
    echo "DATABRIDGE_SFTP_UID in $1 is 0, but the fixture's sshd refuses UID 0 logins; remove that line and rerun" >&2
    return 1
  fi
  mkdir -p "$2"
  if [ "$fixture_host_uid" = 0 ]; then
    chown "$fixture_uid" "$2"
  fi
}
