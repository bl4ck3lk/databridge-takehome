# Read and append .env settings without running the file as shell code.
# Values are written in single quotes, which the shell, uv, and Docker Compose all read literally.

# ensure_env FILE KEY VALUE: append KEY only when FILE lacks it, so a rerun never replaces a
# trusted value.
ensure_env() {
  case "$3" in
    *"'"*)
      echo "Cannot store $2 in $1: its value contains a single quote" >&2
      return 1
      ;;
  esac
  if ! grep -q "^$2=" "$1" 2> /dev/null; then
    printf "%s='%s'\n" "$2" "$3" >> "$1"
  fi
}

# read_env FILE KEY: print the last value of KEY without its single quotes. A double-quoted value
# is refused: Docker Compose and uv apply escape and interpolation rules inside double quotes,
# which this reader does not, so it would read a different value than they do.
read_env() {
  env_value="$(sed -n "s/^$2=//p" "$1" | tail -n 1)"
  case "$env_value" in
    \"*)
      echo "$2 in $1 is double-quoted; write it as $2='value'" >&2
      return 1
      ;;
  esac
  printf '%s\n' "$env_value" | sed -e "s/^'\(.*\)'\$/\1/"
}
