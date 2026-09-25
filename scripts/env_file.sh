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

# read_env FILE KEY: print the last value of KEY, without its single or double quotes.
read_env() {
  sed -n "s/^$2=//p" "$1" | tail -n 1 | sed -e "s/^'\(.*\)'\$/\1/" -e 's/^"\(.*\)"$/\1/'
}
