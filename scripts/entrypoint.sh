#!/bin/sh
set -eu

# Entrypoint: load secrets from files referenced by environment variables
# For each environment variable named FOO_FILE, read the file at that path
# and export FOO with the file contents (trimmed). Do not print secret values.

echo "entrypoint: loading file-backed secrets (if any)"

# Iterate over env names that end with _FILE
for name in $(env | awk -F= '/_FILE=/ {print $1}'); do
  # evaluate the variable to get the file path
  eval file_path="\$$name"
  base=${name%_FILE}

  if [ -n "${file_path:-}" ] && [ -f "${file_path}" ]; then
    # Read first line (passwords usually single-line). Preserve empty files as empty string.
    secret=$(sed -n '1p' "${file_path}" | tr -d '\n') || secret=""
    # Export base var with secret value
    export "$base"="$secret"
    # Unset the _FILE var to avoid accidental exposure
    unset "$name" 2>/dev/null || true
    printf 'entrypoint: loaded secret for %s from file (hidden)\n' "$base"
  else
    printf 'entrypoint: secret file for %s not found: %s\n' "$name" "${file_path:-}" >&2
  fi
done

# Ensure no accidental debug prints of environment contains secrets
echo "entrypoint: finished loading secrets"

# Exec the supplied command (PID 1 replacement)
exec "$@"
