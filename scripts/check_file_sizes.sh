#!/usr/bin/env bash
set -euo pipefail

readonly MAX_LINES=300
readonly ALLOWLIST_PATH="scripts/check_file_sizes.allowlist"

is_allowlisted() {
  local file="$1"
  if [[ ! -f "$ALLOWLIST_PATH" ]]; then
    return 1
  fi

  # Keep matching explicit and deterministic: exact relative paths only.
  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    [[ "${entry:0:1}" == "#" ]] && continue
    if [[ "$file" == "$entry" ]]; then
      return 0
    fi
  done < "$ALLOWLIST_PATH"

  return 1
}

mapfile -t offenders < <(
  rg --files -g '!target/**' -g '!Cargo.lock' |
    while IFS= read -r file; do
      case "$file" in
        *.rs|*.py|*.md|*.toml|*.yml|*.yaml|*.sh)
          if is_allowlisted "$file"; then
            continue
          fi
          line_count=$(wc -l < "$file")
          if [[ "$line_count" -gt "$MAX_LINES" ]]; then
            printf '%s (%s lines)\n' "$file" "$line_count"
          fi
          ;;
        *)
          ;;
      esac
    done
)

if [[ "${#offenders[@]}" -gt 0 ]]; then
  echo "ERROR: Files must stay under ${MAX_LINES} lines unless explicitly allowlisted."
  echo "To allow a legacy oversized file, add its relative path to ${ALLOWLIST_PATH}."
  printf '%s\n' "${offenders[@]}"
  exit 1
fi

echo "OK: all checked files are <= ${MAX_LINES} lines (or allowlisted)."
