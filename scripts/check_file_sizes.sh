#!/usr/bin/env bash
set -euo pipefail

readonly MAX_LINES=300

mapfile -t offenders < <(
  rg --files -g '!target/**' -g '!Cargo.lock' |
    while IFS= read -r file; do
      case "$file" in
        *.rs|*.py|*.md|*.toml|*.yml|*.yaml|*.sh)
          line_count=$(wc -l < "$file")
          if [ "$line_count" -gt "$MAX_LINES" ]; then
            printf '%s (%s lines)\n' "$file" "$line_count"
          fi
          ;;
      esac
    done
)

if [ "${#offenders[@]}" -gt 0 ]; then
  echo "ERROR: Files must stay under ${MAX_LINES} lines. Split files before merging."
  printf '%s\n' "${offenders[@]}"
  exit 1
fi

echo "OK: all checked files are <= ${MAX_LINES} lines."
