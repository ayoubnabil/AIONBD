#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

args=()
if [[ "${AIONBD_SOAK_DRY_RUN:-0}" == "1" ]]; then
  args+=(--dry-run)
fi

python3 scripts/run_soak_pipeline.py "${args[@]}" "$@"
