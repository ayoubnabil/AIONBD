#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

if [[ "${AIONBD_MVP_LOAD_DRY_RUN:-1}" == "1" ]]; then
  echo "running MVP load profile in dry-run mode"
  exec python3 scripts/run_mvp_load_profile.py --dry-run "$@"
fi

echo "running MVP load profile against live server"
exec python3 scripts/run_mvp_load_profile.py "$@"
