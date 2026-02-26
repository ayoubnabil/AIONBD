#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REQUIRE_DOCKER="${AIONBD_MVP_RELEASE_REQUIRE_DOCKER:-0}"

cd "$ROOT_DIR"

echo "[1/4] checking server compiles"
cargo check -p aionbd-server

echo "[2/4] running server tests"
cargo test -p aionbd-server

echo "[3/4] validating MVP load profile gate"
"$ROOT_DIR/scripts/verify_mvp_load_profile.sh"

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  echo "[4/4] running container smoke"
  "$ROOT_DIR/scripts/smoke_container_mvp.sh"
else
  if [[ "$REQUIRE_DOCKER" == "1" ]]; then
    echo "[4/4] docker daemon is required but unavailable"
    exit 1
  fi
  echo "[4/4] skipping container smoke (docker daemon not available)"
fi

echo "mvp release verification passed"
