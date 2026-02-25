#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cargo test -p aionbd-core persistence::tests_chaos::
cargo test -p aionbd-server --bin aionbd-server tests::persistence_chaos::
