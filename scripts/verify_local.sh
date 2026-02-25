#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

usage() {
  echo "Usage: $0 [--fast|--changed]"
}

mode="${1:-full}"
if [[ "$mode" == "--help" || "$mode" == "-h" ]]; then
  usage
  echo
  echo "Runs local verification checks."
  echo "Default: full workspace checks."
  echo "--fast: reduced Rust scope for quicker local iteration."
  echo "--changed: run only checks impacted by currently changed files."
  echo "For persistence chaos checks, run: ./scripts/verify_chaos.sh"
  exit 0
fi

if [[ "$mode" != "full" && "$mode" != "--fast" && "$mode" != "--changed" ]]; then
  usage
  exit 2
fi

run_python_checks() {
  python3 -m py_compile sdk/python/aionbd/client.py
  python3 -m unittest discover -s sdk/python/tests -v
}

run_ops_checks() {
  ./scripts/check_file_sizes.sh
  python3 scripts/check_alert_runbook_sync.py
  python3 scripts/check_backup_restore_smoke.py
  python3 scripts/check_collection_export_import_smoke.py
  python3 scripts/check_soak_harness_smoke.py
}

run_rust_fast_checks() {
  cargo fmt --all --check
  cargo clippy -p aionbd-core -p aionbd-server --all-targets -- -D warnings
  cargo test -p aionbd-core -p aionbd-server
}

run_rust_full_checks() {
  cargo fmt --all --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
}

if [[ "$mode" == "full" ]]; then
  run_rust_full_checks
  run_python_checks
  run_ops_checks
  exit 0
fi

if [[ "$mode" == "--fast" ]]; then
  run_rust_fast_checks
  run_python_checks
  run_ops_checks
  exit 0
fi

declare -A changed_map=()
if git rev-parse --verify HEAD >/dev/null 2>&1; then
  while IFS= read -r file; do
    [[ -n "$file" ]] || continue
    changed_map["$file"]=1
  done < <(git diff --name-only HEAD)
fi
while IFS= read -r file; do
  [[ -n "$file" ]] || continue
  changed_map["$file"]=1
done < <(git ls-files --others --exclude-standard)

if [[ "${#changed_map[@]}" -eq 0 ]]; then
  echo "No changed files detected; nothing to verify."
  exit 0
fi

rust_changed=0
python_changed=0
ops_changed=0
for file in "${!changed_map[@]}"; do
  case "$file" in
    *.rs|Cargo.toml|Cargo.lock)
      rust_changed=1
      ;;
    sdk/python/*)
      python_changed=1
      ;;
    docs/*|ops/*|scripts/check_file_sizes.sh|scripts/check_alert_runbook_sync.py|scripts/check_backup_restore_smoke.py|scripts/check_collection_export_import_smoke.py|scripts/check_soak_harness_smoke.py|scripts/state_backup_restore.py|scripts/collection_export_import.py|scripts/run_soak_test.py|scripts/verify_local.sh)
      ops_changed=1
      ;;
  esac
done

if [[ "$rust_changed" -eq 1 ]]; then
  run_rust_fast_checks
fi

if [[ "$python_changed" -eq 1 ]]; then
  run_python_checks
fi

if [[ "$ops_changed" -eq 1 ]]; then
  run_ops_checks
fi

if [[ "$rust_changed" -eq 0 && "$python_changed" -eq 0 && "$ops_changed" -eq 0 ]]; then
  echo "No Rust, SDK Python, or ops/runbook changes detected; skipping checks."
fi
