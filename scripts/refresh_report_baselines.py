#!/usr/bin/env python3
"""Refresh or verify dry-run report baselines for soak and chaos pipelines."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_VERIFY_SOAK = Path("scripts/verify_soak.sh")
SCRIPT_VERIFY_CHAOS = Path("scripts/verify_chaos.sh")


def run_command(command: list[str], env_overrides: dict[str, str]) -> None:
    env = os.environ.copy()
    env.update(env_overrides)
    completed = subprocess.run(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(command)}")


def load_rows(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError(f"invalid report rows in {path}")
    parsed: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(f"invalid row type in {path}")
        parsed.append(row)
    return parsed


def canonical_payload(kind: str, rows: list[dict[str, object]]) -> dict[str, object]:
    key = "profile" if kind == "soak" else "suite"
    ordered = sorted(rows, key=lambda row: str(row.get(key, "")))
    return {
        "baseline_kind": kind,
        "generated_by": "scripts/refresh_report_baselines.py",
        "rows": ordered,
    }


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def same_rows(
    left: list[dict[str, object]], right: list[dict[str, object]], kind: str
) -> bool:
    key = "profile" if kind == "soak" else "suite"
    left_map = {str(row.get(key, "")): row for row in left}
    right_map = {str(row.get(key, "")): row for row in right}
    return left_map == right_map


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh or verify soak/chaos dry-run baselines"
    )
    parser.add_argument("--mode", choices=["update", "check"], default="update")
    parser.add_argument(
        "--profiles-file",
        default="ops/soak/longrun_profiles.json",
        help="soak profiles file path used for dry-run baseline generation",
    )
    parser.add_argument(
        "--soak-baseline-path",
        default="ops/baselines/soak_pipeline_dryrun_baseline.json",
    )
    parser.add_argument(
        "--chaos-baseline-path",
        default="ops/baselines/chaos_pipeline_dryrun_baseline.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profiles_file = Path(args.profiles_file)
    soak_baseline_path = Path(args.soak_baseline_path)
    chaos_baseline_path = Path(args.chaos_baseline_path)

    if not SCRIPT_VERIFY_SOAK.exists() or not SCRIPT_VERIFY_CHAOS.exists():
        print("error=missing_verify_scripts", file=sys.stderr)
        return 1
    if not profiles_file.exists() or not profiles_file.is_file():
        print(f"error=missing_profiles_file path={profiles_file}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="aionbd_baseline_refresh_") as temp_dir:
        root = Path(temp_dir)
        soak_report = root / "soak_report.json"
        chaos_report = root / "chaos_report.json"

        run_command(
            [
                "./scripts/verify_soak.sh",
                "--profiles-file",
                str(profiles_file),
                "--report-path",
                str(root / "soak_report.md"),
                "--report-json-path",
                str(soak_report),
            ],
            {
                "AIONBD_SOAK_DRY_RUN": "1",
            },
        )
        run_command(
            [
                "./scripts/verify_chaos.sh",
                "--report-path",
                str(root / "chaos_report.md"),
                "--report-json-path",
                str(chaos_report),
            ],
            {
                "AIONBD_CHAOS_DRY_RUN": "1",
            },
        )

        soak_current_rows = load_rows(soak_report)
        chaos_current_rows = load_rows(chaos_report)
        soak_payload = canonical_payload("soak", soak_current_rows)
        chaos_payload = canonical_payload("chaos", chaos_current_rows)

        if args.mode == "update":
            write_json(soak_baseline_path, soak_payload)
            write_json(chaos_baseline_path, chaos_payload)
            print(f"ok=baseline_updated kind=soak path={soak_baseline_path}")
            print(f"ok=baseline_updated kind=chaos path={chaos_baseline_path}")
            return 0

        if not soak_baseline_path.exists() or not chaos_baseline_path.exists():
            print("error=baseline_files_missing_for_check", file=sys.stderr)
            return 1

        soak_existing_rows = load_rows(soak_baseline_path)
        chaos_existing_rows = load_rows(chaos_baseline_path)

        failures: list[str] = []
        if not same_rows(soak_existing_rows, soak_current_rows, "soak"):
            failures.append(f"kind=soak baseline_mismatch path={soak_baseline_path}")
        if not same_rows(chaos_existing_rows, chaos_current_rows, "chaos"):
            failures.append(f"kind=chaos baseline_mismatch path={chaos_baseline_path}")

        if failures:
            for failure in failures:
                print(f"error=baseline_check_failed {failure}", file=sys.stderr)
            return 1

        print(f"ok=baseline_check kind=soak path={soak_baseline_path}")
        print(f"ok=baseline_check kind=chaos path={chaos_baseline_path}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
