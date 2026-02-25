#!/usr/bin/env python3
"""Smoke test for scripts/run_soak_pipeline.py."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT = Path("scripts/run_soak_pipeline.py")


def run(
    args: list[str], expect_ok: bool, extra_env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
        env=env,
    )
    if expect_ok and completed.returncode != 0:
        raise RuntimeError(
            f"command failed args={args} stdout={completed.stdout} stderr={completed.stderr}"
        )
    if not expect_ok and completed.returncode == 0:
        raise RuntimeError(
            f"command unexpectedly succeeded args={args} stdout={completed.stdout}"
        )
    return completed


def main() -> int:
    if not SCRIPT.exists():
        print(f"error=missing_script path={SCRIPT}", file=sys.stderr)
        return 1
    longrun_profiles = Path("ops/soak/longrun_profiles.json")
    if not longrun_profiles.exists():
        print(f"error=missing_profiles path={longrun_profiles}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="aionbd_soak_pipeline_smoke_") as temp_dir:
        root = Path(temp_dir)
        report_md = root / "soak.md"
        report_json = root / "soak.json"

        run(
            [
                "--dry-run",
                "--profiles",
                "read_heavy,mixed",
                "--report-path",
                str(report_md),
                "--report-json-path",
                str(report_json),
            ],
            expect_ok=True,
        )
        run(
            [
                "--dry-run",
                "--profiles-file",
                str(longrun_profiles),
                "--profiles",
                "read_heavy_24h,mixed_72h",
                "--report-path",
                str(report_md),
                "--report-json-path",
                str(report_json),
            ],
            expect_ok=True,
        )

        payload = json.loads(report_json.read_text(encoding="utf-8"))
        rows = payload.get("rows", [])
        if not isinstance(rows, list) or len(rows) != 2:
            raise RuntimeError("unexpected row count in soak pipeline report")

        failing_profiles = json.dumps(
            [
                {
                    "name": "strict",
                    "duration_seconds": 5,
                    "workers": 1,
                    "write_ratio": 0.1,
                    "min_throughput_ops_per_second": 1000000.0,
                }
            ]
        )
        run(
            [
                "--dry-run",
                "--profiles",
                "strict",
                "--report-path",
                str(report_md),
                "--report-json-path",
                str(report_json),
            ],
            expect_ok=False,
            extra_env={"AIONBD_SOAK_PROFILES_JSON": failing_profiles},
        )

    print("ok=soak_pipeline_smoke")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
