#!/usr/bin/env python3
"""Smoke test for scripts/refresh_report_baselines.py."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT = Path("scripts/refresh_report_baselines.py")
PROFILES = Path("ops/soak/longrun_profiles.json")


def run(args: list[str], expect_ok: bool) -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if expect_ok and completed.returncode != 0:
        raise RuntimeError(
            f"command failed args={args} stdout={completed.stdout} stderr={completed.stderr}"
        )
    if not expect_ok and completed.returncode == 0:
        raise RuntimeError(
            f"command unexpectedly succeeded args={args} stdout={completed.stdout}"
        )


def main() -> int:
    if not SCRIPT.exists():
        print(f"error=missing_script path={SCRIPT}", file=sys.stderr)
        return 1
    if not PROFILES.exists():
        print(f"error=missing_profiles path={PROFILES}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="aionbd_refresh_baseline_smoke_") as temp_dir:
        root = Path(temp_dir)
        soak_path = root / "soak_baseline.json"
        chaos_path = root / "chaos_baseline.json"

        run(
            [
                "--mode",
                "update",
                "--profiles-file",
                str(PROFILES),
                "--soak-baseline-path",
                str(soak_path),
                "--chaos-baseline-path",
                str(chaos_path),
            ],
            expect_ok=True,
        )

        run(
            [
                "--mode",
                "check",
                "--profiles-file",
                str(PROFILES),
                "--soak-baseline-path",
                str(soak_path),
                "--chaos-baseline-path",
                str(chaos_path),
            ],
            expect_ok=True,
        )

        mutated = json.loads(soak_path.read_text(encoding="utf-8"))
        rows = mutated.get("rows", [])
        if isinstance(rows, list) and rows:
            rows[0]["throughput_ops_per_second"] = 0.0
            soak_path.write_text(json.dumps(mutated, indent=2) + "\n", encoding="utf-8")

        run(
            [
                "--mode",
                "check",
                "--profiles-file",
                str(PROFILES),
                "--soak-baseline-path",
                str(soak_path),
                "--chaos-baseline-path",
                str(chaos_path),
            ],
            expect_ok=False,
        )

    print("ok=refresh_report_baselines_smoke")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
