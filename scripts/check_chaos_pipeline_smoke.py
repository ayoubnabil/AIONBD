#!/usr/bin/env python3
"""Smoke test for scripts/run_chaos_pipeline.py."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT = Path("scripts/run_chaos_pipeline.py")


def run(args: list[str], expect_ok: bool, extra_env: dict[str, str] | None = None) -> None:
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


def main() -> int:
    if not SCRIPT.exists():
        print(f"error=missing_script path={SCRIPT}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="aionbd_chaos_pipeline_smoke_") as temp_dir:
        root = Path(temp_dir)
        report_md = root / "chaos.md"
        report_json = root / "chaos.json"

        run(
            [
                "--dry-run",
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
            raise RuntimeError("unexpected chaos pipeline rows")

        run(
            [
                "--dry-run",
                "--report-path",
                str(report_md),
                "--report-json-path",
                str(report_json),
            ],
            expect_ok=False,
            extra_env={"AIONBD_CHAOS_MIN_TESTS_SERVER": "999"},
        )

    print("ok=chaos_pipeline_smoke")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
