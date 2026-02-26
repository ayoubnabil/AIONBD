#!/usr/bin/env python3
"""Smoke checks for scripts/run_soak_test.py."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPT = Path("scripts/run_soak_test.py")


def run(args: list[str], expect_ok: bool) -> subprocess.CompletedProcess[str]:
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
    return completed


def main() -> int:
    if not SCRIPT.exists():
        print(f"error=missing_script path={SCRIPT}", file=sys.stderr)
        return 1

    run(
        [
            "--dry-run",
            "--base-url",
            "http://127.0.0.1:8080",
            "--collection",
            "soak_smoke",
            "--duration-seconds",
            "5",
            "--workers",
            "2",
        ],
        expect_ok=True,
    )
    run(["--dry-run", "--write-ratio", "1.1"], expect_ok=False)
    run(["--dry-run", "--duration-seconds", "0"], expect_ok=False)

    print("ok=soak_harness_smoke")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
