#!/usr/bin/env python3
"""Smoke test for scripts/compare_report_regressions.py."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT = Path("scripts/compare_report_regressions.py")


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


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    if not SCRIPT.exists():
        print(f"error=missing_script path={SCRIPT}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="aionbd_report_regression_smoke_") as temp_dir:
        root = Path(temp_dir)

        soak_base = root / "soak_base.json"
        soak_cur_ok = root / "soak_cur_ok.json"
        soak_cur_bad = root / "soak_cur_bad.json"
        write_json(
            soak_base,
            {
                "rows": [
                    {
                        "profile": "p",
                        "throughput_ops_per_second": 100.0,
                        "latency_us_p95": 1000,
                        "latency_us_p99": 2000,
                        "error_rate": 0.0,
                    }
                ]
            },
        )
        write_json(
            soak_cur_ok,
            {
                "rows": [
                    {
                        "profile": "p",
                        "throughput_ops_per_second": 100.0,
                        "latency_us_p95": 1000,
                        "latency_us_p99": 2000,
                        "error_rate": 0.0,
                    }
                ]
            },
        )
        write_json(
            soak_cur_bad,
            {
                "rows": [
                    {
                        "profile": "p",
                        "throughput_ops_per_second": 10.0,
                        "latency_us_p95": 9000,
                        "latency_us_p99": 12000,
                        "error_rate": 0.5,
                    }
                ]
            },
        )

        run(
            ["--kind", "soak", "--baseline", str(soak_base), "--current", str(soak_cur_ok)],
            expect_ok=True,
        )
        run(
            ["--kind", "soak", "--baseline", str(soak_base), "--current", str(soak_cur_bad)],
            expect_ok=False,
        )

        chaos_base = root / "chaos_base.json"
        chaos_cur_ok = root / "chaos_cur_ok.json"
        chaos_cur_bad = root / "chaos_cur_bad.json"
        write_json(
            chaos_base,
            {"rows": [{"suite": "s", "status": "ok", "passed": 3, "failed": 0, "duration_seconds": 1.0}]},
        )
        write_json(
            chaos_cur_ok,
            {"rows": [{"suite": "s", "status": "ok", "passed": 3, "failed": 0, "duration_seconds": 2.0}]},
        )
        write_json(
            chaos_cur_bad,
            {"rows": [{"suite": "s", "status": "failed", "passed": 0, "failed": 1, "duration_seconds": 20.0}]},
        )

        run(
            ["--kind", "chaos", "--baseline", str(chaos_base), "--current", str(chaos_cur_ok)],
            expect_ok=True,
        )
        run(
            ["--kind", "chaos", "--baseline", str(chaos_base), "--current", str(chaos_cur_bad)],
            expect_ok=False,
        )

    print("ok=report_regressions_smoke")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
