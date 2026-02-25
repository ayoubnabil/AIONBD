#!/usr/bin/env python3
"""Run chaos regression suites and publish reports."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RESULT_RE = re.compile(
    r"test result: (ok|FAILED)\. "
    r"(\d+) passed; "
    r"(\d+) failed; "
    r"(\d+) ignored; "
    r"(\d+) measured; "
    r"(\d+) filtered out"
)
DURATION_RE = re.compile(r"finished in ([0-9]+(?:\.[0-9]+)?)s")

SUITES = (
    {
        "name": "core_persistence_chaos",
        "command": ["cargo", "test", "-p", "aionbd-core", "persistence::tests_chaos::"],
        "min_tests_env": "AIONBD_CHAOS_MIN_TESTS_CORE",
        "default_min_tests": 1,
        "max_duration_env": "AIONBD_CHAOS_MAX_DURATION_CORE_S",
        "dry_run_passed": 1,
        "dry_run_duration_seconds": 0.15,
    },
    {
        "name": "server_persistence_chaos",
        "command": [
            "cargo",
            "test",
            "-p",
            "aionbd-server",
            "--bin",
            "aionbd-server",
            "tests::persistence_chaos::",
        ],
        "min_tests_env": "AIONBD_CHAOS_MIN_TESTS_SERVER",
        "default_min_tests": 3,
        "max_duration_env": "AIONBD_CHAOS_MAX_DURATION_SERVER_S",
        "dry_run_passed": 3,
        "dry_run_duration_seconds": 0.45,
    },
)


def parse_output(text: str) -> dict[str, float | int | str]:
    matches = list(RESULT_RE.finditer(text))
    if not matches:
        raise RuntimeError("unable to parse cargo test summary")

    status, passed, failed, ignored, measured, filtered_out = matches[-1].groups()
    duration_matches = list(DURATION_RE.finditer(text))
    duration_seconds = float(duration_matches[-1].group(1)) if duration_matches else 0.0

    return {
        "status": "ok" if status == "ok" else "failed",
        "passed": int(passed),
        "failed": int(failed),
        "ignored": int(ignored),
        "measured": int(measured),
        "filtered_out": int(filtered_out),
        "duration_seconds": duration_seconds,
    }


def run_suite(suite: dict[str, object], dry_run: bool) -> dict[str, object]:
    if dry_run:
        return {
            "suite": str(suite["name"]),
            "status": "ok",
            "passed": int(suite["dry_run_passed"]),
            "failed": 0,
            "ignored": 0,
            "measured": 0,
            "filtered_out": 0,
            "duration_seconds": float(suite["dry_run_duration_seconds"]),
            "command": " ".join(suite["command"]),
            "exit_code": 0,
        }

    command = [str(token) for token in suite["command"]]
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)

    parsed = parse_output(f"{completed.stdout}\n{completed.stderr}")
    parsed["suite"] = str(suite["name"])
    parsed["command"] = " ".join(command)
    parsed["exit_code"] = completed.returncode
    if completed.returncode != 0:
        parsed["status"] = "failed"
    return parsed


def evaluate(result: dict[str, object], suite: dict[str, object]) -> list[str]:
    failures: list[str] = []

    min_tests = int(
        os.environ.get(str(suite["min_tests_env"]), str(suite["default_min_tests"]))
    )
    total_tests = int(result["passed"]) + int(result["failed"]) + int(result["ignored"])
    if total_tests < min_tests:
        failures.append(
            f"suite={result['suite']} total_tests={total_tests} < min={min_tests}"
        )

    max_duration_raw = os.environ.get(str(suite["max_duration_env"]), "").strip()
    if max_duration_raw:
        max_duration = float(max_duration_raw)
        if float(result["duration_seconds"]) > max_duration:
            failures.append(
                f"suite={result['suite']} duration_seconds={result['duration_seconds']} > max={max_duration}"
            )

    if str(result["status"]) != "ok":
        failures.append(
            f"suite={result['suite']} status={result['status']} exit_code={result['exit_code']}"
        )

    return failures


def markdown_report(generated_at: str, rows: list[dict[str, object]]) -> str:
    lines = [
        "# Chaos Pipeline Report",
        "",
        f"generated_at_utc: {generated_at}",
        "",
        "| suite | status | passed | failed | ignored | duration_s |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {suite} | {status} | {passed} | {failed} | {ignored} | {duration:.3f} |".format(
                suite=row["suite"],
                status=row["status"],
                passed=int(row["passed"]),
                failed=int(row["failed"]),
                ignored=int(row["ignored"]),
                duration=float(row["duration_seconds"]),
            )
        )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AIONBD chaos pipeline")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--suites", help="comma-separated suite names")
    parser.add_argument(
        "--report-path",
        default=os.environ.get(
            "AIONBD_CHAOS_REPORT_PATH", "bench/reports/chaos_pipeline_report.md"
        ),
    )
    parser.add_argument(
        "--report-json-path",
        default=os.environ.get(
            "AIONBD_CHAOS_REPORT_JSON_PATH", "bench/reports/chaos_pipeline_report.json"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    generated_at = datetime.now(timezone.utc).isoformat()

    suites = list(SUITES)
    if args.suites:
        selected = {item.strip() for item in args.suites.split(",") if item.strip()}
        suites = [suite for suite in suites if str(suite["name"]) in selected]
        if not suites:
            print("error=no_suites_selected", file=sys.stderr)
            return 1

    rows: list[dict[str, object]] = []
    failures: list[str] = []
    for suite in suites:
        result = run_suite(suite, dry_run=args.dry_run)
        rows.append(result)
        failures.extend(evaluate(result, suite))

    report_path = Path(args.report_path)
    report_json_path = Path(args.report_json_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_json_path.parent.mkdir(parents=True, exist_ok=True)

    report_path.write_text(markdown_report(generated_at, rows), encoding="utf-8")
    payload = {"generated_at_utc": generated_at, "dry_run": args.dry_run, "rows": rows}
    report_json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(f"report_markdown={report_path}")
    print(f"report_json={report_json_path}")

    if failures:
        for failure in failures:
            print(f"error=chaos_threshold_failed {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
