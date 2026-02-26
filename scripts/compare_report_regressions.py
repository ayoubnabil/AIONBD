#!/usr/bin/env python3
"""Compare current chaos/soak reports against versioned baselines."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def load_rows(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError(f"report rows must be a list: {path}")
    parsed: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(f"report row must be an object: {path}")
        parsed.append(row)
    return parsed


def by_key(rows: list[dict[str, object]], key: str) -> dict[str, dict[str, object]]:
    mapped: dict[str, dict[str, object]] = {}
    for row in rows:
        raw = row.get(key)
        if not isinstance(raw, str) or not raw:
            raise ValueError(f"row key '{key}' must be a non-empty string")
        mapped[raw] = row
    return mapped


def compare_soak(
    baseline: list[dict[str, object]],
    current: list[dict[str, object]],
) -> list[str]:
    min_throughput_ratio = float(
        os.environ.get("AIONBD_SOAK_REGRESSION_MIN_THROUGHPUT_RATIO", "0.95")
    )
    max_p95_ratio = float(os.environ.get("AIONBD_SOAK_REGRESSION_MAX_P95_RATIO", "1.10"))
    max_p99_ratio = float(os.environ.get("AIONBD_SOAK_REGRESSION_MAX_P99_RATIO", "1.10"))
    max_error_delta = float(
        os.environ.get("AIONBD_SOAK_REGRESSION_MAX_ERROR_RATE_DELTA", "0.001")
    )

    failures: list[str] = []
    base = by_key(baseline, "profile")
    cur = by_key(current, "profile")

    for profile, base_row in base.items():
        current_row = cur.get(profile)
        if current_row is None:
            failures.append(f"profile={profile} missing_in_current")
            continue

        base_t = float(base_row.get("throughput_ops_per_second", 0.0))
        cur_t = float(current_row.get("throughput_ops_per_second", 0.0))
        if base_t > 0.0 and (cur_t / base_t) < min_throughput_ratio:
            failures.append(
                f"profile={profile} throughput_ratio={cur_t / base_t:.6f} < min={min_throughput_ratio:.6f}"
            )

        base_p95 = float(base_row.get("latency_us_p95", 0.0))
        cur_p95 = float(current_row.get("latency_us_p95", 0.0))
        if base_p95 > 0.0 and (cur_p95 / base_p95) > max_p95_ratio:
            failures.append(
                f"profile={profile} p95_ratio={cur_p95 / base_p95:.6f} > max={max_p95_ratio:.6f}"
            )

        base_p99 = float(base_row.get("latency_us_p99", 0.0))
        cur_p99 = float(current_row.get("latency_us_p99", 0.0))
        if base_p99 > 0.0 and (cur_p99 / base_p99) > max_p99_ratio:
            failures.append(
                f"profile={profile} p99_ratio={cur_p99 / base_p99:.6f} > max={max_p99_ratio:.6f}"
            )

        base_error = float(base_row.get("error_rate", 0.0))
        cur_error = float(current_row.get("error_rate", 0.0))
        if (cur_error - base_error) > max_error_delta:
            failures.append(
                f"profile={profile} error_rate_delta={cur_error - base_error:.6f} > max={max_error_delta:.6f}"
            )

    return failures


def compare_chaos(
    baseline: list[dict[str, object]],
    current: list[dict[str, object]],
) -> list[str]:
    min_passed_ratio = float(
        os.environ.get("AIONBD_CHAOS_REGRESSION_MIN_PASSED_RATIO", "1.00")
    )
    max_duration_ratio = float(
        os.environ.get("AIONBD_CHAOS_REGRESSION_MAX_DURATION_RATIO", "5.00")
    )

    failures: list[str] = []
    base = by_key(baseline, "suite")
    cur = by_key(current, "suite")

    for suite, base_row in base.items():
        current_row = cur.get(suite)
        if current_row is None:
            failures.append(f"suite={suite} missing_in_current")
            continue

        base_passed = float(base_row.get("passed", 0.0))
        cur_passed = float(current_row.get("passed", 0.0))
        if base_passed > 0.0 and (cur_passed / base_passed) < min_passed_ratio:
            failures.append(
                f"suite={suite} passed_ratio={cur_passed / base_passed:.6f} < min={min_passed_ratio:.6f}"
            )

        base_failed = int(base_row.get("failed", 0))
        cur_failed = int(current_row.get("failed", 0))
        if cur_failed > base_failed:
            failures.append(f"suite={suite} failed={cur_failed} > baseline={base_failed}")

        base_duration = float(base_row.get("duration_seconds", 0.0))
        cur_duration = float(current_row.get("duration_seconds", 0.0))
        if base_duration > 0.0 and (cur_duration / base_duration) > max_duration_ratio:
            failures.append(
                f"suite={suite} duration_ratio={cur_duration / base_duration:.6f} > max={max_duration_ratio:.6f}"
            )

        if str(current_row.get("status")) != "ok":
            failures.append(f"suite={suite} status={current_row.get('status')} != ok")

    return failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare current report against baseline")
    parser.add_argument("--kind", choices=["soak", "chaos"], required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--current", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    baseline_path = Path(args.baseline)
    current_path = Path(args.current)
    if not baseline_path.exists() or not baseline_path.is_file():
        print(f"error=missing_baseline path={baseline_path}", file=sys.stderr)
        return 1
    if not current_path.exists() or not current_path.is_file():
        print(f"error=missing_current path={current_path}", file=sys.stderr)
        return 1

    try:
        baseline_rows = load_rows(baseline_path)
        current_rows = load_rows(current_path)
    except Exception as error:  # noqa: BLE001
        print(f"error=invalid_report_format detail={error}", file=sys.stderr)
        return 1

    failures = (
        compare_soak(baseline_rows, current_rows)
        if args.kind == "soak"
        else compare_chaos(baseline_rows, current_rows)
    )

    if failures:
        for failure in failures:
            print(f"error=report_regression kind={args.kind} {failure}", file=sys.stderr)
        return 1

    print(
        f"ok=report_regression kind={args.kind} baseline={baseline_path} current={current_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
