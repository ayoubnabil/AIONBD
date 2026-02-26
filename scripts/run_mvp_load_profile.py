#!/usr/bin/env python3
"""Run and evaluate a short MVP load profile against AIONBD."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from path_guard import resolve_io_path
except ModuleNotFoundError:
    from scripts.path_guard import resolve_io_path

SOAK_SCRIPT = Path(__file__).resolve().parent / "run_soak_test.py"
REQUIRED_FIELDS = (
    "duration_seconds",
    "workers",
    "write_ratio",
    "throughput_ops_per_second",
    "error_rate",
    "latency_us_p50",
    "latency_us_p95",
    "latency_us_p99",
    "total_ops",
    "read_ops",
    "write_ops",
)
SOAK_STRICT_MAX_ERROR_RATE = 1.0


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def ensure_trusted_io_path(path: Path, *, label: str, must_exist: bool = False) -> Path:
    """Re-validate path close to sink operations for static and runtime safety."""
    resolved = path.resolve()
    workspace_root = Path.cwd().resolve()
    temp_root = Path(tempfile.gettempdir()).resolve()
    if not (_is_within(resolved, workspace_root) or _is_within(resolved, temp_root)):
        raise ValueError(
            f"{label} must stay under '{workspace_root}' or '{temp_root}': {resolved}"
        )
    if must_exist and not resolved.exists():
        raise FileNotFoundError(f"{label} does not exist: {resolved}")
    return resolved


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run MVP load profile and evaluate thresholds"
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--collection", default="mvp_load")
    parser.add_argument("--dimension", type=int, default=128)
    parser.add_argument("--duration-seconds", type=int, default=90)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--write-ratio", type=float, default=0.25)
    parser.add_argument("--point-space", type=int, default=50_000)
    parser.add_argument("--metric", default="l2", choices=["l2", "dot", "cosine"])
    parser.add_argument(
        "--search-mode", default="auto", choices=["auto", "exact", "ivf"]
    )
    parser.add_argument("--search-limit", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--strict-max-error-rate",
        type=float,
        default=float(os.environ.get("AIONBD_MVP_LOAD_MAX_ERROR_RATE", "0.05")),
    )
    parser.add_argument(
        "--strict-max-latency-us-p95",
        type=int,
        default=int(os.environ.get("AIONBD_MVP_LOAD_MAX_LATENCY_US_P95", "300000")),
    )
    parser.add_argument(
        "--strict-max-latency-us-p99",
        type=int,
        default=int(os.environ.get("AIONBD_MVP_LOAD_MAX_LATENCY_US_P99", "600000")),
    )
    parser.add_argument(
        "--strict-min-throughput-ops-per-second",
        type=float,
        default=float(
            os.environ.get("AIONBD_MVP_LOAD_MIN_THROUGHPUT_OPS_PER_SECOND", "20")
        ),
    )
    parser.add_argument(
        "--report-path",
        default=os.environ.get(
            "AIONBD_MVP_LOAD_REPORT_PATH", "bench/reports/mvp_load_profile_report.md"
        ),
    )
    parser.add_argument(
        "--report-json-path",
        default=os.environ.get(
            "AIONBD_MVP_LOAD_REPORT_JSON_PATH",
            "bench/reports/mvp_load_profile_report.json",
        ),
    )
    parser.add_argument(
        "--raw-report-path",
        default=os.environ.get(
            "AIONBD_MVP_LOAD_RAW_REPORT_PATH", "bench/reports/mvp_load_profile_raw.json"
        ),
    )
    parser.add_argument("--recreate-collection", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.duration_seconds <= 0:
        raise ValueError("duration-seconds must be > 0")
    if args.workers <= 0:
        raise ValueError("workers must be > 0")
    if args.dimension <= 0:
        raise ValueError("dimension must be > 0")
    if args.point_space <= 0:
        raise ValueError("point-space must be > 0")
    if args.search_limit <= 0:
        raise ValueError("search-limit must be > 0")
    if args.timeout_seconds <= 0:
        raise ValueError("timeout-seconds must be > 0")
    if args.seed < 0:
        raise ValueError("seed must be >= 0")
    if args.strict_max_error_rate < 0:
        raise ValueError("strict-max-error-rate must be >= 0")
    if args.strict_max_latency_us_p95 <= 0:
        raise ValueError("strict-max-latency-us-p95 must be > 0")
    if args.strict_max_latency_us_p99 <= 0:
        raise ValueError("strict-max-latency-us-p99 must be > 0")
    if args.strict_min_throughput_ops_per_second < 0:
        raise ValueError("strict-min-throughput-ops-per-second must be >= 0")
    if not (0.0 <= args.write_ratio <= 1.0):
        raise ValueError("write-ratio must be in [0.0, 1.0]")


def run_soak_profile(args: argparse.Namespace, raw_report_path: Path) -> dict[str, Any]:
    raw_report_path = ensure_trusted_io_path(raw_report_path, label="raw-report-path")
    raw_report_path.parent.mkdir(parents=True, exist_ok=True)
    if raw_report_path.exists():
        raw_report_path.unlink()

    if args.dry_run:
        return {
            "duration_seconds": float(args.duration_seconds),
            "workers": args.workers,
            "write_ratio": args.write_ratio,
            "throughput_ops_per_second": 100.0,
            "error_rate": 0.0,
            "latency_us_p50": 5_000,
            "latency_us_p95": 20_000,
            "latency_us_p99": 40_000,
            "total_ops": int(args.duration_seconds * 100),
            "read_ops": int(args.duration_seconds * 75),
            "write_ops": int(args.duration_seconds * 25),
            "error_ops": 0,
            "metric": args.metric,
            "search_mode": args.search_mode,
        }

    command = [
        sys.executable,
        str(SOAK_SCRIPT),
        "--base-url",
        args.base_url,
        "--collection",
        args.collection,
        "--dimension",
        str(args.dimension),
        "--duration-seconds",
        str(args.duration_seconds),
        "--workers",
        str(args.workers),
        "--write-ratio",
        str(args.write_ratio),
        "--point-space",
        str(args.point_space),
        "--metric",
        args.metric,
        "--search-mode",
        args.search_mode,
        "--search-limit",
        str(args.search_limit),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--seed",
        str(args.seed),
        "--strict-max-error-rate",
        str(SOAK_STRICT_MAX_ERROR_RATE),
        "--report-json",
        str(raw_report_path),
    ]
    if args.recreate_collection:
        command.append("--recreate-collection")

    completed = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode != 0:
        raise RuntimeError("mvp load soak run failed")

    payload = json.loads(
        ensure_trusted_io_path(
            raw_report_path, label="raw-report-path", must_exist=True
        ).read_text(encoding="utf-8")
    )
    for field in REQUIRED_FIELDS:
        if field not in payload:
            raise RuntimeError(f"missing required field in soak report: {field}")
    return payload


def evaluate(args: argparse.Namespace, payload: dict[str, Any]) -> list[str]:
    failures: list[str] = []

    error_rate = float(payload["error_rate"])
    p95 = int(payload["latency_us_p95"])
    p99 = int(payload["latency_us_p99"])
    throughput = float(payload["throughput_ops_per_second"])

    if error_rate > args.strict_max_error_rate:
        failures.append(
            f"error_rate {error_rate:.6f} > max {args.strict_max_error_rate:.6f}"
        )
    if p95 > args.strict_max_latency_us_p95:
        failures.append(f"latency_us_p95 {p95} > max {args.strict_max_latency_us_p95}")
    if p99 > args.strict_max_latency_us_p99:
        failures.append(f"latency_us_p99 {p99} > max {args.strict_max_latency_us_p99}")
    if throughput < args.strict_min_throughput_ops_per_second:
        failures.append(
            f"throughput_ops_per_second {throughput:.3f} < min {args.strict_min_throughput_ops_per_second:.3f}"
        )

    return failures


def markdown_report(
    generated_at: str,
    args: argparse.Namespace,
    payload: dict[str, Any],
    failures: list[str],
) -> str:
    status = "PASS" if not failures else "FAIL"
    lines = [
        "# MVP Load Profile Report",
        "",
        f"generated_at_utc: {generated_at}",
        f"status: {status}",
        f"base_url: {args.base_url}",
        f"collection: {args.collection}",
        "",
        "## Workload",
        "",
        f"- duration_seconds: {args.duration_seconds}",
        f"- workers: {args.workers}",
        f"- write_ratio: {args.write_ratio:.3f}",
        f"- point_space: {args.point_space}",
        f"- dimension: {args.dimension}",
        f"- metric: {args.metric}",
        f"- search_mode: {args.search_mode}",
        "",
        "## Results",
        "",
        "| total_ops | read_ops | write_ops | error_rate | throughput_ops_s | p50_us | p95_us | p99_us |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
        (
            f"| {int(payload['total_ops'])} | {int(payload['read_ops'])} | {int(payload['write_ops'])} | "
            f"{float(payload['error_rate']):.6f} | {float(payload['throughput_ops_per_second']):.3f} | "
            f"{int(payload['latency_us_p50'])} | {int(payload['latency_us_p95'])} | {int(payload['latency_us_p99'])} |"
        ),
        "",
        "## Thresholds",
        "",
        f"- strict_max_error_rate: {args.strict_max_error_rate:.6f}",
        f"- strict_max_latency_us_p95: {args.strict_max_latency_us_p95}",
        f"- strict_max_latency_us_p99: {args.strict_max_latency_us_p99}",
        f"- strict_min_throughput_ops_per_second: {args.strict_min_throughput_ops_per_second:.3f}",
    ]

    if failures:
        lines.extend(["", "## Failures", ""])
        for failure in failures:
            lines.append(f"- {failure}")

    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    validate_args(args)

    raw_report_path = ensure_trusted_io_path(
        resolve_io_path(args.raw_report_path, label="raw-report-path"),
        label="raw-report-path",
    )
    report_md_path = ensure_trusted_io_path(
        resolve_io_path(args.report_path, label="report-path"),
        label="report-path",
    )
    report_json_path = ensure_trusted_io_path(
        resolve_io_path(args.report_json_path, label="report-json-path"),
        label="report-json-path",
    )

    payload = run_soak_profile(args, raw_report_path)
    failures = evaluate(args, payload)
    generated_at = datetime.now(timezone.utc).isoformat()

    report_md_path.parent.mkdir(parents=True, exist_ok=True)
    report_json_path.parent.mkdir(parents=True, exist_ok=True)

    report_payload = {
        "generated_at_utc": generated_at,
        "status": "pass" if not failures else "fail",
        "thresholds": {
            "strict_max_error_rate": args.strict_max_error_rate,
            "strict_max_latency_us_p95": args.strict_max_latency_us_p95,
            "strict_max_latency_us_p99": args.strict_max_latency_us_p99,
            "strict_min_throughput_ops_per_second": args.strict_min_throughput_ops_per_second,
        },
        "workload": {
            "base_url": args.base_url,
            "collection": args.collection,
            "duration_seconds": args.duration_seconds,
            "workers": args.workers,
            "write_ratio": args.write_ratio,
            "point_space": args.point_space,
            "dimension": args.dimension,
            "metric": args.metric,
            "search_mode": args.search_mode,
            "search_limit": args.search_limit,
            "seed": args.seed,
        },
        "result": payload,
        "failures": failures,
    }

    ensure_trusted_io_path(report_md_path, label="report-path").write_text(
        markdown_report(generated_at, args, payload, failures),
        encoding="utf-8",
    )
    ensure_trusted_io_path(report_json_path, label="report-json-path").write_text(
        json.dumps(report_payload, indent=2) + "\n", encoding="utf-8"
    )

    print(f"report_markdown={report_md_path}")
    print(f"report_json={report_json_path}")

    if failures:
        for failure in failures:
            print(f"error=mvp_load_threshold_failed {failure}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"error={error}", file=sys.stderr)
        raise SystemExit(1)
