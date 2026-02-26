#!/usr/bin/env python3
"""Run soak profiles and publish comparative reports."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from path_guard import resolve_io_path, safe_name_component
except ModuleNotFoundError:
    from scripts.path_guard import resolve_io_path, safe_name_component

SOAK_SCRIPT = Path(__file__).resolve().parent / "run_soak_test.py"
REQUIRED_REPORT_FIELDS = (
    "duration_seconds",
    "workers",
    "write_ratio",
    "throughput_ops_per_second",
    "error_rate",
    "latency_us_p50",
    "latency_us_p95",
    "latency_us_p99",
)


def parse_inf_env(key: str) -> float:
    raw = os.environ.get(key)
    return float("inf") if raw is None or raw.strip() == "" else float(raw)


def default_profiles() -> list[dict[str, object]]:
    base = {
        "duration_seconds": 600,
        "workers": 8,
        "write_ratio": 0.2,
        "metric": "l2",
        "search_mode": "auto",
        "search_limit": 10,
        "point_space": 100_000,
        "dimension": 256,
        "timeout_seconds": 5.0,
        "strict_max_error_rate": float(
            os.environ.get("AIONBD_SOAK_MAX_ERROR_RATE", "0.05")
        ),
        "min_throughput_ops_per_second": float(
            os.environ.get("AIONBD_SOAK_MIN_THROUGHPUT_OPS_PER_SECOND", "0")
        ),
        "max_latency_us_p95": parse_inf_env("AIONBD_SOAK_MAX_LATENCY_US_P95"),
        "max_latency_us_p99": parse_inf_env("AIONBD_SOAK_MAX_LATENCY_US_P99"),
    }
    return [
        {**base, "name": "read_heavy", "write_ratio": 0.10},
        {**base, "name": "mixed", "write_ratio": 0.30},
    ]


def validate_profile(profile: dict[str, object]) -> None:
    name = str(profile.get("name", ""))
    if not name:
        raise ValueError("profile name must not be empty")
    checks = [
        (int(profile["duration_seconds"]) > 0, "duration_seconds must be > 0"),
        (int(profile["workers"]) > 0, "workers must be > 0"),
        (
            0.0 <= float(profile["write_ratio"]) <= 1.0,
            "write_ratio must be in [0.0, 1.0]",
        ),
        (int(profile["search_limit"]) > 0, "search_limit must be > 0"),
        (int(profile["point_space"]) > 0, "point_space must be > 0"),
        (int(profile["dimension"]) > 0, "dimension must be > 0"),
        (float(profile["timeout_seconds"]) > 0, "timeout_seconds must be > 0"),
    ]
    for ok, message in checks:
        if not ok:
            raise ValueError(f"profile={name} {message}")


def load_profiles() -> list[dict[str, object]]:
    defaults = default_profiles()
    raw_json = os.environ.get("AIONBD_SOAK_PROFILES_JSON", "").strip()
    if not raw_json:
        profiles = defaults
    else:
        parsed = json.loads(raw_json)
        if not isinstance(parsed, list):
            raise ValueError("AIONBD_SOAK_PROFILES_JSON must be a JSON list")
        base = defaults[0]
        profiles = [{**base, **item} for item in parsed if isinstance(item, dict)]
    if not profiles:
        raise ValueError("no soak profiles configured")
    for profile in profiles:
        validate_profile(profile)
    return profiles


def dry_run_result(profile: dict[str, object]) -> dict[str, object]:
    write_ratio = float(profile["write_ratio"])
    workers = int(profile["workers"])
    duration = int(profile["duration_seconds"])
    throughput = max(1.0, workers * (85.0 - (write_ratio * 20.0)))
    total_ops = int(duration * throughput)
    write_ops = int(total_ops * write_ratio)
    return {
        "duration_seconds": float(duration),
        "workers": workers,
        "metric": profile["metric"],
        "search_mode": profile["search_mode"],
        "write_ratio": write_ratio,
        "total_ops": total_ops,
        "read_ops": total_ops - write_ops,
        "write_ops": write_ops,
        "error_ops": 0,
        "error_rate": 0.0,
        "throughput_ops_per_second": throughput,
        "latency_us_p50": int(2_000 + (write_ratio * 2_000)),
        "latency_us_p95": int(10_000 + (write_ratio * 12_000)),
        "latency_us_p99": int(20_000 + (write_ratio * 20_000)),
    }


def run_profile(
    profile: dict[str, object],
    base_url: str,
    collection_prefix: str,
    reports_dir: Path,
    recreate_collections: bool,
    dry_run: bool,
) -> dict[str, object]:
    if dry_run:
        return dry_run_result(profile)
    reports_dir.mkdir(parents=True, exist_ok=True)
    profile_name = safe_name_component(str(profile["name"]), label="profile name")
    report_path = reports_dir / f"{profile_name}.json"
    command = [
        sys.executable,
        str(SOAK_SCRIPT),
        "--base-url",
        base_url,
        "--collection",
        f"{collection_prefix}_{profile_name}",
        "--dimension",
        str(profile["dimension"]),
        "--duration-seconds",
        str(profile["duration_seconds"]),
        "--workers",
        str(profile["workers"]),
        "--write-ratio",
        str(profile["write_ratio"]),
        "--point-space",
        str(profile["point_space"]),
        "--metric",
        str(profile["metric"]),
        "--search-mode",
        str(profile["search_mode"]),
        "--search-limit",
        str(profile["search_limit"]),
        "--timeout-seconds",
        str(profile["timeout_seconds"]),
        "--strict-max-error-rate",
        str(profile["strict_max_error_rate"]),
        "--report-json",
        str(report_path),
    ]
    if recreate_collections:
        command.append("--recreate-collection")
    completed = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode != 0:
        raise RuntimeError(f"soak profile '{profile_name}' failed")
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    for field in REQUIRED_REPORT_FIELDS:
        if field not in payload:
            raise RuntimeError(
                f"soak profile '{profile_name}' missing report field: {field}"
            )
    return payload


def evaluate(profile: dict[str, object], row: dict[str, object]) -> list[str]:
    name = str(profile["name"])
    failures: list[str] = []
    if float(row["error_rate"]) > float(profile["strict_max_error_rate"]):
        failures.append(
            f"profile={name} error_rate={row['error_rate']} > max={profile['strict_max_error_rate']}"
        )
    if float(row["throughput_ops_per_second"]) < float(
        profile["min_throughput_ops_per_second"]
    ):
        failures.append(
            f"profile={name} throughput_ops_per_second={row['throughput_ops_per_second']} < min={profile['min_throughput_ops_per_second']}"
        )
    if float(row["latency_us_p95"]) > float(profile["max_latency_us_p95"]):
        failures.append(
            f"profile={name} latency_us_p95={row['latency_us_p95']} > max={profile['max_latency_us_p95']}"
        )
    if float(row["latency_us_p99"]) > float(profile["max_latency_us_p99"]):
        failures.append(
            f"profile={name} latency_us_p99={row['latency_us_p99']} > max={profile['max_latency_us_p99']}"
        )
    return failures


def markdown_report(generated_at: str, rows: list[dict[str, object]]) -> str:
    lines = [
        "# Soak Pipeline Report",
        "",
        f"generated_at_utc: {generated_at}",
        "",
        "| profile | duration_s | workers | write_ratio | throughput_ops_s | error_rate | p50_us | p95_us | p99_us |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {profile} | {duration:.3f} | {workers} | {write_ratio:.3f} | {throughput:.3f} | {error_rate:.6f} | {p50} | {p95} | {p99} |".format(
                profile=row["profile"],
                duration=float(row["duration_seconds"]),
                workers=int(row["workers"]),
                write_ratio=float(row["write_ratio"]),
                throughput=float(row["throughput_ops_per_second"]),
                error_rate=float(row["error_rate"]),
                p50=int(row["latency_us_p50"]),
                p95=int(row["latency_us_p95"]),
                p99=int(row["latency_us_p99"]),
            )
        )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run AIONBD soak profiles and report results"
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--collection-prefix", default="soak_pipeline")
    parser.add_argument("--profiles", help="comma-separated profile names to run")
    parser.add_argument("--profiles-file", help="path to JSON list of soak profiles")
    parser.add_argument("--recreate-collections", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--report-path",
        default=os.environ.get(
            "AIONBD_SOAK_REPORT_PATH", "bench/reports/soak_pipeline_report.md"
        ),
    )
    parser.add_argument(
        "--report-json-path",
        default=os.environ.get(
            "AIONBD_SOAK_REPORT_JSON_PATH", "bench/reports/soak_pipeline_report.json"
        ),
    )
    parser.add_argument(
        "--profile-reports-dir",
        default=os.environ.get(
            "AIONBD_SOAK_PROFILE_REPORTS_DIR", "bench/reports/soak_profiles"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.profiles_file:
        profiles_file_path = resolve_io_path(
            args.profiles_file, label="profiles-file", must_exist=True
        )
        os.environ["AIONBD_SOAK_PROFILES_JSON"] = profiles_file_path.read_text(
            encoding="utf-8"
        )
    generated_at = datetime.now(timezone.utc).isoformat()
    profiles = load_profiles()
    if args.profiles:
        selected = {item.strip() for item in args.profiles.split(",") if item.strip()}
        profiles = [profile for profile in profiles if str(profile["name"]) in selected]
        if not profiles:
            print("error=no_profiles_selected", file=sys.stderr)
            return 1
    rows: list[dict[str, object]] = []
    failures: list[str] = []
    reports_dir = resolve_io_path(args.profile_reports_dir, label="profile-reports-dir")
    for profile in profiles:
        row = run_profile(
            profile,
            base_url=args.base_url,
            collection_prefix=args.collection_prefix,
            reports_dir=reports_dir,
            recreate_collections=args.recreate_collections,
            dry_run=args.dry_run,
        )
        row["profile"] = str(profile["name"])
        rows.append(row)
        failures.extend(evaluate(profile, row))
    report_path = resolve_io_path(args.report_path, label="report-path")
    report_json_path = resolve_io_path(args.report_json_path, label="report-json-path")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_json_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(markdown_report(generated_at, rows), encoding="utf-8")
    payload = {"generated_at_utc": generated_at, "dry_run": args.dry_run, "rows": rows}
    report_json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"report_markdown={report_path}")
    print(f"report_json={report_json_path}")
    if failures:
        for failure in failures:
            print(f"error=soak_threshold_failed {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
