#!/usr/bin/env python3
"""Runs persistence write benchmark and publishes comparative reports."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROW_PREFIX = "bench=persistence_write_row "
BASELINE_STRATEGY = "single_sync_each_write"
STRATEGY_ORDER = {
    BASELINE_STRATEGY: 0,
    "single_sync_every_n": 1,
    "group_sync_each_batch": 2,
    "group_sync_every_n": 3,
}


@dataclass(frozen=True)
class BenchRow:
    strategy: str
    p50_ms: float
    p95_ms: float
    p99_ms: float
    avg_ms: float
    qps: float
    wal_bytes: int

    @property
    def wal_mb(self) -> float:
        return self.wal_bytes / (1024.0 * 1024.0)


def parse_row(line: str) -> BenchRow | None:
    if not line.startswith(ROW_PREFIX):
        return None

    fields: dict[str, str] = {}
    for token in line.strip().split()[1:]:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        fields[key] = value

    required = ("strategy", "p50_ms", "p95_ms", "p99_ms", "avg_ms", "qps", "wal_bytes")
    missing = [field for field in required if field not in fields]
    if missing:
        raise ValueError(f"missing benchmark fields {missing} in line: {line.strip()}")

    return BenchRow(
        strategy=fields["strategy"],
        p50_ms=float(fields["p50_ms"]),
        p95_ms=float(fields["p95_ms"]),
        p99_ms=float(fields["p99_ms"]),
        avg_ms=float(fields["avg_ms"]),
        qps=float(fields["qps"]),
        wal_bytes=int(fields["wal_bytes"]),
    )


def run_persistence_write_bench() -> list[BenchRow]:
    env = os.environ.copy()
    env["AIONBD_BENCH_SCENARIO"] = "persistence_write"
    command = ["cargo", "run", "--release", "-p", "aionbd-bench"]
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
        raise RuntimeError("persistence write benchmark command failed")

    rows: list[BenchRow] = []
    for line in completed.stdout.splitlines():
        parsed = parse_row(line)
        if parsed is not None:
            rows.append(parsed)

    if not rows:
        raise RuntimeError(
            "no benchmark rows found; expected lines prefixed with "
            f"'{ROW_PREFIX.strip()}'"
        )
    return rows


def ratio_or_zero(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return numerator / denominator


def benchmark_report(rows: list[BenchRow], generated_at: str) -> str:
    sorted_rows = sorted(rows, key=lambda row: STRATEGY_ORDER.get(row.strategy, 99))
    baseline = next(
        (row for row in sorted_rows if row.strategy == BASELINE_STRATEGY), None
    )
    if baseline is None:
        raise RuntimeError(f"missing baseline strategy row: {BASELINE_STRATEGY}")

    lines = [
        "# Persistence Write Benchmark Report",
        "",
        f"generated_at_utc: {generated_at}",
        "",
        "| strategy | p50_ms | p95_ms | p99_ms | avg_ms | qps | wal_mb | qps_vs_baseline | p95_vs_baseline | wal_vs_baseline |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in sorted_rows:
        lines.append(
            "| "
            f"{row.strategy} | {row.p50_ms:.6f} | {row.p95_ms:.6f} | {row.p99_ms:.6f} | "
            f"{row.avg_ms:.6f} | {row.qps:.2f} | {row.wal_mb:.3f} | "
            f"{ratio_or_zero(row.qps, baseline.qps):.3f}x | "
            f"{ratio_or_zero(row.p95_ms, baseline.p95_ms):.3f}x | "
            f"{ratio_or_zero(row.wal_mb, baseline.wal_mb):.3f}x |"
        )
    return "\n".join(lines) + "\n"


def validate_thresholds(rows: list[BenchRow]) -> list[str]:
    by_strategy = {row.strategy: row for row in rows}
    baseline = by_strategy.get(BASELINE_STRATEGY)
    if baseline is None:
        return [f"strategy={BASELINE_STRATEGY} missing_baseline"]

    failures: list[str] = []
    for strategy, qps_env, wal_env in [
        (
            "single_sync_every_n",
            "AIONBD_BENCH_MIN_QPS_RATIO_SINGLE_SYNC_EVERY_N",
            "AIONBD_BENCH_MAX_WAL_RATIO_SINGLE_SYNC_EVERY_N",
        ),
        (
            "group_sync_each_batch",
            "AIONBD_BENCH_MIN_QPS_RATIO_GROUP_SYNC_EACH_BATCH",
            "AIONBD_BENCH_MAX_WAL_RATIO_GROUP_SYNC_EACH_BATCH",
        ),
        (
            "group_sync_every_n",
            "AIONBD_BENCH_MIN_QPS_RATIO_GROUP_SYNC_EVERY_N",
            "AIONBD_BENCH_MAX_WAL_RATIO_GROUP_SYNC_EVERY_N",
        ),
    ]:
        row = by_strategy.get(strategy)
        if row is None:
            failures.append(f"strategy={strategy} missing_row")
            continue

        min_qps_ratio = float(os.environ.get(qps_env, "0.0"))
        max_wal_ratio = float(os.environ.get(wal_env, "inf"))
        qps_ratio = ratio_or_zero(row.qps, baseline.qps)
        wal_ratio = ratio_or_zero(row.wal_mb, baseline.wal_mb)
        if qps_ratio < min_qps_ratio:
            failures.append(
                f"strategy={strategy} qps_ratio={qps_ratio:.6f} < min={min_qps_ratio:.6f}"
            )
        if wal_ratio > max_wal_ratio:
            failures.append(
                f"strategy={strategy} wal_ratio={wal_ratio:.6f} > max={max_wal_ratio:.6f}"
            )
    return failures


def json_report_payload(rows: list[BenchRow], generated_at: str) -> dict[str, object]:
    return {
        "generated_at_utc": generated_at,
        "rows": [
            {
                "strategy": row.strategy,
                "p50_ms": row.p50_ms,
                "p95_ms": row.p95_ms,
                "p99_ms": row.p99_ms,
                "avg_ms": row.avg_ms,
                "qps": row.qps,
                "wal_bytes": row.wal_bytes,
                "wal_mb": row.wal_mb,
            }
            for row in sorted(
                rows, key=lambda row: STRATEGY_ORDER.get(row.strategy, 99)
            )
        ],
    }


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def main() -> int:
    generated_at = datetime.now(timezone.utc).isoformat()
    try:
        rows = run_persistence_write_bench()
    except Exception as exc:  # pylint: disable=broad-except
        print(f"error=bench_pipeline_failed message={exc}", file=sys.stderr)
        return 1

    failures = validate_thresholds(rows)
    if failures:
        for failure in failures:
            print(f"error=benchmark_threshold_failed {failure}", file=sys.stderr)
        return 1

    markdown_path = Path(
        os.environ.get(
            "AIONBD_BENCH_PERSISTENCE_REPORT_PATH",
            "bench/reports/persistence_write_report.md",
        )
    )
    json_path = Path(
        os.environ.get(
            "AIONBD_BENCH_PERSISTENCE_REPORT_JSON_PATH",
            "bench/reports/persistence_write_report.json",
        )
    )

    markdown = benchmark_report(rows, generated_at)
    payload = json_report_payload(rows, generated_at)
    write_text(markdown_path, markdown)
    write_json(json_path, payload)

    print(f"report_markdown={markdown_path}")
    print(f"report_json={json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
