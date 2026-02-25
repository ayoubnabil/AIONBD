#!/usr/bin/env python3
"""Runs search-quality benchmark and publishes comparable dataset reports."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROW_PREFIX = "bench=search_quality_row "
MODE_ORDER = {"exact": 0, "ivf": 1, "auto": 2}


@dataclass(frozen=True)
class BenchRow:
    dataset: str
    mode: str
    recall_at_k: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    memory_bytes: int

    @property
    def memory_mb(self) -> float:
        return self.memory_bytes / (1024.0 * 1024.0)


def parse_row(line: str) -> BenchRow | None:
    if not line.startswith(ROW_PREFIX):
        return None

    fields: dict[str, str] = {}
    for token in line.strip().split()[1:]:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        fields[key] = value

    required = (
        "dataset",
        "mode",
        "recall_at_k",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "memory_bytes",
    )
    missing = [field for field in required if field not in fields]
    if missing:
        raise ValueError(f"missing benchmark fields {missing} in line: {line.strip()}")

    return BenchRow(
        dataset=fields["dataset"],
        mode=fields["mode"],
        recall_at_k=float(fields["recall_at_k"]),
        p50_ms=float(fields["p50_ms"]),
        p95_ms=float(fields["p95_ms"]),
        p99_ms=float(fields["p99_ms"]),
        memory_bytes=int(fields["memory_bytes"]),
    )


def run_search_quality_bench() -> list[BenchRow]:
    env = os.environ.copy()
    env["AIONBD_BENCH_SCENARIO"] = "search_quality"
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
        raise RuntimeError("search quality benchmark command failed")

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
    datasets = sorted({row.dataset for row in rows})
    lines = [
        "# Search Quality Benchmark Report",
        "",
        f"generated_at_utc: {generated_at}",
        "",
        "| dataset | mode | recall@k | p50_ms | p95_ms | p99_ms | memory_mb | p95_vs_exact | mem_vs_exact |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for dataset in datasets:
        dataset_rows = sorted(
            [row for row in rows if row.dataset == dataset],
            key=lambda row: MODE_ORDER.get(row.mode, 99),
        )
        exact = next((row for row in dataset_rows if row.mode == "exact"), None)
        if exact is None:
            raise RuntimeError(f"dataset '{dataset}' is missing exact mode row")
        for row in dataset_rows:
            lines.append(
                "| "
                f"{row.dataset} | {row.mode} | {row.recall_at_k:.4f} | "
                f"{row.p50_ms:.6f} | {row.p95_ms:.6f} | {row.p99_ms:.6f} | "
                f"{row.memory_mb:.3f} | "
                f"{ratio_or_zero(row.p95_ms, exact.p95_ms):.3f}x | "
                f"{ratio_or_zero(row.memory_mb, exact.memory_mb):.3f}x |"
            )

    return "\n".join(lines) + "\n"


def validate_recall_thresholds(rows: list[BenchRow]) -> list[str]:
    min_ivf = float(os.environ.get("AIONBD_BENCH_MIN_RECALL_IVF", "0.0"))
    min_auto = float(os.environ.get("AIONBD_BENCH_MIN_RECALL_AUTO", "0.0"))

    failures: list[str] = []
    for row in rows:
        if row.mode == "ivf" and row.recall_at_k < min_ivf:
            failures.append(
                f"dataset={row.dataset} mode=ivf recall_at_k={row.recall_at_k:.6f} "
                f"< min={min_ivf:.6f}"
            )
        if row.mode == "auto" and row.recall_at_k < min_auto:
            failures.append(
                f"dataset={row.dataset} mode=auto recall_at_k={row.recall_at_k:.6f} "
                f"< min={min_auto:.6f}"
            )
    return failures


def validate_perf_memory_thresholds(rows: list[BenchRow]) -> list[str]:
    max_p95_ratio_ivf = float(os.environ.get("AIONBD_BENCH_MAX_P95_RATIO_IVF", "inf"))
    max_p95_ratio_auto = float(os.environ.get("AIONBD_BENCH_MAX_P95_RATIO_AUTO", "inf"))
    max_mem_ratio_ivf = float(
        os.environ.get("AIONBD_BENCH_MAX_MEMORY_RATIO_IVF", "inf")
    )
    max_mem_ratio_auto = float(
        os.environ.get("AIONBD_BENCH_MAX_MEMORY_RATIO_AUTO", "inf")
    )

    failures: list[str] = []
    datasets = sorted({row.dataset for row in rows})
    for dataset in datasets:
        dataset_rows = [row for row in rows if row.dataset == dataset]
        exact = next((row for row in dataset_rows if row.mode == "exact"), None)
        if exact is None:
            failures.append(f"dataset={dataset} mode=exact missing_exact_baseline")
            continue

        for row in dataset_rows:
            p95_ratio = ratio_or_zero(row.p95_ms, exact.p95_ms)
            mem_ratio = ratio_or_zero(row.memory_mb, exact.memory_mb)
            if row.mode == "ivf":
                if p95_ratio > max_p95_ratio_ivf:
                    failures.append(
                        "dataset={dataset} mode=ivf p95_ratio={ratio:.6f} > max={max_ratio:.6f}".format(
                            dataset=dataset,
                            ratio=p95_ratio,
                            max_ratio=max_p95_ratio_ivf,
                        )
                    )
                if mem_ratio > max_mem_ratio_ivf:
                    failures.append(
                        "dataset={dataset} mode=ivf memory_ratio={ratio:.6f} > max={max_ratio:.6f}".format(
                            dataset=dataset,
                            ratio=mem_ratio,
                            max_ratio=max_mem_ratio_ivf,
                        )
                    )
            if row.mode == "auto":
                if p95_ratio > max_p95_ratio_auto:
                    failures.append(
                        "dataset={dataset} mode=auto p95_ratio={ratio:.6f} > max={max_ratio:.6f}".format(
                            dataset=dataset,
                            ratio=p95_ratio,
                            max_ratio=max_p95_ratio_auto,
                        )
                    )
                if mem_ratio > max_mem_ratio_auto:
                    failures.append(
                        "dataset={dataset} mode=auto memory_ratio={ratio:.6f} > max={max_ratio:.6f}".format(
                            dataset=dataset,
                            ratio=mem_ratio,
                            max_ratio=max_mem_ratio_auto,
                        )
                    )
    return failures


def json_report_payload(rows: list[BenchRow], generated_at: str) -> dict[str, object]:
    return {
        "generated_at_utc": generated_at,
        "rows": [
            {
                "dataset": row.dataset,
                "mode": row.mode,
                "recall_at_k": row.recall_at_k,
                "p50_ms": row.p50_ms,
                "p95_ms": row.p95_ms,
                "p99_ms": row.p99_ms,
                "memory_bytes": row.memory_bytes,
                "memory_mb": row.memory_mb,
            }
            for row in sorted(
                rows, key=lambda row: (row.dataset, MODE_ORDER.get(row.mode, 99))
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
        rows = run_search_quality_bench()
    except Exception as exc:  # pylint: disable=broad-except
        print(f"error=bench_pipeline_failed message={exc}", file=sys.stderr)
        return 1

    failures = validate_recall_thresholds(rows)
    failures.extend(validate_perf_memory_thresholds(rows))
    if failures:
        for failure in failures:
            print(f"error=benchmark_threshold_failed {failure}", file=sys.stderr)
        return 1

    markdown_path = Path(
        os.environ.get(
            "AIONBD_BENCH_REPORT_PATH",
            "bench/reports/search_quality_report.md",
        )
    )
    json_path = Path(
        os.environ.get(
            "AIONBD_BENCH_REPORT_JSON_PATH",
            "bench/reports/search_quality_report.json",
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
