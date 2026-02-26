#!/usr/bin/env python3
"""Run a configurable read/write soak test against AIONBD."""

from __future__ import annotations

import argparse
import json
import random
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_BASE_URL = "http://127.0.0.1:8080"


def make_client(base_url: str, timeout: float) -> Any:
    repo_root = Path(__file__).resolve().parents[1]
    sdk_root = repo_root / "sdk" / "python"
    if str(sdk_root) not in sys.path:
        sys.path.insert(0, str(sdk_root))

    from aionbd import AionBDClient  # pylint: disable=import-error

    return AionBDClient(base_url=base_url, timeout=timeout)


class LatencyHistogram:
    """Fixed buckets histogram for low-overhead percentile estimation."""

    BOUNDS_US = (
        50,
        100,
        250,
        500,
        1_000,
        2_000,
        5_000,
        10_000,
        20_000,
        50_000,
        100_000,
        250_000,
        500_000,
        1_000_000,
        2_000_000,
        5_000_000,
        10_000_000,
    )

    def __init__(self) -> None:
        self.counts = [0 for _ in range(len(self.BOUNDS_US) + 1)]
        self.total = 0

    def observe(self, latency_us: int) -> None:
        for index, bound in enumerate(self.BOUNDS_US):
            if latency_us <= bound:
                self.counts[index] += 1
                self.total += 1
                return
        self.counts[-1] += 1
        self.total += 1

    def percentile(self, p: float) -> int:
        if self.total == 0:
            return 0
        target = max(1, int(round(self.total * p)))
        running = 0
        for index, count in enumerate(self.counts):
            running += count
            if running >= target:
                if index < len(self.BOUNDS_US):
                    return self.BOUNDS_US[index]
                return self.BOUNDS_US[-1]
        return self.BOUNDS_US[-1]


@dataclass
class Counters:
    reads: int = 0
    writes: int = 0
    errors: int = 0


class SharedState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.counters = Counters()
        self.histogram = LatencyHistogram()

    def record(self, op: str, latency_us: int, error: bool) -> None:
        with self.lock:
            self.histogram.observe(latency_us)
            if op == "write":
                self.counters.writes += 1
            else:
                self.counters.reads += 1
            if error:
                self.counters.errors += 1



def deterministic_vector(rng: random.Random, dimension: int) -> list[float]:
    return [rng.uniform(-1.0, 1.0) for _ in range(dimension)]


def worker_loop(
    worker_id: int,
    args: argparse.Namespace,
    stop_event: threading.Event,
    state: SharedState,
) -> None:
    client = make_client(args.base_url, args.timeout_seconds)
    rng = random.Random(args.seed + worker_id)

    while not stop_event.is_set():
        op_is_write = rng.random() < args.write_ratio
        op = "write" if op_is_write else "read"
        start = time.perf_counter()
        error = False
        try:
            if op_is_write:
                point_id = rng.randint(1, args.point_space)
                values = deterministic_vector(rng, args.dimension)
                payload = {
                    "worker": worker_id,
                    "sequence": rng.randint(1, 1_000_000_000),
                    "ts_ms": int(time.time() * 1000),
                }
                client.upsert_point(args.collection, point_id, values, payload=payload)
            else:
                query = deterministic_vector(rng, args.dimension)
                client.search_collection_top_k(
                    args.collection,
                    query,
                    limit=args.search_limit,
                    metric=args.metric,
                    mode=args.search_mode,
                )
        except Exception:  # noqa: BLE001
            error = True
        duration_us = int((time.perf_counter() - start) * 1_000_000)
        state.record(op, duration_us, error)



def ensure_collection(client: Any, args: argparse.Namespace) -> None:
    if args.recreate_collection:
        try:
            client.delete_collection(args.collection)
        except Exception:  # noqa: BLE001
            pass

    try:
        info = client.get_collection(args.collection)
    except Exception:  # noqa: BLE001
        info = client.create_collection(args.collection, args.dimension, strict_finite=True)

    if int(info.dimension) != args.dimension:
        raise RuntimeError(
            f"collection dimension mismatch: existing={info.dimension} requested={args.dimension}"
        )


def build_report(state: SharedState, duration_seconds: float, args: argparse.Namespace) -> dict[str, Any]:
    with state.lock:
        reads = state.counters.reads
        writes = state.counters.writes
        errors = state.counters.errors
        p50 = state.histogram.percentile(0.50)
        p95 = state.histogram.percentile(0.95)
        p99 = state.histogram.percentile(0.99)

    total = reads + writes
    throughput = total / duration_seconds if duration_seconds > 0 else 0.0
    error_rate = errors / total if total > 0 else 0.0

    return {
        "base_url": args.base_url,
        "collection": args.collection,
        "duration_seconds": round(duration_seconds, 3),
        "workers": args.workers,
        "metric": args.metric,
        "search_mode": args.search_mode,
        "write_ratio": args.write_ratio,
        "total_ops": total,
        "read_ops": reads,
        "write_ops": writes,
        "error_ops": errors,
        "error_rate": round(error_rate, 6),
        "throughput_ops_per_second": round(throughput, 3),
        "latency_us_p50": p50,
        "latency_us_p95": p95,
        "latency_us_p99": p99,
    }



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AIONBD soak test workload")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--collection", default="soak")
    parser.add_argument("--dimension", type=int, default=64)
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--write-ratio", type=float, default=0.2)
    parser.add_argument("--point-space", type=int, default=100_000)
    parser.add_argument("--metric", default="l2", choices=["l2", "dot", "cosine"])
    parser.add_argument("--search-mode", default="auto", choices=["auto", "exact", "ivf"])
    parser.add_argument("--search-limit", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--strict-max-error-rate", type=float, default=0.05)
    parser.add_argument("--recreate-collection", action="store_true")
    parser.add_argument("--report-json")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()



def validate_args(args: argparse.Namespace) -> None:
    if args.dimension <= 0:
        raise ValueError("dimension must be > 0")
    if args.duration_seconds <= 0:
        raise ValueError("duration-seconds must be > 0")
    if args.workers <= 0:
        raise ValueError("workers must be > 0")
    if not (0.0 <= args.write_ratio <= 1.0):
        raise ValueError("write-ratio must be in [0.0, 1.0]")
    if args.point_space <= 0:
        raise ValueError("point-space must be > 0")
    if args.search_limit <= 0:
        raise ValueError("search-limit must be > 0")
    if args.timeout_seconds <= 0:
        raise ValueError("timeout-seconds must be > 0")



def main() -> int:
    args = parse_args()
    validate_args(args)

    if args.dry_run:
        print(
            f"ok=soak_dry_run base_url={args.base_url} collection={args.collection} "
            f"duration_seconds={args.duration_seconds} workers={args.workers}"
        )
        return 0

    client = make_client(args.base_url, args.timeout_seconds)
    ensure_collection(client, args)

    state = SharedState()
    stop_event = threading.Event()
    threads = []

    start = time.perf_counter()
    for worker_id in range(args.workers):
        thread = threading.Thread(
            target=worker_loop,
            args=(worker_id, args, stop_event, state),
            daemon=True,
        )
        thread.start()
        threads.append(thread)

    time.sleep(args.duration_seconds)
    stop_event.set()
    for thread in threads:
        thread.join(timeout=10)

    duration = time.perf_counter() - start
    report = build_report(state, duration, args)

    if args.report_json:
        output_path = Path(args.report_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(report, separators=(",", ":")))

    if report["error_rate"] > args.strict_max_error_rate:
        print(
            "error=soak_error_rate_exceeded "
            f"actual={report['error_rate']} threshold={args.strict_max_error_rate}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"error={error}", file=sys.stderr)
        raise SystemExit(1)
