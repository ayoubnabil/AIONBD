#!/usr/bin/env python3
"""Microbenchmark exact L2 batch search fast path with payload hydration.

This benchmark compares:
- fast path enabled (default)
- fast path disabled via AIONBD_EXACT_BATCH_SMALL_TOPK_LIMIT=0

It is intentionally local and deterministic to validate regressions in CI/dev.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import requests


@dataclass(frozen=True)
class BenchConfig:
    points: int
    dimension: int
    query_pool: int
    batch_size: int
    rounds: int
    topk: int
    upsert_batch: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run exact batch fast-path microbench (payload-enabled)."
    )
    parser.add_argument("--aionbd-bin", default="target/release/aionbd-server")
    parser.add_argument("--port", type=int, default=19180)
    parser.add_argument("--points", type=int, default=12000)
    parser.add_argument("--dimension", type=int, default=128)
    parser.add_argument("--query-pool", type=int, default=1024)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--rounds", type=int, default=200)
    parser.add_argument("--topk", type=int, default=10)
    parser.add_argument("--upsert-batch", type=int, default=128)
    parser.add_argument(
        "--seed",
        type=int,
        default=12345,
        help="deterministic RNG seed",
    )
    return parser.parse_args()


def wait_http(session: requests.Session, url: str, timeout_seconds: float = 30.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = session.get(url, timeout=1.5)
            if response.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"endpoint not ready: {url}")


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    idx = int(round((len(values_sorted) - 1) * p))
    return values_sorted[idx]


def ensure_collection(session: requests.Session, base_url: str, dimension: int) -> None:
    delete_response = session.delete(f"{base_url}/collections/bench_payload", timeout=20)
    if delete_response.status_code not in (200, 404):
        delete_response.raise_for_status()

    create_response = session.post(
        f"{base_url}/collections",
        json={
            "name": "bench_payload",
            "dimension": dimension,
            "strict_finite": True,
        },
        timeout=20,
    )
    create_response.raise_for_status()


def ingest_points(
    session: requests.Session,
    base_url: str,
    points: np.ndarray,
    upsert_batch: int,
) -> None:
    count = len(points)
    for start in range(0, count, upsert_batch):
        batch = points[start : start + upsert_batch]
        payload = {
            "points": [
                {
                    "id": int(idx),
                    "values": vec.tolist(),
                    "payload": {"tenant": "edge", "bucket": int(idx % 17)},
                }
                for idx, vec in zip(range(start, start + len(batch)), batch, strict=True)
            ]
        }
        response = session.post(
            f"{base_url}/collections/bench_payload/points",
            json=payload,
            timeout=45,
        )
        response.raise_for_status()


def run_case(
    *,
    case_name: str,
    aionbd_bin: Path,
    port: int,
    config: BenchConfig,
    points: np.ndarray,
    queries: np.ndarray,
    extra_env: dict[str, str],
) -> dict[str, Any]:
    env = os.environ.copy()
    env.update(
        {
            "AIONBD_BIND": f"127.0.0.1:{port}",
            "AIONBD_PERSISTENCE_ENABLED": "false",
        }
    )
    env.update(extra_env)

    base_url = f"http://127.0.0.1:{port}"
    proc = subprocess.Popen(
        [str(aionbd_bin)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    session = requests.Session()

    try:
        wait_http(session, f"{base_url}/live")
        ensure_collection(session, base_url, config.dimension)
        ingest_points(session, base_url, points, config.upsert_batch)

        warmup_body = {
            "queries": queries[: config.batch_size].tolist(),
            "metric": "l2",
            "mode": "exact",
            "limit": config.topk,
            "include_payload": True,
        }
        for _ in range(8):
            response = session.post(
                f"{base_url}/collections/bench_payload/search/topk/batch",
                json=warmup_body,
                timeout=30,
            )
            response.raise_for_status()

        latencies_ms: list[float] = []
        start = time.perf_counter()
        for i in range(config.rounds):
            offset = (i * config.batch_size) % (config.query_pool - config.batch_size)
            body = {
                "queries": queries[offset : offset + config.batch_size].tolist(),
                "metric": "l2",
                "mode": "exact",
                "limit": config.topk,
                "include_payload": True,
            }
            t0 = time.perf_counter()
            response = session.post(
                f"{base_url}/collections/bench_payload/search/topk/batch",
                json=body,
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if len(payload.get("results", [])) != config.batch_size:
                raise RuntimeError("unexpected result count from batch search")
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)

        elapsed = max(time.perf_counter() - start, 1e-9)
        per_query_ms = [value / config.batch_size for value in latencies_ms]
        return {
            "case": case_name,
            "queries": config.rounds * config.batch_size,
            "qps": (config.rounds * config.batch_size) / elapsed,
            "latency_ms_p50_per_query": percentile(per_query_ms, 0.50),
            "latency_ms_p95_per_query": percentile(per_query_ms, 0.95),
            "latency_ms_p99_per_query": percentile(per_query_ms, 0.99),
        }
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=8)


def main() -> None:
    args = parse_args()
    config = BenchConfig(
        points=args.points,
        dimension=args.dimension,
        query_pool=args.query_pool,
        batch_size=args.batch_size,
        rounds=args.rounds,
        topk=args.topk,
        upsert_batch=args.upsert_batch,
    )

    aionbd_bin = Path(args.aionbd_bin)
    if not aionbd_bin.exists():
        raise SystemExit(f"aionbd binary not found: {aionbd_bin}")

    rng = np.random.default_rng(args.seed)
    points = rng.random((config.points, config.dimension), dtype=np.float32)
    queries = rng.random((config.query_pool, config.dimension), dtype=np.float32)

    results = [
        run_case(
            case_name="fast_path_enabled",
            aionbd_bin=aionbd_bin,
            port=args.port,
            config=config,
            points=points,
            queries=queries,
            extra_env={},
        ),
        run_case(
            case_name="fast_path_disabled_small_topk_limit_0",
            aionbd_bin=aionbd_bin,
            port=args.port,
            config=config,
            points=points,
            queries=queries,
            extra_env={"AIONBD_EXACT_BATCH_SMALL_TOPK_LIMIT": "0"},
        ),
    ]

    print(json.dumps({"config": vars(args), "results": results}, indent=2))


if __name__ == "__main__":
    main()
