#!/usr/bin/env python3
"""Run a local ANN-Benchmarks-style comparison for AIONBD and open-source engines.

This wrapper intentionally uses an official ANN-Benchmarks dataset format (HDF5)
and reports recall + latency + throughput for exact search mode.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import requests

try:
    import hnswlib
except Exception:  # noqa: BLE001
    hnswlib = None

DATASET_URL = "https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5"
DEFAULT_DATASET_PATH = Path("bench/data/ann/fashion-mnist-784-euclidean.hdf5")
DEFAULT_REPORT_JSON = Path(
    "bench/reports/open_source_bench/ann_open_wrapper_report.json"
)
DEFAULT_REPORT_MD = Path("bench/reports/open_source_bench/ann_open_wrapper_report.md")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ANN-Benchmarks-style local comparison for AIONBD and other open-source engines."
    )
    parser.add_argument("--dataset-path", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument("--dataset-url", default=DATASET_URL)
    parser.add_argument("--train-size", type=int, default=5000)
    parser.add_argument("--test-size", type=int, default=200)
    parser.add_argument("--topk", type=int, default=10)
    parser.add_argument("--ingest-batch-size", type=int, default=128)
    parser.add_argument(
        "--aionbd-upsert-batch-size",
        type=int,
        default=64,
        help="number of points per AIONBD batch upsert request during ingestion (1 disables batch endpoint)",
    )
    parser.add_argument(
        "--aionbd-mode-ready-timeout-seconds",
        type=float,
        default=180.0,
        help="max seconds to wait for AIONBD mode/index readiness before skipping the mode",
    )
    parser.add_argument("--aionbd-bin", default="target/release/aionbd-server")
    parser.add_argument("--aionbd-port", type=int, default=18080)
    parser.add_argument(
        "--aionbd-cpu-affinity",
        default="",
        help="optional CPU set for AIONBD process, e.g. '0-3' or '0,2,4'",
    )
    parser.add_argument(
        "--bench-cpu-affinity",
        default="",
        help="optional CPU set for benchmark client process, e.g. '4-7'",
    )
    parser.add_argument(
        "--aionbd-persistence-enabled",
        choices=["true", "false"],
        default="false",
        help="enable AIONBD persistence during benchmark run",
    )
    parser.add_argument(
        "--aionbd-wal-sync-on-write",
        choices=["true", "false"],
        default="false",
        help="AIONBD WAL sync-on-write mode",
    )
    parser.add_argument("--aionbd-wal-sync-every-n-writes", type=int, default=0)
    parser.add_argument("--aionbd-wal-sync-interval-seconds", type=int, default=0)
    parser.add_argument("--qdrant-port", type=int, default=16333)
    parser.add_argument(
        "--engines",
        default="aionbd,hnswlib",
        help="comma-separated engines: aionbd,hnswlib,qdrant",
    )
    parser.add_argument(
        "--aionbd-modes",
        default="exact",
        help="comma-separated AIONBD search modes: exact,ivf,auto",
    )
    parser.add_argument("--collection-prefix", default="ann_open_wrapper")
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="number of repeated runs for reproducibility (default: 1)",
    )
    parser.add_argument(
        "--sleep-between-runs",
        type=float,
        default=0.0,
        help="seconds to sleep between repeated runs (default: 0.0)",
    )
    parser.add_argument(
        "--warmup-queries",
        type=int,
        default=16,
        help="number of unmeasured warmup queries before timed queries per engine/mode",
    )
    parser.add_argument(
        "--aionbd-batch-size",
        type=int,
        default=1,
        help="number of queries per AIONBD batch request (1 disables batch endpoint)",
    )
    parser.add_argument("--report-json", default=str(DEFAULT_REPORT_JSON))
    parser.add_argument("--report-md", default=str(DEFAULT_REPORT_MD))
    return parser.parse_args()


def parse_cpu_affinity(value: str) -> set[int]:
    cpus: set[int] = set()
    for token in value.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            start_text, end_text = token.split("-", 1)
            start = int(start_text)
            end = int(end_text)
            if start > end:
                raise ValueError(f"invalid cpu range '{token}'")
            cpus.update(range(start, end + 1))
            continue
        cpus.add(int(token))
    if not cpus:
        raise ValueError("cpu affinity set must not be empty")
    return cpus


def apply_bench_cpu_affinity(value: str) -> None:
    if not value.strip():
        return
    if not hasattr(os, "sched_setaffinity"):
        raise RuntimeError("bench cpu affinity is unsupported on this platform")
    cpus = parse_cpu_affinity(value)
    os.sched_setaffinity(0, cpus)


def ensure_dataset(dataset_path: Path, dataset_url: str) -> None:
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    if dataset_path.exists():
        return
    print(f"downloading dataset: {dataset_url} -> {dataset_path}")
    response = requests.get(dataset_url, stream=True, timeout=120)
    response.raise_for_status()
    with dataset_path.open("wb") as output:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                output.write(chunk)


def load_dataset(
    dataset_path: Path, train_size: int, test_size: int
) -> tuple[np.ndarray, np.ndarray]:
    with h5py.File(dataset_path, "r") as h5f:
        train = np.array(h5f["train"][:train_size], dtype=np.float32)
        test = np.array(h5f["test"][:test_size], dtype=np.float32)
    return train, test


def exact_ground_truth_ids(
    train: np.ndarray, test: np.ndarray, topk: int
) -> np.ndarray:
    train_norm = np.sum(train * train, axis=1)
    gt = np.zeros((test.shape[0], topk), dtype=np.int64)
    block = 64
    for start in range(0, test.shape[0], block):
        chunk = test[start : start + block]
        chunk_norm = np.sum(chunk * chunk, axis=1)
        # Squared L2 distance: ||q||^2 + ||x||^2 - 2 q.x
        distances = (
            chunk_norm[:, None] + train_norm[None, :] - 2.0 * np.dot(chunk, train.T)
        )
        top = np.argpartition(distances, kth=topk - 1, axis=1)[:, :topk]
        top_dist = np.take_along_axis(distances, top, axis=1)
        order = np.argsort(top_dist, axis=1)
        sorted_top = np.take_along_axis(top, order, axis=1)
        gt[start : start + chunk.shape[0], :] = sorted_top
    return gt


def wait_http(url: str, timeout_seconds: float = 30.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=1.5)
            if response.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"endpoint not ready: {url}")


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    idx = int(round((len(values_sorted) - 1) * p))
    return values_sorted[idx]


def eval_recall(results: list[list[int]], ground_truth: np.ndarray, topk: int) -> float:
    total = 0.0
    for idx, predicted in enumerate(results):
        truth = set(int(item) for item in ground_truth[idx, :topk])
        hits = sum(1 for item in predicted[:topk] if int(item) in truth)
        total += hits / float(topk)
    return total / float(len(results))


def start_aionbd(args: argparse.Namespace) -> tuple[subprocess.Popen[bytes], str]:
    base_url = f"http://127.0.0.1:{args.aionbd_port}"
    try:
        existing = requests.get(f"{base_url}/live", timeout=0.8)
        if existing.ok:
            raise RuntimeError(
                f"aionbd port {args.aionbd_port} is already in use by a live server"
            )
    except requests.RequestException:
        pass

    env = os.environ.copy()
    env["AIONBD_BIND"] = f"127.0.0.1:{args.aionbd_port}"
    env["AIONBD_PERSISTENCE_ENABLED"] = args.aionbd_persistence_enabled
    env["AIONBD_WAL_SYNC_ON_WRITE"] = args.aionbd_wal_sync_on_write
    env["AIONBD_WAL_SYNC_EVERY_N_WRITES"] = str(args.aionbd_wal_sync_every_n_writes)
    env["AIONBD_WAL_SYNC_INTERVAL_SECONDS"] = str(args.aionbd_wal_sync_interval_seconds)
    existing_batch_limit = 0
    if env.get("AIONBD_UPSERT_BATCH_MAX_POINTS"):
        try:
            existing_batch_limit = max(int(env["AIONBD_UPSERT_BATCH_MAX_POINTS"]), 0)
        except ValueError:
            existing_batch_limit = 0
    env["AIONBD_UPSERT_BATCH_MAX_POINTS"] = str(
        max(existing_batch_limit, args.aionbd_upsert_batch_size)
    )
    if args.aionbd_persistence_enabled == "true":
        persistence_root = Path("bench/tmp/persistence") / f"aionbd_{args.aionbd_port}"
        if persistence_root.exists():
            shutil.rmtree(persistence_root)
        persistence_root.mkdir(parents=True, exist_ok=True)
        env["AIONBD_SNAPSHOT_PATH"] = str(persistence_root / "snapshot.json")
        env["AIONBD_WAL_PATH"] = str(persistence_root / "wal.jsonl")

    cmd = [args.aionbd_bin]
    if args.aionbd_cpu_affinity.strip():
        cmd = ["taskset", "-c", args.aionbd_cpu_affinity.strip(), args.aionbd_bin]
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
    except FileNotFoundError as error:
        raise RuntimeError(
            "taskset is required for --aionbd-cpu-affinity but was not found"
        ) from error
    wait_http(f"{base_url}/live")
    return proc, base_url


def stop_process(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None:
        return
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


def aionbd_search_topk(
    session: requests.Session,
    base_url: str,
    collection: str,
    query: np.ndarray,
    topk: int,
    mode: str,
) -> dict[str, Any]:
    response = session.post(
        f"{base_url}/collections/{collection}/search/topk",
        json={
            "query": query.tolist(),
            "metric": "l2",
            "mode": mode,
            "limit": topk,
        },
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def aionbd_search_topk_batch(
    session: requests.Session,
    base_url: str,
    collection: str,
    queries: np.ndarray,
    topk: int,
    mode: str,
) -> dict[str, Any]:
    response = session.post(
        f"{base_url}/collections/{collection}/search/topk/batch",
        json={
            "queries": queries.tolist(),
            "metric": "l2",
            "mode": mode,
            "limit": topk,
        },
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def aionbd_upsert_points_batch(
    session: requests.Session,
    base_url: str,
    collection: str,
    ids: np.ndarray,
    vectors: np.ndarray,
) -> dict[str, Any]:
    points = [
        {"id": int(point_id), "values": vector.tolist()}
        for point_id, vector in zip(ids, vectors, strict=True)
    ]
    response = session.post(
        f"{base_url}/collections/{collection}/points",
        json={"points": points},
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def ensure_aionbd_collection_reset(
    session: requests.Session,
    base_url: str,
    collection: str,
    dimension: int,
) -> None:
    delete_response = session.delete(
        f"{base_url}/collections/{collection}",
        timeout=120,
    )
    if delete_response.status_code not in (200, 404):
        delete_response.raise_for_status()

    create_response = session.post(
        f"{base_url}/collections",
        json={
            "name": collection,
            "dimension": dimension,
            "strict_finite": True,
        },
        timeout=120,
    )
    if create_response.status_code == 409:
        # Retry once after best-effort delete in case of stale state.
        retry_delete = session.delete(
            f"{base_url}/collections/{collection}",
            timeout=120,
        )
        if retry_delete.status_code not in (200, 404):
            retry_delete.raise_for_status()
        create_response = session.post(
            f"{base_url}/collections",
            json={
                "name": collection,
                "dimension": dimension,
                "strict_finite": True,
            },
            timeout=120,
        )
    create_response.raise_for_status()


def wait_aionbd_mode_ready(
    session: requests.Session,
    base_url: str,
    collection: str,
    probe_query: np.ndarray,
    topk: int,
    mode: str,
    timeout_seconds: float = 30.0,
) -> None:
    if mode == "exact":
        return
    if mode == "auto":
        _ = aionbd_search_topk(
            session=session,
            base_url=base_url,
            collection=collection,
            query=probe_query,
            topk=topk,
            mode=mode,
        )
        return

    deadline = time.time() + timeout_seconds
    last_mode = ""
    while time.time() < deadline:
        payload = aionbd_search_topk(
            session=session,
            base_url=base_url,
            collection=collection,
            query=probe_query,
            topk=topk,
            mode=mode,
        )
        last_mode = str(payload.get("mode", ""))
        if last_mode == mode:
            return
        time.sleep(0.05)
    raise RuntimeError(
        f"aionbd mode warmup timeout for mode={mode} (last_mode={last_mode or 'unknown'})"
    )


def run_aionbd_mode_bench(
    session: requests.Session,
    base_url: str,
    collection: str,
    test: np.ndarray,
    ground_truth: np.ndarray,
    topk: int,
    mode: str,
    warmup_queries: int,
    aionbd_batch_size: int,
) -> dict[str, Any]:
    if aionbd_batch_size <= 1:
        for query in test[:warmup_queries]:
            _ = aionbd_search_topk(
                session=session,
                base_url=base_url,
                collection=collection,
                query=query,
                topk=topk,
                mode=mode,
            )
    else:
        for start_idx in range(0, min(warmup_queries, len(test)), aionbd_batch_size):
            queries_chunk = test[start_idx : start_idx + aionbd_batch_size]
            _ = aionbd_search_topk_batch(
                session=session,
                base_url=base_url,
                collection=collection,
                queries=queries_chunk,
                topk=topk,
                mode=mode,
            )

    latencies_ms: list[float] = []
    results: list[list[int]] = []
    effective_modes: dict[str, int] = {}
    start = time.perf_counter()
    if aionbd_batch_size <= 1:
        for query in test:
            t0 = time.perf_counter()
            payload = aionbd_search_topk(
                session=session,
                base_url=base_url,
                collection=collection,
                query=query,
                topk=topk,
                mode=mode,
            )
            payload_mode = str(payload.get("mode", mode))
            effective_modes[payload_mode] = effective_modes.get(payload_mode, 0) + 1
            hits = [int(item["id"]) for item in payload["hits"]]
            results.append(hits)
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)
    else:
        for start_idx in range(0, len(test), aionbd_batch_size):
            queries_chunk = test[start_idx : start_idx + aionbd_batch_size]
            t0 = time.perf_counter()
            payload = aionbd_search_topk_batch(
                session=session,
                base_url=base_url,
                collection=collection,
                queries=queries_chunk,
                topk=topk,
                mode=mode,
            )
            batch_elapsed_ms = (time.perf_counter() - t0) * 1000.0
            result_items = list(payload.get("results", []))
            if len(result_items) != len(queries_chunk):
                raise RuntimeError(
                    "aionbd batch search returned unexpected result count: "
                    f"got {len(result_items)}, expected {len(queries_chunk)}"
                )
            per_query_ms = batch_elapsed_ms / max(len(queries_chunk), 1)
            for result_item in result_items:
                payload_mode = str(result_item.get("mode", mode))
                effective_modes[payload_mode] = effective_modes.get(payload_mode, 0) + 1
                hits = [int(item["id"]) for item in result_item["hits"]]
                results.append(hits)
                latencies_ms.append(per_query_ms)
    elapsed = max(time.perf_counter() - start, 1e-9)

    return {
        "engine": "aionbd",
        "mode": mode,
        "queries": len(test),
        "topk": topk,
        "recall_at_k": eval_recall(results, ground_truth, topk),
        "qps": len(test) / elapsed,
        "latency_ms_p50": percentile(latencies_ms, 0.50),
        "latency_ms_p95": percentile(latencies_ms, 0.95),
        "latency_ms_p99": percentile(latencies_ms, 0.99),
        "effective_modes": effective_modes,
    }


def run_aionbd_bench(
    base_url: str,
    collection: str,
    train: np.ndarray,
    test: np.ndarray,
    ground_truth: np.ndarray,
    topk: int,
    modes: list[str],
    warmup_queries: int,
    aionbd_batch_size: int,
    aionbd_upsert_batch_size: int,
    mode_ready_timeout_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with requests.Session() as session:
        ensure_aionbd_collection_reset(
            session=session,
            base_url=base_url,
            collection=collection,
            dimension=int(train.shape[1]),
        )

        if aionbd_upsert_batch_size <= 1:
            for idx, vector in enumerate(train):
                response = session.put(
                    f"{base_url}/collections/{collection}/points/{idx}",
                    json={"values": vector.tolist()},
                    timeout=120,
                )
                response.raise_for_status()
        else:
            use_batch_upsert = True
            for start_idx in range(0, len(train), aionbd_upsert_batch_size):
                batch_vectors = train[start_idx : start_idx + aionbd_upsert_batch_size]
                batch_ids = np.arange(
                    start_idx,
                    start_idx + len(batch_vectors),
                    dtype=np.int64,
                )
                if use_batch_upsert:
                    try:
                        _ = aionbd_upsert_points_batch(
                            session=session,
                            base_url=base_url,
                            collection=collection,
                            ids=batch_ids,
                            vectors=batch_vectors,
                        )
                        continue
                    except requests.HTTPError as error:
                        status_code = (
                            error.response.status_code
                            if error.response is not None
                            else None
                        )
                        if status_code == 404:
                            use_batch_upsert = False
                        else:
                            raise

                for idx, vector in zip(batch_ids, batch_vectors, strict=True):
                    response = session.put(
                        f"{base_url}/collections/{collection}/points/{int(idx)}",
                        json={"values": vector.tolist()},
                        timeout=120,
                    )
                    response.raise_for_status()

        rows: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        probe_query = test[0]
        for mode in modes:
            try:
                wait_aionbd_mode_ready(
                    session=session,
                    base_url=base_url,
                    collection=collection,
                    probe_query=probe_query,
                    topk=topk,
                    mode=mode,
                    timeout_seconds=mode_ready_timeout_seconds,
                )
                rows.append(
                    run_aionbd_mode_bench(
                        session=session,
                        base_url=base_url,
                        collection=collection,
                        test=test,
                        ground_truth=ground_truth,
                        topk=topk,
                        mode=mode,
                        warmup_queries=warmup_queries,
                        aionbd_batch_size=aionbd_batch_size,
                    )
                )
            except Exception as error:  # noqa: BLE001
                skipped.append({"engine": "aionbd", "mode": mode, "reason": str(error)})
    return rows, skipped


def run_hnswlib_bench(
    train: np.ndarray,
    test: np.ndarray,
    ground_truth: np.ndarray,
    topk: int,
    warmup_queries: int,
) -> dict[str, Any]:
    if hnswlib is None:
        raise RuntimeError("hnswlib not installed")

    index = hnswlib.Index(space="l2", dim=int(train.shape[1]))
    index.init_index(max_elements=int(train.shape[0]), ef_construction=200, M=16)
    ids = np.arange(train.shape[0], dtype=np.int64)
    index.add_items(train, ids)
    index.set_ef(max(64, topk * 16))

    for query in test[:warmup_queries]:
        _labels, _distances = index.knn_query(query, k=topk)

    latencies_ms: list[float] = []
    results: list[list[int]] = []
    start = time.perf_counter()
    for query in test:
        t0 = time.perf_counter()
        labels, _distances = index.knn_query(query, k=topk)
        latencies_ms.append((time.perf_counter() - t0) * 1000.0)
        results.append([int(item) for item in labels[0]])
    elapsed = max(time.perf_counter() - start, 1e-9)

    return {
        "engine": "hnswlib",
        "mode": "approx",
        "queries": len(test),
        "topk": topk,
        "recall_at_k": eval_recall(results, ground_truth, topk),
        "qps": len(test) / elapsed,
        "latency_ms_p50": percentile(latencies_ms, 0.50),
        "latency_ms_p95": percentile(latencies_ms, 0.95),
        "latency_ms_p99": percentile(latencies_ms, 0.99),
    }


def start_qdrant(args: argparse.Namespace) -> str:
    container = "aionbd-open-bench-qdrant"
    subprocess.run(
        ["docker", "rm", "-f", container],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    run = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container,
            "-p",
            f"{args.qdrant_port}:6333",
            "qdrant/qdrant:latest",
        ],
        capture_output=True,
        text=True,
    )
    if run.returncode != 0:
        detail = (run.stderr or run.stdout or "").strip()
        raise RuntimeError(f"failed to start qdrant docker container: {detail}")
    base_url = f"http://127.0.0.1:{args.qdrant_port}"
    wait_http(f"{base_url}/collections")
    return base_url


def stop_qdrant() -> None:
    subprocess.run(
        ["docker", "rm", "-f", "aionbd-open-bench-qdrant"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def run_qdrant_bench(
    base_url: str,
    collection: str,
    train: np.ndarray,
    test: np.ndarray,
    ground_truth: np.ndarray,
    topk: int,
    ingest_batch_size: int,
    warmup_queries: int,
) -> dict[str, Any]:
    with requests.Session() as session:
        response = session.put(
            f"{base_url}/collections/{collection}",
            json={"vectors": {"size": int(train.shape[1]), "distance": "Euclid"}},
            timeout=30,
        )
        response.raise_for_status()

        for start in range(0, len(train), ingest_batch_size):
            chunk = train[start : start + ingest_batch_size]
            payload = {
                "points": [
                    {"id": int(start + idx), "vector": vector.tolist()}
                    for idx, vector in enumerate(chunk)
                ]
            }
            response = session.put(
                f"{base_url}/collections/{collection}/points",
                params={"wait": "true"},
                json=payload,
                timeout=60,
            )
            response.raise_for_status()

        for query in test[:warmup_queries]:
            response = session.post(
                f"{base_url}/collections/{collection}/points/search",
                json={
                    "vector": query.tolist(),
                    "limit": topk,
                    "with_payload": False,
                    "with_vector": False,
                    "params": {"exact": True},
                },
                timeout=30,
            )
            response.raise_for_status()

        latencies_ms: list[float] = []
        results: list[list[int]] = []
        start_time = time.perf_counter()
        for query in test:
            t0 = time.perf_counter()
            response = session.post(
                f"{base_url}/collections/{collection}/points/search",
                json={
                    "vector": query.tolist(),
                    "limit": topk,
                    "with_payload": False,
                    "with_vector": False,
                    "params": {"exact": True},
                },
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            hits = [int(item["id"]) for item in payload["result"]]
            results.append(hits)
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)
    elapsed = max(time.perf_counter() - start_time, 1e-9)

    return {
        "engine": "qdrant",
        "mode": "exact",
        "queries": len(test),
        "topk": topk,
        "recall_at_k": eval_recall(results, ground_truth, topk),
        "qps": len(test) / elapsed,
        "latency_ms_p50": percentile(latencies_ms, 0.50),
        "latency_ms_p95": percentile(latencies_ms, 0.95),
        "latency_ms_p99": percentile(latencies_ms, 0.99),
    }


def run_single_benchmark(
    args: argparse.Namespace,
    selected_engines: set[str],
    selected_aionbd_modes: list[str],
    train: np.ndarray,
    test: np.ndarray,
    ground_truth: np.ndarray,
    topk: int,
    run_index: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    run_suffix = f"run{run_index}"

    aionbd_proc: subprocess.Popen[bytes] | None = None
    if "aionbd" in selected_engines:
        try:
            aionbd_proc, aionbd_url = start_aionbd(args)
            aionbd_rows, aionbd_skipped = run_aionbd_bench(
                base_url=aionbd_url,
                collection=f"{args.collection_prefix}_{run_suffix}_aionbd",
                train=train,
                test=test,
                ground_truth=ground_truth,
                topk=topk,
                modes=selected_aionbd_modes,
                warmup_queries=args.warmup_queries,
                aionbd_batch_size=args.aionbd_batch_size,
                aionbd_upsert_batch_size=args.aionbd_upsert_batch_size,
                mode_ready_timeout_seconds=args.aionbd_mode_ready_timeout_seconds,
            )
            rows.extend(aionbd_rows)
            for item in aionbd_skipped:
                mode = str(item.get("mode", "unknown"))
                reason = str(item.get("reason", "unknown error"))
                skipped.append(
                    {
                        "run": run_index,
                        "engine": "aionbd",
                        "reason": f"mode={mode}: {reason}",
                    }
                )
            if not aionbd_rows:
                skipped.append(
                    {
                        "run": run_index,
                        "engine": "aionbd",
                        "reason": "all configured modes failed",
                    }
                )
        except Exception as error:  # noqa: BLE001
            skipped.append({"run": run_index, "engine": "aionbd", "reason": str(error)})
        finally:
            stop_process(aionbd_proc)

    if "hnswlib" in selected_engines:
        try:
            rows.append(
                run_hnswlib_bench(
                    train=train,
                    test=test,
                    ground_truth=ground_truth,
                    topk=topk,
                    warmup_queries=args.warmup_queries,
                )
            )
        except Exception as error:  # noqa: BLE001
            skipped.append(
                {"run": run_index, "engine": "hnswlib", "reason": str(error)}
            )

    if "qdrant" in selected_engines:
        try:
            qdrant_url = start_qdrant(args)
            rows.append(
                run_qdrant_bench(
                    base_url=qdrant_url,
                    collection=f"{args.collection_prefix}_{run_suffix}_qdrant",
                    train=train,
                    test=test,
                    ground_truth=ground_truth,
                    topk=topk,
                    ingest_batch_size=args.ingest_batch_size,
                    warmup_queries=args.warmup_queries,
                )
            )
        except Exception as error:  # noqa: BLE001
            skipped.append({"run": run_index, "engine": "qdrant", "reason": str(error)})
        finally:
            stop_qdrant()

    return rows, skipped


def aggregate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["engine"], row["mode"]), []).append(row)

    metrics = [
        "recall_at_k",
        "qps",
        "latency_ms_p50",
        "latency_ms_p95",
        "latency_ms_p99",
    ]
    aggregated: list[dict[str, Any]] = []
    for (_, _), group in grouped.items():
        base = group[0]
        output: dict[str, Any] = {
            "engine": base["engine"],
            "mode": base["mode"],
            "queries": base["queries"],
            "topk": base["topk"],
            "runs": len(group),
        }
        for metric in metrics:
            values = [float(item[metric]) for item in group]
            output[metric] = statistics.mean(values)
            output[f"{metric}_median"] = statistics.median(values)
            output[f"{metric}_min"] = min(values)
            output[f"{metric}_max"] = max(values)
        aggregated.append(output)

    return sorted(aggregated, key=lambda item: (item["engine"], item["mode"]))


def summarize_skipped(skipped: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for item in skipped:
        key = (str(item["engine"]), str(item["reason"]))
        if key not in by_key:
            by_key[key] = {"engine": key[0], "reason": key[1], "count": 0}
        by_key[key]["count"] += 1
    return sorted(by_key.values(), key=lambda row: (row["engine"], row["reason"]))


def build_markdown(
    report: dict[str, Any],
    rows: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
) -> str:
    repetitions = int(report.get("repetitions", 1))
    lines = [
        "# Open-Source ANN Wrapper Benchmark",
        "",
        "Benchmark basis:",
        "- Dataset format and source: ANN-Benchmarks HDF5",
        f"- Dataset URL: {report['dataset_url']}",
        "- Distance: Euclidean",
        f"- Repetitions: {repetitions}",
        f"- Warmup queries: {int(report.get('warmup_queries', 0))}",
        f"- AIONBD query batch size: {int(report.get('aionbd_batch_size', 1))}",
        f"- AIONBD upsert batch size: {int(report.get('aionbd_upsert_batch_size', 1))}",
        f"- AIONBD persistence enabled: {bool(report.get('aionbd_persistence_enabled', False))}",
        f"- AIONBD WAL sync on write: {bool(report.get('aionbd_wal_sync_on_write', False))}",
        f"- AIONBD WAL sync every N writes: {int(report.get('aionbd_wal_sync_every_n_writes', 0))}",
        f"- AIONBD WAL sync interval seconds: {int(report.get('aionbd_wal_sync_interval_seconds', 0))}",
        f"- AIONBD CPU affinity: {report.get('aionbd_cpu_affinity') or 'none'}",
        f"- Benchmark client CPU affinity: {report.get('bench_cpu_affinity') or 'none'}",
        "",
        "| Engine | Mode | Runs | Recall@k (mean) | QPS (mean) | QPS (median) | p95 ms (mean) | p95 ms (median) |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        runs = int(row.get("runs", 1))
        recall_mean = float(row.get("recall_at_k", 0.0))
        qps_mean = float(row.get("qps", 0.0))
        qps_median = float(row.get("qps_median", qps_mean))
        p95_mean = float(row.get("latency_ms_p95", 0.0))
        p95_median = float(row.get("latency_ms_p95_median", p95_mean))
        lines.append(
            f"| {row['engine']} | {row['mode']} | {runs} | {recall_mean:.4f} | {qps_mean:.2f} | {qps_median:.2f} | "
            f"{p95_mean:.3f} | {p95_median:.3f} |"
        )
    lines.append("")
    run_summaries = report.get("runs", [])
    if repetitions > 1 and isinstance(run_summaries, list):
        lines.append("Per-run results:")
        lines.append("")
        for run in run_summaries:
            lines.append(f"- Run {run['run']}:")
            lines.append("")
            lines.append("| Engine | Mode | Recall@k | QPS | p95 ms |")
            lines.append("|---|---|---:|---:|---:|")
            for row in sorted(
                run.get("results", []), key=lambda item: (item["engine"], item["mode"])
            ):
                lines.append(
                    f"| {row['engine']} | {row['mode']} | {row['recall_at_k']:.4f} | {row['qps']:.2f} | {row['latency_ms_p95']:.3f} |"
                )
            lines.append("")
    lines.append("Notes:")
    lines.append(
        "- This wrapper is intended for reproducible local positioning, not a full ANN-Benchmarks leaderboard submission."
    )
    lines.append(
        "- AIONBD ingestion path can use batch upsert (`--aionbd-upsert-batch-size`) with automatic fallback to per-point upsert when batch endpoint is unavailable."
    )
    if skipped:
        lines.append("- Skipped engines:")
        for item in skipped:
            count = int(item.get("count", 1))
            lines.append(
                f"  - `{item['engine']}` skipped ({count} run(s)): {item['reason']}"
            )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    if args.train_size <= 0 or args.test_size <= 0 or args.topk <= 0:
        raise ValueError("train-size, test-size, and topk must be > 0")
    if args.repeat <= 0:
        raise ValueError("repeat must be > 0")
    if args.sleep_between_runs < 0:
        raise ValueError("sleep-between-runs must be >= 0")
    if args.warmup_queries < 0:
        raise ValueError("warmup-queries must be >= 0")
    if args.aionbd_batch_size <= 0:
        raise ValueError("aionbd-batch-size must be > 0")
    if args.aionbd_upsert_batch_size <= 0:
        raise ValueError("aionbd-upsert-batch-size must be > 0")
    if args.aionbd_mode_ready_timeout_seconds <= 0:
        raise ValueError("aionbd-mode-ready-timeout-seconds must be > 0")
    if args.aionbd_wal_sync_every_n_writes < 0:
        raise ValueError("aionbd-wal-sync-every-n-writes must be >= 0")
    if args.aionbd_wal_sync_interval_seconds < 0:
        raise ValueError("aionbd-wal-sync-interval-seconds must be >= 0")
    if args.bench_cpu_affinity.strip():
        apply_bench_cpu_affinity(args.bench_cpu_affinity)
    selected_engines = {
        item.strip().lower() for item in args.engines.split(",") if item.strip()
    }
    selected_aionbd_modes = [
        item.strip().lower() for item in args.aionbd_modes.split(",") if item.strip()
    ]
    allowed = {"aionbd", "hnswlib", "qdrant"}
    unknown = sorted(selected_engines.difference(allowed))
    if unknown:
        raise ValueError(f"unsupported engines: {', '.join(unknown)}")
    if not selected_aionbd_modes:
        raise ValueError("aionbd-modes must include at least one mode")
    allowed_modes = {"exact", "ivf", "auto"}
    unknown_modes = sorted(set(selected_aionbd_modes).difference(allowed_modes))
    if unknown_modes:
        raise ValueError(f"unsupported aionbd modes: {', '.join(unknown_modes)}")

    dataset_path = Path(args.dataset_path)
    ensure_dataset(dataset_path, args.dataset_url)
    train, test = load_dataset(dataset_path, args.train_size, args.test_size)
    topk = min(args.topk, train.shape[0])
    ground_truth = exact_ground_truth_ids(train, test, topk)

    all_rows: list[dict[str, Any]] = []
    all_skipped: list[dict[str, Any]] = []
    runs: list[dict[str, Any]] = []
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for run_index in range(1, args.repeat + 1):
        rows, skipped = run_single_benchmark(
            args=args,
            selected_engines=selected_engines,
            selected_aionbd_modes=selected_aionbd_modes,
            train=train,
            test=test,
            ground_truth=ground_truth,
            topk=topk,
            run_index=run_index,
        )
        rows_sorted = sorted(rows, key=lambda item: (item["engine"], item["mode"]))
        runs.append({"run": run_index, "results": rows_sorted, "skipped": skipped})
        all_rows.extend(rows)
        all_skipped.extend(skipped)
        if run_index < args.repeat and args.sleep_between_runs > 0:
            time.sleep(args.sleep_between_runs)

    if not all_rows:
        if all_skipped:
            details = "; ".join(
                f"{item.get('engine', 'unknown')}: {item.get('reason', 'unknown')}"
                for item in all_skipped
            )
            raise RuntimeError(
                f"no benchmark engine completed successfully ({details})"
            )
        raise RuntimeError("no benchmark engine completed successfully")

    rows_aggregated = aggregate_rows(all_rows)
    skipped_summary = summarize_skipped(all_skipped)
    report = {
        "generated_at": started_at,
        "dataset_url": args.dataset_url,
        "dataset_path": str(dataset_path),
        "train_size": int(train.shape[0]),
        "test_size": int(test.shape[0]),
        "dimension": int(train.shape[1]),
        "topk": topk,
        "engines_requested": sorted(selected_engines),
        "aionbd_modes_requested": selected_aionbd_modes,
        "aionbd_persistence_enabled": args.aionbd_persistence_enabled == "true",
        "aionbd_wal_sync_on_write": args.aionbd_wal_sync_on_write == "true",
        "aionbd_wal_sync_every_n_writes": int(args.aionbd_wal_sync_every_n_writes),
        "aionbd_wal_sync_interval_seconds": int(args.aionbd_wal_sync_interval_seconds),
        "aionbd_cpu_affinity": args.aionbd_cpu_affinity.strip() or None,
        "bench_cpu_affinity": args.bench_cpu_affinity.strip() or None,
        "repetitions": args.repeat,
        "sleep_between_runs_seconds": float(args.sleep_between_runs),
        "warmup_queries": int(args.warmup_queries),
        "aionbd_batch_size": int(args.aionbd_batch_size),
        "aionbd_upsert_batch_size": int(args.aionbd_upsert_batch_size),
        "aionbd_mode_ready_timeout_seconds": float(
            args.aionbd_mode_ready_timeout_seconds
        ),
        "runs": runs,
        "skipped": skipped_summary,
        "results": rows_aggregated,
    }

    report_json_path = Path(args.report_json)
    report_md_path = Path(args.report_md)
    report_json_path.parent.mkdir(parents=True, exist_ok=True)
    report_md_path.parent.mkdir(parents=True, exist_ok=True)
    report_json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    report_md_path.write_text(
        build_markdown(report, rows_aggregated, skipped_summary), encoding="utf-8"
    )

    print(json.dumps(report, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"error={error}", file=sys.stderr)
        raise SystemExit(1)
