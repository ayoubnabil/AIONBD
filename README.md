# AIONBD

AIONBD is an edge-first vector database project focused on low-latency search, offline operation, and constrained devices.

This repository is initialized as a clean monorepo skeleton:
- `core/` Rust library for vector primitives
- `server/` Rust HTTP API skeleton
- `bench/` Rust micro-benchmark CLI
- `sdk/python/` Python SDK skeleton
- `docs/` architecture and development docs

## Quick Start

1. Build and test all Rust crates:
```bash
cargo test --workspace
```

2. Run the API server:
```bash
cargo run -p aionbd-server
```

3. Run the micro-benchmark:
```bash
cargo run --release -p aionbd-bench
```

4. Run the search quality benchmark pipeline report:
```bash
./scripts/verify_bench.sh
```

5. Use the Python SDK:
```bash
cd sdk/python
python -m pip install -e .
python -c "from aionbd import AionBDClient; print(AionBDClient().live())"
```

## Notes

- `ROADMAP_EDGEVECTOR.md` is ignored intentionally in git.
- This step focuses on project structure and baseline quality.
- Contribution flow is branch-first local with mandatory expert review before merge (`CONTRIBUTING.md`).
- Persistence uses WAL per write and periodic snapshot checkpoints (`AIONBD_CHECKPOINT_INTERVAL`, default `32`).
- Search supports explicit modes (`exact`, `ivf`, `auto`) with target recall guarantees
  and metadata filtering (`must`/`should` + range clauses).
- Benchmark pipeline publishes dataset comparative tables (`recall@k`, p50/p95/p99,
  memory cost) to `bench/reports/`.
- Point payload metadata is supported on upsert/get/search responses.
- Persistence rotates WAL into incremental snapshots and compacts periodically to control restart cost.
- Per-collection point capacity is configurable (`AIONBD_MAX_POINTS_PER_COLLECTION`, default `1000000`).
- WAL durability mode is configurable (`AIONBD_WAL_SYNC_ON_WRITE`, default `true`).
- Cached L2 IVF indexes are invalidated automatically on collection and point mutations.
- `/metrics` exposes runtime counts including aggregate readiness, HTTP request counters
  (total/in-flight/2xx/4xx/5xx), and request latency aggregates
  (`http_request_duration_us_total/max/avg`).
- Server endpoints:
  - `GET /live`
  - `GET /ready`
  - `GET /metrics`
  - `GET /metrics/prometheus`
  - `POST /distance`
  - `POST /collections`
  - `GET /collections`
  - `GET /collections/:name`
  - `DELETE /collections/:name`
  - `POST /collections/:name/search`
  - `POST /collections/:name/search/topk`
  - `GET /collections/:name/points`
  - `PUT /collections/:name/points/:id`
  - `GET /collections/:name/points/:id`
  - `DELETE /collections/:name/points/:id`
