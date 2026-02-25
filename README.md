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
- Persistence uses WAL per write and periodic snapshot checkpoints (`AIONBD_CHECKPOINT_INTERVAL`, default `32`); async checkpoint scheduling is opt-in (`AIONBD_ASYNC_CHECKPOINTS=true`).
- Search supports explicit modes (`exact`, `ivf`, `auto`) with target recall guarantees
  and metadata filtering (`must`/`should` + range clauses).
- Benchmark pipelines publish comparative tables (`recall@k`, p50/p95/p99,
  memory cost, WAL write-path throughput/latency) to `bench/reports/`.
- Bench scenarios include persistence write-path profiling (`AIONBD_BENCH_SCENARIO=persistence_write`)
  to compare fsync policies and grouped WAL append behavior.
- Point payload metadata is supported on upsert/get/search responses.
- Persistence rotates WAL into incremental snapshots and compacts periodically to control restart cost.
- Per-collection point capacity is configurable (`AIONBD_MAX_POINTS_PER_COLLECTION`, default `1000000`).
- WAL durability mode is configurable (`AIONBD_WAL_SYNC_ON_WRITE`, default `true`) with optional periodic fsync cadence (`AIONBD_WAL_SYNC_EVERY_N_WRITES`, default `0`).
- WAL group commit batching is configurable (`AIONBD_WAL_GROUP_COMMIT_MAX_BATCH`, default `16`).
- WAL group commit flush delay is configurable (`AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS`, default `0`).
- Incremental checkpoint compaction threshold is configurable (`AIONBD_CHECKPOINT_COMPACT_AFTER`, default `64`).
- Ops backup/restore helpers are available via `scripts/state_backup_restore.py` (with smoke check `scripts/check_backup_restore_smoke.py`).
- Collection export/import helpers are available via `scripts/collection_export_import.py` (with smoke check `scripts/check_collection_export_import_smoke.py`).
- A mixed read/write soak harness is available via `scripts/run_soak_test.py` (with smoke check `scripts/check_soak_harness_smoke.py`).
- Optional TLS termination is supported in-process via rustls (`AIONBD_TLS_ENABLED=true` with cert/key paths).
- Production guides are available in `docs/production_sizing.md`, `docs/performance_tuning.md`, and `docs/security_notes.md`.
- IVF async rebuild cooldown is configurable (`AIONBD_L2_INDEX_BUILD_COOLDOWN_MS`, default `1000`).
- IVF async build concurrency is configurable (`AIONBD_L2_INDEX_BUILD_MAX_IN_FLIGHT`, default `2`).
- IVF warmup at boot is configurable (`AIONBD_L2_INDEX_WARMUP_ON_BOOT`, default `true`).
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
