# AIONBD Production Sizing Guide

This guide provides a practical baseline for capacity planning before production rollout.

## 1. Inputs to collect

For each collection, estimate:
- `N`: total points
- `D`: vector dimension
- payload size per point (bytes)
- write rate (points/s)
- search rate (queries/s)
- expected top-k and filter usage

## 2. Memory sizing baseline

### 2.1 Vector memory

Raw vector payload is approximately:

`vector_bytes ~= N * D * 4`

(`f32` values, 4 bytes each)

### 2.2 Payload and indexing overhead

Add headroom for payload maps, per-point structs, collection maps, and runtime caches:
- payload factor: `+10%` to `+50%` depending on metadata volume
- process/runtime overhead: `+15%` to `+30%`
- IVF cache/index headroom: `+10%` to `+30%` for active search collections

### 2.3 Recommended RAM target

Use:

`target_ram_bytes ~= vector_bytes * 1.5 + payload_bytes`

Then validate with real metrics and soak tests.

## 3. Disk sizing baseline

Persistence includes:
- snapshot file
- WAL file
- incremental snapshot segments (between compactions)

Rule-of-thumb baseline:
- snapshot: near serialized full state size
- WAL working set: bounded by write burst + checkpoint cadence
- incrementals: bounded by `AIONBD_CHECKPOINT_COMPACT_AFTER`

Recommended minimum free disk budget:

`>= 3x` snapshot size + WAL burst headroom.

## 4. Write-path sizing

Key knobs:
- `AIONBD_WAL_SYNC_ON_WRITE`
- `AIONBD_WAL_SYNC_EVERY_N_WRITES`
- `AIONBD_WAL_GROUP_COMMIT_MAX_BATCH`
- `AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS`
- `AIONBD_CHECKPOINT_INTERVAL`
- `AIONBD_ASYNC_CHECKPOINTS`

For durability-first workloads:
- keep `AIONBD_WAL_SYNC_ON_WRITE=true`
- tune `AIONBD_WAL_GROUP_COMMIT_MAX_BATCH` conservatively (`8-32`)

For throughput-first workloads (accepted durability tradeoff):
- `AIONBD_WAL_SYNC_ON_WRITE=false`
- enable periodic sync via `AIONBD_WAL_SYNC_EVERY_N_WRITES`
- monitor `aionbd_persistence_wal_tail_open`

## 5. Search-path sizing

Knobs and limits:
- `AIONBD_MAX_TOPK_LIMIT`
- `AIONBD_MAX_PAGE_LIMIT`
- `AIONBD_L2_INDEX_BUILD_MAX_IN_FLIGHT`
- `AIONBD_L2_INDEX_BUILD_COOLDOWN_MS`

Guidance:
- keep API limits aligned with SLO
- avoid very large default `top_k`
- monitor IVF fallback ratio and index cache hit ratio before raising concurrency

## 6. Concurrency and ingress limits

Primary guardrails:
- `AIONBD_MAX_CONCURRENCY`
- `AIONBD_REQUEST_TIMEOUT_MS`
- `AIONBD_MAX_BODY_BYTES`

Sizing flow:
1. Start with conservative concurrency (for example `128-256`).
2. Run soak test for realistic read/write mix.
3. Increase until p95/p99 latency or error rate degrades.
4. Keep `20-30%` headroom from saturation point.

## 7. Validation checklist before go-live

1. Run `./scripts/verify_local.sh` and `./scripts/verify_bench.sh`.
2. Run mixed soak scenario with `scripts/run_soak_test.py` for representative load windows.
3. Confirm alerting on persistence backlog, checkpoint errors, and readiness.
4. Confirm backup/restore (`scripts/state_backup_restore.py`) and export/import (`scripts/collection_export_import.py`) procedures.
5. Confirm restart behavior under WAL replay with current data volume.

## 8. Example initial profile

Small production pilot profile (starting point):
- `AIONBD_MAX_CONCURRENCY=256`
- `AIONBD_REQUEST_TIMEOUT_MS=2000`
- `AIONBD_MAX_TOPK_LIMIT=200`
- `AIONBD_MAX_PAGE_LIMIT=500`
- `AIONBD_CHECKPOINT_INTERVAL=32`
- `AIONBD_ASYNC_CHECKPOINTS=true`
- `AIONBD_WAL_GROUP_COMMIT_MAX_BATCH=16`
- `AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS=1`
- `AIONBD_L2_INDEX_BUILD_MAX_IN_FLIGHT=2`

Treat this as a baseline only; tune from metrics and workload traces.
