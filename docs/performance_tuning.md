# AIONBD Performance Tuning Guide

This guide maps runtime knobs to measurable effects on latency, throughput, and durability.

## 1. First principles

1. Tune with production-like datasets and realistic query/write mix.
2. Make one category of change at a time.
3. Always validate with metrics (`/metrics`, `/metrics/prometheus`) and soak runs.

## 2. Read-path tuning

### 2.1 Query limits

- `AIONBD_MAX_TOPK_LIMIT`: upper bound for search fanout
- `AIONBD_MAX_PAGE_LIMIT`: upper bound for list pagination requests

Lower limits reduce CPU spikes and tail latency.

### 2.2 IVF build pressure

- `AIONBD_L2_INDEX_BUILD_MAX_IN_FLIGHT`: concurrent build jobs
- `AIONBD_L2_INDEX_BUILD_COOLDOWN_MS`: throttles rebuild churn
- `AIONBD_L2_INDEX_WARMUP_ON_BOOT`: prebuild on startup

Watch:
- `aionbd_l2_index_cache_hit_ratio`
- `aionbd_l2_index_build_in_flight`
- `aionbd_search_ivf_fallback_exact_total`

If fallback ratio is high:
1. Increase cache/build capacity carefully.
2. Reduce mutation pressure on hot collections.
3. Validate mode selection in client workloads.

## 3. Write-path tuning

### 3.1 Durability vs throughput

- `AIONBD_WAL_SYNC_ON_WRITE=true`: strongest ACK durability, lower throughput
- `AIONBD_WAL_SYNC_ON_WRITE=false`: higher throughput, weaker crash guarantees
- `AIONBD_WAL_SYNC_EVERY_N_WRITES`: bounded periodic durability when sync-on-write disabled

### 3.2 Group commit

- `AIONBD_WAL_GROUP_COMMIT_MAX_BATCH`: queue batch size
- `AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS`: coalescing window

Increase gradually and monitor:
- `aionbd_persistence_wal_group_queue_depth`
- `aionbd_persistence_wal_group_commits_total`
- request latency p95/p99

## 4. Checkpoint tuning

- `AIONBD_CHECKPOINT_INTERVAL`: checkpoint cadence in write count
- `AIONBD_ASYNC_CHECKPOINTS`: offload checkpointing from write handler path
- `AIONBD_CHECKPOINT_COMPACT_AFTER`: incremental compaction threshold

Monitor:
- `aionbd_persistence_checkpoint_in_flight`
- `aionbd_persistence_checkpoint_error_total`
- `aionbd_persistence_checkpoint_degraded_total`
- `aionbd_persistence_wal_size_bytes`
- `aionbd_persistence_incremental_segments`

If WAL/incremental backlog grows:
1. Reduce ingest burst or increase storage throughput.
2. Lower checkpoint interval.
3. Validate filesystem health and latency.

## 5. API runtime limits

- `AIONBD_MAX_CONCURRENCY`: request concurrency gate
- `AIONBD_REQUEST_TIMEOUT_MS`: timeout budget
- `AIONBD_MAX_BODY_BYTES`: payload size control

Tuning pattern:
1. Raise concurrency until error rate or tail latency worsens.
2. Set timeout to slightly above healthy p99.
3. Keep body limit strict to avoid parse and memory spikes.

## 6. Security/runtime overhead

TLS support is optional:
- `AIONBD_TLS_ENABLED=true`
- `AIONBD_TLS_CERT_PATH`, `AIONBD_TLS_KEY_PATH`

Expect a CPU cost under TLS. Re-tune concurrency after enabling it.

## 7. Practical tuning workflow

1. Baseline:
   - run `./scripts/verify_bench.sh`
   - run short soak (`scripts/run_soak_test.py`)
2. Write-path pass:
   - tune WAL sync/batching/checkpointing
3. Read-path pass:
   - tune index build concurrency/cooldown and top-k caps
4. Runtime pass:
   - tune concurrency/timeout/body size
5. Soak pass:
   - run longer soak and compare p95/p99, errors, backlog trends

## 8. Rollback criteria

Rollback to previous config if any of these regress materially:
- 5xx ratio
- p95/p99 latency
- readiness stability (`aionbd_ready`)
- checkpoint error/degraded rates
- WAL backlog growth rate

Keep previous known-good config checked into deployment automation.
