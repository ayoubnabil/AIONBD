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
- `AIONBD_IVF_NPROBE_DEFAULT`: default IVF probe count used when request does not specify `target_recall`
- `AIONBD_IVF_KMEANS_MAX_TRAINING_POINTS`: max sampled points used for IVF centroid training (reduces build warmup cost on larger collections).
- Default: `8192`. Lower = faster build and potentially lower recall, higher = slower build and potentially higher recall.
- Default: `8` lists. Lower = faster and less recall, higher = slower and more recall.

Watch:
- `aionbd_l2_index_cache_hit_ratio`
- `aionbd_l2_index_build_in_flight`
- `aionbd_search_ivf_fallback_exact_total`

If fallback ratio is high:
1. Increase cache/build capacity carefully.
2. Reduce mutation pressure on hot collections.
3. Validate mode selection in client workloads.

### 2.3 Exact-scan parallelism threshold

- `AIONBD_PARALLEL_SCORE_MIN_POINTS`: minimum collection size before exact scan uses Rayon parallel scoring.
- Default: `256` points.
- `AIONBD_PARALLEL_SCORE_MIN_WORK`: minimum estimated scalar work (`points * dimension`) before exact scan uses Rayon parallel scoring.
- Default: `200000`.
- `AIONBD_PARALLEL_CANDIDATE_IDS_MIN_LEN`: minimum candidate-id list length before candidate scoring uses Rayon parallelism.
- Default: `256` ids.
- `AIONBD_PARALLEL_CANDIDATE_MIN_WORK`: minimum estimated scalar work (`candidates * dimension`) before candidate scoring uses Rayon parallelism.
- Default: `200000`.
- `AIONBD_PARALLEL_SCORE_MIN_CHUNK_LEN`: minimum Rayon chunk length per worker in parallel scoring loops.
- Default: `32` (reduces per-task scheduling overhead on exact scan hot paths).
- `AIONBD_SEARCH_INLINE_MAX_POINTS`: run search inline (without `spawn_blocking`) when collection size is below this threshold and lock is immediately available.
- Default: `8192` points.
- `AIONBD_SEARCH_INLINE_MAX_WORK`: run search inline only when estimated scalar work (`points * dimension * queries`) is below this threshold.
- Default: `1000000` (set `0` to disable the work cap and rely only on `AIONBD_SEARCH_INLINE_MAX_POINTS`).
- `AIONBD_SEARCH_INLINE_LIGHT_LOAD_MAX_WORK`: opportunistic inline cap when request concurrency is low, even if `AIONBD_SEARCH_INLINE_MAX_WORK` is exceeded.
- Default: `20000000` (set `0` to disable opportunistic inline mode).
- `AIONBD_SEARCH_INLINE_LIGHT_LOAD_MAX_IN_FLIGHT`: maximum in-flight HTTP requests allowed for opportunistic inline mode.
- Default: `1` (set `0` to disable opportunistic inline mode).
- `AIONBD_EXACT_BATCH_TRANSPOSE_MIN_QUERIES`: enables an experimental transposed exact-batch kernel when `queries` length is at or above this threshold.
- Default: `160` (enabled by default for larger batch-search requests).

If p95 worsens on small datasets, raise the threshold.  
If CPU cores are under-utilized on larger datasets, lower it gradually.

### 2.4 Payload transfer control

- Search APIs support `include_payload` (`true` by default).
- Set `include_payload=false` for `POST /collections/:name/search`, `POST /collections/:name/search/topk`, and `POST /collections/:name/search/topk/batch` when callers do not need metadata payloads.

Impact:
- Lower response size and JSON serialization cost.
- Avoids payload cloning on the read path.
- Can unlock fast-path scoring for payload-bearing collections when filters are not used.

## 3. Write-path tuning

### 3.1 Durability vs throughput

- `AIONBD_WAL_SYNC_ON_WRITE=true`: strongest ACK durability, lower throughput
- `AIONBD_WAL_SYNC_ON_WRITE=false`: higher throughput, weaker crash guarantees
- `AIONBD_WAL_SYNC_EVERY_N_WRITES`: bounded periodic durability when sync-on-write disabled
- `AIONBD_WAL_SYNC_INTERVAL_SECONDS`: time-based fsync cadence when sync-on-write is disabled (e.g. `10`)

Safety note:
- `AIONBD_WAL_SYNC_ON_WRITE=false` can lose acknowledged writes after crash/power loss.
- `AIONBD_WAL_SYNC_INTERVAL_SECONDS=10` means up to ~10 seconds of acknowledged writes can be lost on abrupt failure.
- Keep defaults untouched for maximum durability.

### 3.2 Group commit

- `AIONBD_WAL_GROUP_COMMIT_MAX_BATCH`: queue batch size
- `AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS`: coalescing window

Increase gradually and monitor:
- `aionbd_persistence_wal_group_queue_depth`
- `aionbd_persistence_wal_group_commits_total`
- request latency p95/p99

### 3.3 Batch upsert

- `POST /collections/:name/points`: upsert multiple points in one request.
- `AIONBD_UPSERT_BATCH_MAX_POINTS`: server-side cap on `points` length for batch upsert.
- Default: `256`.

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
