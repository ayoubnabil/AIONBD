# Optimizations And Benchmarks

This document tracks optimization-oriented details and reproducible benchmark workflows.

## Internal Benchmarks

Run benchmarks locally with the commands below and keep result artifacts in your own storage.

### Internal Suite (`aionbd-bench`)

| Scenario | Key results |
|---|---|
| Core dot scan (`10k x d128`) | `6,945,199.54` QPS, p50 `1.420` ms, p95 `1.577` ms |
| Core l2 scan (`10k x d128`) | `7,030,746.86` QPS, p50 `1.382` ms, p95 `1.660` ms |
| Collection list_points (`50k`, page `256`) | cursor `713,509.28` pages/s vs offset `11,823.63` pages/s (`60.35x`) |
| Search quality (uniform `20k d128`) | exact recall@10 `1.0`, p95 `9.722` ms; auto recall@10 `1.0`, p95 `0.620` ms |
| Persistence write path (`4096` writes, `d64`) | `single_sync_each_write`: `1,685.88` QPS; `single_sync_every_n`: `9,189.78` QPS (`5.45x`) |
| MVP mixed profile (`60s`, `8` workers, `25%` writes) | throughput `1,272.387` ops/s, error_rate `0.0079%`, p95 `20 ms`, p99 `20 ms` |

## Open-Source Matrix

Dataset: Fashion-MNIST `d784`, `topk=10`, AIONBD memory budget constrained below `2GB`.

Batch-serving profile (`aionbd-batch-size=128`):

| Dataset size | AIONBD auto QPS / p95 | AIONBD exact QPS / p95 | Qdrant exact QPS / p95 |
|---|---:|---:|---:|
| train `20000`, test `500` (`repeat=3`, median) | `1455.44` / `0.73` ms | `1536.60` / `0.73` ms | `110.74` / `12.65` ms |
| train `50000`, test `500` (`repeat=2`, median) | `761.29` / `1.41` ms | `776.30` / `1.39` ms | `63.71` / `21.02` ms |
| train `60000`, test `500` (`repeat=2`, median) | `611.47` / `1.73` ms | `675.36` / `1.66` ms | `57.22` / `22.98` ms |

Pinned-CPU profile on a loaded workstation (`bench-cpu-affinity=0`, `aionbd-cpu-affinity=1-11`, train `20000`, test `500`, `repeat=2`, median):

| AIONBD auto QPS / p95 | AIONBD exact QPS / p95 | Qdrant exact QPS / p95 |
|---:|---:|---:|
| `767.13` / `1.40` ms | `683.36` / `1.77` ms | `64.89` / `21.03` ms |

Single-query profile (`aionbd-batch-size=1`, train `20000`, test `500`, `repeat=3`, median):

| Engine/mode | QPS | p95 |
|---|---:|---:|
| AIONBD auto | `175.01` | `7.62` ms |
| Qdrant exact | `115.73` | `11.85` ms |

Persistence-enabled open-source profile (`d784`, `topk=10`, local run, `repeat=1`):

| Scenario | AIONBD mode | AIONBD QPS / p95 / recall@10 | Qdrant exact QPS / p95 / recall@10 |
|---|---|---:|---:|
| train `100000`, test `300`, `aionbd-batch-size=1` | auto (effective exact), exact | auto: `53.19` / `20.94` ms / `0.9807`; exact: `46.50` / `24.09` ms / `0.9807` | `31.35` / `54.38` ms / `0.9787` |
| train `500000`, test `100`, `aionbd-batch-size=128` | auto (effective exact) | `94.78` / `10.55` ms / `0.8710` | `6.80` / `222.41` ms / `0.8710` |

Reference report artifacts:
- `bench/reports/open_source_bench/aionbd_qdrant_100k_persist_opt_local.json`
- `bench/reports/open_source_bench/aionbd_qdrant_500k_persist_batch128_opt_local.json`

Interpretation guardrails:
- Qdrant path in this wrapper is exact search (`params.exact=true`) and per-query HTTP.
- AIONBD numbers include both single-query and batch-query serving profiles.
- Wrapper reports are for reproducible local positioning, not a leaderboard claim.
- Always run on your target hardware before publishing claims.

## Optimization Validation

Exact batch fast-path (payload-enabled) can be validated with:

```bash
python3 scripts/run_exact_batch_fastpath_microbench.py \
  --aionbd-bin target/release/aionbd-server \
  --points 20000 \
  --dimension 784 \
  --query-pool 512 \
  --batch-size 64 \
  --rounds 40 \
  --topk 10 \
  --upsert-batch 32
```

Reference output on this machine:
- fast path enabled: `~589` QPS, p95 `~1.96 ms/query`
- fast path disabled (`AIONBD_EXACT_BATCH_SMALL_TOPK_LIMIT=0`): `~189` QPS, p95 `~5.74 ms/query`
- net impact: about `+211%` QPS and `-66%` p95 on this scenario

Practical guidance:
- For throughput-sensitive workloads, prefer `POST /collections/:name/search/topk/batch`.
- Keep `search/topk` for low-volume or latency-sensitive single-query integrations.

## Memory Budget Validation

Resource-manager checks under constrained RAM:
- With `AIONBD_MEMORY_BUDGET_MB=1536`, ingesting `60000` points (`d784`) uses `188,160,000` bytes and stays under budget.
- With `AIONBD_MEMORY_BUDGET_MB=100`, ingestion is rejected with `429 resource_exhausted` around `33,408` points, with metrics reporting budget boundary usage.

## Reproduction Commands

```bash
./scripts/verify_bench.sh
AIONBD_BENCH_SCENARIO=all cargo run --release -p aionbd-bench

docker context use default
AIONBD_MEMORY_BUDGET_MB=1536 python3 scripts/run_ann_open_bench_wrapper.py \
  --train-size 20000 \
  --test-size 500 \
  --topk 10 \
  --engines aionbd,qdrant \
  --aionbd-modes exact,auto \
  --aionbd-batch-size 128 \
  --repeat 3 \
  --sleep-between-runs 0.3 \
  --report-json /tmp/aionbd_qdrant_20k_500_under2gb.json \
  --report-md /tmp/aionbd_qdrant_20k_500_under2gb.md

AIONBD_MEMORY_BUDGET_MB=1536 python3 scripts/run_ann_open_bench_wrapper.py \
  --train-size 50000 \
  --test-size 500 \
  --topk 10 \
  --engines aionbd,qdrant \
  --aionbd-modes exact,auto \
  --aionbd-batch-size 128 \
  --repeat 2 \
  --sleep-between-runs 0.3 \
  --report-json /tmp/aionbd_qdrant_50k_500_under2gb.json \
  --report-md /tmp/aionbd_qdrant_50k_500_under2gb.md

AIONBD_MEMORY_BUDGET_MB=1536 python3 scripts/run_ann_open_bench_wrapper.py \
  --train-size 60000 \
  --test-size 500 \
  --topk 10 \
  --engines aionbd,qdrant \
  --aionbd-modes exact,auto \
  --aionbd-batch-size 128 \
  --repeat 2 \
  --sleep-between-runs 0.3 \
  --report-json /tmp/aionbd_qdrant_60k_500_under2gb.json \
  --report-md /tmp/aionbd_qdrant_60k_500_under2gb.md

AIONBD_MEMORY_BUDGET_MB=1536 python3 scripts/run_ann_open_bench_wrapper.py \
  --train-size 20000 \
  --test-size 500 \
  --topk 10 \
  --engines aionbd,qdrant \
  --aionbd-modes auto \
  --aionbd-batch-size 1 \
  --repeat 3 \
  --sleep-between-runs 0.3 \
  --report-json /tmp/aionbd_qdrant_20k_500_single_query_under2gb.json \
  --report-md /tmp/aionbd_qdrant_20k_500_single_query_under2gb.md
```

For better reproducibility on busy developer workstations, pin client/server to separate CPU sets:

```bash
python3 scripts/run_ann_open_bench_wrapper.py \
  ... \
  --bench-cpu-affinity 0-3 \
  --aionbd-cpu-affinity 4-7

python3 scripts/run_ann_open_bench_wrapper.py \
  --dataset-path bench/data/ann/fashion-mnist-100k.hdf5 \
  --train-size 100000 \
  --test-size 300 \
  --topk 10 \
  --engines aionbd,qdrant \
  --aionbd-modes exact,auto \
  --aionbd-batch-size 1 \
  --aionbd-upsert-batch-size 1024 \
  --aionbd-persistence-enabled true \
  --aionbd-wal-sync-on-write false \
  --report-json bench/reports/open_source_bench/aionbd_qdrant_100k_persist_opt_local.json \
  --report-md bench/reports/open_source_bench/aionbd_qdrant_100k_persist_opt_local.md

python3 scripts/run_ann_open_bench_wrapper.py \
  --dataset-path bench/data/ann/fashion-mnist-500k.hdf5 \
  --train-size 500000 \
  --test-size 100 \
  --topk 10 \
  --engines aionbd,qdrant \
  --aionbd-modes auto \
  --aionbd-batch-size 128 \
  --aionbd-upsert-batch-size 1024 \
  --aionbd-persistence-enabled true \
  --aionbd-wal-sync-on-write false \
  --report-json bench/reports/open_source_bench/aionbd_qdrant_500k_persist_batch128_opt_local.json \
  --report-md bench/reports/open_source_bench/aionbd_qdrant_500k_persist_batch128_opt_local.md
```

## Optimization Tuning Variables

| Variable | Default | Purpose |
|---|---|---|
| `AIONBD_PARALLEL_SCORE_MIN_CHUNK_LEN` | `32` | Minimum Rayon chunk size used by parallel scoring loops (higher values reduce scheduling overhead on exact scan paths) |
| `AIONBD_EXACT_BATCH_SMALL_TOPK_LIMIT` | `64` | Enables exact batch small-topk fast path up to this `limit` (`0` disables, effective hard cap: `64`) |
| `AIONBD_EXACT_BATCH_TRANSPOSE_MIN_QUERIES` | `160` | Query count threshold to switch to transposed exact batch kernel |
| `AIONBD_IVF_KMEANS_MAX_TRAINING_POINTS` | `8192` | IVF training sample cap for centroid build warmup cost control |
