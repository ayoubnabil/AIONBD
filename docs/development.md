# AIONBD Development Guide

## Prerequisites

- Rust toolchain (stable)
- Python 3.10+

## Rust commands

Format:
```bash
cargo fmt --all
```

Lint:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Test:
```bash
cargo test --workspace
```

Full local verification:
```bash
./scripts/verify_local.sh
./scripts/verify_local.sh --fast
./scripts/verify_local.sh --changed
./scripts/verify_chaos.sh
```

Run API:
```bash
cargo run -p aionbd-server
```

Run bench:
```bash
cargo run --release -p aionbd-bench
```

Bench scenarios:
```bash
AIONBD_BENCH_SCENARIO=all cargo run --release -p aionbd-bench
AIONBD_BENCH_SCENARIO=dot cargo run --release -p aionbd-bench
AIONBD_BENCH_SCENARIO=l2 cargo run --release -p aionbd-bench
AIONBD_BENCH_SCENARIO=collection cargo run --release -p aionbd-bench
AIONBD_BENCH_SCENARIO=list_points cargo run --release -p aionbd-bench
AIONBD_BENCH_SCENARIO=persistence_write cargo run --release -p aionbd-bench
AIONBD_BENCH_SCENARIO=search_quality cargo run --release -p aionbd-bench
```

Benchmark pipelines (search quality + persistence write reports):
```bash
./scripts/verify_bench.sh
```

Optional quality gates for recall/perf/memory targets:
```bash
AIONBD_BENCH_MIN_RECALL_IVF=0.80 \
AIONBD_BENCH_MIN_RECALL_AUTO=0.95 \
AIONBD_BENCH_MAX_P95_RATIO_IVF=1.00 \
AIONBD_BENCH_MAX_P95_RATIO_AUTO=1.00 \
AIONBD_BENCH_MAX_MEMORY_RATIO_IVF=1.50 \
AIONBD_BENCH_MAX_MEMORY_RATIO_AUTO=1.50 \
./scripts/verify_bench.sh
```

Optional report output paths:
- `AIONBD_BENCH_REPORT_PATH` (default: `bench/reports/search_quality_report.md`)
- `AIONBD_BENCH_REPORT_JSON_PATH` (default: `bench/reports/search_quality_report.json`)
- `AIONBD_BENCH_PERSISTENCE_REPORT_PATH` (default: `bench/reports/persistence_write_report.md`)
- `AIONBD_BENCH_PERSISTENCE_REPORT_JSON_PATH` (default: `bench/reports/persistence_write_report.json`)

Default benchmark gates used by `./scripts/verify_bench.sh`:
- `AIONBD_BENCH_MIN_RECALL_IVF=0.90`
- `AIONBD_BENCH_MIN_RECALL_AUTO=0.90`
- `AIONBD_BENCH_MAX_P95_RATIO_IVF=1.00`
- `AIONBD_BENCH_MAX_P95_RATIO_AUTO=1.00`
- `AIONBD_BENCH_MAX_MEMORY_RATIO_IVF=1.50`
- `AIONBD_BENCH_MAX_MEMORY_RATIO_AUTO=1.50`
- `AIONBD_BENCH_MIN_QPS_RATIO_SINGLE_SYNC_EVERY_N=2.00`
- `AIONBD_BENCH_MIN_QPS_RATIO_GROUP_SYNC_EACH_BATCH=1.20`
- `AIONBD_BENCH_MIN_QPS_RATIO_GROUP_SYNC_EVERY_N=1.50`
- `AIONBD_BENCH_MAX_WAL_RATIO_SINGLE_SYNC_EVERY_N=1.10`
- `AIONBD_BENCH_MAX_WAL_RATIO_GROUP_SYNC_EACH_BATCH=1.10`
- `AIONBD_BENCH_MAX_WAL_RATIO_GROUP_SYNC_EVERY_N=1.10`

## Server runtime configuration

- `AIONBD_BIND` (default: `127.0.0.1:8080`)
- `AIONBD_TLS_ENABLED` (default: `false`; when `true`, serve HTTPS using rustls)
- `AIONBD_TLS_CERT_PATH` (required when TLS enabled; PEM certificate chain file)
- `AIONBD_TLS_KEY_PATH` (required when TLS enabled; PEM private key file)
- `AIONBD_MAX_DIMENSION` (default: `4096`)
- `AIONBD_MAX_POINTS_PER_COLLECTION` (default: `1000000`, must be `> 0`)
- `AIONBD_STRICT_FINITE` (default: `true`)
- `AIONBD_REQUEST_TIMEOUT_MS` (default: `2000`)
- `AIONBD_MAX_BODY_BYTES` (default: `1048576`)
- `AIONBD_MAX_CONCURRENCY` (default: `256`)
- `AIONBD_MAX_PAGE_LIMIT` (default: `1000`)
- `AIONBD_MAX_TOPK_LIMIT` (default: `1000`)
- `AIONBD_CHECKPOINT_INTERVAL` (default: `32`)
- `AIONBD_PERSISTENCE_ENABLED` (default: `true`)
- `AIONBD_WAL_SYNC_ON_WRITE` (default: `true`; set `false` only for throughput-over-durability tradeoff)
- `AIONBD_WAL_SYNC_EVERY_N_WRITES` (default: `0`; when `AIONBD_WAL_SYNC_ON_WRITE=false`, force fsync every N writes)
- `AIONBD_WAL_GROUP_COMMIT_MAX_BATCH` (default: `16`; max number of queued WAL writes coalesced per group commit)
- `AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS` (default: `0`; coalescing window before WAL group commit flush)
- `AIONBD_ASYNC_CHECKPOINTS` (default: `false`; set `true` to run periodic checkpointing off the write request path)
- `AIONBD_CHECKPOINT_COMPACT_AFTER` (default: `64`; compact incremental segments into snapshot after this many segments)
- `AIONBD_SNAPSHOT_PATH` (default: `data/aionbd_snapshot.json`)
- `AIONBD_WAL_PATH` (default: `data/aionbd_wal.jsonl`)
- `AIONBD_AUTH_MODE` (default: `disabled`, values: `disabled|api_key|bearer_token|api_key_or_bearer_token|jwt|api_key_or_jwt`)
- `AIONBD_AUTH_API_KEYS` (default: empty, format `<tenant>:<api_key>[,...]`)
- `AIONBD_AUTH_BEARER_TOKENS` (default: empty, format `<tenant>:<token>[,...]`)
- `AIONBD_AUTH_JWT_HS256_SECRET` (required when mode includes `jwt`)
- `AIONBD_AUTH_JWT_ISSUER` (optional exact issuer)
- `AIONBD_AUTH_JWT_AUDIENCE` (optional comma-separated accepted audiences)
- `AIONBD_AUTH_JWT_TENANT_CLAIM` (optional, default: `tenant`)
- `AIONBD_AUTH_JWT_PRINCIPAL_CLAIM` (optional, default: `sub`)
- `AIONBD_AUTH_RATE_LIMIT_PER_MINUTE` (default: `0`, disabled when `0`)
- `AIONBD_AUTH_RATE_WINDOW_RETENTION_MINUTES` (default: `60`, must be `> 0`)
- `AIONBD_AUTH_TENANT_MAX_COLLECTIONS` (default: `0`, disabled when `0`)
- `AIONBD_AUTH_TENANT_MAX_POINTS` (default: `0`, disabled when `0`)
- `AIONBD_L2_INDEX_BUILD_COOLDOWN_MS` (default: `1000`; set `0` to disable cooldown throttling)
- `AIONBD_L2_INDEX_BUILD_MAX_IN_FLIGHT` (default: `2`; maximum concurrent asynchronous IVF build jobs)
- `AIONBD_L2_INDEX_WARMUP_ON_BOOT` (default: `true`; set `false` to skip startup warmup builds)

## API endpoints

- `GET /live`: liveness endpoint
- `GET /ready`: readiness endpoint
- `GET /metrics`: JSON metrics payload (SLO/ops)
- `GET /metrics/prometheus`: Prometheus text format
- `POST /distance`: vector operation endpoint with input validation
- `POST /collections`: create collection `{name, dimension, strict_finite}`
- `GET /collections`: list collections
- `GET /collections/:name`: collection metadata
- `DELETE /collections/:name`: delete collection
- `POST /collections/:name/search`: top-1 search
  `{query, metric, mode, target_recall, filter}`
- `POST /collections/:name/search/topk`: top-k search
  `{query, metric, limit, mode, target_recall, filter}`
  with `limit <= AIONBD_MAX_TOPK_LIMIT` (default `10` when omitted, capped by config)
- `GET /collections/:name/points`: list point ids with pagination:
  `?offset=<n>&limit=<n>` (offset mode) or `?after_id=<id>&limit=<n>` (cursor mode),
  with `limit <= AIONBD_MAX_PAGE_LIMIT` (default `100` when omitted, capped by config)
- `PUT /collections/:name/points/:id`: upsert point `{values, payload}`
- `GET /collections/:name/points/:id`: read point
- `DELETE /collections/:name/points/:id`: delete point

## Ops artifacts

- Prometheus alert rules: `ops/prometheus/aionbd-alerts.yml`
- Grafana dashboard: `ops/grafana/aionbd-overview.json`
- Sizing guide: `docs/production_sizing.md`
- Performance tuning guide: `docs/performance_tuning.md`
- Security notes: `docs/security_notes.md`
- Soak runbook: `docs/soak_runbook.md`

## Backup and restore

Create a persistence backup archive (`snapshot`, `wal`, and incremental segments):
```bash
python3 scripts/state_backup_restore.py backup \
  --snapshot-path data/aionbd_snapshot.json \
  --wal-path data/aionbd_wal.jsonl \
  --output backups/aionbd-backup.tar.gz
```

Restore from an archive (use `--force` only when overwrite is intended):
```bash
python3 scripts/state_backup_restore.py restore \
  --input backups/aionbd-backup.tar.gz \
  --snapshot-path data/aionbd_snapshot.json \
  --wal-path data/aionbd_wal.jsonl \
  --force
```

Ops smoke check for backup/restore tooling:
```bash
python3 scripts/check_backup_restore_smoke.py
```

## Collection export/import

Export one collection as NDJSON stream:
```bash
python3 scripts/collection_export_import.py export \
  --base-url http://127.0.0.1:8080 \
  --collection demo \
  --output exports/demo.ndjson
```

Import into a target collection:
```bash
python3 scripts/collection_export_import.py import \
  --base-url http://127.0.0.1:8080 \
  --input exports/demo.ndjson \
  --collection demo_copy \
  --if-exists fail
```

Offline smoke check for export/import tooling:
```bash
python3 scripts/check_collection_export_import_smoke.py
```

## Soak test harness

Run a mixed read/write soak scenario (example 10 minutes):
```bash
python3 scripts/run_soak_test.py \
  --base-url http://127.0.0.1:8080 \
  --collection soak \
  --dimension 256 \
  --duration-seconds 600 \
  --workers 8 \
  --write-ratio 0.2 \
  --metric l2 \
  --search-mode auto \
  --report-json bench/reports/soak_report.json
```

Fast harness smoke check (no server required):
```bash
python3 scripts/check_soak_harness_smoke.py
```

Run soak pipeline profiles with report publication:
```bash
python3 scripts/run_soak_pipeline.py \
  --base-url http://127.0.0.1:8080 \
  --collection-prefix soak_pipeline \
  --report-path bench/reports/soak_pipeline_report.md \
  --report-json-path bench/reports/soak_pipeline_report.json
```

Run long soak profiles from file (24h/72h template):
```bash
python3 scripts/run_soak_pipeline.py \
  --base-url http://127.0.0.1:8080 \
  --collection-prefix soak_longrun \
  --profiles-file ops/soak/longrun_profiles.json \
  --report-path bench/reports/soak_longrun_report.md \
  --report-json-path bench/reports/soak_longrun_report.json
```

Quick dry-run validation of soak pipeline:
```bash
python3 scripts/check_soak_pipeline_smoke.py
```

Wrapper for soak pipeline runs:
```bash
./scripts/verify_soak.sh
AIONBD_SOAK_DRY_RUN=1 ./scripts/verify_soak.sh
AIONBD_SOAK_DRY_RUN=1 ./scripts/verify_soak.sh --profiles-file ops/soak/longrun_profiles.json
```

## Python SDK commands

From `sdk/python/`:
```bash
python -m pip install -e .
python -c "from aionbd import AionBDClient; print(AionBDClient().live())"
```

## Coding standards

1. No unsafe Rust in this phase.
2. Public interfaces should be documented.
3. New features must include at least baseline tests.
4. Bench-visible changes must document expected impact.
5. Avoid oversized files: split files before they exceed 300 lines.

## Git and review policy

1. Every change must be done on a dedicated branch.
2. Direct work on `main` is forbidden.
3. Every branch must be reviewed by an expert before merge.
4. Merge to `main` is allowed only after explicit expert validation.
