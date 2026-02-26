# AIONBD

> Licensing notice: this repository is currently under an evaluation license.  
> Production use is prohibited for now.

AIONBD is an edge-first vector database for AI retrieval workloads that need predictable behavior, low operational overhead, and explicit control over durability/performance tradeoffs.

Built for teams that deploy outside large centralized clusters:
- on constrained machines
- with intermittent connectivity
- with strict resource budgets
- with clear SLO and recovery requirements

## Why AIONBD

- `Edge-ready by default`: single Rust server, local-first persistence, no mandatory external control plane.
- `Deterministic limits`: explicit caps for memory budget, collection size, payload size, request concurrency, and query fanout.
- `Search modes you can control`: `exact`, `ivf`, and `auto` with target recall.
- `Durability is explicit`: safe default (`sync-on-write`) with optional high-throughput WAL sync modes.
- `Operationally observable`: `/metrics` JSON + `/metrics/prometheus`, plus runbooks, soak/chaos tooling, and regression gates.

## Current Status

AIONBD is in public preview with active performance work.

Important:
- The codebase can be tested and evaluated.
- Production use is currently prohibited by the project license.
- A separate production/commercial authorization will be required later.

What is already production-oriented:
- persistence with WAL + snapshots + incremental compaction
- memory budget enforcement via resource manager
- TLS and auth modes (API key, bearer token, JWT)
- tenant quotas and rate limits
- backup/restore + collection export/import tooling

## Development Goals Before Stable Release

- Complete final durability and recovery hardening pass under stress/chaos scenarios.
- Lock API and SDK compatibility policy for `v1.0.0`.
- Expand reproducible benchmark matrix across additional datasets and hardware profiles.
- Finalize deployment packaging quality gates (container, systemd, Kubernetes, Helm, release bundles).
- Finish security hardening checklist and publish the stable support policy.

## Quick Start

### 1. Build and run

```bash
cargo run -p aionbd-server
```

Server defaults to `127.0.0.1:8080`.

### 2. Create a collection

```bash
curl -sS -X POST http://127.0.0.1:8080/collections \
  -H 'content-type: application/json' \
  -d '{"name":"demo","dimension":4,"strict_finite":true}'
```

### 3. Upsert points (batch)

```bash
curl -sS -X POST http://127.0.0.1:8080/collections/demo/points \
  -H 'content-type: application/json' \
  -d '{
    "points": [
      {"id": 1, "values": [0.1, 0.2, 0.3, 0.4], "payload": {"label": "alpha"}},
      {"id": 2, "values": [0.2, 0.1, 0.4, 0.3], "payload": {"label": "beta"}},
      {"id": 3, "values": [0.9, 0.8, 0.7, 0.6], "payload": {"label": "gamma"}}
    ]
  }'
```

### 4. Search top-k

```bash
curl -sS -X POST http://127.0.0.1:8080/collections/demo/search/topk \
  -H 'content-type: application/json' \
  -d '{
    "query": [0.1, 0.2, 0.3, 0.4],
    "metric": "l2",
    "limit": 2,
    "mode": "auto",
    "include_payload": true
  }'
```

### 5. Check health and metrics

```bash
curl -sS http://127.0.0.1:8080/live
curl -sS http://127.0.0.1:8080/ready
curl -sS http://127.0.0.1:8080/metrics
curl -sS http://127.0.0.1:8080/metrics/prometheus
```

### 6. Run with Docker Compose (MVP packaging)

```bash
cp ops/deploy/.env.mvp.example ops/deploy/.env.mvp
docker compose -f ops/deploy/docker-compose.mvp.yml --env-file ops/deploy/.env.mvp up -d --build
./scripts/smoke_container_mvp.sh
```

### 7. JavaScript SDK quick start

```bash
cd sdk/js
npm install
node -e "import('./src/index.js').then(async ({ AionBDClient }) => { const c = new AionBDClient('http://127.0.0.1:8080'); console.log(await c.live()); })"
```

Full JS SDK docs: `sdk/js/README.md`.
TypeScript typings are bundled with the JS SDK package.
JS SDK changelog: `sdk/js/CHANGELOG.md`.

### 8. Python SDK quick start

```bash
cd sdk/python
python -m pip install -e .
python -c "from aionbd import AionBDClient; print(AionBDClient().live())"
```

### 9. Authentication quick start (API key mode)

```bash
export AIONBD_AUTH_MODE=api_key
export AIONBD_AUTH_API_KEYS='tenant-a:secret-key-a'
export AIONBD_AUTH_API_KEY_SCOPES='secret-key-a:admin'
cargo run -p aionbd-server --features exp_auth_api_key_scopes
```

Then call endpoints with:

```bash
curl -sS -X GET http://127.0.0.1:8080/collections \
  -H 'x-api-key: secret-key-a'
```

### 10. Memory budget quick start

```bash
export AIONBD_MEMORY_BUDGET_MB=128
cargo run -p aionbd-server
```

## API Surface

- `GET /live`
- `GET /ready`
- `GET /metrics`
- `GET /metrics/prometheus`
- `POST /distance`
- `POST /collections`
- `GET /collections`
- `GET /collections/:name`
- `DELETE /collections/:name`
- `PUT /collections/:name/points/:id`
- `GET /collections/:name/points/:id`
- `DELETE /collections/:name/points/:id`
- `POST /collections/:name/points` (batch upsert)
- `GET /collections/:name/points` (offset/cursor pagination)
- `POST /collections/:name/points/count` (feature: `exp_points_count`)
- `POST /collections/:name/points/payload/set` (feature: `exp_payload_mutation_api`)
- `POST /collections/:name/points/payload/delete` (feature: `exp_payload_mutation_api`)
- `POST /collections/:name/search` (top-1)
- `POST /collections/:name/search/topk`
- `POST /collections/:name/search/topk/batch`

## Experimental Build Features

All experimental features are compile-time opt-in and disabled by default.

- `exp_auth_api_key_scopes`:
  - Purpose: enforce API key scopes (`read|write|admin`) on write routes.
  - Enable:
    ```bash
    cargo run -p aionbd-server --features exp_auth_api_key_scopes
    ```
  - Minimal setup:
    ```bash
    export AIONBD_AUTH_MODE=api_key
    export AIONBD_AUTH_API_KEYS='tenant-a:secret-key-a'
    export AIONBD_AUTH_API_KEY_SCOPES='secret-key-a:read'
    ```
  - Result: read routes are allowed, write routes return `403` with read-only keys.

- `exp_filter_must_not`:
  - Purpose: add negative filter clauses (`must_not`) for search/count filters.
  - Enable:
    ```bash
    cargo run -p aionbd-server --features exp_filter_must_not
    ```
  - Search example:
    ```bash
    curl -sS -X POST http://127.0.0.1:8080/collections/demo/search/topk \
      -H 'content-type: application/json' \
      -d '{
        "query": [0.1, 0.2, 0.3, 0.4],
        "metric": "l2",
        "limit": 5,
        "filter": {
          "must_not": [{"field": "tier", "value": "gold"}]
        }
      }'
    ```
  - Without this feature, requests using `must_not` return `400`.

- `exp_points_count`:
  - Purpose: enable `POST /collections/:name/points/count` with optional filter.
  - Enable:
    ```bash
    cargo run -p aionbd-server --features exp_points_count
    ```
  - Endpoint example:
    ```bash
    curl -sS -X POST http://127.0.0.1:8080/collections/demo/points/count \
      -H 'content-type: application/json' \
      -d '{"filter":{"must":[{"field":"tier","value":"gold"}]}}'
    ```
    Response shape:
    ```json
    {"count": 42}
    ```

- `exp_payload_mutation_api`:
  - Purpose: enable partial metadata updates without re-sending vectors.
  - Endpoints:
    - `POST /collections/:name/points/payload/set`
    - `POST /collections/:name/points/payload/delete`
  - Enable:
    ```bash
    cargo run -p aionbd-server --features exp_payload_mutation_api
    ```
  - Set payload fields:
    ```bash
    curl -sS -X POST http://127.0.0.1:8080/collections/demo/points/payload/set \
      -H 'content-type: application/json' \
      -d '{"points":[1,2], "payload":{"tier":"pro","region":"eu"}}'
    ```
  - Delete payload fields:
    ```bash
    curl -sS -X POST http://127.0.0.1:8080/collections/demo/points/payload/delete \
      -H 'content-type: application/json' \
      -d '{"points":[1,2], "keys":["region"]}'
    ```

Enable several experimental features together:

```bash
cargo run -p aionbd-server --features exp_auth_api_key_scopes,exp_filter_must_not,exp_points_count,exp_payload_mutation_api
```

## Performance and Benchmarks

Performance details, optimization notes, and reproducible benchmark commands are in:
- `docs/optimizations_and_benchmarks.md`
- `docs/development.md`
- `docs/README.md` (documentation index)

Recent local benchmark positioning (reproducible command and raw report):
- AIONBD auto: ~`767 QPS`, p95 ~`1.40 ms`, recall@10 `1.0`
- AIONBD exact: ~`683 QPS`, p95 ~`1.77 ms`, recall@10 `1.0`
- Qdrant exact: ~`65 QPS`, p95 ~`21.03 ms`, recall@10 `1.0`
- Command: `python3 scripts/run_ann_open_bench_wrapper.py --train-size 20000 --test-size 500 --topk 10 --engines aionbd,qdrant --aionbd-modes exact,auto --aionbd-batch-size 128`

## Configuration Reference (Most Used)

Full runtime list: `docs/development.md`.

### Core runtime and limits

| Variable | Default | Purpose |
|---|---|---|
| `AIONBD_BIND` | `127.0.0.1:8080` | HTTP bind address |
| `AIONBD_MAX_DIMENSION` | `4096` | Max vector dimension |
| `AIONBD_MAX_POINTS_PER_COLLECTION` | `1000000` | Per-collection capacity guardrail |
| `AIONBD_MEMORY_BUDGET_MB` / `AIONBD_MEMORY_BUDGET_BYTES` | `0` | Memory budget (`0` = unlimited) |
| `AIONBD_MAX_CONCURRENCY` | `256` | Request concurrency cap |
| `AIONBD_REQUEST_TIMEOUT_MS` | `2000` | Request timeout |
| `AIONBD_MAX_BODY_BYTES` | `1048576` | Max request payload size |
| `AIONBD_MAX_TOPK_LIMIT` | `1000` | Search `limit` cap |
| `AIONBD_MAX_PAGE_LIMIT` | `1000` | List pagination cap |
| `AIONBD_UPSERT_BATCH_MAX_POINTS` | `256` | Batch upsert points cap |
| `AIONBD_SEARCH_BATCH_MAX_QUERIES` | `256` | Batch search query cap |

### Persistence and durability

| Variable | Default | Purpose |
|---|---|---|
| `AIONBD_PERSISTENCE_ENABLED` | `true` | Enable WAL + snapshot persistence |
| `AIONBD_WAL_SYNC_ON_WRITE` | `true` | Strongest ACK durability |
| `AIONBD_WAL_SYNC_EVERY_N_WRITES` | `0` | Optional fsync cadence by write count |
| `AIONBD_WAL_SYNC_INTERVAL_SECONDS` | `0` | Optional fsync cadence by time |
| `AIONBD_WAL_GROUP_COMMIT_MAX_BATCH` | `16` | Max grouped WAL batch size |
| `AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS` | `0` | WAL coalescing window |
| `AIONBD_CHECKPOINT_INTERVAL` | `32` | Checkpoint cadence |
| `AIONBD_ASYNC_CHECKPOINTS` | `false` | Offload checkpointing from write request path |
| `AIONBD_CHECKPOINT_COMPACT_AFTER` | `64` | Compact incrementals threshold |
| `AIONBD_SNAPSHOT_PATH` | `data/aionbd_snapshot.json` | Snapshot file path |
| `AIONBD_WAL_PATH` | `data/aionbd_wal.jsonl` | WAL file path |

Durability warning:
- Keep defaults for maximum safety.
- If `AIONBD_WAL_SYNC_ON_WRITE=false`, acknowledged writes can be lost on crash/power loss.
- With `AIONBD_WAL_SYNC_INTERVAL_SECONDS=10`, loss window can be up to around 10 seconds.

### Search/index tuning

| Variable | Default | Purpose |
|---|---|---|
| `AIONBD_IVF_NPROBE_DEFAULT` | `8` | IVF probe width (speed/recall tradeoff) |
| `AIONBD_L2_INDEX_BUILD_MAX_IN_FLIGHT` | `2` | Concurrent IVF build jobs |
| `AIONBD_L2_INDEX_BUILD_COOLDOWN_MS` | `1000` | Rebuild cooldown |
| `AIONBD_L2_INDEX_WARMUP_ON_BOOT` | `true` | Warmup IVF cache at startup |
| `AIONBD_PARALLEL_SCORE_MIN_POINTS` | `256` | Exact-scan parallel threshold (points) |
| `AIONBD_PARALLEL_SCORE_MIN_WORK` | `200000` | Exact-scan parallel threshold (work) |
| `AIONBD_PARALLEL_TOP1_MIN_POINTS` | `8192` | Top1-specific parallel threshold (points) |
| `AIONBD_PARALLEL_TOP1_MIN_WORK` | `4000000` | Top1-specific parallel threshold (work) |
| `AIONBD_SEARCH_INLINE_MAX_POINTS` | `8192` | Inline search threshold (points) |
| `AIONBD_SEARCH_INLINE_MAX_WORK` | `1000000` | Inline search threshold (work) |
| `AIONBD_SEARCH_INLINE_LIGHT_LOAD_MAX_WORK` | `4000000` | Opportunistic inline threshold under low load |
| `AIONBD_SEARCH_INLINE_LIGHT_LOAD_MAX_IN_FLIGHT` | `2` | Max in-flight requests for opportunistic inline |

### Security and tenancy

| Variable | Default | Purpose |
|---|---|---|
| `AIONBD_TLS_ENABLED` | `false` | Enable HTTPS (rustls) |
| `AIONBD_TLS_CERT_PATH` | none | TLS cert path (required when TLS enabled) |
| `AIONBD_TLS_KEY_PATH` | none | TLS key path (required when TLS enabled) |
| `AIONBD_AUTH_MODE` | `disabled` | Auth mode (`disabled`, `api_key`, `bearer_token`, `jwt`, mixed modes) |
| `AIONBD_AUTH_API_KEYS` | empty | API key credentials (`tenant:key`) |
| `AIONBD_AUTH_API_KEY_SCOPES` | empty | API key scopes (`key:read|write|admin`), defaults to `admin` when omitted; enforced only with build feature `exp_auth_api_key_scopes` |
| `AIONBD_AUTH_BEARER_TOKENS` | empty | Bearer token credentials (`tenant:token`) |
| `AIONBD_AUTH_JWT_HS256_SECRET` | none | JWT secret (required in JWT mode) |
| `AIONBD_AUTH_RATE_LIMIT_PER_MINUTE` | `0` | Tenant rate limiting (`0` disables) |
| `AIONBD_AUTH_TENANT_MAX_COLLECTIONS` | `0` | Tenant collections quota (`0` disables) |
| `AIONBD_AUTH_TENANT_MAX_POINTS` | `0` | Tenant points quota (`0` disables) |

## Recommended Profiles

### Safety-first (default durability)

```bash
export AIONBD_PERSISTENCE_ENABLED=true
export AIONBD_WAL_SYNC_ON_WRITE=true
export AIONBD_ASYNC_CHECKPOINTS=true
export AIONBD_CHECKPOINT_INTERVAL=32
export AIONBD_CHECKPOINT_COMPACT_AFTER=64
```

### Throughput-first (explicitly risky)

```bash
export AIONBD_PERSISTENCE_ENABLED=true
export AIONBD_WAL_SYNC_ON_WRITE=false
export AIONBD_WAL_SYNC_INTERVAL_SECONDS=10
export AIONBD_WAL_GROUP_COMMIT_MAX_BATCH=32
export AIONBD_WAL_GROUP_COMMIT_FLUSH_DELAY_MS=1
```

Use throughput-first only if potential acknowledged-write loss on crash is acceptable for your workload.

## Observability and Operations

- JSON metrics: `GET /metrics`
- Prometheus metrics: `GET /metrics/prometheus`
- Grafana dashboard: `ops/grafana/aionbd-overview.json`
- Prometheus alerts: `ops/prometheus/aionbd-alerts.yml`
- Performance tuning guide: `docs/performance_tuning.md`
- Optimization and benchmark details: `docs/optimizations_and_benchmarks.md`
- Production sizing guide: `docs/production_sizing.md`
- Security baseline: `docs/security_notes.md`
- Platform guide: `docs/platform_guide.md`
- Cloud operations guide: `docs/cloud_operations_guide.md`
- Packaging and distribution: `docs/packaging_and_distribution.md`
- Whitepaper: `docs/whitepaper.md`

## Release Packaging

- Build release bundle (binary + deploy assets + checksums):

```bash
./scripts/package_release.sh
```

- Production deployment assets:
  - Docker Compose: `ops/deploy/docker-compose.prod.yml`
  - systemd unit: `ops/deploy/systemd/aionbd.service`
  - Kubernetes manifest: `ops/deploy/kubernetes/aionbd.yaml`
  - Helm chart: `ops/deploy/helm/aionbd`
  - CI release pipeline: `.github/workflows/release.yml`
  - Test release flow: run `Release` workflow manually with version like `0.1.0-rc.1` (creates `v0.1.0-rc.1` tag + prerelease assets)

## Backup, Restore, Export, Import

Backup persistence state:

```bash
python3 scripts/state_backup_restore.py backup \
  --snapshot-path data/aionbd_snapshot.json \
  --wal-path data/aionbd_wal.jsonl \
  --output backups/aionbd-backup.tar.gz
```

Restore:

```bash
python3 scripts/state_backup_restore.py restore \
  --input backups/aionbd-backup.tar.gz \
  --snapshot-path data/aionbd_snapshot.json \
  --wal-path data/aionbd_wal.jsonl \
  --force
```

Export one collection:

```bash
python3 scripts/collection_export_import.py export \
  --base-url http://127.0.0.1:8080 \
  --collection demo \
  --output exports/demo.ndjson
```

Import:

```bash
python3 scripts/collection_export_import.py import \
  --base-url http://127.0.0.1:8080 \
  --input exports/demo.ndjson \
  --collection demo_copy \
  --if-exists fail
```

## Production Go-Live Checklist

1. Enable TLS (`AIONBD_TLS_ENABLED=true`) and set cert/key paths.
2. Use non-disabled auth mode (`api_key`, `bearer_token`, or `jwt`).
3. Set explicit tenant quotas and rate limits.
4. Choose durability profile and document it (`WAL_SYNC_ON_WRITE` on/off policy).
5. Set memory budget and request limits (`MAX_BODY_BYTES`, `MAX_CONCURRENCY`, `MAX_TOPK_LIMIT`, `MAX_PAGE_LIMIT`).
6. Validate with `./scripts/verify_local.sh` and `./scripts/verify_bench.sh`.
7. Run soak/chaos pipelines before release (`./scripts/verify_soak.sh`, `./scripts/verify_chaos.sh`).

## Project Layout

- `core/` Rust vector math and collection primitives
- `server/` Axum HTTP server and runtime
- `bench/` reproducible bench scenarios and reports
- `sdk/js/` JavaScript client SDK
- `sdk/python/` Python client SDK
- `docs/` operations, sizing, performance, security
- `ops/` deployment files, dashboards, alert rules, baselines
- `scripts/` verification, benchmark pipelines, tooling

## Development and Contribution

```bash
cargo test --workspace
./scripts/verify_local.sh
```

Additional validation pipelines:
- `./scripts/verify_bench.sh`
- `./scripts/verify_soak.sh`
- `./scripts/verify_chaos.sh`
- `./scripts/verify_mvp_release.sh`

Contribution workflow: see `CONTRIBUTING.md`.
