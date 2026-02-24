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
```

## Server runtime configuration

- `AIONBD_BIND` (default: `127.0.0.1:8080`)
- `AIONBD_MAX_DIMENSION` (default: `4096`)
- `AIONBD_STRICT_FINITE` (default: `true`)
- `AIONBD_REQUEST_TIMEOUT_MS` (default: `2000`)
- `AIONBD_MAX_BODY_BYTES` (default: `1048576`)
- `AIONBD_MAX_CONCURRENCY` (default: `256`)
- `AIONBD_PERSISTENCE_ENABLED` (default: `true`)
- `AIONBD_SNAPSHOT_PATH` (default: `data/aionbd_snapshot.json`)
- `AIONBD_WAL_PATH` (default: `data/aionbd_wal.jsonl`)

## API endpoints

- `GET /live`: liveness endpoint
- `GET /ready`: readiness endpoint
- `POST /distance`: vector operation endpoint with input validation
- `POST /collections`: create collection `{name, dimension, strict_finite}`
- `GET /collections`: list collections
- `GET /collections/:name`: collection metadata
- `DELETE /collections/:name`: delete collection
- `POST /collections/:name/search`: top-1 search `{query, metric}`
- `PUT /collections/:name/points/:id`: upsert point `{values}`
- `GET /collections/:name/points/:id`: read point
- `DELETE /collections/:name/points/:id`: delete point

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
