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

## Server runtime configuration

- `AIONBD_BIND` (default: `127.0.0.1:8080`)
- `AIONBD_MAX_DIMENSION` (default: `4096`)
- `AIONBD_STRICT_FINITE` (default: `true`)
- `AIONBD_REQUEST_TIMEOUT_MS` (default: `2000`)
- `AIONBD_MAX_BODY_BYTES` (default: `1048576`)
- `AIONBD_MAX_CONCURRENCY` (default: `256`)

## API endpoints

- `GET /live`: liveness endpoint
- `GET /ready`: readiness endpoint
- `POST /distance`: vector operation endpoint with input validation

## Python SDK commands

From `sdk/python/`:
```bash
python -m pip install -e .
python -c "from aionbd import AionBDClient; print(AionBDClient().health())"
```

## Coding standards

1. No unsafe Rust in this phase.
2. Public interfaces should be documented.
3. New features must include at least baseline tests.
4. Bench-visible changes must document expected impact.
