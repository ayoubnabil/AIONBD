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

4. Use the Python SDK:
```bash
cd sdk/python
python -m pip install -e .
python -c "from aionbd import AionBDClient; print(AionBDClient().health())"
```

## Notes

- `ROADMAP_EDGEVECTOR.md` is ignored intentionally in git.
- This step focuses on project structure and baseline quality.
- Server endpoints:
  - `GET /live`
  - `GET /ready`
  - `POST /distance`
