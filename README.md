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
python -c "from aionbd import AionBDClient; print(AionBDClient().live())"
```

## Notes

- `ROADMAP_EDGEVECTOR.md` is ignored intentionally in git.
- This step focuses on project structure and baseline quality.
- Contribution flow is branch-first local with mandatory expert review before merge (`CONTRIBUTING.md`).
- Persistence uses WAL per write and periodic snapshot checkpoints (`AIONBD_CHECKPOINT_INTERVAL`, default `32`).
- Search uses an IVF candidate index for large L2 collections; dot/cosine currently remain exact linear scan.
- Server endpoints:
  - `GET /live`
  - `GET /ready`
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
