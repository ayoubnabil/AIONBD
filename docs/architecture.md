# AIONBD Architecture (Initial Skeleton)

## Objective

Provide a clean base that can grow into an edge-first vector database without
mixing concerns too early.

## Module boundaries

- `core/`:
  - deterministic vector math
  - reusable primitives for ANN, scoring, and validation
- `server/`:
  - HTTP API surface
  - request validation and response shaping
  - integration point for storage/index engine later
- `bench/`:
  - reproducible benchmark scenarios
  - regression visibility for performance changes
- `sdk/python/`:
  - lightweight client for POC and integration tests

## Design principles

1. Keep core deterministic and testable.
2. Keep API contracts explicit and typed.
3. Keep benchmarking reproducible from day one.
4. Keep docs close to code to reduce drift.

## Next architecture steps

1. Add collection abstraction in `core/`.
2. Add persistence layer (WAL + snapshot) design in `core/`.
3. Expand `server/` routes to CRUD collection/point operations.
4. Add benchmark harness scenarios mapped to roadmap KPI.

