# AIONBD Platform Guide

## Executive Summary

AIONBD is designed as an edge-first vector data platform for AI retrieval workloads that need:

- strict control of memory and durability behavior
- predictable operations on constrained infrastructure
- low-complexity deployment without a large control plane

## Product Scope

AIONBD provides a single runtime with:

- vector collection lifecycle APIs
- exact and IVF search execution modes
- WAL and snapshot persistence
- auth, quotas, and runtime guardrails
- metrics and operational hooks for production monitoring

## Platform Principles

- Safety by default: durable write acknowledgments and bounded request behavior.
- Deterministic guardrails: explicit limits for points, payload size, concurrency, and memory.
- Controlled performance: tunable search and persistence profiles for workload-specific tradeoffs.
- Operational clarity: production-facing metrics, alerts, runbooks, and verification scripts.

## Reference Topologies

### Single Edge Node

- One AIONBD instance per site/device.
- Local persistent volume for WAL and snapshots.
- Best fit for low-latency local retrieval with intermittent WAN.

### Regional Edge Cluster

- Multiple independent AIONBD instances per region.
- Traffic sharded by tenant/domain in an external gateway layer.
- Best fit for regional isolation and controlled failure domains.

### Hybrid Core + Edge

- AIONBD on edge nodes for low-latency reads.
- Upstream synchronization pipeline managed externally.
- Best fit when some datasets must be centrally managed.

## Reliability Model

- Liveness endpoint: `/live`.
- Readiness endpoint: `/ready`.
- Persistence model: append WAL then apply in-memory mutation.
- Durability profile:
  - default: `AIONBD_WAL_SYNC_ON_WRITE=true`
  - throughput mode: explicit risk acceptance with periodic sync

## Security Model

- TLS termination in-process (`rustls`) or via trusted edge proxy.
- Multi-mode authentication (`api_key`, `bearer_token`, `jwt`).
- Tenant quotas and rate limits for noisy-neighbor protection.
- Hard caps for request size and query limits.

## Capacity and Performance Engineering

- Memory budget guardrail using resource manager (`AIONBD_MEMORY_BUDGET_MB`).
- Search path controls for exact and IVF behavior.
- Batch endpoints for ingestion and query efficiency.
- Benchmark tooling for reproducible local and CI positioning.

## Operational Lifecycle

- Deploy with container, systemd, or Kubernetes packaging.
- Observe via JSON and Prometheus metrics.
- Validate with local checks, soak, chaos, and benchmark pipelines.
- Roll forward by immutable package/image updates and explicit config versioning.

## Documentation Map

- Packaging and distribution: `docs/packaging_and_distribution.md`
- Cloud operations: `docs/cloud_operations_guide.md`
- Whitepaper: `docs/whitepaper.md`
- Performance tuning: `docs/performance_tuning.md`
- Sizing: `docs/production_sizing.md`
- Security: `docs/security_notes.md`
- Operational runbooks: `docs/operations_observability.md`
