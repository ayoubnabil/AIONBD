# AIONBD Whitepaper

## Abstract

AIONBD is an edge-first vector database for AI retrieval systems that require deterministic resource behavior, explicit durability controls, and low operational overhead. The system is optimized for constrained environments where memory, I/O, and failure handling must be predictable. This document describes the architecture, storage model, query execution paths, security controls, and deployment model used to operate AIONBD in production.

## 1. Problem Statement

Modern AI retrieval pipelines are increasingly deployed outside centralized clusters, including branch sites, embedded servers, and private edge zones. In these environments, teams need:

- strict runtime limits to avoid resource collapse
- low-latency vector search with predictable behavior
- operational safety when power, networking, or storage can degrade
- simple packaging and upgrade paths without heavy platform dependencies

Traditional cloud-centric vector stacks often assume abundant resources and central orchestration. AIONBD focuses on deterministic operation under tighter constraints.

## 2. Design Goals

- Deterministic operations: explicit hard limits for memory, request size, and query fanout.
- Safe defaults: durability and guardrails enabled out of the box.
- Tunable tradeoffs: performance-oriented modes must be explicit and auditable.
- Operational visibility: first-class metrics and runbook-driven operations.
- Minimal runtime footprint: single server process and direct API surface.

## 3. System Overview

AIONBD is implemented as a Rust HTTP server backed by an in-memory collection engine with persistence and indexing extensions.

Core modules:

- `core/`: vector math, collection structures, persistence primitives.
- `server/`: API handlers, policy enforcement, auth, quotas, runtime control.
- `bench/`: reproducible benchmark harness and regression workflows.
- `sdk/`: language clients for integration workflows.

## 4. Data Model and API

A collection is defined by:

- name
- fixed vector dimension
- strictness policy for numeric validation

A point consists of:

- integer id
- dense vector (`Vec<f32>`)
- optional JSON payload

API supports:

- collection create/list/read/delete
- point upsert/get/delete
- batch upsert
- top-1, top-k, and batch top-k search
- health and metrics endpoints

## 5. Query Execution Model

### 5.1 Search Modes

- `exact`: full candidate scoring.
- `ivf`: centroid-based candidate pruning.
- `auto`: policy-based strategy selection.

### 5.2 Exact Path

The exact path includes optimized scoring kernels and guarded parallelization thresholds. For batch workloads, dedicated fast paths are used when shape and constraints are compatible.

### 5.3 IVF Path

IVF improves throughput for large candidate sets with recall-speed tradeoffs controlled by probe width and training sample limits.

## 6. Persistence and Durability

AIONBD persistence uses:

- WAL append for mutation intent
- in-memory apply for query-serving state
- snapshots and incremental compaction for bounded recovery cost

Durability profile is explicit:

- Safe default: `AIONBD_WAL_SYNC_ON_WRITE=true`
- Throughput profile: asynchronous sync with acknowledged-loss risk on abrupt failure

This policy makes risk acceptance intentional rather than implicit.

## 7. Resource Management

AIONBD enforces runtime capacity using:

- memory budget guard (`AIONBD_MEMORY_BUDGET_MB` / bytes)
- per-collection point caps
- request-body limits
- concurrency limits
- top-k and pagination bounds

These controls prevent unbounded growth and improve multi-tenant safety under hostile or accidental load.

## 8. Security and Multi-Tenancy

Security features include:

- TLS support (`rustls`)
- authentication modes: API key, bearer token, JWT
- tenant-scoped quotas for collections and points
- tenant rate limiting
- operational metrics for auth and quota rejection visibility

For regulated deployments, AIONBD is intended to run behind standard perimeter controls (private networking, ingress policy, secret rotation, and centralized logging).

## 9. Observability and SRE Workflow

The platform exposes:

- `GET /metrics` (JSON)
- `GET /metrics/prometheus`

Operational assets include:

- Prometheus alert rules
- Grafana dashboard templates
- soak and chaos scripts
- baseline and regression comparison workflows

This enables a standard SRE loop: define SLOs, observe error budget drift, validate changes in staging, and gate production rollouts with reproducible tests.

## 10. Performance Positioning

Local reproducible benchmarking (ANN dataset format, exact mode) shows AIONBD outperforming Qdrant in the tested edge profile, with materially higher QPS and lower p95 latency at equal recall. Results remain workload- and hardware-dependent, so benchmark commands are published to support independent verification.

## 11. Packaging and Deployment

AIONBD ships with deployment assets for:

- Docker Compose production profile
- systemd service installation
- Kubernetes starter manifest
- Helm chart packaging for Kubernetes-native rollout management
- release tarball packaging with checksums
- automated CI release workflow for image + artifact publication

This supports both cloud-native and bare-metal operational models.

## 12. Known Limits and Future Work

Current focus areas:

- larger-scale memory layout optimizations
- broader ANN strategy coverage beyond current IVF behavior
- deeper benchmark coverage across diverse datasets and load patterns
- stronger automation for release qualification pipelines

## Conclusion

AIONBD targets teams that need strong operational control and high-performance vector retrieval in constrained, distributed environments. Its design prioritizes predictable behavior, explicit safety boundaries, and practical deployability over hidden automation. This creates a platform suitable for production edge AI systems where reliability and control are as important as raw throughput.
