# AIONBD Observability And SLO Guide

## Scope

This document defines a practical baseline for production monitoring:
- what to graph
- what to alert on
- what to do first when an alert fires

Metrics come from:
- `GET /metrics` (JSON)
- `GET /metrics/prometheus` (Prometheus exposition format)

## Packaged Artifacts

Versioned ops files are provided in this repository:
- Prometheus alert rules: `ops/prometheus/aionbd-alerts.yml`
- Grafana dashboard: `ops/grafana/aionbd-overview.json`

## Recommended SLOs

These are starter values. Tune them after collecting real traffic data.

1. Availability:
   `aionbd_ready == 1` at least `99.9%` over 30 days.
2. Error budget:
   `5xx ratio < 1%` over 5 minutes.
3. Saturation:
   in-flight requests stays below 80% of configured concurrency most of the time.
4. Persistence health:
   no sustained checkpoint degradation (`wal-only`) for more than 15 minutes.
5. Persistence backlog:
   WAL/incremental backlog remains bounded (no sustained growth above operational thresholds).
6. IVF quality guard:
   explicit `ivf` fallback ratio stays below `25%` over 10 minutes.

## Dashboard Panels

Create one dashboard with these panels:

1. Request volume:
   `rate(aionbd_http_requests_total[5m])`
2. 5xx ratio:
   `rate(aionbd_http_requests_5xx_total[5m]) / clamp_min(rate(aionbd_http_requests_total[5m]), 1)`
3. In-flight pressure:
   `aionbd_http_requests_in_flight`
4. Request duration max and avg:
   `aionbd_http_request_duration_us_max`, `aionbd_http_request_duration_us_avg`
5. Readiness:
   `aionbd_ready`, `aionbd_engine_loaded`, `aionbd_storage_available`
6. Persistence behavior:
   `rate(aionbd_persistence_writes[5m])`, `rate(aionbd_persistence_checkpoint_degraded_total[5m])`
   `rate(aionbd_persistence_checkpoint_success_total[5m])`
   `rate(aionbd_persistence_checkpoint_error_total[5m])`
   `aionbd_persistence_wal_size_bytes`
   `aionbd_persistence_wal_tail_open`
   `aionbd_persistence_incremental_segments`
   `aionbd_persistence_incremental_size_bytes`
7. Index cache quality:
   `aionbd_l2_index_cache_hit_ratio`, `aionbd_l2_index_build_in_flight`, `rate(aionbd_l2_index_build_failures[5m])`
8. Quota and abuse signals:
   `rate(aionbd_tenant_quota_collection_rejections_total[5m])`
   `rate(aionbd_tenant_quota_point_rejections_total[5m])`
   `rate(aionbd_rate_limit_rejections_total[5m])`
9. Cardinality/load indicators:
   `aionbd_collections`, `aionbd_points`, `aionbd_l2_indexes`
   `aionbd_collection_write_lock_entries`
   `aionbd_tenant_rate_window_entries`
   `aionbd_tenant_quota_lock_entries`
10. Search execution quality:
   `rate(aionbd_search_queries_total[5m])`
   `rate(aionbd_search_ivf_queries_total[5m])`
   `rate(aionbd_search_ivf_fallback_exact_total[5m])`
11. IVF fallback ratio:
   `rate(aionbd_search_ivf_fallback_exact_total[5m]) / clamp_min(rate(aionbd_search_ivf_queries_total[5m]), 1)`

## Alert Rules (Prometheus Examples)

```yaml
groups:
  - name: aionbd-alerts
    rules:
      - alert: AionbdNotReady
        expr: aionbd_ready == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "AIONBD is not ready"
          description: "Readiness stayed down for more than 2 minutes."

      - alert: AionbdHigh5xxRatio
        expr: |
          (
            rate(aionbd_http_requests_5xx_total[5m])
            /
            clamp_min(rate(aionbd_http_requests_total[5m]), 1)
          ) > 0.01
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "AIONBD 5xx ratio above 1%"
          description: "Server error ratio is above target over 10 minutes."

      - alert: AionbdCheckpointDegraded
        expr: rate(aionbd_persistence_checkpoint_degraded_total[5m]) > 0
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD persistence running in WAL-only degraded mode"
          description: "Checkpointing keeps degrading; investigate snapshot and disk health."

      - alert: AionbdCheckpointError
        expr: rate(aionbd_persistence_checkpoint_error_total[5m]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD checkpoint attempts are failing"
          description: "Checkpoint worker reported internal errors; inspect persistence and runtime health."

      - alert: AionbdWalTailOpen
        expr: aionbd_persistence_enabled == 1 and aionbd_persistence_wal_tail_open == 1
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD WAL tail looks truncated"
          description: "WAL file does not end with a newline for a sustained period; inspect storage health and restart behavior."

      - alert: AionbdWalBacklogGrowing
        expr: |
          aionbd_persistence_enabled == 1
          and
          aionbd_persistence_wal_size_bytes > 268435456
        for: 20m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD WAL backlog is growing"
          description: "WAL size stayed above 256 MiB; checkpoint throughput may be insufficient."

      - alert: AionbdIncrementalBacklogGrowing
        expr: |
          aionbd_persistence_enabled == 1
          and
          (
            aionbd_persistence_incremental_segments > 64
            or
            aionbd_persistence_incremental_size_bytes > 1073741824
          )
        for: 20m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD incremental snapshot backlog is growing"
          description: "Incremental backlog stayed high; compaction may be lagging."

      - alert: AionbdIndexBuildFailing
        expr: rate(aionbd_l2_index_build_failures[5m]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD IVF index builds are failing"
          description: "Asynchronous L2 index build failures detected."

      - alert: AionbdQuotaPressure
        expr: |
          rate(aionbd_tenant_quota_collection_rejections_total[10m]) > 0
          or
          rate(aionbd_tenant_quota_point_rejections_total[10m]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD tenant quota pressure"
          description: "Writes are being rejected by quota enforcement."

      - alert: AionbdTenantTrackingCardinalityHigh
        expr: |
          aionbd_tenant_rate_window_entries > 10000
          or
          aionbd_tenant_quota_lock_entries > 10000
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD tenant tracking cardinality is high"
          description: "Tenant tracking maps grew beyond expected bounds; inspect abusive traffic and retention behavior."

      - alert: AionbdRateLimitPressure
        expr: rate(aionbd_rate_limit_rejections_total[10m]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD request rate limiting active"
          description: "Tenant request traffic exceeds configured rate limits."

      - alert: AionbdIvfFallbackRatioHigh
        expr: |
          (
            rate(aionbd_search_ivf_fallback_exact_total[10m])
            /
            clamp_min(rate(aionbd_search_ivf_queries_total[10m]), 1)
          ) > 0.25
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "AIONBD explicit IVF requests are frequently falling back to exact"
          description: "Fallback ratio stayed above 25%; investigate index cache and build throughput."
```

## First Response Runbook

When `AionbdNotReady` fires:
1. Check `aionbd_engine_loaded` and `aionbd_storage_available`.
2. If storage is down, inspect disk free space, filesystem errors, and persistence logs.
3. Verify recent `aionbd_persistence_checkpoint_degraded_total` trend.

When `AionbdCheckpointDegraded` fires:
1. Confirm WAL write rate (`aionbd_persistence_writes`) and checkpoint degradation rate.
2. Inspect snapshot path permissions and available disk.
3. Trigger controlled restart only after confirming WAL/snapshot files are healthy.

When `AionbdCheckpointError` fires:
1. Inspect server logs around checkpoint worker failures.
2. Correlate with runtime saturation and `aionbd_storage_available`.
3. Treat sustained errors as a persistence incident and reduce write pressure.

When `AionbdWalTailOpen` fires:
1. Check `aionbd_persistence_wal_tail_open` together with `aionbd_persistence_wal_size_bytes`.
2. Correlate with recent crashes/restarts and disk errors.
3. If signal persists, plan controlled restart and inspect WAL replay behavior.

When `AionbdWalBacklogGrowing` or `AionbdIncrementalBacklogGrowing` fires:
1. Check `aionbd_persistence_wal_size_bytes`, `aionbd_persistence_incremental_segments`, and `aionbd_persistence_incremental_size_bytes`.
2. Correlate with `rate(aionbd_persistence_writes[5m])` and checkpoint degradation events.
3. If growth is sustained, reduce ingest pressure and investigate checkpoint/compaction throughput.

When `AionbdTenantTrackingCardinalityHigh` fires:
1. Inspect `aionbd_tenant_rate_window_entries` and `aionbd_tenant_quota_lock_entries`.
2. Correlate with rate-limit pressure and tenant churn.
3. Investigate abusive credential rotation and tighten ingress controls.

When `AionbdHigh5xxRatio` fires:
1. Split by endpoint in request logs.
2. Check whether failures correlate with persistence or index build failures.
3. If persistence is implicated, lower ingest pressure and investigate disk immediately.

When `AionbdIvfFallbackRatioHigh` fires:
1. Check `aionbd_l2_index_cache_hit_ratio` and `aionbd_l2_index_build_in_flight`.
2. Check `rate(aionbd_l2_index_build_failures[5m])` and recent mutation rate.
3. If fallback persists, increase indexing capacity and investigate cache invalidation churn.

## Notes

- `AIONBD_AUTH_TENANT_MAX_COLLECTIONS` and `AIONBD_AUTH_TENANT_MAX_POINTS` use `0` as disabled.
- Quota rejections are expected if limits are intentionally strict; alerting should be warning-level first.
- Keep alert routing simple at the start: pager only for `critical`, ticket/chat for `warning`.
