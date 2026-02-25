# AIONBD Observability And SLO Guide

## Scope

This document defines a practical production baseline:
- what to monitor
- what to alert on
- what to do first when an alert fires

Metrics are exposed by:
- `GET /metrics` (JSON)
- `GET /metrics/prometheus` (Prometheus text format)

Ops artifacts versioned in this repository:
- alert rules: `ops/prometheus/aionbd-alerts.yml`
- dashboard: `ops/grafana/aionbd-overview.json`

## Baseline SLOs

Starter values to tune with real traffic:

1. Availability: `aionbd_ready == 1` at least `99.9%` over 30 days.
2. API error budget: 5xx ratio `< 1%` over 5 minutes.
3. Runtime saturation: in-flight requests usually `< 80%` of configured concurrency.
4. Checkpoint health: no sustained WAL-only degradation (`> 15m`).
5. Backlog bounds: WAL and incremental segments must stay operationally bounded.
6. Search quality guard: IVF fallback ratio `< 25%` over 10 minutes.
7. Security pressure: sustained auth/rate-limit/quota rejections are investigated.

## Dashboard Baseline

Recommended panels:

1. Request volume: `rate(aionbd_http_requests_total[5m])`
2. 5xx ratio: `rate(aionbd_http_requests_5xx_total[5m]) / clamp_min(rate(aionbd_http_requests_total[5m]), 1)`
3. In-flight pressure: `aionbd_http_requests_in_flight`
4. Latency: `aionbd_http_request_duration_us_avg`, `aionbd_http_request_duration_us_max`
5. Readiness: `aionbd_ready`, `aionbd_engine_loaded`, `aionbd_storage_available`
6. Persistence pressure:
   - `rate(aionbd_persistence_writes[5m])`
   - `rate(aionbd_persistence_checkpoint_degraded_total[5m])`
   - `rate(aionbd_persistence_checkpoint_error_total[5m])`
   - `aionbd_persistence_wal_size_bytes`
   - `aionbd_persistence_incremental_segments`
7. Index/cache health:
   - `aionbd_l2_index_cache_hit_ratio`
   - `aionbd_l2_index_build_in_flight`
   - `rate(aionbd_l2_index_build_failures[5m])`
8. Security and tenancy pressure:
   - `rate(aionbd_auth_failures_total[5m])`
   - `rate(aionbd_rate_limit_rejections_total[5m])`
   - `rate(aionbd_tenant_quota_collection_rejections_total[5m])`
   - `rate(aionbd_tenant_quota_point_rejections_total[5m])`
9. Search quality:
   - `rate(aionbd_search_ivf_queries_total[5m])`
   - `rate(aionbd_search_ivf_fallback_exact_total[5m])`
   - `rate(aionbd_search_ivf_fallback_exact_total[5m]) / clamp_min(rate(aionbd_search_ivf_queries_total[5m]), 1)`
10. Scheduler + group-commit pressure:
   - `rate(aionbd_persistence_checkpoint_schedule_skips_total[5m])`
   - `aionbd_persistence_checkpoint_in_flight`
   - `aionbd_persistence_wal_group_queue_depth`
   - `rate(aionbd_persistence_wal_grouped_records_total[5m]) / clamp_min(rate(aionbd_persistence_wal_group_commits_total[5m]), 1)`

## Alert Catalog

Canonical definitions live in `ops/prometheus/aionbd-alerts.yml`.

| Alert | Severity | Trigger intent |
|---|---|---|
| `AionbdNotReady` | critical | readiness down |
| `AionbdHigh5xxRatio` | critical | 5xx ratio above budget |
| `AionbdStorageUnavailable` | critical | storage layer unavailable |
| `AionbdWalSyncDisabled` | warning | durability mode relaxed |
| `AionbdCheckpointDegraded` | warning | checkpoints falling back to WAL-only |
| `AionbdCheckpointError` | warning | checkpoint execution errors |
| `AionbdWalTailOpen` | warning | WAL appears truncated/open |
| `AionbdWalBacklogGrowing` | warning | WAL backlog too large |
| `AionbdIncrementalBacklogGrowing` | warning | incremental backlog too large |
| `AionbdIndexBuildFailing` | warning | IVF builds failing |
| `AionbdIndexBuildCooldownSkipsHigh` | warning | rebuild demand exceeds cooldown policy |
| `AionbdAuthFailures` | warning | authentication failures sustained |
| `AionbdRateLimitPressure` | warning | sustained rate-limit rejections |
| `AionbdTenantQuotaPressure` | warning | sustained tenant quota rejections |
| `AionbdTenantTrackingCardinalityHigh` | warning | tenant tracking maps too large |
| `AionbdCollectionWriteLocksCardinalityHigh` | warning | collection lock map too large |
| `AionbdIvfFallbackRatioHigh` | warning | explicit IVF often falls back to exact |
| `AionbdWalGroupQueueDepthHigh` | warning | WAL queue depth remains high |
| `AionbdCheckpointSchedulerSaturated` | warning | due checkpoints skipped while busy |
| `AionbdCheckpointInFlightStuck` | warning | checkpoint remains in-flight too long |

## First Response Runbook

### Availability incidents

For `AionbdNotReady`, `AionbdStorageUnavailable`, or `AionbdHigh5xxRatio`:
1. Confirm `aionbd_engine_loaded` and `aionbd_storage_available`.
2. Correlate spikes in 5xx with persistence and checkpoint metrics.
3. Check disk free space, filesystem errors, and recent restart/crash events.
4. If persistence is implicated, reduce ingest pressure before restart actions.

### Persistence and durability incidents

For `AionbdWalSyncDisabled`:
1. Verify this mode is intentionally configured.
2. If not intentional, restore `AIONBD_WAL_SYNC_ON_WRITE=true`.
3. Log the durability tradeoff decision in incident/change notes.

For `AionbdCheckpointDegraded`, `AionbdCheckpointError`, `AionbdWalTailOpen`:
1. Inspect checkpoint and WAL errors in server logs.
2. Validate snapshot/WAL paths and filesystem permissions.
3. Check `aionbd_persistence_wal_tail_open` persistence across restarts.
4. Treat sustained errors as storage incidents.

For `AionbdWalBacklogGrowing`, `AionbdIncrementalBacklogGrowing`:
1. Track WAL/incremental growth versus write rate.
2. Correlate with checkpoint success/error/degraded rates.
3. Throttle ingest temporarily if backlog remains unbounded.

For `AionbdCheckpointSchedulerSaturated`, `AionbdCheckpointInFlightStuck`, `AionbdWalGroupQueueDepthHigh`:
1. Check `aionbd_persistence_checkpoint_in_flight` and skip rate.
2. Check `aionbd_persistence_wal_group_queue_depth` and average group size.
3. Investigate sustained disk latency, checkpoint throughput, and write bursts.

### Index and search quality incidents

For `AionbdIndexBuildFailing`, `AionbdIndexBuildCooldownSkipsHigh`, `AionbdIvfFallbackRatioHigh`:
1. Check `aionbd_l2_index_cache_hit_ratio` and build failure/cooldown skip rates.
2. Correlate with mutation intensity and cache invalidation churn.
3. Increase index build capacity or tune mutation/index policy if needed.

### Security and tenant pressure incidents

For `AionbdAuthFailures`, `AionbdRateLimitPressure`, `AionbdTenantQuotaPressure`:
1. Identify offending tenants/credentials from audit logs.
2. Validate auth headers, token/key rotation, and rate policy expectations.
3. Escalate abuse patterns to ingress or identity controls.

For `AionbdTenantTrackingCardinalityHigh`, `AionbdCollectionWriteLocksCardinalityHigh`:
1. Inspect cardinality trends in tracking/lock maps.
2. Correlate with high churn, unknown collection writes, or abusive traffic.
3. Apply traffic controls and review cleanup behavior.

## Escalation Checklist

1. Confirm whether impact is correctness, durability, latency, or availability.
2. Snapshot the key metrics at incident start and after each mitigation step.
3. Prefer reducing write pressure before heavy operational actions.
4. After mitigation, validate recovery via readiness + backlog + error trends.
5. Record root cause, metric signatures, and follow-up hardening tasks.

## Notes

- Quota env vars (`AIONBD_AUTH_TENANT_MAX_COLLECTIONS`, `AIONBD_AUTH_TENANT_MAX_POINTS`) use `0` as disabled.
- `AIONBD_WAL_SYNC_ON_WRITE=false` improves throughput but weakens crash durability guarantees.
- Recommended routing: pager for `critical`, ticket/chat for sustained `warning` alerts.
