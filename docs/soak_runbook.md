# AIONBD Soak Runbook

This runbook standardizes long-duration soak execution and interpretation.

## 1. Goal

Validate service stability and SLO posture under sustained mixed read/write load.

Primary signals:
- error rate
- throughput drift
- p95/p99 latency drift
- readiness/storage stability during run
- persistence backlog behavior (WAL and incrementals)

## 2. Preconditions

1. Deploy target build and config intended for production.
2. Confirm auth/TLS mode is the intended one for the environment.
3. Confirm alerting is active (`ops/prometheus/aionbd-alerts.yml`).
4. Confirm enough disk and memory headroom for run duration.

## 3. Baseline command

Dry-run validation (no server load):

```bash
AIONBD_SOAK_DRY_RUN=1 ./scripts/verify_soak.sh --profiles-file ops/soak/longrun_profiles.json
```

Actual long-run execution:

```bash
python3 scripts/run_soak_pipeline.py \
  --base-url http://127.0.0.1:8080 \
  --collection-prefix soak_longrun \
  --profiles-file ops/soak/longrun_profiles.json \
  --report-path bench/reports/soak_longrun_report.md \
  --report-json-path bench/reports/soak_longrun_report.json \
  --profile-reports-dir bench/reports/soak_profiles
```

## 4. During-run monitoring

Track these metrics continuously:
- `aionbd_ready`
- `aionbd_storage_available`
- `aionbd_http_requests_5xx_total`
- `aionbd_http_request_duration_us_avg`
- `aionbd_persistence_wal_size_bytes`
- `aionbd_persistence_incremental_segments`
- `aionbd_persistence_checkpoint_error_total`
- `aionbd_persistence_checkpoint_degraded_total`

Operational checks:
1. Watch for sustained readiness drops.
2. Watch for sustained backlog growth without recovery.
3. Watch for persistent checkpoint errors/degraded mode.

## 5. Success criteria

A run is considered passing when all are true:
1. Soak pipeline exits `0` (no threshold failures).
2. No sustained readiness or storage-unavailable incidents.
3. No uncontrolled persistence backlog growth.
4. No unexplained p95/p99 regression versus previous accepted baseline.

## 6. Failure triage

If soak fails due to thresholds:
1. Inspect `bench/reports/soak_pipeline_report.json`.
2. Identify failing profile and metric (`error_rate`, `throughput`, `p95`, `p99`).
3. Correlate timing with server metrics and alerts.
4. Re-run isolated profile with same parameters for reproduction.

If readiness/storage incidents occur:
1. Check checkpoint and WAL logs.
2. Check filesystem pressure/latency.
3. Validate `AIONBD_WAL_SYNC_ON_WRITE` and checkpoint cadence settings.

## 7. Artifacts to archive

Persist these artifacts per run:
- `bench/reports/soak_pipeline_report.md`
- `bench/reports/soak_pipeline_report.json`
- `bench/reports/soak_profiles/*.json`
- metrics snapshots and alert timeline

Recommended naming:
- include date/time, git commit, environment, and profile set.

## 8. Change control

Before changing soak thresholds or profile definitions:
1. Record rationale in PR description.
2. Compare with last accepted baseline run.
3. Update `ops/soak/longrun_profiles.json` when profile contracts change.
4. Ensure dry-run smoke still passes in CI.
5. If dry-run outputs changed intentionally, refresh baselines via `docs/baseline_refresh_runbook.md`.
