# AIONBD Security Notes

This document defines baseline security practices for operating AIONBD.

## 1. Transport security

Use TLS in any non-local environment:
- `AIONBD_TLS_ENABLED=true`
- `AIONBD_TLS_CERT_PATH=/path/to/cert.pem`
- `AIONBD_TLS_KEY_PATH=/path/to/key.pem`

Rotate certificates before expiration and verify startup logs include TLS enabled.

## 2. Authentication modes

`AIONBD_AUTH_MODE` supports:
- `disabled` (development only)
- `api_key`
- `bearer_token`
- `api_key_or_bearer_token`
- `jwt`
- `api_key_or_jwt`

Production recommendation:
- avoid `disabled`
- prefer `jwt` or `api_key_or_jwt`
- rotate secrets/keys on a schedule

## 3. Tenant isolation and quotas

Isolation is enforced by tenant-scoped collection names derived from auth context.

Use quotas to limit abuse blast radius:
- `AIONBD_AUTH_TENANT_MAX_COLLECTIONS`
- `AIONBD_AUTH_TENANT_MAX_POINTS`
- `AIONBD_AUTH_RATE_LIMIT_PER_MINUTE`

Treat `0` as disabled for quota/rate controls and enable intentionally.

## 4. Persistence durability choices

Durability-sensitive deployments should keep:
- `AIONBD_WAL_SYNC_ON_WRITE=true`

If disabled (`false`), writes acknowledged by API may be lost on abrupt crash/power loss.
Use only when the throughput tradeoff is explicitly accepted.

## 5. Request-surface hardening

Recommended defaults:
- strict body size cap via `AIONBD_MAX_BODY_BYTES`
- bounded concurrency via `AIONBD_MAX_CONCURRENCY`
- bounded execution time via `AIONBD_REQUEST_TIMEOUT_MS`
- conservative API limits (`AIONBD_MAX_TOPK_LIMIT`, `AIONBD_MAX_PAGE_LIMIT`)

## 6. Monitoring security signals

Track and alert on:
- `aionbd_auth_failures_total`
- `aionbd_rate_limit_rejections_total`
- `aionbd_tenant_quota_collection_rejections_total`
- `aionbd_tenant_quota_point_rejections_total`

Correlate repeated failures with client identity, source IP, and deployment events.

## 7. Backup and recovery hygiene

Use signed/controlled storage for backup archives.
Operational tools:
- `scripts/state_backup_restore.py`
- `scripts/collection_export_import.py`

Validate restore procedures regularly in non-production environments.

## 8. Production baseline checklist

1. TLS enabled and certificate rotation procedure documented.
2. Non-disabled auth mode enforced.
3. Rate limit and tenant quotas enabled with explicit values.
4. WAL durability mode reviewed and accepted.
5. Alerting routed for auth failures and persistence health.
6. Backup/restore and restart recovery drills executed.
