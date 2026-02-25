# Changelog

## Unreleased

- `AionBDClient.list_points(...)` now supports cursor pagination with
  `after_id`.
- Added `next_after_id: int | None` in `list_points(...)` responses.
- `AionBDClient.list_points(..., limit=None)` now omits the `limit` query param.
- `AionBDClient.search_collection_top_k(..., limit=None)` now omits the `limit`
  field in request payloads.
- Added `AionBDClient.metrics()` returning typed `MetricsResult` with aggregate readiness
  and HTTP request counters, including `2xx` and `4xx` counters, plus latency
  aggregates (`http_request_duration_us_total`, `http_request_duration_us_max`,
  `http_request_duration_us_avg`).
- Added `AionBDClient.metrics_prometheus()` returning raw text metrics.

## 0.2.0

- Breaking: `AionBDClient.list_points(...)` now returns pagination metadata:
  - `points: list[int]`
  - `total: int`
  - `next_offset: int | None`
- Previous behavior returned only `list[int]`.
