# AIONBD Python SDK

This package provides a minimal client for the AIONBD HTTP server.

## Install (editable)

```bash
python -m pip install -e .
```

## Run tests

```bash
python -m unittest discover -s tests -v
```

## Example

```python
from aionbd import AionBDClient

client = AionBDClient("http://127.0.0.1:8080")
print(client.live())
print(client.ready())
print(client.metrics())
print(client.metrics_prometheus())
print(client.distance([1.0, 2.0], [2.0, 3.0], metric="dot"))

collection = client.create_collection("demo", dimension=3, strict_finite=True)
print(collection)
print(client.list_collections())
print(client.upsert_point("demo", 1, [1.0, 2.0, 3.0], payload={"tenant": "edge", "score": 0.9}))
print(client.search_collection("demo", [1.0, 2.0, 3.0], metric="dot", mode="exact"))
print(
    client.search_collection_top_k(
        "demo",
        [1.0, 2.0, 3.0],
        limit=3,
        metric="l2",
        mode="auto",
        target_recall=0.95,
        filter={"must": [{"field": "tenant", "value": "edge"}]},
    )
)
print(client.search_collection_top_k("demo", [1.0, 2.0, 3.0], limit=None, metric="dot"))
first_page = client.list_points("demo", limit=50)
print(first_page)
if first_page["next_after_id"] is not None:
    print(client.list_points("demo", limit=50, after_id=first_page["next_after_id"]))
print(client.list_points("demo", limit=None))
print(client.get_point("demo", 1))
print(client.delete_point("demo", 1))
print(client.delete_collection("demo"))
```

## Compatibility note

- `aionbd-sdk` `0.2.0` contains a breaking change:
- `list_points(...)` now returns pagination metadata:
  - `points: list[int]`
  - `total: int`
  - `next_offset: int | None` (offset mode)
  - `next_after_id: int | None` (cursor mode)
- `list_points(..., after_id=<id>)` enables cursor-based pagination.
- `list_points(..., limit=None)` and `search_collection_top_k(..., limit=None)` omit the
  `limit` parameter and let server defaults apply.
- `metrics()` returns typed runtime counters/state as `MetricsResult` including
  HTTP request counters
  (`http_requests_total`, `http_requests_in_flight`, `http_responses_2xx_total`, `http_responses_4xx_total`, `http_requests_5xx_total`).
  and latency aggregates
  (`http_request_duration_us_total`, `http_request_duration_us_max`, `http_request_duration_us_avg`).
  and an aggregate readiness flag (`ready`).
- `metrics_prometheus()` returns raw Prometheus exposition text.
