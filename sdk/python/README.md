# AIONBD Python SDK

This package provides a minimal client for the AIONBD HTTP server.

## Install (editable)

```bash
python -m pip install -e .
```

## Example

```python
from aionbd import AionBDClient

client = AionBDClient("http://127.0.0.1:8080")
print(client.live())
print(client.ready())
print(client.distance([1.0, 2.0], [2.0, 3.0], metric="dot"))

collection = client.create_collection("demo", dimension=3, strict_finite=True)
print(collection)
print(client.list_collections())
print(client.upsert_point("demo", 1, [1.0, 2.0, 3.0]))
print(client.search_collection("demo", [1.0, 2.0, 3.0], metric="dot"))
print(client.search_collection_top_k("demo", [1.0, 2.0, 3.0], limit=3, metric="dot"))
print(client.list_points("demo", offset=0, limit=50))
print(client.get_point("demo", 1))
print(client.delete_point("demo", 1))
print(client.delete_collection("demo"))
```

## Compatibility note

- `aionbd-sdk` `0.2.0` contains a breaking change:
- `list_points(...)` now returns `{"points": [...], "total": int, "next_offset": int|None}` instead of only a list of IDs.
