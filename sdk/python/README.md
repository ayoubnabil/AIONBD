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
print(client.health())
print(client.distance([1.0, 2.0], [2.0, 3.0], metric="dot"))
```

