# Changelog

## 0.2.0

- Breaking: `AionBDClient.list_points(...)` now returns pagination metadata:
  - `points: list[int]`
  - `total: int`
  - `next_offset: int | None`
- Previous behavior returned only `list[int]`.
