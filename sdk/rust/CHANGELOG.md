# Changelog

## 0.1.0

- Initial Rust SDK release.
- Added blocking HTTP client for AIONBD endpoints (health, metrics, distance, collections, points, search).
- Added auth support (`x-api-key`, `Authorization: Bearer ...`) and configurable timeout/default headers.
- Added typed request/response models for core API workflows.
- Added unit tests covering request shape, pagination semantics, auth headers, and error behavior.
- Current transport scope is `http://` base URLs.
