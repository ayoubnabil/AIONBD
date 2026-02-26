# Changelog

## Unreleased

- No unreleased changes.

## 0.2.0 - 2026-02-26

- Added a full JavaScript SDK client for AIONBD HTTP API.
- Added endpoint coverage for health, metrics, distance, collections, points, and search APIs.
- Added auth header support (`x-api-key`, `Authorization: Bearer ...`).
- Added typed error surface via `AionBDError` (status/method/path/body).
- Added TypeScript declarations (`src/index.d.ts`) bundled in package exports.
- Added unit and real integration tests (integration boots a local `aionbd-server`).
- Added npm release scripts:
  - `npm run release:check`
  - `npm run publish:dry-run`
