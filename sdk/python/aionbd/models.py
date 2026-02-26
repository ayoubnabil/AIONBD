"""SDK data models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DistanceResult:
    """Represents a distance operation response."""

    metric: str
    value: float


@dataclass(frozen=True)
class SearchResult:
    """Represents a top-1 collection search response."""

    id: int
    metric: str
    value: float
    mode: str = "exact"
    recall_at_k: float | None = None
    payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class CollectionInfo:
    """Represents collection metadata returned by the server."""

    name: str
    dimension: int
    strict_finite: bool
    point_count: int


@dataclass(frozen=True)
class UpsertPointResult:
    """Represents point upsert result."""

    id: int
    created: bool


@dataclass(frozen=True)
class PointResult:
    """Represents a stored point payload."""

    id: int
    values: list[float]
    payload: dict[str, Any]


@dataclass(frozen=True)
class MetricsResult:
    """Represents runtime server metrics payload."""

    uptime_ms: int
    ready: bool
    engine_loaded: bool
    storage_available: bool
    http_requests_total: int
    http_requests_in_flight: int
    http_responses_2xx_total: int
    http_responses_4xx_total: int
    http_requests_5xx_total: int
    http_request_duration_us_total: int
    http_request_duration_us_max: int
    http_request_duration_us_avg: float
    collections: int
    points: int
    l2_indexes: int
    persistence_enabled: bool
    persistence_writes: int


@dataclass(frozen=True)
class DeletePointResult:
    """Represents point delete result."""

    id: int
    deleted: bool


@dataclass(frozen=True)
class DeleteCollectionResult:
    """Represents collection delete result."""

    name: str
    deleted: bool
