from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aionbd import AionBDClient, AionBDError, MetricsResult


class RecordingClient(AionBDClient):
    def __init__(self, response: Any) -> None:
        super().__init__("http://unit.test")
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.raw_flags: list[bool] = []
        self._response = response

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        raw: bool = False,
    ) -> Any:
        self.calls.append((method, path, body))
        self.raw_flags.append(raw)
        return self._response


class SearchTopKRequestTests(unittest.TestCase):
    def test_omits_limit_when_none(self) -> None:
        client = RecordingClient({"metric": "dot", "hits": [{"id": 1, "value": 0.5}]})

        hits = client.search_collection_top_k(
            "demo", [1.0, 2.0], limit=None, metric="dot"
        )

        self.assertEqual(len(hits), 1)
        self.assertEqual(len(client.calls), 1)
        method, path, body = client.calls[0]
        self.assertEqual(method, "POST")
        self.assertEqual(path, "/collections/demo/search/topk")
        self.assertEqual(body, {"query": [1.0, 2.0], "metric": "dot", "mode": "auto"})

    def test_includes_limit_when_provided(self) -> None:
        client = RecordingClient({"metric": "dot", "hits": [{"id": 1, "value": 0.5}]})

        client.search_collection_top_k("demo", [1.0, 2.0], limit=3, metric="dot")

        _, _, body = client.calls[0]
        self.assertEqual(
            body, {"query": [1.0, 2.0], "metric": "dot", "mode": "auto", "limit": 3}
        )

    def test_rejects_non_positive_limit(self) -> None:
        client = RecordingClient({"metric": "dot", "hits": []})

        for invalid_limit in [0, -1]:
            with self.subTest(limit=invalid_limit):
                with self.assertRaises(ValueError):
                    client.search_collection_top_k(
                        "demo", [1.0, 2.0], limit=invalid_limit
                    )


class ListPointsRequestTests(unittest.TestCase):
    def test_omits_limit_when_none_and_uses_offset_mode(self) -> None:
        client = RecordingClient(
            {
                "points": [{"id": 1}, {"id": 2}],
                "total": 3,
                "next_offset": 2,
                "next_after_id": 2,
            }
        )

        payload = client.list_points("demo", offset=0, limit=None)

        self.assertEqual(payload["points"], [1, 2])
        self.assertEqual(payload["next_offset"], 2)
        self.assertEqual(payload["next_after_id"], 2)
        _, path, _ = client.calls[0]
        self.assertEqual(path, "/collections/demo/points?offset=0")
        self.assertNotIn("limit=", path)

    def test_omits_limit_when_none_and_uses_cursor_mode(self) -> None:
        client = RecordingClient(
            {
                "points": [{"id": 3}],
                "total": 3,
                "next_offset": None,
                "next_after_id": None,
            }
        )

        client.list_points("demo", limit=None, after_id=2)

        _, path, _ = client.calls[0]
        self.assertEqual(path, "/collections/demo/points?after_id=2")
        self.assertNotIn("limit=", path)

    def test_includes_limit_when_provided(self) -> None:
        client = RecordingClient(
            {"points": [], "total": 0, "next_offset": None, "next_after_id": None}
        )

        client.list_points("demo", offset=3, limit=50)

        _, path, _ = client.calls[0]
        self.assertEqual(path, "/collections/demo/points?limit=50&offset=3")

    def test_rejects_non_positive_limit(self) -> None:
        client = RecordingClient(
            {"points": [], "total": 0, "next_offset": None, "next_after_id": None}
        )

        for invalid_limit in [0, -1]:
            with self.subTest(limit=invalid_limit):
                with self.assertRaises(ValueError):
                    client.list_points("demo", limit=invalid_limit)

    def test_rejects_mixed_offset_and_after_id(self) -> None:
        client = RecordingClient(
            {"points": [], "total": 0, "next_offset": None, "next_after_id": None}
        )

        with self.assertRaises(ValueError):
            client.list_points("demo", offset=1, after_id=2)


class PointPayloadTests(unittest.TestCase):
    def test_upsert_includes_payload_when_provided(self) -> None:
        client = RecordingClient({"id": 7, "created": True})

        result = client.upsert_point("demo", 7, [1.0, 2.0], payload={"tenant": "edge"})

        self.assertEqual(result.id, 7)
        self.assertTrue(result.created)
        method, path, body = client.calls[0]
        self.assertEqual(method, "PUT")
        self.assertEqual(path, "/collections/demo/points/7")
        self.assertEqual(body, {"values": [1.0, 2.0], "payload": {"tenant": "edge"}})

    def test_get_point_parses_payload(self) -> None:
        client = RecordingClient(
            {"id": 5, "values": [1.0, 2.0], "payload": {"tenant": "edge"}}
        )

        point = client.get_point("demo", 5)

        self.assertEqual(point.id, 5)
        self.assertEqual(point.values, [1.0, 2.0])
        self.assertEqual(point.payload, {"tenant": "edge"})


class MetricsRequestTests(unittest.TestCase):
    def test_metrics_calls_endpoint_and_parses_response(self) -> None:
        client = RecordingClient(
            {
                "uptime_ms": 42,
                "ready": True,
                "engine_loaded": True,
                "storage_available": True,
                "http_requests_total": 9,
                "http_requests_in_flight": 1,
                "http_responses_2xx_total": 8,
                "http_responses_4xx_total": 1,
                "http_requests_5xx_total": 0,
                "http_request_duration_us_total": 2500,
                "http_request_duration_us_max": 900,
                "http_request_duration_us_avg": 277.78,
                "collections": 3,
                "points": 10,
                "l2_indexes": 2,
                "persistence_enabled": False,
                "persistence_writes": 7,
            }
        )

        metrics = client.metrics()

        self.assertIsInstance(metrics, MetricsResult)
        self.assertEqual(metrics.collections, 3)
        self.assertEqual(metrics.points, 10)
        self.assertEqual(metrics.persistence_writes, 7)
        self.assertTrue(metrics.ready)
        self.assertEqual(metrics.http_requests_total, 9)
        self.assertEqual(metrics.http_requests_in_flight, 1)
        self.assertEqual(metrics.http_responses_2xx_total, 8)
        self.assertEqual(metrics.http_responses_4xx_total, 1)
        self.assertEqual(metrics.http_requests_5xx_total, 0)
        self.assertEqual(metrics.http_request_duration_us_total, 2500)
        self.assertEqual(metrics.http_request_duration_us_max, 900)
        self.assertAlmostEqual(metrics.http_request_duration_us_avg, 277.78, places=2)
        method, path, body = client.calls[0]
        self.assertEqual(method, "GET")
        self.assertEqual(path, "/metrics")
        self.assertIsNone(body)

    def test_metrics_rejects_invalid_payload(self) -> None:
        client = RecordingClient({"uptime_ms": 42})

        with self.assertRaises(AionBDError):
            client.metrics()

    def test_metrics_prometheus_returns_raw_text(self) -> None:
        client = RecordingClient("aionbd_collections 3\n")

        payload = client.metrics_prometheus()

        self.assertEqual(payload, "aionbd_collections 3\n")
        method, path, body = client.calls[0]
        self.assertEqual(method, "GET")
        self.assertEqual(path, "/metrics/prometheus")
        self.assertIsNone(body)
        self.assertEqual(client.raw_flags, [True])


if __name__ == "__main__":
    unittest.main()
