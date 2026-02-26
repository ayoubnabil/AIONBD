#!/usr/bin/env python3
"""Offline smoke test for scripts/collection_export_import.py."""

from __future__ import annotations

import importlib.util
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCRIPT_PATH = Path("scripts/collection_export_import.py")


@dataclass
class FakeCollectionInfo:
    name: str
    dimension: int
    strict_finite: bool
    point_count: int


@dataclass
class FakePointResult:
    id: int
    values: list[float]
    payload: dict[str, Any]


class FakeClient:
    def __init__(self) -> None:
        self.collections: dict[str, FakeCollectionInfo] = {}
        self.points: dict[str, dict[int, FakePointResult]] = {}

    def create_collection(
        self, name: str, dimension: int, strict_finite: bool
    ) -> FakeCollectionInfo:
        if name in self.collections:
            raise RuntimeError(f"HTTP 409 on POST /collections: {name}")
        info = FakeCollectionInfo(
            name=name,
            dimension=int(dimension),
            strict_finite=bool(strict_finite),
            point_count=0,
        )
        self.collections[name] = info
        self.points[name] = {}
        return info

    def delete_collection(self, name: str) -> dict[str, Any]:
        if name not in self.collections:
            raise RuntimeError(f"HTTP 404 on DELETE /collections/{name}")
        del self.collections[name]
        del self.points[name]
        return {"name": name, "deleted": True}

    def get_collection(self, name: str) -> FakeCollectionInfo:
        info = self.collections.get(name)
        if info is None:
            raise RuntimeError(f"HTTP 404 on GET /collections/{name}")
        return info

    def list_points(
        self,
        collection: str,
        offset: int = 0,
        limit: int | None = 100,
        after_id: int | None = None,
    ) -> dict[str, Any]:
        if collection not in self.collections:
            raise RuntimeError(f"HTTP 404 on GET /collections/{collection}/points")
        ids = sorted(self.points[collection].keys())

        if after_id is None:
            start = int(offset)
            window = ids[start : start + (limit or len(ids))]
            next_offset = start + len(window)
            if next_offset >= len(ids):
                next_offset = None
            next_after_id = window[-1] if window and next_offset is not None else None
            return {
                "points": window,
                "total": len(ids),
                "next_offset": next_offset,
                "next_after_id": next_after_id,
            }

        start_idx = 0
        while start_idx < len(ids) and ids[start_idx] <= int(after_id):
            start_idx += 1
        batch_limit = int(limit) if limit is not None else len(ids)
        window = ids[start_idx : start_idx + batch_limit]
        next_after_id = (
            window[-1] if window and (start_idx + batch_limit) < len(ids) else None
        )
        return {
            "points": window,
            "total": len(ids),
            "next_offset": None,
            "next_after_id": next_after_id,
        }

    def get_point(self, collection: str, point_id: int) -> FakePointResult:
        if collection not in self.collections:
            raise RuntimeError(
                f"HTTP 404 on GET /collections/{collection}/points/{point_id}"
            )
        point = self.points[collection].get(int(point_id))
        if point is None:
            raise RuntimeError(
                f"HTTP 404 on GET /collections/{collection}/points/{point_id}"
            )
        return point

    def upsert_point(
        self,
        collection: str,
        point_id: int,
        values: list[float],
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if collection not in self.collections:
            raise RuntimeError(
                f"HTTP 404 on PUT /collections/{collection}/points/{point_id}"
            )
        if len(values) != self.collections[collection].dimension:
            raise RuntimeError("HTTP 400 on PUT: dimension mismatch")

        pid = int(point_id)
        created = pid not in self.points[collection]
        self.points[collection][pid] = FakePointResult(
            id=pid,
            values=[float(value) for value in values],
            payload=dict(payload or {}),
        )
        self.collections[collection].point_count = len(self.points[collection])
        return {"id": pid, "created": created}


def load_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "collection_export_import", SCRIPT_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load collection_export_import module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_export_import_roundtrip(module: Any) -> None:
    source = FakeClient()
    source.create_collection("demo", dimension=3, strict_finite=True)
    source.upsert_point("demo", 2, [2.0, 2.1, 2.2], payload={"tenant": "a"})
    source.upsert_point(
        "demo", 4, [4.0, 4.1, 4.2], payload={"tenant": "b", "score": 0.9}
    )
    source.upsert_point("demo", 7, [7.0, 7.1, 7.2], payload={})

    destination = FakeClient()

    with tempfile.TemporaryDirectory(prefix="aionbd_export_import_smoke_") as temp_dir:
        export_path = Path(temp_dir) / "demo.ndjson"
        module.export_collection(source, "demo", export_path, page_limit=2)
        module.import_collection(
            destination,
            input_path=export_path,
            target_collection="demo_copy",
            if_exists="fail",
        )

    imported = destination.points["demo_copy"]
    assert len(imported) == 3
    assert imported[2].payload == {"tenant": "a"}
    assert imported[4].payload == {"tenant": "b", "score": 0.9}
    assert imported[7].values == [7.0, 7.1, 7.2]


def test_if_exists_policies(module: Any) -> None:
    source = FakeClient()
    source.create_collection("demo", dimension=2, strict_finite=True)
    source.upsert_point("demo", 1, [1.0, 1.0], payload={"a": 1})

    with tempfile.TemporaryDirectory(prefix="aionbd_export_import_policy_") as temp_dir:
        export_path = Path(temp_dir) / "demo.ndjson"
        module.export_collection(source, "demo", export_path, page_limit=10)

        target = FakeClient()
        target.create_collection("demo", dimension=2, strict_finite=True)
        target.upsert_point("demo", 9, [9.0, 9.0], payload={"legacy": True})

        try:
            module.import_collection(target, export_path, "demo", "fail")
        except RuntimeError:
            pass
        else:
            raise AssertionError("if_exists=fail should reject existing collection")

        module.import_collection(target, export_path, "demo", "replace")
        assert sorted(target.points["demo"].keys()) == [1]

        module.import_collection(target, export_path, "demo", "append")
        assert sorted(target.points["demo"].keys()) == [1]


def main() -> int:
    if not SCRIPT_PATH.exists():
        raise RuntimeError(f"missing script: {SCRIPT_PATH}")

    module = load_module()
    test_export_import_roundtrip(module)
    test_if_exists_policies(module)
    print("ok=collection_export_import_smoke")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
