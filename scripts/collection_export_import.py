#!/usr/bin/env python3
"""Export and import AIONBD collections as NDJSON streams."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

FORMAT_VERSION = 1
DEFAULT_BASE_URL = "http://127.0.0.1:8080"


def default_export_path(collection: str) -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return str(Path.cwd() / "exports" / f"{collection}-{stamp}.ndjson")


def make_client(base_url: str, timeout: float) -> Any:
    repo_root = Path(__file__).resolve().parents[1]
    sdk_root = repo_root / "sdk" / "python"
    if str(sdk_root) not in sys.path:
        sys.path.insert(0, str(sdk_root))

    try:
        from aionbd import AionBDClient  # pylint: disable=import-error
    except ImportError as error:
        raise RuntimeError(
            f"failed to import SDK from {sdk_root}; run from repository root"
        ) from error

    return AionBDClient(base_url=base_url, timeout=timeout)


def is_not_found(error: Exception) -> bool:
    return "HTTP 404" in str(error)


def parse_json_line(path: Path, line_number: int, line: str) -> dict[str, Any]:
    try:
        record = json.loads(line)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"invalid JSON in {path}:{line_number}: {error.msg}"
        ) from error
    if not isinstance(record, dict):
        raise ValueError(f"invalid record in {path}:{line_number}: expected object")
    return record


def parse_header(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            header = parse_json_line(path, line_number, stripped)
            break
        else:
            raise ValueError(f"empty import file: {path}")

    if header.get("type") != "collection":
        raise ValueError("first record must have type=collection")

    if header.get("format_version") != FORMAT_VERSION:
        raise ValueError(
            "unsupported format_version="
            f"{header.get('format_version')} expected={FORMAT_VERSION}"
        )

    name = header.get("name")
    dimension = header.get("dimension")
    strict_finite = header.get("strict_finite")
    if not isinstance(name, str) or not name:
        raise ValueError("header name must be a non-empty string")
    if not isinstance(dimension, int) or dimension <= 0:
        raise ValueError("header dimension must be a positive integer")
    if not isinstance(strict_finite, bool):
        raise ValueError("header strict_finite must be a boolean")

    return {
        "name": name,
        "dimension": dimension,
        "strict_finite": strict_finite,
    }


def export_collection(client: Any, collection: str, output: Path, page_limit: int) -> int:
    if page_limit <= 0:
        raise ValueError("page_limit must be > 0")

    info = client.get_collection(collection)
    output.parent.mkdir(parents=True, exist_ok=True)

    exported = 0
    after_id: int | None = None
    with output.open("w", encoding="utf-8") as handle:
        header = {
            "type": "collection",
            "format_version": FORMAT_VERSION,
            "name": info.name,
            "dimension": int(info.dimension),
            "strict_finite": bool(info.strict_finite),
        }
        handle.write(json.dumps(header, separators=(",", ":")) + "\n")

        while True:
            page = client.list_points(collection, limit=page_limit, after_id=after_id)
            point_ids = page.get("points")
            if not isinstance(point_ids, list):
                raise RuntimeError("list_points response is missing points list")

            for point_id in point_ids:
                point = client.get_point(collection, int(point_id))
                record = {
                    "type": "point",
                    "id": int(point.id),
                    "values": point.values,
                    "payload": point.payload,
                }
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")
                exported += 1

            next_after_id = page.get("next_after_id")
            if next_after_id is None:
                break
            after_id = int(next_after_id)

    print(
        "ok=collection_export "
        f"collection={collection} output={output} points={exported} page_limit={page_limit}"
    )
    return 0


def ensure_target_collection(
    client: Any,
    target_collection: str,
    header: dict[str, Any],
    if_exists: str,
) -> None:
    existing = None
    try:
        existing = client.get_collection(target_collection)
    except Exception as error:  # noqa: BLE001
        if not is_not_found(error):
            raise

    if existing is not None and if_exists == "fail":
        raise RuntimeError(
            f"target collection already exists: {target_collection} (use --if-exists append|replace)"
        )

    if existing is not None and if_exists == "replace":
        client.delete_collection(target_collection)
        existing = None

    if existing is None:
        client.create_collection(
            target_collection,
            dimension=header["dimension"],
            strict_finite=header["strict_finite"],
        )
        return

    if (
        int(existing.dimension) != int(header["dimension"])
        or bool(existing.strict_finite) != bool(header["strict_finite"])
    ):
        raise RuntimeError(
            "target collection config mismatch: "
            f"existing=(dimension={existing.dimension}, strict_finite={existing.strict_finite}) "
            f"import=(dimension={header['dimension']}, strict_finite={header['strict_finite']})"
        )


def import_collection(
    client: Any,
    input_path: Path,
    target_collection: str | None,
    if_exists: str,
) -> int:
    if not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"import file not found: {input_path}")

    header = parse_header(input_path)
    collection_name = target_collection if target_collection else header["name"]
    ensure_target_collection(client, collection_name, header, if_exists)

    imported = 0
    header_consumed = False
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            if not header_consumed:
                header_consumed = True
                continue

            record = parse_json_line(input_path, line_number, stripped)
            if record.get("type") != "point":
                raise ValueError(f"invalid point record type in {input_path}:{line_number}")

            point_id = record.get("id")
            values = record.get("values")
            payload = record.get("payload")
            if not isinstance(point_id, int):
                raise ValueError(f"point id must be integer in {input_path}:{line_number}")
            if not isinstance(values, list) or not all(
                isinstance(value, (int, float)) for value in values
            ):
                raise ValueError(
                    f"point values must be a numeric array in {input_path}:{line_number}"
                )
            if payload is None:
                payload = {}
            if not isinstance(payload, dict):
                raise ValueError(f"point payload must be an object in {input_path}:{line_number}")

            client.upsert_point(collection_name, point_id, values, payload=payload)
            imported += 1

    print(
        "ok=collection_import "
        f"collection={collection_name} input={input_path} points={imported} if_exists={if_exists}"
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export/import AIONBD collections as NDJSON"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export one collection")
    export_parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    export_parser.add_argument("--timeout", type=float, default=10.0)
    export_parser.add_argument("--collection", required=True)
    export_parser.add_argument("--output")
    export_parser.add_argument("--page-limit", type=int, default=1000)

    import_parser = subparsers.add_parser("import", help="Import one collection")
    import_parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    import_parser.add_argument("--timeout", type=float, default=10.0)
    import_parser.add_argument("--input", required=True)
    import_parser.add_argument("--collection")
    import_parser.add_argument(
        "--if-exists",
        choices=["fail", "append", "replace"],
        default="fail",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = make_client(args.base_url, args.timeout)

    if args.command == "export":
        output = Path(args.output) if args.output else Path(default_export_path(args.collection))
        return export_collection(client, args.collection, output, args.page_limit)

    return import_collection(
        client,
        input_path=Path(args.input),
        target_collection=args.collection,
        if_exists=args.if_exists,
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"error={error}", file=sys.stderr)
        raise SystemExit(1)
