#!/usr/bin/env python3
"""Create and restore AIONBD persistence backups (snapshot/WAL/incrementals)."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path

FORMAT_VERSION = 1
DEFAULT_SNAPSHOT_PATH = str(Path.cwd() / "data" / "aionbd_snapshot.json")
DEFAULT_WAL_PATH = str(Path.cwd() / "data" / "aionbd_wal.jsonl")

def default_backup_path() -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return str(Path.cwd() / "backups" / f"aionbd-backup-{stamp}.tar.gz")

def incremental_dir(snapshot_path: Path) -> Path:
    return snapshot_path.with_suffix(".incrementals")

def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)

def build_manifest(
    staging_root: Path, snapshot_path: Path, wal_path: Path
) -> dict[str, object]:
    entries: list[dict[str, object]] = []
    for path in sorted(staging_root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(staging_root).as_posix()
        entries.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )

    return {
        "format_version": FORMAT_VERSION,
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": {
            "snapshot_path": str(snapshot_path),
            "wal_path": str(wal_path),
            "incremental_path": str(incremental_dir(snapshot_path)),
        },
        "entries": entries,
    }

def backup(snapshot_path: Path, wal_path: Path, output: Path) -> int:
    incremental_path = incremental_dir(snapshot_path)

    with tempfile.TemporaryDirectory(prefix="aionbd_backup_stage_") as temp_dir:
        staging_root = Path(temp_dir)
        copied_anything = False

        if snapshot_path.exists():
            if not snapshot_path.is_file():
                print(
                    f"error=invalid_snapshot_path path={snapshot_path}", file=sys.stderr
                )
                return 1
            copy_file(snapshot_path, staging_root / "snapshot.json")
            copied_anything = True

        if wal_path.exists():
            if not wal_path.is_file():
                print(f"error=invalid_wal_path path={wal_path}", file=sys.stderr)
                return 1
            copy_file(wal_path, staging_root / "wal.jsonl")
            copied_anything = True

        if incremental_path.exists():
            if not incremental_path.is_dir():
                print(
                    f"error=invalid_incremental_path path={incremental_path}",
                    file=sys.stderr,
                )
                return 1
            shutil.copytree(incremental_path, staging_root / "incrementals")
            copied_anything = True

        if not copied_anything:
            print(
                "error=no_persistence_files_found "
                f"snapshot={snapshot_path} wal={wal_path} incremental={incremental_path}",
                file=sys.stderr,
            )
            return 1

        manifest = build_manifest(staging_root, snapshot_path, wal_path)
        (staging_root / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=False) + "\n",
            encoding="utf-8",
        )

        output.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(output, mode="w:gz") as archive:
            for path in sorted(staging_root.rglob("*")):
                archive.add(path, arcname=path.relative_to(staging_root))

    print(
        "ok=backup_created "
        f"output={output} "
        f"entries={len(manifest['entries'])} "
        f"snapshot_present={snapshot_path.exists()} "
        f"wal_present={wal_path.exists()} "
        f"incremental_present={incremental_path.exists()}"
    )
    return 0

def load_manifest(extract_root: Path) -> dict[str, object]:
    manifest_path = extract_root / "manifest.json"
    if not manifest_path.exists() or not manifest_path.is_file():
        raise ValueError("backup archive is missing manifest.json")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format_version") != FORMAT_VERSION:
        raise ValueError(
            "unsupported backup format_version="
            f"{manifest.get('format_version')} expected={FORMAT_VERSION}"
        )

    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise ValueError("manifest entries must be a list")

    return manifest

def verify_archive_files(extract_root: Path, manifest: dict[str, object]) -> None:
    entries = manifest["entries"]
    assert isinstance(entries, list)

    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("manifest entry must be an object")

        relative = entry.get("path")
        expected_size = entry.get("size_bytes")
        expected_hash = entry.get("sha256")
        if not isinstance(relative, str):
            raise ValueError("manifest entry path must be a string")
        if not isinstance(expected_size, int):
            raise ValueError("manifest entry size_bytes must be an int")
        if not isinstance(expected_hash, str):
            raise ValueError("manifest entry sha256 must be a string")

        actual = extract_root / relative
        if not actual.exists() or not actual.is_file():
            raise ValueError(f"archive entry is missing: {relative}")

        actual_size = actual.stat().st_size
        if actual_size != expected_size:
            raise ValueError(
                f"size mismatch for {relative}: actual={actual_size} expected={expected_size}"
            )

        actual_hash = sha256_file(actual)
        if actual_hash != expected_hash:
            raise ValueError(
                f"sha256 mismatch for {relative}: actual={actual_hash} expected={expected_hash}"
            )

def safe_extract_archive(archive: tarfile.TarFile, destination: Path) -> None:
    destination_root = destination.resolve()
    for member in archive.getmembers():
        target = (destination / member.name).resolve()
        if not target.is_relative_to(destination_root):
            raise ValueError(f"archive contains unsafe path: {member.name}")
    archive.extractall(path=destination)

def restore(
    input_archive: Path, snapshot_path: Path, wal_path: Path, force: bool
) -> int:
    if not input_archive.exists() or not input_archive.is_file():
        print(f"error=backup_archive_not_found path={input_archive}", file=sys.stderr)
        return 1

    incremental_path = incremental_dir(snapshot_path)
    restore_plan: list[tuple[Path, Path]] = []

    with tempfile.TemporaryDirectory(prefix="aionbd_restore_stage_") as temp_dir:
        extract_root = Path(temp_dir)
        with tarfile.open(input_archive, mode="r:gz") as archive:
            try:
                safe_extract_archive(archive, extract_root)
            except ValueError as error:
                print(f'error=invalid_backup_archive detail="{error}"', file=sys.stderr)
                return 1

        try:
            manifest = load_manifest(extract_root)
            verify_archive_files(extract_root, manifest)
        except ValueError as error:
            print(f'error=invalid_backup_archive detail="{error}"', file=sys.stderr)
            return 1

        snapshot_src = extract_root / "snapshot.json"
        wal_src = extract_root / "wal.jsonl"
        incrementals_src = extract_root / "incrementals"

        if snapshot_src.exists():
            restore_plan.append((snapshot_src, snapshot_path))
        if wal_src.exists():
            restore_plan.append((wal_src, wal_path))
        if incrementals_src.exists():
            restore_plan.append((incrementals_src, incremental_path))
        if not restore_plan:
            print("error=restore_payload_is_empty", file=sys.stderr)
            return 1

        existing = [dst for _, dst in restore_plan if dst.exists()]
        if existing and not force:
            print(
                "error=restore_target_exists use=--force "
                f"paths={','.join(str(path) for path in existing)}",
                file=sys.stderr,
            )
            return 1

        if force:
            for path in existing:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()

        for src, dst in restore_plan:
            dst.parent.mkdir(parents=True, exist_ok=True)
            if src.is_dir():
                shutil.copytree(src, dst)
            else:
                temp_target = dst.with_suffix(dst.suffix + ".restore_tmp")
                if temp_target.exists():
                    temp_target.unlink()
                shutil.copy2(src, temp_target)
                temp_target.replace(dst)

    print(
        "ok=restore_completed "
        f"archive={input_archive} "
        f"snapshot={snapshot_path} wal={wal_path} incremental={incremental_path} "
        f"force={int(force)}"
    )
    return 0

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backup and restore AIONBD persistence state"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup_parser = subparsers.add_parser("backup", help="Create backup archive")
    backup_parser.add_argument("--snapshot-path", default=DEFAULT_SNAPSHOT_PATH)
    backup_parser.add_argument("--wal-path", default=DEFAULT_WAL_PATH)
    backup_parser.add_argument("--output", default=default_backup_path())

    restore_parser = subparsers.add_parser("restore", help="Restore backup archive")
    restore_parser.add_argument("--input", required=True)
    restore_parser.add_argument("--snapshot-path", default=DEFAULT_SNAPSHOT_PATH)
    restore_parser.add_argument("--wal-path", default=DEFAULT_WAL_PATH)
    restore_parser.add_argument("--force", action="store_true")

    return parser.parse_args()

def main() -> int:
    args = parse_args()

    if args.command == "backup":
        return backup(
            snapshot_path=Path(args.snapshot_path),
            wal_path=Path(args.wal_path),
            output=Path(args.output),
        )
    return restore(
        input_archive=Path(args.input),
        snapshot_path=Path(args.snapshot_path),
        wal_path=Path(args.wal_path),
        force=args.force,
    )

if __name__ == "__main__":
    raise SystemExit(main())
