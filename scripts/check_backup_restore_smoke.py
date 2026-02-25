#!/usr/bin/env python3
"""Smoke test for scripts/state_backup_restore.py."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT = Path("scripts/state_backup_restore.py")


def run_command(args: list[str], expect_ok: bool) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if expect_ok and completed.returncode != 0:
        raise RuntimeError(
            f"command failed: {args}\nstdout={completed.stdout}\nstderr={completed.stderr}"
        )
    if not expect_ok and completed.returncode == 0:
        raise RuntimeError(
            f"command unexpectedly succeeded: {args}\nstdout={completed.stdout}"
        )
    return completed


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> int:
    if not SCRIPT.exists():
        print(f"error=missing_script path={SCRIPT}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="aionbd_backup_smoke_") as temp_dir:
        root = Path(temp_dir)
        live = root / "live"
        snapshot = live / "snapshot.json"
        wal = live / "wal.jsonl"
        incrementals = snapshot.with_suffix(".incrementals")

        expected_snapshot = '{"collections": 1}\n'
        expected_wal = '{"type":"upsert"}\n'
        expected_incremental = '{"segment": 1}\n'

        write_text(snapshot, expected_snapshot)
        write_text(wal, expected_wal)
        write_text(incrementals / "000001.jsonl", expected_incremental)

        backup_archive = root / "backup" / "state.tar.gz"
        run_command(
            [
                "backup",
                "--snapshot-path",
                str(snapshot),
                "--wal-path",
                str(wal),
                "--output",
                str(backup_archive),
            ],
            expect_ok=True,
        )

        write_text(snapshot, '{"collections": 999}\n')
        write_text(wal, '{"type":"corrupted"}\n')
        shutil.rmtree(incrementals)
        write_text(incrementals / "000999.jsonl", '{"segment": 999}\n')

        run_command(
            [
                "restore",
                "--input",
                str(backup_archive),
                "--snapshot-path",
                str(snapshot),
                "--wal-path",
                str(wal),
            ],
            expect_ok=False,
        )

        run_command(
            [
                "restore",
                "--input",
                str(backup_archive),
                "--snapshot-path",
                str(snapshot),
                "--wal-path",
                str(wal),
                "--force",
            ],
            expect_ok=True,
        )

        if snapshot.read_text(encoding="utf-8") != expected_snapshot:
            raise RuntimeError("restored snapshot content mismatch")
        if wal.read_text(encoding="utf-8") != expected_wal:
            raise RuntimeError("restored wal content mismatch")
        if (incrementals / "000001.jsonl").read_text(encoding="utf-8") != expected_incremental:
            raise RuntimeError("restored incremental content mismatch")

    print("ok=backup_restore_smoke")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
