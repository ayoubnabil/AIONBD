#!/usr/bin/env python3
"""Smoke test for scripts/refresh_report_baselines.py."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    from path_guard import resolve_io_path
except ModuleNotFoundError:
    from scripts.path_guard import resolve_io_path

SCRIPT = Path("scripts/refresh_report_baselines.py")
PROFILES = Path("ops/soak/longrun_profiles.json")


def run(script_path: Path, args: list[str], expect_ok: bool) -> None:
    completed = subprocess.run(
        [sys.executable, str(script_path), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if expect_ok and completed.returncode != 0:
        raise RuntimeError(
            f"command failed args={args} stdout={completed.stdout} stderr={completed.stderr}"
        )
    if not expect_ok and completed.returncode == 0:
        raise RuntimeError(
            f"command unexpectedly succeeded args={args} stdout={completed.stdout}"
        )


def ensure_within_root(
    path: Path, *, root: Path, label: str, must_exist: bool = False
) -> Path:
    """Ensure path remains inside the trusted temporary root."""
    resolved = path.resolve()
    root_resolved = root.resolve()
    if resolved != root_resolved and root_resolved not in resolved.parents:
        raise ValueError(f"{label} must stay under {root_resolved}: {resolved}")
    if must_exist and not resolved.exists():
        raise FileNotFoundError(f"{label} does not exist: {resolved}")
    return resolved


def main() -> int:
    script_path = resolve_io_path(str(SCRIPT), label="script path", must_exist=True)
    profiles_path = resolve_io_path(
        str(PROFILES), label="profiles path", must_exist=True
    )

    if not script_path.exists():
        print(f"error=missing_script path={script_path}", file=sys.stderr)
        return 1
    if not profiles_path.exists():
        print(f"error=missing_profiles path={profiles_path}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(
        prefix="aionbd_refresh_baseline_smoke_"
    ) as temp_dir:
        root = Path(temp_dir).resolve()
        soak_path = ensure_within_root(
            root / "soak_baseline.json",
            root=root,
            label="soak baseline path",
        )
        chaos_path = ensure_within_root(
            root / "chaos_baseline.json",
            root=root,
            label="chaos baseline path",
        )

        run(
            script_path,
            [
                "--mode",
                "update",
                "--profiles-file",
                str(profiles_path),
                "--soak-baseline-path",
                str(soak_path),
                "--chaos-baseline-path",
                str(chaos_path),
            ],
            expect_ok=True,
        )

        run(
            script_path,
            [
                "--mode",
                "check",
                "--profiles-file",
                str(profiles_path),
                "--soak-baseline-path",
                str(soak_path),
                "--chaos-baseline-path",
                str(chaos_path),
            ],
            expect_ok=True,
        )

        trusted_soak_path = ensure_within_root(
            soak_path, root=root, label="soak baseline path", must_exist=True
        )
        mutated = json.loads(trusted_soak_path.read_text(encoding="utf-8"))
        rows = mutated.get("rows", [])
        if isinstance(rows, list) and rows:
            rows[0]["throughput_ops_per_second"] = 0.0
            trusted_soak_path.write_text(
                json.dumps(mutated, indent=2) + "\n", encoding="utf-8"
            )

        run(
            script_path,
            [
                "--mode",
                "check",
                "--profiles-file",
                str(profiles_path),
                "--soak-baseline-path",
                str(soak_path),
                "--chaos-baseline-path",
                str(chaos_path),
            ],
            expect_ok=False,
        )

    print("ok=refresh_report_baselines_smoke")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
