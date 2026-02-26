#!/usr/bin/env python3
"""Helpers to keep script file I/O paths within trusted roots."""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

WORKSPACE_ROOT = Path.cwd().resolve()
TEMP_ROOT = Path(tempfile.gettempdir()).resolve()
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def resolve_io_path(path_value: str, *, label: str, must_exist: bool = False) -> Path:
    """Resolve and validate a path provided by CLI/env input."""
    raw = Path(path_value).expanduser()
    resolved = raw.resolve() if raw.is_absolute() else (WORKSPACE_ROOT / raw).resolve()

    allowed_roots = (WORKSPACE_ROOT, TEMP_ROOT)
    if not any(_is_within(resolved, root) for root in allowed_roots):
        raise ValueError(
            f"{label} must stay under '{WORKSPACE_ROOT}' or '{TEMP_ROOT}': {resolved}"
        )

    if must_exist and not resolved.exists():
        raise FileNotFoundError(f"{label} does not exist: {resolved}")

    return resolved


def safe_name_component(value: str, *, label: str) -> str:
    """Allow only safe filename characters for dynamically generated names."""
    if not SAFE_NAME_RE.fullmatch(value):
        raise ValueError(f"{label} contains unsafe characters: {value!r}")
    return value
