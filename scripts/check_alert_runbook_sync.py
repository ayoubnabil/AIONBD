#!/usr/bin/env python3
"""Ensures Prometheus alert names are synchronized with the observability runbook."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ALERT_RULES_PATH = Path("ops/prometheus/aionbd-alerts.yml")
RUNBOOK_PATH = Path("docs/operations_observability.md")
ALERT_NAME_PATTERN = re.compile(r"\bAionbd[A-Za-z0-9]+\b")
ALERT_RULE_PATTERN = re.compile(r"^\s*-\s*alert:\s*([A-Za-z0-9_]+)\s*$")


def load_alert_names(path: Path) -> list[str]:
    names: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        matched = ALERT_RULE_PATTERN.match(line)
        if matched:
            names.add(matched.group(1))
    return sorted(names)


def load_runbook_names(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    return sorted(set(ALERT_NAME_PATTERN.findall(text)))


def main() -> int:
    if not ALERT_RULES_PATH.exists():
        print(f"error=missing_alert_rules path={ALERT_RULES_PATH}", file=sys.stderr)
        return 1
    if not RUNBOOK_PATH.exists():
        print(f"error=missing_runbook path={RUNBOOK_PATH}", file=sys.stderr)
        return 1

    alert_names = load_alert_names(ALERT_RULES_PATH)
    if not alert_names:
        print("error=no_alerts_found_in_rules", file=sys.stderr)
        return 1

    runbook_names = load_runbook_names(RUNBOOK_PATH)
    missing = [name for name in alert_names if name not in runbook_names]
    stale = [name for name in runbook_names if name not in alert_names]

    if missing:
        print(
            "error=runbook_missing_alerts names=" + ",".join(missing),
            file=sys.stderr,
        )
    if stale:
        print(
            "error=runbook_contains_stale_alert_names names=" + ",".join(stale),
            file=sys.stderr,
        )
    if missing or stale:
        return 1

    print(f"ok=alert_runbook_sync alerts={len(alert_names)} runbook={RUNBOOK_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
